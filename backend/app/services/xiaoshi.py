"""Read-only Xiaoshi data integration.

The integration deliberately stays outside the application's canonical market
provider registry.  Xiaoshi history is only available through its published
Manifest and the local ``xiaoshi-data`` command; current API data is exposed
through explicit read-only calls.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urljoin, urlsplit

import httpx

from app import __version__
from app.config import settings

XIAOSHI_BASE_URL = "https://api.shizixi.com"
XIAOSHI_MANIFEST_PATH = "/api/v3/manifest"
XIAOSHI_HISTORY_MANIFEST_PATH = "/api/v3/history/manifest"
XIAOSHI_DATASET_NAMES = frozenset(
    {
        "cn-minute",
        "cn-daily",
        "adjustment-factors",
        "global-daily",
        "sector-constituents",
        "event-timeline",
        "financial-current",
        "financial-as-reported",
    }
)
_REDACTED = "[REDACTED]"
_SECRET_WORDS = frozenset(
    {
        "authorization",
        "api_key",
        "apikey",
        "key",
        "password",
        "secret",
        "token",
    }
)


class XiaoshiError(RuntimeError):
    """An operational error with safe, reproducible diagnostic context."""

    def __init__(self, message: str, *, context: Mapping[str, Any] | None = None) -> None:
        self.context = dict(context or {})
        super().__init__(message)


class XiaoshiProtectionError(XiaoshiError):
    """A controlled platform protection response such as 429/bulk download."""

    def __init__(
        self,
        message: str,
        *,
        error_code: str,
        retry_after_seconds: int | float | None,
        alternative: Any,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        self.error_code = error_code
        self.retry_after_seconds = retry_after_seconds
        self.alternative = alternative
        super().__init__(message, context=context)


class XiaoshiHistoryUnavailable(XiaoshiError):
    """The local xiaoshi-data executable is not available or failed."""


@dataclass(frozen=True)
class XiaoshiResourceState:
    active: bool
    manifest_version: str | None
    prompt_version: str | None
    skill_version: str | None
    api_schema_version: str | None
    checksums: dict[str, str]
    resource_dir: str | None
    last_error: str | None


def _resource_root() -> Path:
    return settings.data_dir / "user_data" / "xiaoshi"


def _active_pointer_path() -> Path:
    return _resource_root() / "active.json"


def _read_active_pointer() -> dict[str, Any]:
    try:
        value = json.loads(_active_pointer_path().read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _safe_scalar(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _safe_scalar(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_safe_scalar(item) for item in value]
    if isinstance(value, tuple):
        return [_safe_scalar(item) for item in value]
    return value


def redact_params(params: Mapping[str, Any] | None) -> dict[str, Any]:
    """Redact credential-shaped parameter names without retaining secrets."""
    result: dict[str, Any] = {}
    for key, value in (params or {}).items():
        key_text = str(key)
        lowered = key_text.casefold().replace("-", "_")
        if any(word in lowered for word in _SECRET_WORDS):
            result[key_text] = _REDACTED
        else:
            result[key_text] = _safe_scalar(value)
    return result


def _fingerprint(*parts: Any) -> str:
    payload = json.dumps(_safe_scalar(parts), ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _error_context(
    *,
    endpoint: str,
    params: Mapping[str, Any] | None = None,
    status_code: int | None = None,
    detail: Any = None,
    manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    context = {
        "application_version": __version__,
        "data_service": "xiaoshi",
        "data_version": None,
        "manifest_version": None,
        "prompt_version": None,
        "skill_version": None,
        "endpoint": endpoint,
        "parameters": redact_params(params),
    }
    if status_code is not None:
        context["status_code"] = status_code
    if isinstance(detail, Mapping):
        request_id = detail.get("request_id") or detail.get("requestId")
        if request_id:
            context["request_id"] = str(request_id)
        if detail.get("error"):
            context["service_error"] = str(detail["error"])
    if manifest:
        for name in ("manifest_version", "prompt_version", "skill_version"):
            if manifest.get(name):
                context[name] = str(manifest[name])
        if manifest.get("manifest_version"):
            context["data_version"] = str(manifest["manifest_version"])
        checksums = manifest.get("checksums")
        if isinstance(checksums, Mapping):
            context["manifest_checksums"] = {
                str(key): str(value) for key, value in checksums.items()
            }
    else:
        pointer = _read_active_pointer()
        for name in ("manifest_version", "prompt_version", "skill_version"):
            if pointer.get(name):
                context[name] = str(pointer[name])
        if pointer.get("manifest_version"):
            context["data_version"] = str(pointer["manifest_version"])
        if pointer.get("checksums"):
            context["manifest_checksums"] = dict(pointer["checksums"])
    context["error_fingerprint"] = _fingerprint(
        context["endpoint"],
        context["parameters"],
        context.get("status_code"),
        context.get("service_error"),
        context.get("request_id"),
    )
    return context


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _require_https_api_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname != "api.shizixi.com":
        raise ValueError("Xiaoshi API URL must use https://api.shizixi.com")
    return url.rstrip("/")


def _is_timezone_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None


class XiaoshiClient:
    """Small HTTP client for explicit Xiaoshi API calls.

    The key is read from the process environment or passed by the caller.  It
    is never written to the local secrets store and is never included in
    exception context.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = XIAOSHI_BASE_URL,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = _require_https_api_url(base_url)
        self._api_key = (api_key if api_key is not None else os.getenv("XIAOSHI_API_KEY", "")).strip()
        self.timeout = timeout
        self._transport = transport

    @property
    def configured(self) -> bool:
        return bool(self._api_key)

    def get_manifest(self) -> dict[str, Any]:
        return self.request_json(XIAOSHI_MANIFEST_PATH, auth=False, cache_control=True)

    def get_history_manifest(self) -> dict[str, Any]:
        return self.request_json(XIAOSHI_HISTORY_MANIFEST_PATH, auth=True, cache_control=True)

    def request_json(
        self,
        path: str,
        *,
        method: str = "GET",
        params: Mapping[str, Any] | None = None,
        body: Any = None,
        auth: bool = True,
        cache_control: bool = False,
    ) -> dict[str, Any]:
        if auth and not self._api_key:
            raise XiaoshiError(
                "Xiaoshi API Key is not configured",
                context=_error_context(endpoint=path, params=params),
            )
        url = urljoin(f"{self.base_url}/", path.lstrip("/"))
        parsed = urlsplit(url)
        if parsed.hostname != "api.shizixi.com":
            raise ValueError("Xiaoshi authenticated requests must stay on api.shizixi.com")

        headers = {"Accept": "application/json"}
        if auth:
            headers["Authorization"] = f"Bearer {self._api_key}"
        if cache_control:
            headers["Cache-Control"] = "no-store, no-cache"

        attempts = 0
        while True:
            attempts += 1
            try:
                with httpx.Client(
                    timeout=self.timeout,
                    transport=self._transport,
                    follow_redirects=False,
                ) as client:
                    response = client.request(
                        method.upper(),
                        url,
                        params=dict(params or {}),
                        json=body,
                        headers=headers,
                    )
            except httpx.HTTPError as exc:
                if attempts < 2:
                    continue
                raise XiaoshiError(
                    "Xiaoshi request failed",
                    context=_error_context(endpoint=path, params=params, detail=str(exc)),
                ) from exc

            detail = _response_detail(response)
            detail_error = _detail_error_code(detail)
            if response.status_code == 429 or detail_error == "bulk_download_required":
                error_code = detail_error or "rate_limit_exceeded"
                retry_after = _retry_after(response, detail)
                raise XiaoshiProtectionError(
                    "Xiaoshi request was rate limited or requires bulk download",
                    error_code=error_code,
                    retry_after_seconds=retry_after,
                    alternative=_detail_value(detail, "alternative"),
                    context=_error_context(
                        endpoint=path,
                        params=params,
                        status_code=response.status_code,
                        detail=detail,
                    ),
                )

            if response.status_code >= 500 and attempts < 2:
                continue
            if response.status_code >= 400:
                raise XiaoshiError(
                    f"Xiaoshi request returned HTTP {response.status_code}",
                    context=_error_context(
                        endpoint=path,
                        params=params,
                        status_code=response.status_code,
                        detail=detail,
                    ),
                )
            if not isinstance(detail, dict):
                raise XiaoshiError(
                    "Xiaoshi returned a non-object JSON response",
                    context=_error_context(
                        endpoint=path,
                        params=params,
                        status_code=response.status_code,
                    ),
                )
            return detail


def _response_detail(response: httpx.Response) -> Any:
    try:
        value = response.json()
    except ValueError:
        return {}
    if isinstance(value, Mapping) and isinstance(value.get("detail"), Mapping):
        return value["detail"]
    return value


def _detail_error_code(detail: Any) -> str | None:
    if not isinstance(detail, Mapping):
        return None
    value = detail.get("error")
    if isinstance(value, Mapping):
        value = value.get("code") or value.get("type")
    return str(value) if value else None


def _detail_value(detail: Any, key: str) -> Any:
    if not isinstance(detail, Mapping):
        return None
    value = detail.get(key)
    if value is not None:
        return value
    nested = detail.get("detail")
    return nested.get(key) if isinstance(nested, Mapping) else None


def _retry_after(response: httpx.Response, detail: Any) -> int | float | None:
    raw = response.headers.get("Retry-After")
    if raw is None:
        raw = _detail_value(detail, "retry_after_seconds")
    if raw is None:
        return None
    try:
        return float(raw) if "." in str(raw) else int(raw)
    except (TypeError, ValueError):
        return None


class XiaoshiResourceManager:
    """Versioned, atomically replaced local Prompt/Skill/API-schema bundle."""

    def __init__(
        self,
        *,
        client: XiaoshiClient | None = None,
        root: Path | None = None,
    ) -> None:
        self.client = client or XiaoshiClient()
        self.root = root or _resource_root()
        self.root.mkdir(parents=True, exist_ok=True)

    def state(self) -> XiaoshiResourceState:
        pointer = self._read_json(_active_pointer_path_for(self.root))
        if not pointer:
            return XiaoshiResourceState(
                active=False,
                manifest_version=None,
                prompt_version=None,
                skill_version=None,
                api_schema_version=None,
                checksums={},
                resource_dir=None,
                last_error=None,
            )
        return XiaoshiResourceState(
            active=True,
            manifest_version=pointer.get("manifest_version"),
            prompt_version=pointer.get("prompt_version"),
            skill_version=pointer.get("skill_version"),
            api_schema_version=pointer.get("api_schema_version"),
            checksums=dict(pointer.get("checksums") or {}),
            resource_dir=pointer.get("resource_dir"),
            last_error=pointer.get("last_error"),
        )

    def refresh(self, *, explicit_update: bool = False) -> XiaoshiResourceState:
        manifest = self.client.get_manifest()
        self._validate_manifest_metadata(manifest)
        current = self.state()
        if not explicit_update and _same_publication(current, manifest):
            return current

        candidate_dir = Path(tempfile.mkdtemp(prefix=".candidate-", dir=self.root))
        try:
            self._write_candidate(candidate_dir, manifest)
            release_name = _release_name(manifest)
            pointer = {
                "manifest_version": str(manifest["manifest_version"]),
                "prompt_version": str(manifest["prompt_version"]),
                "skill_version": str(manifest["skill_version"]),
                "api_schema_version": str(manifest.get("api_schema_version") or manifest["api_schema_url"]),
                "checksums": dict(manifest["checksums"]),
                "resource_dir": str(self.root / "releases" / release_name),
                "last_error": None,
            }
            self._publish(candidate_dir, pointer)
            return self.state()
        except Exception as exc:
            shutil.rmtree(candidate_dir, ignore_errors=True)
            if current.active:
                raise XiaoshiError(
                    "Xiaoshi resource update failed; last verified release retained",
                    context=_error_context(
                        endpoint=XIAOSHI_MANIFEST_PATH,
                        detail=str(exc),
                        manifest=manifest,
                    ),
                ) from exc
            raise

    def _write_candidate(self, candidate_dir: Path, manifest: Mapping[str, Any]) -> None:
        checksums = manifest["checksums"]
        prompt_url = str(manifest["prompt_url"])
        skill_files = manifest["skill_files"]
        api_schema_url = str(manifest["api_schema_url"])
        resources: list[tuple[str, str, str]] = [
            ("prompt.txt", prompt_url, str(checksums["prompt_sha256"])),
            ("api-schema.txt", api_schema_url, str(checksums["api_schema_sha256"])),
        ]
        for item in skill_files:
            relative_path = _safe_resource_path(str(item["path"]))
            resources.append((relative_path, str(item["url"]), str(item["sha256"])))

        downloaded: dict[str, bytes] = {}
        for relative_path, url, expected_sha in resources:
            content = self._download_public_resource(url)
            if _sha256(content) != expected_sha:
                raise ValueError(f"resource checksum mismatch: {relative_path}")
            expected_size = next(
                (
                    int(item["size"])
                    for item in skill_files
                    if str(item["path"]) == relative_path
                ),
                None,
            )
            if expected_size is not None and len(content) != expected_size:
                raise ValueError(f"resource size mismatch: {relative_path}")
            downloaded[relative_path] = content

        if _skill_package_sha256(skill_files) != str(checksums["skill_package_sha256"]):
            raise ValueError("skill package checksum mismatch")

        self._write_bytes(candidate_dir / "manifest.json", _json_bytes(manifest))
        self._write_bytes(candidate_dir / "prompt.txt", downloaded["prompt.txt"])
        self._write_bytes(candidate_dir / "api-schema.txt", downloaded["api-schema.txt"])
        for relative_path, content in downloaded.items():
            if relative_path in {"prompt.txt", "api-schema.txt"}:
                continue
            self._write_bytes(candidate_dir / "skill" / relative_path, content)

    def _publish(self, candidate_dir: Path, pointer: Mapping[str, Any]) -> None:
        releases = self.root / "releases"
        releases.mkdir(parents=True, exist_ok=True)
        release_dir = Path(str(pointer["resource_dir"]))
        if release_dir.exists():
            shutil.rmtree(candidate_dir, ignore_errors=True)
        else:
            os.replace(candidate_dir, release_dir)
        self._write_json(_active_pointer_path_for(self.root), pointer)

    def _download_public_resource(self, url: str) -> bytes:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname != "api.shizixi.com":
            raise ValueError("Xiaoshi resources must be downloaded from api.shizixi.com")
        with httpx.Client(timeout=self.client.timeout, follow_redirects=False) as client:
            response = client.get(url, headers={"Cache-Control": "no-store, no-cache"})
        if response.status_code >= 400:
            raise ValueError(f"resource returned HTTP {response.status_code}")
        return response.content

    @staticmethod
    def _validate_manifest_metadata(manifest: Mapping[str, Any]) -> None:
        required = (
            "manifest_version",
            "prompt_version",
            "skill_version",
            "prompt_url",
            "skill_url",
            "api_schema_url",
            "checksums",
            "skill_files",
            "min_compatibility",
        )
        missing = [name for name in required if not manifest.get(name)]
        if missing:
            raise ValueError(f"Xiaoshi Manifest missing fields: {', '.join(missing)}")
        compatibility = manifest["min_compatibility"]
        if compatibility.get("api_schema") != "v3":
            raise ValueError("unsupported Xiaoshi API schema compatibility")
        if not isinstance(manifest["skill_files"], list) or not manifest["skill_files"]:
            raise ValueError("Xiaoshi Manifest skill_files is empty")
        checksums = manifest["checksums"]
        required_checksums = (
            "prompt_sha256",
            "skill_sha256",
            "skill_package_sha256",
            "api_schema_sha256",
        )
        if not isinstance(checksums, Mapping) or any(
            not checksums.get(name) for name in required_checksums
        ):
            raise ValueError("Xiaoshi Manifest checksums are incomplete")
        skill_entries = {
            str(item.get("path")): item
            for item in manifest["skill_files"]
            if isinstance(item, Mapping)
        }
        if skill_entries.get("SKILL.md", {}).get("sha256") != checksums["skill_sha256"]:
            raise ValueError("Xiaoshi Manifest SKILL.md checksum is inconsistent")
        for item in manifest["skill_files"]:
            if not isinstance(item, Mapping) or not all(
                item.get(name) for name in ("path", "size", "sha256", "url")
            ):
                raise ValueError("invalid Xiaoshi skill file metadata")
            _safe_resource_path(str(item["path"]))
            if int(item["size"]) < 0:
                raise ValueError("invalid Xiaoshi skill file size")

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError):
            return {}
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _write_json(path: Path, value: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(_json_bytes(value) + b"\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, path)
            _fsync_directory(path.parent)
        finally:
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass

    @staticmethod
    def _write_bytes(path: Path, content: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())


def _active_pointer_path_for(root: Path) -> Path:
    return root / "active.json"


def _safe_resource_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or "\\" in value
        or ".." in path.parts
        or str(path) != value
    ):
        raise ValueError(f"invalid Xiaoshi resource path: {value}")
    return value


def _release_name(manifest: Mapping[str, Any]) -> str:
    version = _safe_resource_path(str(manifest["manifest_version"]))
    package_hash = str(manifest["checksums"]["skill_package_sha256"])
    return f"{version}-{package_hash[:16]}"


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _same_publication(state: XiaoshiResourceState, manifest: Mapping[str, Any]) -> bool:
    if not state.active:
        return False
    checksums = manifest.get("checksums")
    return (
        state.manifest_version == str(manifest.get("manifest_version"))
        and state.prompt_version == str(manifest.get("prompt_version"))
        and state.skill_version == str(manifest.get("skill_version"))
        and state.checksums == dict(checksums or {})
    )


def _skill_package_sha256(skill_files: Iterable[Mapping[str, Any]]) -> str:
    canonical = [
        {
            "path": str(item["path"]),
            "size": int(item["size"]),
            "sha256": str(item["sha256"]),
            "url": str(item["url"]),
        }
        for item in sorted(skill_files, key=lambda item: str(item["path"]))
    ]
    return _sha256(_json_bytes(canonical))


def _manifest_records(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        if any(key in value for key in ("dataset", "dataset_name", "name")):
            yield value
        for child in value.values():
            yield from _manifest_records(child)
    elif isinstance(value, list):
        for child in value:
            yield from _manifest_records(child)


def _record_dataset(record: Mapping[str, Any]) -> str | None:
    for key in ("dataset", "dataset_name", "name"):
        value = record.get(key)
        if value:
            return str(value)
    return None


def _coverage_allows(
    record: Mapping[str, Any],
    *,
    market: str | None,
    symbol: str | None,
    since: str | None,
    to: str | None,
) -> bool:
    publication_scope = record.get("publication_scope")
    coverage = record.get("coverage")
    if not isinstance(publication_scope, Mapping) or not isinstance(coverage, Mapping):
        return False
    quality_status = record.get("quality_status")
    if quality_status is not None and str(quality_status).casefold() not in {"passed", "ready", "full"}:
        return False
    publication_gate = record.get("publication_gate")
    if isinstance(publication_gate, Mapping):
        gate_status = publication_gate.get("status")
        if gate_status is not None and str(gate_status).casefold() not in {"ready", "passed"}:
            return False

    def _contains(key: str, value: str | None) -> bool:
        if value is None:
            return True
        allowed = coverage.get(key)
        if allowed is None:
            allowed = publication_scope.get(key)
        if allowed is None:
            return False
        if isinstance(allowed, str):
            return value == allowed
        if isinstance(allowed, list):
            return value in {str(item) for item in allowed}
        return True

    if not _contains("market", market) and not _contains("markets", market):
        return False
    if not _contains("symbol", symbol) and not _contains("symbols", symbol):
        return False
    # Date range overlap is intentionally conservative: an absent date bound
    # cannot prove coverage and therefore blocks the selection.
    first = coverage.get("first_date") or coverage.get("since") or publication_scope.get("since")
    last = coverage.get("last_date") or coverage.get("to") or publication_scope.get("to")
    if since and (not last or str(last) < since):
        return False
    if to and (not first or str(first) > to):
        return False
    return True


@dataclass(frozen=True)
class HistorySelection:
    dataset: str
    market: str | None
    symbol: str | None
    since: str | None
    to: str | None
    as_of: datetime | None


class XiaoshiHistory:
    """Guarded access to the local xiaoshi-data executable."""

    def __init__(
        self,
        *,
        client: XiaoshiClient | None = None,
        executable: str = "xiaoshi-data",
        runner: Any = subprocess.run,
    ) -> None:
        self.client = client or XiaoshiClient()
        self.executable = executable
        self._runner = runner
        self._history_manifest: dict[str, Any] | None = None

    def query(
        self,
        *,
        dataset: str,
        market: str | None = None,
        symbol: str | None = None,
        since: str | None = None,
        to: str | None = None,
        as_of: datetime | None = None,
        extra_args: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        safe_extra_args = _safe_extra_args(extra_args)
        selection = self._validate_selection(
            dataset=dataset,
            market=market,
            symbol=symbol,
            since=since,
            to=to,
            as_of=as_of,
        )
        # These are mandatory gates before every history query.  Their output
        # is intentionally kept at the CLI boundary rather than guessed here.
        self._run("catalog", "--json")
        self._run("schema", "--dataset", selection.dataset, "--json")
        self._run("coverage", "--dataset", selection.dataset, "--json")
        args = ["query", "--dataset", selection.dataset, "--json"]
        if market:
            args.extend(["--market", market])
        if symbol:
            args.extend(["--symbol", symbol])
        if since:
            args.extend(["--since", since])
        if to:
            args.extend(["--to", to])
        if as_of is not None:
            args.extend(["--as-of", as_of.isoformat()])
        args.extend(safe_extra_args)
        rows = _parse_json_rows(self._run(*args))
        if selection.dataset == "financial-as-reported":
            rows = _filter_financial_rows(rows, as_of)
        return rows

    def _validate_selection(
        self,
        *,
        dataset: str,
        market: str | None,
        symbol: str | None,
        since: str | None,
        to: str | None,
        as_of: datetime | None,
    ) -> HistorySelection:
        if dataset not in XIAOSHI_DATASET_NAMES:
            raise XiaoshiError(
                f"unsupported Xiaoshi history dataset: {dataset}",
                context=_error_context(endpoint="xiaoshi-data"),
            )
        if dataset == "financial-as-reported" and (
            as_of is None or not _is_timezone_aware(as_of)
        ):
            raise XiaoshiError(
                "financial-as-reported requires a timezone-aware as_of",
                context=_error_context(
                    endpoint="xiaoshi-data query",
                    params={"dataset": dataset, "as_of": as_of.isoformat() if as_of else None},
                ),
            )
        manifest = self._history_manifest or self.client.get_history_manifest()
        self._history_manifest = manifest
        records = [
            record
            for record in _manifest_records(manifest)
            if _record_dataset(record) == dataset
            and _coverage_allows(
                record,
                market=market,
                symbol=symbol,
                since=since,
                to=to,
            )
        ]
        if not records:
            raise XiaoshiError(
                "requested Xiaoshi history range is not covered by the current Manifest",
                context=_error_context(
                    endpoint=XIAOSHI_HISTORY_MANIFEST_PATH,
                    params={
                        "dataset": dataset,
                        "market": market,
                        "symbol": symbol,
                        "since": since,
                        "to": to,
                        "as_of": as_of.isoformat() if as_of else None,
                    },
                    manifest=manifest,
                ),
            )
        return HistorySelection(dataset, market, symbol, since, to, as_of)

    def _run(self, *args: str) -> str:
        command = [self.executable, *args]
        try:
            result = self._runner(
                command,
                capture_output=True,
                text=True,
                check=False,
                timeout=300,
            )
        except (FileNotFoundError, OSError, subprocess.TimeoutExpired) as exc:
            raise XiaoshiHistoryUnavailable(
                "xiaoshi-data is unavailable",
                context=_error_context(
                    endpoint=" ".join(command[:2]),
                    params={"arguments": list(args[1:])},
                    detail=str(exc),
                ),
            ) from exc
        if result.returncode != 0:
            raise XiaoshiHistoryUnavailable(
                "xiaoshi-data command failed",
                context=_error_context(
                    endpoint=" ".join(command[:2]),
                    params={"arguments": list(args[1:])},
                    status_code=result.returncode,
                    detail="command failed",
                ),
            )
        return result.stdout


def _parse_json_rows(output: str) -> list[dict[str, Any]]:
    try:
        value = json.loads(output)
    except ValueError as exc:
        raise XiaoshiHistoryUnavailable(
            "xiaoshi-data returned invalid JSON",
            context=_error_context(endpoint="xiaoshi-data", detail="invalid json"),
        ) from exc
    if isinstance(value, list):
        rows = value
    elif isinstance(value, Mapping):
        rows = value.get("rows") or value.get("data") or []
    else:
        rows = []
    if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
        raise XiaoshiHistoryUnavailable(
            "xiaoshi-data returned an invalid row payload",
            context=_error_context(endpoint="xiaoshi-data", detail="invalid rows"),
        )
    return [dict(row) for row in rows]


def _safe_extra_args(extra_args: Sequence[str]) -> list[str]:
    reserved = {
        "--as-of",
        "--dataset",
        "--json",
        "--market",
        "--since",
        "--symbol",
        "--to",
    }
    result = [str(arg) for arg in extra_args]
    if any(arg in reserved or arg.split("=", 1)[0] in reserved for arg in result):
        raise XiaoshiError(
            "xiaoshi-data query arguments cannot override Manifest-controlled fields",
            context=_error_context(endpoint="xiaoshi-data query"),
        )
    if any(arg.startswith(("-", "--")) and "\x00" in arg for arg in result):
        raise XiaoshiError(
            "xiaoshi-data query arguments contain an invalid value",
            context=_error_context(endpoint="xiaoshi-data query"),
        )
    return result


def _filter_financial_rows(rows: list[dict[str, Any]], as_of: datetime | None) -> list[dict[str, Any]]:
    if as_of is None or not _is_timezone_aware(as_of):
        raise XiaoshiError(
            "financial-as-reported requires a timezone-aware as_of",
            context=_error_context(
                endpoint="xiaoshi-data query",
                params={"dataset": "financial-as-reported"},
            ),
        )
    filtered: list[dict[str, Any]] = []
    for row in rows:
        raw = row.get("available_at")
        if not raw:
            raise XiaoshiError(
                "financial-as-reported row is missing available_at",
                context=_error_context(endpoint="xiaoshi-data query"),
            )
        try:
            available_at = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError as exc:
            raise XiaoshiError(
                "financial-as-reported row has invalid available_at",
                context=_error_context(endpoint="xiaoshi-data query"),
            ) from exc
        if not _is_timezone_aware(available_at):
            raise XiaoshiError(
                "financial-as-reported available_at must be timezone-aware",
                context=_error_context(endpoint="xiaoshi-data query"),
            )
        if available_at <= as_of:
            filtered.append(row)
    return filtered


def get_resource_manager() -> XiaoshiResourceManager:
    return XiaoshiResourceManager()


def get_status() -> dict[str, Any]:
    state = get_resource_manager().state()
    return {
        "service": "xiaoshi",
        "configured": XiaoshiClient().configured,
        "read_only": True,
        "history_cli": shutil.which("xiaoshi-data") is not None,
        "active": state.active,
        "manifest_version": state.manifest_version,
        "prompt_version": state.prompt_version,
        "skill_version": state.skill_version,
        "api_schema_version": state.api_schema_version,
        "checksums": state.checksums,
        "last_error": state.last_error,
    }
