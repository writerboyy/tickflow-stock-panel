"""Read-only health summary for external-source ingestion manifests."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


_SUCCESS_STATUSES = {
    "complete",
    "completed",
    "published",
    "section_absent",
    "unsupported",
    "valid_empty",
}
_FAILURE_STATUSES = {
    "failed",
    "incomplete",
    "page_limit_reached",
    "parse_rejected",
    "source_error",
}


def _read_manifest(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid ingestion manifest: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"invalid ingestion manifest: {path}")
    return value


def _snapshot_order(path: Path) -> tuple[str, int]:
    return path.stem, path.stat().st_mtime_ns


def summarize_ingestion_health(
    data_dir: Path,
    *,
    sources: set[str] | None = None,
) -> dict[str, Any]:
    root = Path(data_dir) / "ext_data" / "_ingestion"
    paths = sorted(root.glob("*/*/*.json")) if root.exists() else []
    if sources is not None:
        paths = [path for path in paths if path.relative_to(root).parts[0] in sources]

    latest_paths: dict[tuple[str, str], Path] = {}
    for path in paths:
        relative = path.relative_to(root)
        key = relative.parts[0], relative.parts[1]
        current = latest_paths.get(key)
        if current is None or _snapshot_order(path) > _snapshot_order(current):
            latest_paths[key] = path

    latest: dict[tuple[str, str], tuple[Path, dict[str, Any]]] = {}
    invalid: list[dict[str, str]] = []
    for key, path in sorted(latest_paths.items()):
        relative = path.relative_to(root)
        source, dataset = relative.parts[:2]
        try:
            manifest = _read_manifest(path)
            expected = {
                "source": source,
                "dataset": dataset,
                "logical_snapshot": path.stem,
            }
            mismatches = [
                field
                for field, value in expected.items()
                if str(manifest.get(field) or "") != value
            ]
            if mismatches:
                raise ValueError(
                    f"ingestion manifest path mismatch: {', '.join(mismatches)}"
                )
        except ValueError as exc:
            invalid.append({
                "path": relative.as_posix(),
                "error": str(exc),
            })
            continue
        latest[key] = (path, manifest)

    status_counts: Counter[str] = Counter()
    batch_status_counts: Counter[str] = Counter()
    issues: list[dict[str, Any]] = []
    datasets: list[dict[str, Any]] = []
    published_rows = 0
    rejected_rows = 0
    for (source, dataset), (path, manifest) in sorted(latest.items()):
        status = str(manifest.get("status") or "unknown")
        batches = manifest.get("batches") or {}
        if not isinstance(batches, dict):
            batches = {}
            status = "invalid_batches"
        status_counts[status] += 1
        failed_batches = set(str(value) for value in manifest.get("failed_batches") or [])
        for batch_id, batch in batches.items():
            batch_status = (
                str(batch.get("status") or "unknown")
                if isinstance(batch, dict)
                else "invalid"
            )
            batch_status_counts[batch_status] += 1
            if batch_status in _FAILURE_STATUSES or batch_status in {"invalid", "unknown"}:
                failed_batches.add(str(batch_id))
                if isinstance(batch, dict):
                    rejected_rows += int(batch.get("row_count") or 0)
        published_rows += int(manifest.get("published_rows") or 0)
        item = {
            "source": source,
            "dataset": dataset,
            "logical_snapshot": str(manifest.get("logical_snapshot") or path.stem),
            "status": status,
            "published_rows": int(manifest.get("published_rows") or 0),
            "failed_batches": sorted(failed_batches),
            "error_code": manifest.get("error_code"),
            "manifest": path.relative_to(root).as_posix(),
        }
        datasets.append(item)
        if (
            status not in _SUCCESS_STATUSES
            or failed_batches
            or manifest.get("error_code")
        ):
            issues.append(item)

    if invalid or issues:
        status = "unhealthy"
    elif datasets:
        status = "healthy"
    else:
        status = "no_data"
    return {
        "status": status,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "manifest_root": str(root),
        "manifest_files": len(paths),
        "latest_datasets": len(datasets),
        "published_rows": published_rows,
        "rejected_rows": rejected_rows,
        "status_counts": dict(sorted(status_counts.items())),
        "batch_status_counts": dict(sorted(batch_status_counts.items())),
        "invalid_manifests": invalid,
        "issues": issues,
        "datasets": datasets,
    }
