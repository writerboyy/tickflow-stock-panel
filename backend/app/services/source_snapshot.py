"""Content-addressed source snapshots for reproducible derived datasets."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Iterable


def capture_source_snapshot(
    data_dir: Path,
    sources: Iterable[str],
    *,
    previous: dict[str, dict[str, object]] | None = None,
) -> dict[str, dict[str, object]]:
    """Return stable file counts, sizes, and content hashes for source paths."""
    root = Path(data_dir)
    snapshots: dict[str, dict[str, object]] = {}
    previous = previous or {}
    for source in sources:
        source_path = root / source
        files = (
            [source_path]
            if source_path.is_file()
            else sorted(path for path in source_path.rglob("*.parquet") if path.is_file())
            if source_path.exists()
            else []
        )
        digest = sha256()
        total_bytes = 0
        file_hashes: dict[str, dict[str, object]] = {}
        previous_files = previous.get(source, {}).get("file_hashes", {})
        if not isinstance(previous_files, dict):
            previous_files = {}
        for path in files:
            relative = path.relative_to(root).as_posix()
            stat = path.stat()
            size = stat.st_size
            total_bytes += size
            cached = previous_files.get(relative, {})
            if (
                isinstance(cached, dict)
                and cached.get("size") == size
                and cached.get("mtime_ns") == stat.st_mtime_ns
                and isinstance(cached.get("sha256"), str)
            ):
                file_hash = str(cached["sha256"])
            else:
                file_digest = sha256()
                with path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        file_digest.update(chunk)
                file_hash = file_digest.hexdigest()
            file_hashes[relative] = {
                "size": size,
                "mtime_ns": stat.st_mtime_ns,
                "sha256": file_hash,
            }
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            digest.update(str(size).encode("ascii"))
            digest.update(b"\0")
            digest.update(bytes.fromhex(file_hash))
        snapshots[source] = {
            "files": len(files),
            "bytes": total_bytes,
            "sha256": digest.hexdigest(),
            "file_hashes": file_hashes,
        }
    return snapshots
