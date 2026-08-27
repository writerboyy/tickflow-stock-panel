"""Storage safety checks shared by local data importers."""
from __future__ import annotations

import os
from pathlib import Path


MIN_FREE_BYTES = 50 * 1024**3


def free_space_bytes(path: Path) -> int:
    stat = os.statvfs(path)
    return int(stat.f_bavail * stat.f_frsize)


def assert_disk_reserve(path: Path, *, minimum: int = MIN_FREE_BYTES) -> None:
    available = free_space_bytes(path)
    if available < minimum:
        raise RuntimeError(f"free space reserve reached: {available / 1024**3:.1f} GiB available")
