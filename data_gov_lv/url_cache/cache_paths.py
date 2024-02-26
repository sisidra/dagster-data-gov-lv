import hashlib
from pathlib import Path

from dagster import DagsterInstance


def cache_basename(url: str) -> Path:
    version = b"v1"  # A way to invalidate cache
    hasher = hashlib.sha1(url.encode("utf-8"))
    hasher.update(version)
    hash_part = hasher.hexdigest()
    file_path = url.split("/")[-1]
    return Path(hash_part + "." + file_path)


def cache_path(cache_root: Path, url: str) -> Path:
    return cache_root / cache_basename(url)


def dagster_cache_root(instance: DagsterInstance) -> Path:
    return Path(instance.root_directory) / "data-gov-lv.cache"
