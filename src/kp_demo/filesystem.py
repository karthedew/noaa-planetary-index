"""Local immutable sidecars. Copy the entire data directory when backing up."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path

from filelock import FileLock


def data_home(root=None) -> Path:
    return Path(root or os.environ.get("KP_DEMO_DATA", "data")).expanduser().resolve()


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_json(path: Path, value) -> None:
    atomic_write(path, canonical(value))


def immutable_json(root, folder: str, value) -> str:
    raw = canonical(value)
    key = digest(raw)
    path = data_home(root) / folder / f"{key}.json"
    if not path.exists():
        atomic_write(path, raw)
    return key


def read_json(root, folder: str, key: str):
    if not key or any(c not in "0123456789abcdef" for c in key):
        raise ValueError("Invalid content-addressed identifier")
    path = data_home(root) / folder / f"{key}.json"
    content = path.read_bytes()
    if digest(content) != key:
        raise ValueError(f"Checksum mismatch: {path}")
    return json.loads(content)


def writer_lock(root):
    home = data_home(root)
    home.mkdir(parents=True, exist_ok=True)
    return FileLock(str(home / ".writer.lock"), timeout=120)
