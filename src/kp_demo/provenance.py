"""Value-level lineage, immutable source records and archived executable source text."""
from __future__ import annotations

import json
import subprocess
import uuid
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import numpy as np

from .filesystem import canonical, data_home, digest, immutable_json, read_json, write_json
from .noaa import normalize_time, observation_identity

PRODUCERS = {"ap_equivalent": "ap_run", "ap_24h_mean": "mean_run", "storm_level": "storm_run"}


def save_source(root, row, *, normalization_code=None):
    identity = observation_identity(row)
    sha = digest(canonical(identity))
    key = int(sha[:15], 16)  # collision checked; int64-friendly, zero reserved for gaps
    path = data_home(root) / "sources" / f"{key}.json"
    if path.exists():
        if json.loads(path.read_text())["identity_sha256"] != sha:
            raise ValueError("Source identifier collision")
    else:
        write_json(path, {"id": key, "identity_sha256": sha, "record": row, "normalization_code_sha256": normalization_code})
    return key


def source(root, key):
    body = json.loads((data_home(root) / "sources" / f"{int(key)}.json").read_text())
    sha = digest(canonical(observation_identity(body["record"])))
    if sha != body["identity_sha256"] or int(sha[:15], 16) != int(key):
        raise ValueError("Source record checksum mismatch")
    return body


def code_bundle(root):
    folder = Path(__file__).parent
    files = {p.name: p.read_text() for p in sorted(folder.glob("*.py"))}
    versions = {}
    for name in ("icechunk", "zarr", "xarray", "numpy", "pandas", "noaa-planetary-index"):
        try:
            versions[name] = version(name)
        except PackageNotFoundError:
            versions[name] = "not-installed"
    git_commit, dirty = None, None
    try:
        git_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=folder,
                                             stderr=subprocess.DEVNULL, timeout=2).decode().strip()
        dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=folder,
                                             stderr=subprocess.DEVNULL, timeout=2).strip())
    except (FileNotFoundError, subprocess.SubprocessError):
        pass
    return immutable_json(root, "code", {"files": files, "versions": versions,
                                          "git_commit": git_commit, "git_dirty": dirty})


def save_run(root, *, parent, operation, parameters, times, sources, selections):
    run_id = uuid.uuid4().int & ((1 << 60) - 1)
    lineage = {}
    for variable, indices in selections.items():
        window = 8 if variable == "ap_24h_mean" else 1
        lineage[variable] = {normalize_time(times[i]): [int(s) for s in sources[max(0, i-window+1):i+1]]
                             for i in indices}
    body = {"run_id": run_id, "parent_snapshot": parent, "operation": operation,
            "parameters": parameters, "created_at": datetime.now(timezone.utc).isoformat(),
            "code_sha256": code_bundle(root), "lineage": lineage,
            "functions": {"ap_equivalent": ["processing.kp_to_ap"],
                          "ap_24h_mean": ["processing.compute_derived", "processing.kp_to_ap"],
                          "storm_level": ["processing.storm_levels"]}}
    path = data_home(root) / "runs" / f"{run_id}.json"
    if path.exists():
        raise FileExistsError("Run identifier collision")
    write_json(path, body)
    return body


def trace_value(ds, *, variable, time, root=None):
    key = normalize_time(time)
    indices = np.flatnonzero(ds.time.values == np.datetime64(key))
    if len(indices) != 1:
        raise KeyError(f"Timestamp not found: {key}")
    i = int(indices[0])
    value = ds[variable].values[i].item()
    if isinstance(value, float) and not np.isfinite(value):
        value = None
    result = {"time_utc": key, "variable": variable, "value": value}
    if variable not in PRODUCERS:
        source_id = int(ds.source_id.values[i])
        return {**result, "source_records": [source(root, source_id)] if source_id else [],
                "note": "Raw NOAA input, or explicit missing observation; NOAA's internal code is outside this lineage."}
    run_id = int(ds[PRODUCERS[variable]].values[i])
    if not run_id:
        return {**result, "source_records": [], "note": "Unprocessed/missing position"}
    run = json.loads((data_home(root) / "runs" / f"{run_id}.json").read_text())
    inputs = run["lineage"][variable][key]
    from .storage import list_history
    producer = next((h["id"] for h in list_history(root)
                     if str((h["metadata"] or {}).get("run_id")) == str(run_id)), None)
    return {**result, "run_id": run_id, "first_produced_snapshot": producer,
            "source_records": [source(root, s) if s else {"missing": True} for s in inputs],
            "functions": run["functions"][variable],
            "run": {k: v for k, v in run.items() if k != "lineage"},
            "code": read_json(root, "code", run["code_sha256"])}
