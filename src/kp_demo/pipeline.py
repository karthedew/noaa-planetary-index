"""Atomic append, gap filling, source revisions, and dependency-aware region writes."""
from __future__ import annotations

import numpy as np
import pandas as pd
import xarray as xr

from . import processing, storage
from .filesystem import writer_lock
from .noaa import LIVE, normalize_time, parse_records
from .provenance import PRODUCERS, code_bundle, save_run, save_source


def empty_dataset(times):
    n = len(times)
    data = {}
    for name in ("kp", "kp_resolution", "ap_equivalent", "ap_24h_mean"):
        data[name] = ("time", np.full(n, np.nan, dtype="float64"))
    for name in ("a_running", "station_count", "storm_level"):
        data[name] = ("time", np.full(n, -1, dtype="int32"))
    for name in ("source_priority", "demo_marker_a", "demo_marker_b"):
        data[name] = ("time", np.zeros(n, dtype="int32"))
    for name in ("source_id", "ap_run", "mean_run", "storm_run"):
        data[name] = ("time", np.zeros(n, dtype="int64"))
    return xr.Dataset(data, coords={"time": np.asarray(times, dtype="datetime64[ns]")})


def _update_derived(ds, selections, *, bias=0.0):
    # Only selected output regions and their seven-input halos are evaluated.
    for variable, indices in selections.items():
        for lo, hi in processing.runs_of_indices(indices):
            read_start = max(0, lo - processing.WINDOW + 1) if variable == "ap_24h_mean" else lo
            values = processing.compute_derived(ds.kp.values[read_start:hi], bias=bias)[variable]
            ds[variable].values[lo:hi] = values[lo-read_start:]


def ingest_records(records, root=None, *, origin=LIVE, grid_start=None, grid_end=None):
    """Upsert on a regular UTC grid. End is exclusive; never interpolate gaps.

    Priority is archive=1, recent DGD=2, live JSON=3. A lower-priority source
    cannot erase a previously captured live observation. Same-priority source
    revisions are applied. Changes and lineage become visible in one commit.
    """
    records = parse_records(records, origin=origin) if records else []
    if not records and (grid_start is None or grid_end is None):
        raise ValueError("Empty inputs require explicit grid_start and grid_end")
    with writer_lock(root):
        repo = storage.repository(root, create=True)
        session = repo.writable_session("main")
        parent = session.snapshot_id
        exists = storage.dataset_exists(root)
        old = storage.read_dataset(root, snapshot_id=parent) if exists else None
        start = pd.Timestamp(normalize_time(grid_start or records[0]["time_tag"]))
        end = (pd.Timestamp(normalize_time(grid_end)) if grid_end else
               pd.Timestamp(records[-1]["time_tag"]) + pd.Timedelta(hours=3))
        if start != start.floor("3h") or end != end.floor("3h") or end <= start:
            raise ValueError("Grid must use ordered three-hour UTC boundaries")
        if old is not None:
            if start < pd.Timestamp(old.time.values[0]):
                raise ValueError("Cannot prepend to this demo store. Initialize history in a fresh --data-dir.")
            start = pd.Timestamp(old.time.values[0])
            end = max(end, pd.Timestamp(old.time.values[-1]) + pd.Timedelta(hours=3))
        times = pd.date_range(start, end, freq="3h", inclusive="left")
        updated = empty_dataset(times)
        n_old = old.sizes["time"] if old is not None else 0
        if old is not None:
            for name in old.data_vars:
                updated[name].values[:n_old] = old[name].values
        lookup = {normalize_time(t): i for i, t in enumerate(times)}
        changed = []
        normalization_code = code_bundle(root)
        for row in records:
            i = lookup.get(row["time_tag"])
            if i is None:
                continue
            existing_valid = np.isfinite(updated.kp.values[i])
            if existing_valid and (row["Kp"] is None or
                                   row["source_priority"] < updated.source_priority.values[i]):
                continue
            source_id = save_source(root, row, normalization_code=normalization_code)
            if source_id == int(updated.source_id.values[i]):
                continue
            changed.append(i)
            for column, field in (("kp", "Kp"), ("a_running", "a_running"),
                                   ("station_count", "station_count"),
                                   ("kp_resolution", "kp_resolution"), ("source_priority", "source_priority")):
                value = row[field]
                updated[column].values[i] = value if value is not None else (np.nan if column == "kp" else -1)
            updated.source_id.values[i] = source_id
        appended = list(range(n_old, len(times)))
        if not changed and not appended:
            return {"changed": False, "new": 0, "revised": 0, "snapshot": parent}
        direct = sorted(set(changed + appended))
        selections = {"ap_equivalent": direct, "storm_level": direct,
                      "ap_24h_mean": sorted(set(appended + processing.affected_outputs(changed, len(times))))}
        _update_derived(updated, selections)
        run = save_run(root, parent=parent, operation="NOAA ingest", parameters={"window": 8, "bias": 0.0},
                       times=updated.time.values, sources=updated.source_id.values, selections=selections)
        for variable, indices in selections.items():
            updated[PRODUCERS[variable]].values[indices] = run["run_id"]
        if old is None:
            storage.initialize(session, updated)
        else:
            if appended:
                storage.append(session, updated.isel(time=slice(n_old, None)))
            # Each region only rewrites the relevant arrays; no whole-day/file rewrite.
            source_columns = [*storage.GROUPS["observations"], "source_id"]
            for lo, hi in processing.runs_of_indices([i for i in changed if i < n_old]):
                storage.write_region(session, {v: updated[v].values[lo:hi] for v in source_columns}, lo, hi)
            for variable, indices in selections.items():
                for lo, hi in processing.runs_of_indices([i for i in indices if i < n_old]):
                    names = [variable, PRODUCERS[variable]]
                    storage.write_region(session, {v: updated[v].values[lo:hi] for v in names}, lo, hi)
        snapshot = session.commit(
            f"NOAA: {len(appended)} new grid positions, {sum(i < n_old for i in changed)} revised",
            metadata={"run_id": str(run["run_id"]), "code_sha256": run["code_sha256"], "operation": "ingest"},
        )
        return {"changed": True, "new": len(appended), "revised": sum(i < n_old for i in changed),
                "snapshot": snapshot, "run_id": run["run_id"],
                "missing_kp": int(np.isnan(updated.kp.values).sum())}


def reprocess_range(start, end, root=None, *, branch="main", bias=0.0, operation_id=None):
    """Inclusive output range; read only that range + a seven-observation halo."""
    with writer_lock(root):
        repo = storage.repository(root)
        if operation_id:
            for prior in repo.ancestry(branch=branch):
                if (prior.metadata or {}).get("operation_id") == operation_id:
                    return {"snapshot": prior.id, "branch": branch, "replayed": True}
        session = repo.writable_session(branch)
        ds = storage.read_dataset(root, snapshot_id=session.snapshot_id, load=False)
        first, last = np.datetime64(normalize_time(start)), np.datetime64(normalize_time(end))
        if first > last:
            raise ValueError("Start is after end")
        indices = np.flatnonzero((ds.time.values >= first) & (ds.time.values <= last))
        if not len(indices):
            raise ValueError("Range does not intersect the dataset")
        lo, hi = int(indices[0]), int(indices[-1]) + 1
        halo = max(0, lo - 7)
        subset = ds.isel(time=slice(halo, hi)).load()
        local_indices = list(range(lo-halo, hi-halo))
        selections = {name: local_indices for name in PRODUCERS}
        _update_derived(subset, selections, bias=bias)
        run = save_run(root, parent=session.snapshot_id, operation="reprocess", parameters={
            "window": 8, "bias": bias, "branch": branch, "start": normalize_time(start), "end": normalize_time(end)},
            times=subset.time.values, sources=subset.source_id.values, selections=selections)
        values = {v: subset[v].values[lo-halo:] for v in PRODUCERS}
        for producer in PRODUCERS.values():
            values[producer] = np.full(hi-lo, run["run_id"], dtype="int64")
        storage.write_region(session, values, lo, hi)
        snapshot = session.commit(f"Reprocess {lo}:{hi} on {branch}, ap mean bias={bias}", metadata={
            "run_id": str(run["run_id"]), "code_sha256": run["code_sha256"], "operation": "reprocess", "operation_id": operation_id})
        return {"snapshot": snapshot, "run_id": run["run_id"], "branch": branch,
                "output_positions": hi-lo, "input_positions_read": hi-halo}


def create_experiment(root=None, *, name="experiment"):
    with writer_lock(root):
        repo = storage.repository(root)
        if name not in repo.list_branches():
            repo.create_branch(name, repo.lookup_branch("main"))
        return {"branch": name, "snapshot": repo.lookup_branch(name)}
