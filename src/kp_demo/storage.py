"""Xarray-only array I/O; all DataTree groups share one Icechunk transaction."""
from __future__ import annotations

import numpy as np
import pandas as pd
import xarray as xr

from .filesystem import data_home, writer_lock

CHUNK = 32  # four days; independent update unit, not a daily-file boundary
GROUPS = {
    "observations": ["kp", "a_running", "station_count", "kp_resolution", "source_priority"],
    "derived": ["ap_equivalent", "ap_24h_mean", "storm_level"],
    "provenance": ["source_id", "ap_run", "mean_run", "storm_run"],
    "demo": ["demo_marker_a", "demo_marker_b"],
}
TIME_ENCODING = {"units": "hours since 1970-01-01 00:00:00",
                 "calendar": "proleptic_gregorian", "dtype": "int64"}


def repository(root=None, *, create=False):
    import icechunk as ic
    path = data_home(root) / "icechunk"
    storage = ic.local_filesystem_storage(str(path))
    if ic.Repository.exists(storage):
        return ic.Repository.open(storage)
    if not create:
        raise FileNotFoundError("Initialize with `kp-ingest history` or the Marimo history button")
    path.mkdir(parents=True, exist_ok=True)
    return ic.Repository.create(storage)


def dataset_exists(root=None):
    try:
        return any(s.parent_id is not None for s in repository(root).ancestry(branch="main"))
    except FileNotFoundError:
        return False


def read_tree(root=None, *, branch="main", snapshot_id=None, load=False):
    repo = repository(root)
    session = (repo.readonly_session(snapshot_id=snapshot_id) if snapshot_id
               else repo.readonly_session(branch=branch))
    tree = xr.open_datatree(session.store, engine="zarr", consolidated=False,
                            chunks=None, mask_and_scale=False)
    if tree.attrs.get("schema_version") != 2:
        raise ValueError("This release needs a fresh schema-v2 data directory; old data is untouched")
    return tree.load() if load else tree


def read_dataset(root=None, *, branch="main", snapshot_id=None, load=True):
    tree = read_tree(root, branch=branch, snapshot_id=snapshot_id, load=load)
    return xr.merge([tree[name].to_dataset() for name in GROUPS], join="exact", compat="equals")


def as_tree(ds):
    root = xr.Dataset(attrs={
        "title": "NOAA estimated planetary Kp", "schema_version": 2,
        "time_reference": "UTC; labels are the starts of three-hour intervals",
        "notes": "NOAA estimates; not definitive GFZ Kp. Missing values are not zero.",
    })
    tree = xr.DataTree.from_dict({"/": root, **{
        f"/{group}": ds[variables] for group, variables in GROUPS.items()
    }})
    for group in GROUPS:
        node = tree[group]
        node["time"].encoding = {**TIME_ENCODING, "chunks": (CHUNK,)}
        for name in GROUPS[group]:
            node[name].encoding = {"chunks": (CHUNK,)}
    tree["observations/kp"].attrs = {"units": "1", "long_name": "NOAA estimated planetary Kp"}
    tree["observations/kp_resolution"].attrs = {"units": "1", "comment": "1=legacy integer, 1/3=thirds"}
    tree["observations/a_running"].attrs = {"comment": "Live product only; -1=not supplied"}
    tree["observations/station_count"].attrs = {"comment": "Live product only; -1=not supplied"}
    tree["derived/ap_equivalent"].attrs = {"units": "2 nT", "comment": "Derived from reported Kp; not definitive ap"}
    tree["derived/ap_24h_mean"].attrs = {"units": "2 nT", "comment": "Trailing eight complete intervals; not calendar-day Ap"}
    tree["derived/storm_level"].attrs = {"comment": "Local thresholds; -1=missing, 0=no storm, 1..5=G-like; not an official alert"}
    return tree


def initialize(session, ds):
    # NumPy-backed arrays only: synchronous local writes, not distributed Dask.
    as_tree(ds).to_zarr(session.store, mode="w", consolidated=False, zarr_format=3)


def append(session, ds):
    for group, names in GROUPS.items():
        part = ds[names].copy()
        for var in part.variables:
            part[var].encoding = {}
        part["time"].encoding = TIME_ENCODING.copy()
        part.to_zarr(session.store, group=group, mode="a", append_dim="time",
                     consolidated=False, zarr_format=3)


def write_region(session, values, start, stop):
    if not values or stop <= start:
        return
    for group, names in GROUPS.items():
        selected = {name: ("time", np.asarray(value)) for name, value in values.items() if name in names}
        if not selected:
            continue
        if any(len(value[1]) != stop - start for value in selected.values()):
            raise ValueError("Region length mismatch")
        part = xr.Dataset(selected)  # no coordinate rewrite in a region operation
        part.to_zarr(session.store, group=group, mode="r+", region={"time": slice(start, stop)},
                     consolidated=False, zarr_format=3)


def list_history(root=None):
    repo = repository(root)
    rows = {}
    for branch in sorted(repo.list_branches()):
        for s in repo.ancestry(branch=branch):
            if s.parent_id is None:
                continue
            row = rows.setdefault(s.id, {"id": s.id, "timestamp": str(s.written_at),
                "message": s.message, "parent": s.parent_id, "metadata": s.metadata, "branches": []})
            row["branches"].append(branch)
    return sorted(rows.values(), key=lambda r: r["timestamp"], reverse=True)


def compare_snapshots(old, new, variable="ap_24h_mean", root=None):
    before = read_dataset(root, snapshot_id=old)[variable].to_series()
    after = read_dataset(root, snapshot_id=new)[variable].to_series()
    table = pd.concat([before.rename("before"), after.rename("after")], axis=1)
    table["delta"] = table["after"] - table["before"]
    table["changed"] = ~np.isclose(table.before, table.after, equal_nan=True)
    table["changed"] |= ~table.index.isin(before.index) | ~table.index.isin(after.index)
    detail = {"numeric_changed": int(table.changed.sum()), "structural_diff": None}
    repo = repository(root)
    if old in {s.id for s in repo.ancestry(snapshot_id=new)}:
        diff = repo.diff(from_snapshot_id=old, to_snapshot_id=new)
        chunks = {str(k): [list(c) for c in coords] for k, coords in diff.updated_chunks.items()}
        detail["structural_diff"] = {"updated_chunks": chunks,
                                    "chunk_count": sum(map(len, chunks.values()))}
    return table.reset_index(), detail


def rollback(target_snapshot, root=None, *, branch="main"):
    with writer_lock(root):
        repo = repository(root)
        current = repo.lookup_branch(branch)
        if current == target_snapshot:
            return {"changed": False, "snapshot": current}
        if target_snapshot not in {s.id for s in repo.ancestry(branch=branch)}:
            raise ValueError("Rollback target must be an ancestor")
        safety = f"saved-{current.lower()}"
        if safety not in repo.list_branches():
            repo.create_branch(safety, current)
        repo.reset_branch(branch, target_snapshot, from_snapshot_id=current)
        return {"changed": True, "snapshot": target_snapshot, "previous": current,
                "safety_branch": safety}
