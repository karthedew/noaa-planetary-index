"""Frozen monthly batches and restartable historical loading, 2016 -> latest complete interval."""
from __future__ import annotations

import json
from datetime import timedelta

import numpy as np
import pandas as pd

from .filesystem import data_home, immutable_json, read_json, write_json
from .noaa import ARCHIVE, RECENT, NOAAClient, archive_urls, bounds, normalize_time, parse_dgd


def prepare_history(root=None, *, start="2016-01-01", end="now", refresh_archives=False, client=None):
    lo, hi = bounds(start, end)
    if client is None:
        with NOAAClient(root) as download:
            return _prepare(root, lo, hi, download, refresh_archives)
    return _prepare(root, lo, hi, client, refresh_archives)


def _prepare(root, lo, hi, client, refresh_archives):
    index, _ = client.fetch(ARCHIVE)
    urls = archive_urls(index.decode(), lo, hi)
    observations = {}
    downloads = []

    def merge(rows):
        for row in rows:
            t = pd.Timestamp(row["time_tag"])
            if not lo <= t < hi:
                continue
            previous = observations.get(row["time_tag"])
            if previous and previous["Kp"] is not None and row["Kp"] is None:
                continue
            observations[row["time_tag"]] = row

    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    for url in urls:
        # Closed archives are immutable cache candidates; explicit refresh can recheck them.
        current_year = str(now.year) in url.rsplit("/", 1)[-1]
        raw, metadata = client.fetch(url, refresh=refresh_archives or current_year)
        downloads.append(metadata)
        merge(parse_dgd(raw.decode(), origin=url, raw_sha256=metadata["sha256"]))
    if hi > now - pd.Timedelta(days=32):
        raw, metadata = client.fetch(RECENT)
        downloads.append(metadata)
        merge(parse_dgd(raw.decode(), origin=RECENT, raw_sha256=metadata["sha256"], priority=2))
        merge(client.live())  # highest-priority source, including live-only ancillary fields
    batches = []
    cursor = lo
    while cursor < hi:
        stop = min(cursor + pd.offsets.MonthBegin(1), hi)
        selected = [r for t, r in sorted(observations.items()) if cursor <= pd.Timestamp(t) < stop]
        body = {"start": normalize_time(cursor), "end": normalize_time(stop), "records": selected}
        batches.append(immutable_json(root, "batches", body))
        cursor = stop
    report = coverage_records(list(observations.values()), lo, hi)
    manifest = {"start": normalize_time(lo), "end_exclusive": normalize_time(hi),
                "batches": batches, "downloads": downloads, "coverage": report}
    key = immutable_json(root, "manifests", manifest)
    return {"manifest": key, "batches": len(batches), **report}


def coverage_records(records, start, end):
    times = pd.date_range(start, end, freq="3h", inclusive="left")
    valid = {r["time_tag"] for r in records if r["Kp"] is not None}
    missing = [normalize_time(t) for t in times if normalize_time(t) not in valid]
    return {"requested_start": normalize_time(start), "end_exclusive": normalize_time(end),
            "expected_observations": len(times), "valid_kp": len(times)-len(missing),
            "missing_kp": len(missing), "first_missing": missing[:12], "complete": not missing}


class LocalState:
    """CLI equivalent of Airflow task state, keyed by the immutable manifest."""
    def __init__(self, root, key):
        self.path = data_home(root) / "checkpoints" / f"{key}.json"
        self.values = json.loads(self.path.read_text()) if self.path.exists() else {}

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value, **kwargs):
        self.values[key] = value
        write_json(self.path, self.values)


def run_manifest(root, manifest_key, *, state=None, fail_after_batches=0, ingest=None, reachable=None):
    """Commit first, checkpoint second. Replaying the gap is idempotent.

    Airflow task state holds a small cursor, not an Xarray object or the history.
    Inputs are frozen by content hash; a retry never silently switches payloads.
    """
    manifest = read_json(root, "manifests", manifest_key)
    if ingest is None:
        from .pipeline import ingest_records
        ingest = ingest_records
    if reachable is None:
        def reachable(snapshot):
            from .storage import repository
            return snapshot in {s.id for s in repository(root).ancestry(branch="main")}
    state = state or LocalState(root, manifest_key)
    checkpoint = state.get("checkpoint", default={})
    start = 0
    snapshot = None
    if checkpoint.get("manifest") == manifest_key:
        snapshot = checkpoint["snapshot"]
        if reachable(snapshot):
            start = int(checkpoint["next_batch"])
        else:
            snapshot = None  # e.g. main was explicitly rolled back; replay batches
    for number in range(start, len(manifest["batches"])):
        batch = read_json(root, "batches", manifest["batches"][number])
        result = ingest(batch["records"], root, grid_start=batch["start"], grid_end=batch["end"])
        snapshot = result["snapshot"]
        # ONLY after commit: a crash here replays an idempotent upsert on retry.
        state.set("checkpoint", {"manifest": manifest_key, "next_batch": number+1,
                                  "snapshot": snapshot}, retention=timedelta(days=30))
        if fail_after_batches and number + 1 >= fail_after_batches and not state.get("demo_failed", False):
            state.set("demo_failed", True)
            raise RuntimeError("Intentional checkpoint demo failure; retry resumes after committed batch")
    return {"snapshot": snapshot, "manifest": manifest_key, "batches": len(manifest["batches"]),
            "resumed_at_batch": start, "coverage": manifest["coverage"]}


def validate_snapshot(root, snapshot_id, *, start=None, end=None, strict=False):
    from .storage import read_dataset
    ds = read_dataset(root, snapshot_id=snapshot_id)
    if start is not None and end is not None:
        lo, hi = pd.Timestamp(normalize_time(start)), pd.Timestamp(normalize_time(end))
    else:
        lo = pd.Timestamp(ds.time.values[0])
        hi = pd.Timestamp(ds.time.values[-1]) + pd.Timedelta(hours=3)
    expected = pd.date_range(lo, hi, freq="3h", inclusive="left")
    actual = ds.kp.reindex(time=expected).values
    missing = ~np.isfinite(actual)
    report = {"snapshot": snapshot_id, "start": normalize_time(lo), "end_exclusive": normalize_time(hi),
              "expected_observations": len(expected), "valid_kp": int((~missing).sum()),
              "missing_kp": int(missing.sum()), "complete": not bool(missing.any()),
              "first_missing": [normalize_time(t) for t in expected[missing][:12]]}
    if strict and report["missing_kp"]:
        raise ValueError(f"NOAA coverage is incomplete: {report}")
    return report


def sync_latest(root=None):
    """Recheck overlapping recent records, not just timestamps beyond a watermark."""
    from .pipeline import ingest_records
    with NOAAClient(root) as client:
        raw, metadata = client.fetch(RECENT)
        recent = parse_dgd(raw.decode(), origin=RECENT, raw_sha256=metadata["sha256"], priority=2)
        rows = {r["time_tag"]: r for r in recent}
        rows.update({r["time_tag"]: r for r in client.live() if r["Kp"] is not None})
    cutoff = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("3h")
    from .storage import dataset_exists, read_dataset
    first = pd.Timestamp(read_dataset(root).time.values[0]) if dataset_exists(root) else pd.Timestamp.min
    records = [r for t, r in sorted(rows.items()) if first <= pd.Timestamp(t) < cutoff]
    if not records:
        raise ValueError("NOAA returned no complete intervals in the local dataset's range")
    return ingest_records(records, root, grid_end=normalize_time(cutoff))
