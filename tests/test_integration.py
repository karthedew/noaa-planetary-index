import numpy as np
import pytest
import xarray as xr

from kp_demo import pipeline, storage
from kp_demo.concurrency import run_concurrency_demo
from kp_demo.provenance import trace_value

pytest.importorskip("icechunk")
pytest.importorskip("zarr")
pytestmark = pytest.mark.integration


def test_datatree_roundtrip_append_and_idempotence(tmp_path, rows):
    a = pipeline.ingest_records(rows[:40], tmp_path)
    b = pipeline.ingest_records(rows, tmp_path)
    c = pipeline.ingest_records(rows, tmp_path)
    assert b["new"] == len(rows)-40
    assert not c["changed"] and c["snapshot"] == b["snapshot"]
    assert storage.read_dataset(tmp_path, snapshot_id=a["snapshot"]).sizes["time"] == 40
    tree = storage.read_tree(tmp_path, load=True)
    assert set(tree.children) == set(storage.GROUPS)
    assert tree["observations/kp"].values[0] == rows[0]["Kp"]
    assert tree["provenance/source_id"].values[0] != 0


def test_region_reprocess_and_value_lineage(tmp_path, rows):
    initial = pipeline.ingest_records(rows, tmp_path)
    start, end = rows[17]["time_tag"], rows[22]["time_tag"]
    bad = pipeline.reprocess_range(start, end, tmp_path, bias=10)
    good = pipeline.reprocess_range(start, end, tmp_path)
    table, stats = storage.compare_snapshots(bad["snapshot"], good["snapshot"], root=tmp_path)
    assert stats["numeric_changed"] == 6
    assert not table.changed.iloc[:17].any() and not table.changed.iloc[23:].any()
    ds = storage.read_dataset(tmp_path)
    trace = trace_value(ds, variable="ap_24h_mean", time=end, root=tmp_path)
    assert len(trace["source_records"]) == 8
    assert trace["first_produced_snapshot"] == good["snapshot"]
    assert "processing.py" in trace["code"]["files"]
    original = storage.read_dataset(tmp_path, snapshot_id=initial["snapshot"])
    xr.testing.assert_equal(ds.kp, original.kp)
    np.testing.assert_allclose(ds.ap_24h_mean, original.ap_24h_mean, equal_nan=True)


def test_missing_calendar_positions_are_explicit(tmp_path, rows):
    result = pipeline.ingest_records([rows[0], rows[2]], tmp_path)
    ds = storage.read_dataset(tmp_path)
    assert ds.sizes["time"] == 3
    assert np.isnan(ds.kp.values[1]) and ds.source_id.values[1] == 0
    assert ds.storm_level.values[1] == -1
    assert result["missing_kp"] == 1
    pipeline.ingest_records(rows[:3], tmp_path)
    assert np.isfinite(storage.read_dataset(tmp_path).kp.values).all()


def test_source_revision_updates_only_dependent_outputs(tmp_path, rows):
    original = pipeline.ingest_records(rows, tmp_path)
    revised = dict(rows[12], Kp=8.)
    result = pipeline.ingest_records([revised], tmp_path)
    before = storage.read_dataset(tmp_path, snapshot_id=original["snapshot"])
    after = storage.read_dataset(tmp_path)
    changed = np.flatnonzero(before.mean_run.values != after.mean_run.values)
    np.testing.assert_equal(changed, np.arange(12, 20))
    assert result["revised"] == 1


def test_experiment_rollback_and_retry_idempotency(tmp_path, rows):
    initial = pipeline.ingest_records(rows, tmp_path)["snapshot"]
    pipeline.create_experiment(tmp_path)
    args = (rows[16]["time_tag"], rows[20]["time_tag"], tmp_path)
    exp = pipeline.reprocess_range(*args, branch="experiment", bias=5, operation_id="airflow-run-1")
    replay = pipeline.reprocess_range(*args, branch="experiment", bias=5, operation_id="airflow-run-1")
    assert exp["snapshot"] == replay["snapshot"] and replay["replayed"]
    assert storage.repository(tmp_path).lookup_branch("main") == initial
    pipeline.reprocess_range(*args, bias=10)
    rolled = storage.rollback(initial, tmp_path)
    assert rolled["changed"] and rolled["safety_branch"] in storage.repository(tmp_path).list_branches()


def test_concurrent_sessions(tmp_path, rows):
    pipeline.ingest_records(rows, tmp_path)
    result = run_concurrency_demo(tmp_path)
    for key in ("old_reader_remained_consistent", "independent_writes_published", "main_unchanged",
                "uncommitted_changes_hidden", "overlapping_write_conflict_detected"):
        assert result[key], result
