"""Whole-repository smoke test using an isolated temporary Icechunk store."""
from tempfile import TemporaryDirectory

from kp_demo.cli import fixture_records
from kp_demo.concurrency import run_concurrency_demo
from kp_demo.pipeline import create_experiment, ingest_records, reprocess_range
from kp_demo.storage import compare_snapshots, read_tree, rollback


def main():
    rows = fixture_records()
    with TemporaryDirectory() as root:
        first = ingest_records(rows[:40], root)
        ingest_records(rows, root)
        assert not ingest_records(rows, root)["changed"]
        bad = reprocess_range(rows[16]["time_tag"], rows[23]["time_tag"], root, bias=10)
        fixed = reprocess_range(rows[16]["time_tag"], rows[23]["time_tag"], root)
        _, stats = compare_snapshots(bad["snapshot"], fixed["snapshot"], root=root)
        assert stats["numeric_changed"] == 8
        create_experiment(root)
        reprocess_range(rows[16]["time_tag"], rows[23]["time_tag"], root, branch="experiment", bias=5)
        concurrent = run_concurrency_demo(root)
        assert concurrent["overlapping_write_conflict_detected"]
        rollback(first["snapshot"], root)
        print(read_tree(root))
        print(concurrent)
    print("PASS: DataTree, append, idempotence, reprocessing, branching, concurrency, rollback")


if __name__ == "__main__":
    main()
