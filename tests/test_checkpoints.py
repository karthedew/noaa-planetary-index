import pytest

from kp_demo.filesystem import immutable_json
from kp_demo.history import LocalState, run_manifest


def plan(root):
    batches = [immutable_json(root, "batches", {"start": f"2016-0{i}-01", "end": f"2016-0{i+1}-01", "records": []})
               for i in (1, 2, 3)]
    return immutable_json(root, "manifests", {"batches": batches, "coverage": {"complete": True}})


def test_resume_after_committed_batch(tmp_path):
    key = plan(tmp_path)
    seen = []
    def ingest(rows, root, **kw):
        seen.append(kw["grid_start"])
        return {"snapshot": f"S{len(seen)}"}
    with pytest.raises(RuntimeError, match="Intentional"):
        run_manifest(tmp_path, key, fail_after_batches=2, ingest=ingest, reachable=lambda s: True)
    result = run_manifest(tmp_path, key, fail_after_batches=2, ingest=ingest, reachable=lambda s: True)
    assert result["resumed_at_batch"] == 2
    assert len(seen) == 3


def test_commit_before_checkpoint_crash_replays_same_frozen_batch(tmp_path):
    key = plan(tmp_path)
    commits = set()
    attempts = []
    def ingest(rows, root, **kw):
        batch = kw["grid_start"]
        attempts.append(batch)
        commits.add(batch)
        return {"snapshot": batch}
    class CrashOnce(LocalState):
        crashed = False
        def set(self, k, v, **kwargs):
            if not self.crashed and k == "checkpoint":
                self.crashed = True
                raise RuntimeError("worker lost after commit")
            super().set(k, v, **kwargs)
    state = CrashOnce(tmp_path, key)
    with pytest.raises(RuntimeError, match="worker lost"):
        run_manifest(tmp_path, key, state=state, ingest=ingest, reachable=lambda s: True)
    run_manifest(tmp_path, key, state=state, ingest=ingest, reachable=lambda s: True)
    assert len(attempts) == 4 and len(commits) == 3
    assert attempts[0] == attempts[1]


def test_rollback_invalidates_cursor(tmp_path):
    key = plan(tmp_path)
    count = []
    def ingest(rows, root, **kwargs):
        count.append(1)
        return {"snapshot": "snapshot"}
    run_manifest(tmp_path, key, ingest=ingest, reachable=lambda s: True)
    run_manifest(tmp_path, key, ingest=ingest, reachable=lambda s: False)
    assert len(count) == 6
