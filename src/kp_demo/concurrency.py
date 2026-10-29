"""Controlled overlapping sessions on a scratch branch; main remains unchanged."""
from __future__ import annotations

import uuid

import numpy as np

from .storage import CHUNK, read_dataset, repository, write_region


def run_concurrency_demo(root=None):
    from icechunk.conflicts import ConflictDetector
    repo = repository(root)
    initial = repo.lookup_branch("main")
    before = read_dataset(root, snapshot_id=initial)
    if before.sizes["time"] < CHUNK + 4:
        raise ValueError(f"Load at least {CHUNK + 4} observations for this demonstration")
    branch = f"concurrency-{uuid.uuid4().hex[:8]}"
    repo.create_branch(branch, initial)

    def marker(session, index, value):
        write_region(session, {"demo_marker_a": np.array([value], dtype="int32"),
                               "demo_marker_b": np.array([value+1], dtype="int32")}, index, index+1)

    first, second = repo.writable_session(branch), repo.writable_session(branch)
    marker(first, 1, 10)
    marker(second, CHUNK+1, 20)
    unpublished = read_dataset(root, branch=branch)
    hidden = int(unpublished.demo_marker_a.values[1]) == int(before.demo_marker_a.values[1])
    s1 = first.commit("Demo: writer A publishes two arrays atomically")
    s2 = second.commit("Demo: writer B rebases a disjoint chunk", rebase_with=ConflictDetector())
    current = read_dataset(root, branch=branch)
    pinned = read_dataset(root, snapshot_id=initial)
    stable = np.array_equal(pinned.demo_marker_a, before.demo_marker_a)
    independent = all((int(current.demo_marker_a.values[1]) == 10,
                       int(current.demo_marker_b.values[1]) == 11,
                       int(current.demo_marker_a.values[CHUNK+1]) == 20,
                       int(current.demo_marker_b.values[CHUNK+1]) == 21))
    third, fourth = repo.writable_session(branch), repo.writable_session(branch)
    # Different elements, SAME physical chunk: conflict must not be ignored.
    marker(third, 2, 30)
    marker(fourth, 3, 40)
    s3 = third.commit("Demo: winner of overlapping chunk writes")
    detected, error_type = False, None
    try:
        fourth.commit("Demo: overlapping loser", rebase_with=ConflictDetector())
    except Exception as exc:
        error_type = type(exc).__name__
        if error_type not in ("ConflictError", "RebaseFailedError", "RebaseFailed"):
            raise
        detected = True
    return {"branch": branch, "snapshots": [s1, s2, s3],
            "uncommitted_changes_hidden": hidden, "old_reader_remained_consistent": stable,
            "independent_writes_published": independent, "overlapping_write_conflict_detected": detected,
            "conflict_type": error_type, "main_unchanged": repo.lookup_branch("main") == initial}
