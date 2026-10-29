"""CLI used by Marimo users and standalone scripts; Airflow calls the same modules."""
from __future__ import annotations

import argparse
import json
import time
from importlib.resources import files


def fixture_records():
    from .noaa import parse_records
    return parse_records(json.loads(files("kp_demo").joinpath("fixtures/live.json").read_text()),
                         origin="fixture:captured-noaa-live")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=None, help="Shared data root (or KP_DEMO_DATA)")
    sub = parser.add_subparsers(dest="command", required=True)
    history = sub.add_parser("history", help="Download and ingest 2016 -> latest completed interval")
    history.add_argument("--start", default="2016-01-01")
    history.add_argument("--end", default="now", help="Exclusive UTC end, or now")
    history.add_argument("--refresh-archives", action="store_true")
    history.add_argument("--strict-coverage", action="store_true")
    history.add_argument("--fail-after-batches", type=int, default=0)
    live = sub.add_parser("ingest", help="Refresh recent observations")
    live.add_argument("--watch", action="store_true")
    live.add_argument("--interval-seconds", type=int, default=900)
    live.add_argument("--notify", action="store_true", help="Opt-in HTTPS webhook delivery")
    seed = sub.add_parser("seed", help="Offline fixture, preferably in a separate --data-dir")
    seed.add_argument("--count", type=int, default=None)
    resume = sub.add_parser("resume", help="Resume an already staged history manifest")
    resume.add_argument("manifest")
    sub.add_parser("snapshots")
    sub.add_parser("inspect")
    sub.add_parser("concurrency")
    repro = sub.add_parser("reprocess")
    repro.add_argument("start")
    repro.add_argument("end")
    repro.add_argument("--branch", default="main")
    repro.add_argument("--bias", type=float, default=0.)
    compare = sub.add_parser("compare")
    compare.add_argument("old")
    compare.add_argument("new")
    compare.add_argument("--variable", default="ap_24h_mean")
    rollback = sub.add_parser("rollback")
    rollback.add_argument("snapshot")
    args = parser.parse_args()
    root = args.data_dir
    from . import history as hist, pipeline, storage
    if args.command == "history":
        prepared = hist.prepare_history(root, start=args.start, end=args.end, refresh_archives=args.refresh_archives)
        print(json.dumps({"prepared": prepared}, indent=2))
        result = hist.run_manifest(root, prepared["manifest"], fail_after_batches=args.fail_after_batches)
        result["validated"] = hist.validate_snapshot(root, result["snapshot"], start=prepared["requested_start"],
                                                    end=prepared["end_exclusive"], strict=args.strict_coverage)
    elif args.command == "resume":
        result = hist.run_manifest(root, args.manifest)
    elif args.command == "ingest":
        from .alerts import monitor
        if args.interval_seconds < 60:
            parser.error("Use a polling interval of at least 60 seconds")
        try:
            while True:
                result = hist.sync_latest(root)
                result["alert"] = monitor(root, snapshot_id=result["snapshot"], notify=args.notify)
                print(json.dumps(result, indent=2), flush=True)
                if not args.watch:
                    return
                time.sleep(args.interval_seconds)
        except KeyboardInterrupt:
            return
    elif args.command == "seed":
        result = pipeline.ingest_records(fixture_records()[:args.count], root)
    elif args.command == "snapshots":
        result = storage.list_history(root)
    elif args.command == "inspect":
        print(storage.read_tree(root))
        result = hist.validate_snapshot(root, storage.repository(root).lookup_branch("main"))
    elif args.command == "concurrency":
        from .concurrency import run_concurrency_demo
        result = run_concurrency_demo(root)
    elif args.command == "reprocess":
        if args.branch != "main":
            pipeline.create_experiment(root, name=args.branch)
        result = pipeline.reprocess_range(args.start, args.end, root, branch=args.branch, bias=args.bias)
    elif args.command == "compare":
        table, result = storage.compare_snapshots(args.old, args.new, args.variable, root)
        print(table[table.changed].to_string(index=False))
    else:
        result = storage.rollback(args.snapshot, root)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
