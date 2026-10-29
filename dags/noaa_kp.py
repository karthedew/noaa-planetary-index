"""Airflow 3.3.2 DAGs. Only orchestration; science and storage stay in kp_demo.

Set KP_DEMO_DATA to the SAME absolute shared directory for every worker and
Marimo. Create the noaa_kp_writer pool with one slot before unpausing DAGs.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Asset, Param, dag, task

ROOT = str(Path(os.environ.get("KP_DEMO_DATA", "/opt/airflow/shared/noaa-kp")).resolve())
KP = Asset("noaa_planetary_kp", uri=f"file://{ROOT}/icechunk")
DEFAULTS = {"retries": 3, "retry_delay": timedelta(minutes=1)}
START = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dag(dag_id="noaa_kp_history", schedule=None, start_date=START, catchup=False,
     max_active_runs=1, default_args=DEFAULTS, tags=["noaa", "icechunk"], params={
         "start": Param("2016-01-01", type="string"),
         "end": Param("now", type="string"),
         "strict_coverage": Param(False, type="boolean"),
         "refresh_archives": Param(False, type="boolean"),
         "fail_after_batches": Param(0, type="integer", minimum=0),
     })
def historical():
    @task
    def prepare(**context):
        from kp_demo.history import prepare_history
        state = context["task_state_store"]
        frozen = state.get("prepared")
        if frozen is not None:
            return frozen
        p = context["params"]
        frozen = prepare_history(ROOT, start=p["start"], end=p["end"],
                                 refresh_archives=p["refresh_archives"])
        state.set("prepared", frozen, retention=timedelta(days=30))
        return frozen

    @task(pool="noaa_kp_writer")
    def load(prepared, **context):
        from kp_demo.history import run_manifest
        return run_manifest(ROOT, prepared["manifest"], state=context["task_state_store"],
                            fail_after_batches=context["params"]["fail_after_batches"])

    @task(outlets=[KP])
    def verify(result, **context):
        from kp_demo.filesystem import read_json
        from kp_demo.history import validate_snapshot
        manifest = read_json(ROOT, "manifests", result["manifest"])
        report = validate_snapshot(ROOT, result["snapshot"], start=manifest["start"],
                                   end=manifest["end_exclusive"], strict=context["params"]["strict_coverage"])
        asset = context["asset_state_store"][KP]
        asset.set("last_history_snapshot", result["snapshot"])
        asset.set("history_coverage", report)
        return report

    verify(load(prepare()))


@dag(dag_id="noaa_kp_live", schedule="*/15 * * * *", start_date=START, catchup=False,
     max_active_runs=1, default_args=DEFAULTS, tags=["noaa", "icechunk"], params={
         "notify": Param(False, type="boolean"),
     })
def live():
    @task(pool="noaa_kp_writer", outlets=[KP])
    def ingest(**context):
        from kp_demo.history import sync_latest
        from kp_demo.storage import dataset_exists
        if not dataset_exists(ROOT):
            raise ValueError("Run noaa_kp_history successfully before enabling live ingestion")
        state = context["task_state_store"]
        result = state.get("committed")
        if result is None:
            result = sync_latest(ROOT)
            state.set("committed", result)
        context["asset_state_store"][KP].set("last_live_snapshot", result["snapshot"])
        return result

    @task
    def observed_alert(result, **context):
        from kp_demo.alerts import monitor
        return monitor(ROOT, snapshot_id=result["snapshot"], notify=context["params"]["notify"])

    observed_alert(ingest())


@dag(dag_id="noaa_kp_reprocess", schedule=None, start_date=START, catchup=False,
     max_active_runs=1, default_args=DEFAULTS, tags=["noaa", "icechunk"], params={
         "start": Param("", type="string"), "end": Param("", type="string"),
         "branch": Param("experiment", type="string"), "bias": Param(0.0, type="number"),
     })
def reprocessing():
    @task(pool="noaa_kp_writer")
    def run(**context):
        from kp_demo.pipeline import create_experiment, reprocess_range
        p = context["params"]
        if not p["start"] or not p["end"]:
            raise ValueError("Supply start and end UTC timestamps when triggering this DAG")
        if p["branch"] != "main":
            create_experiment(ROOT, name=p["branch"])
        # Durable idempotency key in the Icechunk commit closes the commit/checkpoint gap.
        result = reprocess_range(p["start"], p["end"], ROOT, branch=p["branch"], bias=p["bias"],
                                 operation_id=f"airflow:{context['dag_run'].run_id}:reprocess")
        context["task_state_store"].set("committed", result)
        return result

    run()


history_dag = historical()
live_dag = live()
reprocess_dag = reprocessing()
