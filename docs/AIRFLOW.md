# Optional Airflow 3.3.2 orchestration

Airflow calls the same Python functions used by Marimo and the CLI. No separate
science implementation, catalog, message broker, or Kubernetes setup is needed.

## Local setup

Use Python 3.12. From the repository root:

```bash
uv sync --extra dev --extra airflow
export AIRFLOW_HOME="$PWD/airflow-home"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export KP_DEMO_DATA="$PWD/data"
uv run airflow standalone
```

Keep that process running. Follow the local login instructions it prints. In a
second terminal, export the same variables and create the one-slot writer pool:

```bash
uv run airflow pools set noaa_kp_writer 1 'Serialize NOAA repository writers'
uv run airflow dags list
uv run airflow dags trigger noaa_kp_history \
  --conf '{"start":"2016-01-01","end":"now","strict_coverage":false}'
```

Unpause the history DAG in the UI if needed. Review coverage when it completes,
then unpause `noaa_kp_live` for 15-minute refreshes. Do not start live ingestion
on an empty store before initializing history. Marimo must use the same
`KP_DEMO_DATA`; identical directory names on different machines are not shared
storage. All worker processes must see the same persistent bytes.

## Demonstrate task-state resume

On a fresh root, trigger a history run that intentionally fails after its second
committed monthly batch:

```bash
uv run airflow dags trigger noaa_kp_history \
  --conf '{"start":"2016-01-01","end":"2016-05-01","fail_after_batches":2}'
```

The `load` task saves a cursor, raises once, and resumes at the next batch on its
retry within the same task instance. Repeating existing history is correctly
idempotent, so use a fresh root to demonstrate initial writes.

```json
{
  "checkpoint": {
    "manifest": "hash-of-immutable-input-plan",
    "next_batch": 2,
    "snapshot": "icechunk-snapshot-id"
  },
  "demo_failed": true
}
```

The ordering is validate/transform -> write all DataTree groups -> Icechunk
commit -> save task state. A crash between commit and checkpoint replays the
same frozen batch without creating another unchanged snapshot. The code also
checks that the saved snapshot remains in main's ancestry; a rollback behind
that checkpoint forces replay rather than silently skipping needed work.

Task state is explicit progress, not a Python stack or memory snapshot. New
scheduled task instances have separate state. History checkpoints request
30-day retention. Icechunk snapshot retention is separate; this demo does not
expire snapshots. Asset state records history coverage and last published
snapshot references across runs, but is not the authoritative branch head.

## Reprocess a selected interval

```bash
uv run airflow dags trigger noaa_kp_reprocess --conf \
  '{"start":"2024-05-10T00:00:00","end":"2024-05-10T21:00:00","branch":"experiment","bias":10.0}'
```

The interval is inclusive. A nonzero bias intentionally perturbs the derived
24-hour equivalent-ap mean for demonstration; bias=0 restores the normal
calculation. Source Kp remains unchanged. `experiment` is the default branch;
pass `main` deliberately. A DAG-run idempotency token is saved in the Icechunk
commit, so a retry recognizes publication even if task-state persistence failed.

## Notifications

The live DAG's `notify` parameter defaults to false. To deliver observed-storm
messages, set `KP_ALERT_WEBHOOK_URL` in workers' environments and enable the
parameter. The HTTPS endpoint must accept the JSON payload; this is not an
automatic Slack/email integration. Keep secrets outside Git.

Historical backfills never notify. A 15-minute schedule does not turn a
three-hour observed index into a solar-flare detector or early-warning forecast.

## Verification

```bash
uv run pytest -q tests/test_airflow_dags.py
uv run airflow dags list-import-errors
```

The SDK test checks DAG construction. It does not prove the scheduler,
metadata database, task-state service, or a full network backfill ran. Those
must be exercised in an installed Airflow environment.
