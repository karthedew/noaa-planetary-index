# NOAA planetary index: Xarray DataTree + Icechunk + Marimo

A standalone scientific-data demo with historical NOAA Kp ingestion from
**2016-01-01 through the latest completed three-hour interval**. Numerical data
is stored as an Xarray DataTree backed by Zarr v3 and Icechunk. Marimo is the
interactive interface; reusable logic stays in Python modules. Airflow 3.3.2
is an optional orchestration layer, not a requirement for the notebook.

## Run the demo

Python 3.12 and [uv](https://docs.astral.sh/uv/) are recommended.

```bash
git clone git@github.com:karthedew/noaa-planetary-index.git
cd noaa-planetary-index
uv sync --extra dev
uv run kp-ingest history --start 2016-01-01
uv run marimo edit demo.py
```

Alternatively, extract the ZIP, install dependencies, open Marimo, and click
**Download and ingest history**. The notebook defaults to 2016-01-01 and `now`.

**This repository includes the downloader, not a preloaded ten-year database.**
NOAA data is downloaded when you run the command or notebook action. `now` is
resolved at runtime. An explicit end is exclusive: `--end 2025-01-01` includes
observations through December 31, 2024. The current, unfinished three-hour
interval is excluded.

## Historical ingestion

The loader discovers annual/quarterly DGD files in NOAA's archive, preferring
annual files where both exist. It reads the planetary columns, handles legacy
integer values, modern decimal thirds, and packed missing markers, then
supplements the recent end with the 30-day DGD and live JSON feeds.

Exact response bytes and SHA-256 checksums are retained. Immutable monthly
batches make retries reproducible. Each changed batch is published in one
Icechunk transaction; only then is progress checkpointed. An unchanged replay
creates no additional snapshot. Closed archive files are cached; use
`--refresh-archives` to recheck them.

Missing intervals remain explicit NaNs, not interpolated values or zeros.
The loader reports coverage and missing timestamps. Add `--strict-coverage`
to fail validation when observations are missing; previously committed batches
remain available. NOAA archive publication lag can leave gaps near the present.

Source precedence is live JSON > recent DGD > archive DGD. A lower-priority
source cannot replace a previously captured valid live measurement. This is
an explicit demo policy, not a claim that live measurements are definitive.

Historical integer precision is recorded in `kp_resolution`. Missing historical
`a_running` and `station_count` are stored as -1 (unknown); the historical daily
planetary A column is **not** mislabeled as the live `a_running` field.

## DataTree and Python access

```text
/                         schema version and UTC metadata
├── observations          kp, a_running, station_count, kp_resolution, source_priority
├── derived               ap_equivalent, ap_24h_mean, storm_level
├── provenance            source_id, ap_run, mean_run, storm_run
└── demo                  demo_marker_a, demo_marker_b
```

Each group contains a Dataset with a UTC `time` coordinate. Chunks contain 32
observations (four days), independently of monthly ingestion batches. All group
updates in an operation share one Icechunk session and commit atomically.

```python
from kp_demo.storage import read_tree, read_dataset, repository

snapshot = repository("data").lookup_branch("main")
tree = read_tree("data", snapshot_id=snapshot)
kp = tree["observations"].to_dataset()["kp"]
selected = kp.sel(time=slice("2024-05-10", "2024-05-13"))
ds = read_dataset("data", snapshot_id=snapshot)  # merged Dataset convenience view
```

Initialization uses `DataTree.to_zarr`; appends and region writes use
`Dataset.to_zarr` against groups in the same session. Array I/O stays in Xarray.
These are synchronous, NumPy-backed local writes, not distributed Dask writes.
Updating part of a physical chunk can rewrite that chunk, not just one element.

## What Marimo demonstrates

History/live ingestion, DataTree inspection, time series and daily heatmaps,
snapshot comparisons and chunk diffs, partial reprocessing, value-level lineage,
experimental branches, consistent readers, conflicting writers, and rollback.

The bias buttons intentionally alter selected derived outputs; correction
recomputes those outputs using only their input range plus a seven-observation
halo. Source Kp is unchanged. An experimental branch leaves main untouched.
The concurrency demonstration uses a separate scratch branch. Rollback saves
the previous head on a safety branch before moving main.

Derived `ap_equivalent` uses the standard Kp-to-ap table; `ap_24h_mean` averages
eight complete intervals of equivalent ap, not logarithmic Kp. These are derived
estimates, not definitive GFZ ap or calendar-day Ap. Historical integer Kp also
limits derived precision. `storm_level` uses local thresholds, not official
NOAA-issued alerts. Missing inputs invalidate dependent rolling windows.

The field-line viewer is explicitly a **conceptual illustration**, not a
physical simulation or reconstruction of Earth's magnetic field from Kp.

## Live updates and alerts

```bash
uv run kp-ingest ingest
uv run kp-ingest ingest --watch --interval-seconds 900
```

Overlapping recent observations are checked for revisions as well as new
values. Revised inputs update their own outputs and the next seven dependent
rolling means. Refresh the notebook view after an external process commits.

Live ingestion records local observed-storm transitions in an alert outbox.
Stale or missing observations do not trigger an all-clear. History imports
never send notifications. Webhook delivery is opt-in:

```bash
export KP_ALERT_WEBHOOK_URL='https://your-service.example/space-weather'
uv run kp-ingest ingest --watch --notify
```

Use a real HTTPS endpoint accepting the documented JSON fields: event ID,
timestamp, Kp, level, source ID, and snapshot. Deduplicate `event_id` or the
`Idempotency-Key` header: delivery is at-least-once, not exactly-once. Do not
commit credentials. This monitors observed geomagnetic activity, not solar
flares or forecasts. It is not an operational safety warning service.

## Optional Airflow

See [docs/AIRFLOW.md](docs/AIRFLOW.md) for setup and a deliberate failure/retry
walkthrough. The same processing functions are used by three DAGs:

| DAG | Purpose |
| --- | --- |
| `noaa_kp_history` | Manual historical import, monthly commits, task-state checkpoints |
| `noaa_kp_live` | Every 15 minutes, incremental updates and optional alerts |
| `noaa_kp_reprocess` | Manual selected-range reprocessing on an experimental branch |

Airflow task state stores progress and snapshot references. Icechunk snapshots
store the scientific data. This is explicit checkpointing, not a snapshot of
Python memory. Asset state stores cross-run orchestration metadata; Icechunk
remains authoritative for branch heads.

## Provenance and persistence

Values and their producing-run IDs are versioned in the same Icechunk commit.
Immutable sidecars retain raw responses, exact contributing source revisions,
processing parameters, full package source text, source-code digests, runtime
versions, and a Git SHA/dirty status when available. A SHA is not invented when
running outside a Git checkout. NOAA's internal processing code is outside this
lineage.

**Back up the whole data root**, not only `data/icechunk/`: detailed lineage
requires sidecars. Failed transactions can leave unreferenced sidecars; the
demo does not garbage-collect them or expire snapshots.

Normal writers use a shared file lock; Icechunk also detects commit conflicts.
All workers and Marimo must access the same persistent directory. This is a
local/shared-filesystem demo, not a validated multi-host service.

The earlier flat v0.1 data store is not migrated or overwritten. Use a fresh
root for this schema-v2 DataTree, and ingest history before polling live:

```bash
uv run kp-ingest --data-dir ./data-v2 history
KP_DEMO_DATA=./data-v2 uv run marimo edit demo.py
```

A recent-only fixture store cannot be prepended in place. Use a different root
for historical initialization rather than shifting all array positions.

## Tests and offline fixture

```bash
uv run pytest -q
uv run python scripts/smoke.py
uv run marimo check demo.py
uv run marimo export html demo.py -o /tmp/noaa-demo.html

uv run kp-ingest --data-dir ./data-fixture seed --count 40
uv run kp-ingest --data-dir ./data-fixture seed
KP_DEMO_DATA=./data-fixture uv run marimo edit demo.py
```

Integration tests use real temporary Icechunk repositories. The Airflow test
uses its real SDK to construct the DAGs. Missing runtime dependencies cause
explicit collection skips; CI installs them for the relevant jobs.

At packaging, 33 offline tests passed and Python compilation passed. Local
Icechunk/Zarr integration, Marimo execution, Airflow execution, and the complete
network backfill could not be run because the build environment could not
install missing dependencies or download NOAA data. See GitHub Actions for
additional runtime verification; a workflow being present is not a passing run.

`uv sync` creates a local lockfile. This source distribution does not include
a fully verified dependency lock; exact installed versions are recorded with
processing runs.

## Project structure

```text
demo.py                 presentation and interaction
src/kp_demo/            fetching, processing, storage, provenance, plots, CLI
dags/noaa_kp.py          optional Airflow orchestration
scripts/                CLI wrappers and whole-demo smoke test
tests/                  offline, backend-integration, and Airflow SDK tests
docs/AIRFLOW.md         scheduler setup and checkpoint demonstration
```

## Primary references

- [NOAA archive](https://www.ngdc.noaa.gov/stp/space-weather/swpc-products/annual_reports/daily_solar_indices_summaries/daily_geomagnetic_data/)
- [NOAA recent DGD](https://services.swpc.noaa.gov/text/daily-geomagnetic-indices.txt)
- [NOAA live Kp](https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json)
- [Kp/ap definitions: Matzka et al. 2021](https://doi.org/10.1029/2020SW002641)
- [Xarray DataTree](https://docs.xarray.dev/en/stable/generated/xarray.open_datatree.html)
- [Icechunk and Xarray](https://icechunk.io/en/stable/guides/xarray/)
- [Airflow 3.3.2 task state](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/task-state-store.html)
