"""Run with marimo edit demo.py; reusable implementation lives in kp_demo."""
import marimo

__generated_with = "0.18.0"
app = marimo.App(width="full")


@app.cell
def _():
    import json
    import marimo as mo
    import plotly.graph_objects as go
    from kp_demo import history as hist, pipeline, storage, plots
    from kp_demo.alerts import alert_log, monitor
    from kp_demo.concurrency import run_concurrency_demo
    from kp_demo.provenance import trace_value
    return alert_log, go, hist, json, mo, monitor, pipeline, plots, run_concurrency_demo, storage, trace_value


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # NOAA planetary Kp · versioned scientific arrays
    **2016 → latest complete interval · Xarray DataTree · Icechunk + Zarr v3**

    NOAA Kp is a global three-hour geomagnetic activity index, not a local
    magnetic-field measurement. Older archive values have integer precision.
    Missing data stays missing. Derived threshold alerts are not official NOAA
    warnings; the conceptual field viewer is not a physical simulation.
    """)
    return


@app.cell
def _(mo):
    get_epoch, set_epoch = mo.state(0)
    get_action, set_action = mo.state("No action yet.")
    return get_action, get_epoch, set_action, set_epoch


@app.cell
def _(get_epoch, storage):
    _tick = get_epoch()
    ready = storage.dataset_exists()
    latest = storage.read_dataset() if ready else None
    versions = storage.list_history() if ready else []
    return latest, ready, versions


@app.cell(hide_code=True)
def _(mo):
    mo.md("## 1 · Download history and keep it current")
    return


@app.cell
def _(mo):
    history_start = mo.ui.text(value="2016-01-01", label="Start UTC")
    history_end = mo.ui.text(value="now", label="Exclusive end UTC (or now)")
    history_button = mo.ui.run_button(label="Download and ingest history")
    live_button = mo.ui.run_button(label="Fetch recent NOAA updates")
    refresh_button = mo.ui.run_button(label="Refresh view")
    mo.vstack([mo.hstack([history_start, history_end]),
               mo.hstack([history_button, live_button, refresh_button], wrap=True)])
    return history_button, history_end, history_start, live_button, refresh_button


@app.cell
def _(hist, history_button, history_end, history_start, json, live_button, monitor,
      refresh_button, set_action, set_epoch):
    if history_button.value or live_button.value or refresh_button.value:
        try:
            if history_button.value:
                _prepared = hist.prepare_history(start=history_start.value, end=history_end.value)
                _result = hist.run_manifest(None, _prepared["manifest"])
            elif live_button.value:
                _result = hist.sync_latest()
                _result["alert"] = monitor(snapshot_id=_result["snapshot"], notify=False)
            else:
                _result = {"refreshed": True}
            set_action(json.dumps(_result, indent=2, default=str))
            set_epoch(lambda _n: _n+1)
        except Exception as _exc:
            set_action(f"{type(_exc).__name__}: {_exc}")
    return


@app.cell(hide_code=True)
def _(get_action, latest, mo, ready, storage, versions):
    _summary = (f"**{latest.sizes['time']:,} time positions · {len(versions)} snapshots**"
                if ready else "Initialize with the history button, or run `uv run kp-ingest history`.")
    _tree = str(storage.read_tree()) if ready else "No DataTree yet"
    mo.vstack([mo.md(_summary), mo.md(f"```text\n{_tree}\n```"),
               mo.accordion({"Latest operation": mo.md(f"```json\n{get_action()}\n```"),
                 "Automation": mo.md("Run `kp-ingest ingest --watch`, or use the included Airflow 3.3 DAGs. History import does not send alerts.")})])
    return


@app.cell
def _(mo):
    days = mo.ui.slider(start=7, stop=3650, step=1, value=90, label="Display trailing days")
    days
    return days,


@app.cell(hide_code=True)
def _(days, latest, mo, plots):
    _view = (mo.vstack([mo.ui.plotly(plots.timeseries(latest, days=days.value)),
                        mo.ui.plotly(plots.heatmap(latest, days=min(days.value, 366)))])
             if latest is not None else mo.md("Plots appear after ingestion."))
    _view
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("## 2 · Snapshot comparison and historical reads")
    return


@app.cell
def _(mo, versions):
    _options = {f"{v['id'][:12]} | {v['message'][:60]}": v['id'] for v in versions}
    _keys = list(_options)
    old_snapshot = mo.ui.dropdown(options=_options, value=_keys[1] if len(_keys)>1 else (_keys[0] if _keys else None), label="Before", disabled=not _keys)
    new_snapshot = mo.ui.dropdown(options=_options, value=_keys[0] if _keys else None, label="After", disabled=not _keys)
    compare_variable = mo.ui.dropdown(options=["kp", "ap_equivalent", "ap_24h_mean", "storm_level"], value="ap_24h_mean", label="Variable")
    mo.hstack([old_snapshot, new_snapshot, compare_variable], wrap=True)
    return compare_variable, new_snapshot, old_snapshot


@app.cell(hide_code=True)
def _(compare_variable, go, mo, new_snapshot, old_snapshot, storage):
    if old_snapshot.value and new_snapshot.value:
        _table, _detail = storage.compare_snapshots(old_snapshot.value, new_snapshot.value, compare_variable.value)
        _figure = go.Figure()
        _figure.add_trace(go.Scattergl(x=_table.time, y=_table.before, name="Before"))
        _figure.add_trace(go.Scattergl(x=_table.time, y=_table.after, name="After"))
        _figure.update_layout(height=330, xaxis_title="UTC")
        _body = mo.vstack([mo.ui.plotly(_figure), mo.json(_detail), mo.ui.table(_table[_table.changed].head(100))])
    else:
        _body = mo.md("Create snapshots to compare their values and changed chunk coordinates.")
    _body
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("## 3 · Selective reprocessing and isolated experiments")
    return


@app.cell
def _(latest, mo):
    _times = latest.time.values if latest is not None else []
    reprocess_start = mo.ui.text(value=str(_times[max(0, len(_times)-16)]) if len(_times) else "", label="Start, inclusive")
    reprocess_end = mo.ui.text(value=str(_times[max(0, len(_times)-9)]) if len(_times) else "", label="End, inclusive")
    damage_button = mo.ui.run_button(label="Introduce +10 ap-mean bias", disabled=latest is None)
    repair_button = mo.ui.run_button(label="Correct selected outputs", disabled=latest is None)
    experiment_button = mo.ui.run_button(label="Try +5 bias on experiment branch", disabled=latest is None)
    mo.vstack([mo.hstack([reprocess_start, reprocess_end]),
               mo.hstack([damage_button, repair_button, experiment_button], wrap=True)])
    return damage_button, experiment_button, repair_button, reprocess_end, reprocess_start


@app.cell
def _(damage_button, experiment_button, json, pipeline, repair_button, reprocess_end,
      reprocess_start, set_action, set_epoch):
    if damage_button.value or repair_button.value or experiment_button.value:
        try:
            _branch = "experiment" if experiment_button.value else "main"
            if experiment_button.value:
                pipeline.create_experiment()
            _bias = 5. if experiment_button.value else (10. if damage_button.value else 0.)
            _result = pipeline.reprocess_range(reprocess_start.value, reprocess_end.value, branch=_branch, bias=_bias)
            set_action(json.dumps(_result, indent=2))
            set_epoch(lambda _n: _n+1)
        except Exception as _exc:
            set_action(f"{type(_exc).__name__}: {_exc}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""Only selected outputs and a seven-input rolling-window halo are read for reprocessing.
    The 24-hour mean uses **equivalent ap**, not an arithmetic mean of logarithmic Kp.
    It is a derived estimate, not definitive GFZ ap. Source Kp is never changed by these buttons.

    ## 4 · Trace a value to its inputs and processing code
    """)
    return


@app.cell
def _(latest, mo):
    trace_time = mo.ui.text(value=str(latest.time.values[-1]) if latest is not None else "", label="UTC timestamp")
    trace_variable = mo.ui.dropdown(options=["ap_24h_mean", "ap_equivalent", "storm_level", "kp"], value="ap_24h_mean", label="Value to trace (After snapshot)")
    mo.hstack([trace_time, trace_variable])
    return trace_time, trace_variable


@app.cell(hide_code=True)
def _(mo, new_snapshot, storage, trace_time, trace_value, trace_variable):
    _view = mo.md("Choose an existing timestamp and snapshot.")
    if new_snapshot.value and trace_time.value:
        try:
            _ds = storage.read_dataset(snapshot_id=new_snapshot.value)
            _info = trace_value(_ds, variable=trace_variable.value, time=trace_time.value)
            _code = _info.pop("code", None)
            _view = mo.vstack([mo.json(_info), mo.accordion({"Archived processing code": mo.json(_code or {})})])
        except (KeyError, ValueError) as _exc:
            _view = mo.md(str(_exc))
    _view
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("## 5 · Consistent readers, conflicting writers, and rollback")
    return


@app.cell
def _(mo, ready):
    concurrency_button = mo.ui.run_button(label="Run concurrent-session demonstration", disabled=not ready)
    rollback_button = mo.ui.run_button(label="Restore main to Before snapshot (safety branch retained)", disabled=not ready)
    mo.hstack([concurrency_button, rollback_button], wrap=True)
    return concurrency_button, rollback_button


@app.cell
def _(concurrency_button, json, old_snapshot, rollback_button, run_concurrency_demo, set_action, set_epoch, storage):
    if concurrency_button.value or rollback_button.value:
        try:
            _result = (run_concurrency_demo() if concurrency_button.value else storage.rollback(old_snapshot.value))
            set_action(json.dumps(_result, indent=2))
            set_epoch(lambda _n: _n+1)
        except Exception as _exc:
            set_action(f"{type(_exc).__name__}: {_exc}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""## 6 · Observed storm alerts and a conceptual disturbance viewer
    The Kp threshold log updates when live ingestion runs. Webhook delivery is
    **off by default**. Kp cannot reconstruct Earth's actual magnetic field or
    predict a local spacecraft's radiation exposure. The slider below controls
    an artistic diagram only, not a magnetohydrodynamic simulation.
    """)
    return


@app.cell
def _(mo):
    illustrative_kp = mo.ui.slider(start=0., stop=9., step=1., value=3., label="Illustrative Kp")
    illustrative_kp
    return illustrative_kp,


@app.cell(hide_code=True)
def _(alert_log, get_epoch, illustrative_kp, mo, plots):
    _tick = get_epoch()
    mo.vstack([mo.ui.plotly(plots.conceptual_field(illustrative_kp.value)),
               mo.accordion({"Local alert log (no historical notifications)": mo.json(alert_log())})])
    return


if __name__ == "__main__":
    app.run()
