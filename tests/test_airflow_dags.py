import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("airflow")
pytestmark = pytest.mark.airflow


def test_dags_construct_with_airflow_33_sdk():
    path = Path(__file__).parents[1] / "dags" / "noaa_kp.py"
    spec = importlib.util.spec_from_file_location("noaa_kp_dags", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.history_dag.dag_id == "noaa_kp_history"
    assert set(module.history_dag.task_ids) == {"prepare", "load", "verify"}
    assert module.live_dag.max_active_runs == 1
    assert module.history_dag.get_task("load").pool == "noaa_kp_writer"
