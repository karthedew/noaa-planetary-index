import numpy as np
import pandas as pd
import pytest

from kp_demo.alerts import transition
from kp_demo.pipeline import _update_derived, empty_dataset
from kp_demo.processing import affected_outputs, compute_derived, kp_to_ap, runs_of_indices, storm_levels
from kp_demo.storage import as_tree


def test_kp_to_ap_known_values():
    np.testing.assert_equal(kp_to_ap([0, 2.33, 5, 8.67, 9, np.nan]), [0, 9, 48, 300, 400, np.nan])


def test_not_arithmetic_mean_of_kp():
    result = compute_derived([0., 0., 0., 0., 0., 0., 0., 9.])
    assert result["ap_24h_mean"][-1] == 50
    assert np.isnan(result["ap_24h_mean"][:7]).all()


def test_missing_input_invalidates_eight_means():
    values = np.ones(24)
    values[10] = np.nan
    result = compute_derived(values)
    assert np.isnan(result["ap_24h_mean"][10:18]).all()
    assert result["storm_level"][10] == -1


def test_thresholds_and_9_minus():
    np.testing.assert_equal(storm_levels([4.67, 5, 6, 7, 8, 8.67, 9, np.nan]), [0, 1, 2, 3, 4, 4, 5, -1])


def test_halo_dependency_and_regions():
    assert affected_outputs([10], 24) == list(range(10, 18))
    assert runs_of_indices([1, 2, 6, 6, 7]) == [(1, 3), (6, 8)]


def test_small_reprocess_does_not_modify_other_outputs():
    ds = empty_dataset(pd.date_range("2016-01-01", periods=30, freq="3h"))
    ds.kp.values[:] = 3
    ds.ap_24h_mean.values[:] = 99
    _update_derived(ds, {"ap_24h_mean": [15, 16]})
    assert ds.ap_24h_mean.values[15] == ds.ap_24h_mean.values[16] == 15
    assert ds.ap_24h_mean.values[14] == ds.ap_24h_mean.values[17] == 99


def test_datatree_groups_without_backend():
    tree = as_tree(empty_dataset(pd.date_range("2016-01-01", periods=8, freq="3h")))
    assert set(tree.children) == {"observations", "derived", "provenance", "demo"}
    assert tree.attrs["schema_version"] == 2
    assert tree["observations/kp"].sizes["time"] == 8


@pytest.mark.parametrize("previous,current,expected", [(None,0,None), (None,1,"storm_observed"),
    (0,1,"storm_started"), (1,3,"intensified"), (3,1,"weakened"), (1,0,"storm_ended"),
    (1,1,None), (1,-1,None)])
def test_alert_state_machine(previous, current, expected):
    assert transition(previous, current) == expected
