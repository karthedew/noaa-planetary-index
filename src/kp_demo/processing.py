"""Small explicit transforms; Kp is not a linear physical field amplitude."""
from __future__ import annotations

import numpy as np
import pandas as pd

WINDOW = 8
# Standard Kp thirds -> equivalent ap, conventionally in units of 2 nT.
# GFZ Matzka et al. (2021), doi:10.1029/2020SW002641, Table 3.
AP = np.array([0, 2, 3, 4, 5, 6, 7, 9, 12, 15, 18, 22, 27, 32,
               39, 48, 56, 67, 80, 94, 111, 132, 154, 179, 207, 236, 300, 400],
              dtype=float)


def kp_to_ap(kp):
    """Derived equivalent ap, not NOAA daily A or definitive GFZ ap.

    Older DGD integers have lost the +/- thirds; do not pretend the conversion
    restores that precision. kp_resolution is retained with each observation.
    """
    values = np.asarray(kp, dtype=float)
    out = np.full(values.shape, np.nan)
    valid = np.isfinite(values)
    if np.any((values[valid] < 0) | (values[valid] > 9)):
        raise ValueError("Kp must be in [0, 9]")
    thirds = np.rint(values[valid] * 3).astype(int)
    if np.any(np.abs(values[valid] - thirds / 3) > 0.011):
        raise ValueError("Kp must be an integer or a third (allowing decimal rounding)")
    out[valid] = AP[thirds]
    return out


def storm_levels(kp):
    """Local integer thresholds: 0=no storm, 1..5=G-like levels, -1=missing.

    These derived categories are not NOAA-issued alerts or forecasts.
    In particular 8.67 (9-) remains level 4, not level 5.
    """
    values = np.asarray(kp, dtype=float)
    out = np.full(values.shape, -1, dtype="int8")
    valid = np.isfinite(values)
    out[valid] = np.clip(np.floor(values[valid]).astype(int) - 4, 0, 5)
    return out


def compute_derived(kp, *, bias=0.0):
    ap = kp_to_ap(kp)
    if not np.isfinite(bias):
        raise ValueError("Bias must be finite")
    mean = pd.Series(ap).rolling(WINDOW, min_periods=WINDOW).mean().to_numpy() + bias
    return {"ap_equivalent": ap, "ap_24h_mean": mean, "storm_level": storm_levels(kp)}


def affected_outputs(changed, n, *, window=WINDOW):
    return sorted({j for i in changed for j in range(i, min(n, i + window))})


def runs_of_indices(indices):
    values = sorted(set(int(i) for i in indices))
    if not values:
        return []
    result = []
    start = previous = values[0]
    for i in values[1:]:
        if i != previous + 1:
            result.append((start, previous + 1))
            start = i
        previous = i
    return [*result, (start, previous + 1)]
