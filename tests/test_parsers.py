import json

import httpx
import pandas as pd
import pytest

from kp_demo.noaa import (ARCHIVE, LIVE, NOAAClient, archive_urls, bounds, normalize_time,
                          observation_identity, parse_dgd, parse_records)

LEGACY = "2016 01 20    12  0 1 3 3 3 3 3 3    -1 -1-1-1-1-1-1-1-1    25  1 2 4 4 4 5 5 4"
MODERN = "2026 10 29    -1  2 3 1 0-1-1-1-1    -1  0 0 0 0-1-1-1-1     6   1.67  2.33  0.67  0.33 -1.00 -1.00 -1.00 -1.00"


def test_legacy_packed_missing_station_columns():
    rows = parse_dgd(LEGACY, origin=ARCHIVE+"2016_DGD.txt")
    assert [r["Kp"] for r in rows] == [1, 2, 4, 4, 4, 5, 5, 4]
    assert all(r["a_running"] is None and r["station_count"] is None for r in rows)
    assert all(r["kp_resolution"] == 1 for r in rows)


def test_decimal_missing_and_precision():
    rows = parse_dgd(MODERN, origin=ARCHIVE+"2026Q4_DGD.txt")
    assert [r["Kp"] for r in rows] == [1.67, 2.33, 0.67, 0.33, None, None, None, None]
    assert all(r["kp_resolution"] == 1/3 for r in rows)
    assert rows[-1]["time_tag"] == "2026-10-29T21:00:00"


def test_mixed_formats_in_one_file():
    assert len(parse_dgd(LEGACY+"\n"+MODERN, origin=ARCHIVE)) == 16


def test_malformed_row_not_silently_ignored():
    with pytest.raises(ValueError, match="columns"):
        parse_dgd("2016 01 01 1 2 3", origin=ARCHIVE)


def test_header_json_and_objects_match(rows):
    keys = ["time_tag", "Kp", "a_running", "station_count"]
    table = [keys, *[[r[k] for k in keys] for r in rows]]
    assert len(parse_records(table)) == len(rows)
    assert parse_records(table)[0]["Kp"] == rows[0]["Kp"]


def test_source_identity_ignores_batch_sha(rows):
    assert observation_identity(rows[0]) == observation_identity({**rows[0], "raw_sha256": "new", "source_line": 2})


def test_duplicates_and_conflicts(rows):
    assert len(parse_records(rows + rows[:1])) == len(rows)
    with pytest.raises(ValueError, match="Contradictory"):
        parse_records([rows[0], {**rows[0], "Kp": 9.}])


@pytest.mark.parametrize("value", [-2, 10])
def test_invalid_kp(rows, value):
    with pytest.raises(ValueError):
        parse_records([{**rows[0], "Kp": value}])


def test_utc_normalization():
    assert normalize_time("2026-10-29T00:00:00-06:00") == "2026-10-29T06:00:00"


def test_only_completed_intervals():
    lo, hi = bounds(now="2026-10-29T14:11:00Z")
    assert lo == pd.Timestamp("2016-01-01")
    assert hi == pd.Timestamp("2026-10-29T12:00:00")


def test_future_end_is_clamped():
    _, hi = bounds(end="2030-01-01", now="2026-10-29T14:11:00Z")
    assert hi == pd.Timestamp("2026-10-29T12:00:00")


def test_annual_preferred_to_overlapping_quarters():
    html = '<a href="2016_DGD.txt"></a><a href="2016Q1_DGD.txt"></a><a href="2017Q1_DGD.txt"></a>'
    urls = archive_urls(html, pd.Timestamp("2016-01-01"), pd.Timestamp("2017-02-01"))
    assert urls == [ARCHIVE+"2016_DGD.txt", ARCHIVE+"2017Q1_DGD.txt"]


def test_cache_reuses_exact_bytes_and_etag(tmp_path):
    calls = []
    def handle(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(200, content=b'[]', headers={"ETag": '"one"'})
        assert request.headers["if-none-match"] == '"one"'
        return httpx.Response(304)
    with NOAAClient(tmp_path, transport=httpx.MockTransport(handle)) as client:
        first, m1 = client.fetch(LIVE)
        second, m2 = client.fetch(LIVE)
        assert first == second == b'[]'
        assert m1["sha256"] == m2["sha256"]
        assert client.fetch(LIVE, refresh=False)[0] == first
    assert len(calls) == 2


def test_raw_payload_is_recoverable(tmp_path, rows):
    def handle(request):
        return httpx.Response(200, json=rows)
    with NOAAClient(tmp_path, transport=httpx.MockTransport(handle)) as client:
        parsed = client.live()
        raw = client.raw(parsed[0]["raw_sha256"])
    assert json.loads(raw)[0]["Kp"] == rows[0]["Kp"]
