"""NOAA-only adapters: annual/quarterly DGD archives and the rolling live feed.

DGD's daily planetary A is NOT the live feed's a_running. It is deliberately
not copied into that field. Missing historical station counts remain missing.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import httpx
import numpy as np
import pandas as pd

from .filesystem import atomic_write, data_home, digest, write_json

ARCHIVE = ("https://www.ngdc.noaa.gov/stp/space-weather/swpc-products/annual_reports/"
           "daily_solar_indices_summaries/daily_geomagnetic_data/")
RECENT = "https://services.swpc.noaa.gov/text/daily-geomagnetic-indices.txt"
LIVE = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
URL = LIVE
VALUE_FIELDS = ("Kp", "a_running", "station_count", "kp_resolution", "source_priority")


def normalize_time(value) -> str:
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        raise ValueError("Missing timestamp")
    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    return ts.tz_localize(None).isoformat(timespec="seconds")


def bounds(start="2016-01-01", end="now", *, now=None):
    clock = pd.Timestamp(now or datetime.now(timezone.utc))
    clock = pd.Timestamp(normalize_time(clock))
    lo = pd.Timestamp(normalize_time(start))
    hi = clock if end == "now" else pd.Timestamp(normalize_time(end))
    hi = min(hi, clock).floor("3h")
    if lo != lo.floor("3h") or lo >= hi:
        raise ValueError("Start must be a 3-hour UTC boundary before the exclusive end")
    return lo, hi


def _optional(value, *, integer=False):
    if value is None or value == "":
        return None
    number = float(value)
    if not np.isfinite(number) or number == -1:
        return None
    if number < 0 or (integer and not number.is_integer()):
        raise ValueError(f"Invalid nonnegative value: {value}")
    return int(number) if integer else number


def parse_records(payload, *, origin=LIVE, raw_sha256=None) -> list[dict]:
    if not isinstance(payload, list) or not payload:
        raise ValueError("NOAA response must be a nonempty list")
    if isinstance(payload[0], list):
        payload = [dict(zip(payload[0], row, strict=True)) for row in payload[1:]]
    result = {}
    for row in payload:
        key = normalize_time(row["time_tag"])
        ts = pd.Timestamp(key)
        if ts != ts.floor("3h"):
            raise ValueError(f"Not a three-hour boundary: {key}")
        kp = _optional(row.get("Kp"))
        if kp is not None and kp > 9:
            raise ValueError(f"Kp out of range: {kp}")
        record = {
            "time_tag": key, "Kp": kp,
            "a_running": _optional(row.get("a_running"), integer=True),
            "station_count": _optional(row.get("station_count"), integer=True),
            "kp_resolution": float(row.get("kp_resolution", 1 / 3)),
            "source_priority": int(row.get("source_priority", 3)),
            "source_url": row.get("source_url", origin),
            "raw_sha256": row.get("raw_sha256") or raw_sha256,
        }
        if "source_line" in row:
            record["source_line"] = int(row["source_line"])
        if record["kp_resolution"] not in (1., 1 / 3):
            raise ValueError("Unsupported Kp resolution")
        if record["source_priority"] not in (1, 2, 3):
            raise ValueError("Invalid source priority")
        if key in result and observation_identity(result[key]) != observation_identity(record):
            raise ValueError(f"Contradictory records for {key}")
        result[key] = record
    return [result[key] for key in sorted(result)]


def observation_identity(row: dict) -> dict:
    # Do not create a new observation revision merely because NOAA's rolling
    # response changed elsewhere or a row moved to another line in that response.
    return {key: row.get(key) for key in ("time_tag", *VALUE_FIELDS, "source_url")}


def parse_dgd(text: str, *, origin: str, raw_sha256=None, priority=1) -> list[dict]:
    rows = []
    for line_number, line in enumerate(text.splitlines(), 1):
        if not re.match(r"^\s*\d{4}\s+\d{2}\s+\d{2}\s", line):
            continue
        # Regex, not str.split: legacy missing flags can be packed as -1-1-1.
        tokens = re.findall(r"-?\d+(?:\.\d+)?", line)
        if len(tokens) != 30:  # date + 3 stations/groups * (A + eight K values)
            raise ValueError(f"Unexpected DGD columns at {origin}:{line_number}")
        day = pd.Timestamp(year=int(tokens[0]), month=int(tokens[1]), day=int(tokens[2]))
        for block, token in enumerate(tokens[22:30]):
            value = _optional(token)
            if value is not None and value > 9:
                raise ValueError(f"Invalid planetary Kp at {origin}:{line_number}")
            rows.append({
                "time_tag": (day + pd.Timedelta(hours=block * 3)).isoformat(),
                "Kp": value, "a_running": None, "station_count": None,
                "kp_resolution": 1 / 3 if "." in token else 1.,
                "source_priority": priority, "source_url": origin,
                "raw_sha256": raw_sha256, "source_line": line_number,
            })
    if not rows:
        raise ValueError(f"No DGD data rows in {origin}")
    return parse_records(rows, origin=origin, raw_sha256=raw_sha256)


class _Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.links.extend(value for key, value in attrs if key == "href" and value)


def archive_urls(index_html: str, start, end) -> list[str]:
    """Prefer annual files, otherwise available quarters; never guess a URL."""
    parser = _Links()
    parser.feed(index_html)
    found = {}
    for href in parser.links:
        name = href.rsplit("/", 1)[-1]
        match = re.fullmatch(r"(\d{4})(?:Q([1-4]))?_DGD\.txt", name)
        if match:
            found[(int(match[1]), int(match[2]) if match[2] else 0)] = urljoin(ARCHIVE, name)
    chosen = []
    for year in range(start.year, end.year + 1):
        if (year, 0) in found:
            chosen.append(found[(year, 0)])
        else:
            for quarter in range(1, 5):
                lo = pd.Timestamp(year=year, month=3 * quarter - 2, day=1)
                hi = lo + pd.DateOffset(months=3)
                if lo < end and hi > start and (year, quarter) in found:
                    chosen.append(found[(year, quarter)])
    if not chosen:
        raise ValueError("No NOAA archive files match the requested range")
    return chosen


class NOAAClient:
    """Small retrying downloader; caches exact response bytes with SHA-256."""
    def __init__(self, root=None, *, transport=None):
        self.root = root
        self.http = httpx.Client(
            timeout=60, follow_redirects=True,
            transport=transport or httpx.HTTPTransport(retries=3),
            headers={"User-Agent": "noaa-planetary-index/0.2 (+scientific-demo)"},
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.http.close()

    def fetch(self, url: str, *, refresh=True):
        if urlparse(url).hostname not in ("www.ngdc.noaa.gov", "services.swpc.noaa.gov"):
            raise ValueError("Only the configured NOAA hosts are permitted")
        cache_path = data_home(self.root) / "http-cache" / f"{digest(url.encode())}.json"
        cached = json.loads(cache_path.read_text()) if cache_path.exists() else None
        if cached and not refresh:
            raw = self.raw(cached["sha256"])
            return raw, cached
        headers = {}
        if cached:
            if cached.get("etag"):
                headers["If-None-Match"] = cached["etag"]
            if cached.get("last_modified"):
                headers["If-Modified-Since"] = cached["last_modified"]
        response = self.http.get(url, headers=headers)
        if response.status_code == 304 and cached:
            return self.raw(cached["sha256"]), cached
        response.raise_for_status()
        raw = response.content
        sha = digest(raw)
        atomic_write(data_home(self.root) / "raw" / sha, raw)
        metadata = {
            "url": url, "sha256": sha,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "etag": response.headers.get("etag"),
            "last_modified": response.headers.get("last-modified"),
        }
        write_json(cache_path, metadata)
        write_json(data_home(self.root) / "raw" / f"{sha}.json", metadata)
        return raw, metadata

    def raw(self, sha):
        raw = (data_home(self.root) / "raw" / sha).read_bytes()
        if digest(raw) != sha:
            raise ValueError("Cached NOAA response failed checksum verification")
        return raw

    def live(self):
        raw, metadata = self.fetch(LIVE)
        return parse_records(json.loads(raw), raw_sha256=metadata["sha256"])


def fetch_live(root=None):
    with NOAAClient(root) as client:
        return client.live()
