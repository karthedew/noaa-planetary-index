"""Local observed-Kp alerts, not official NOAA warnings or solar-flare forecasts.

History imports never notify. Delivery is opt-in; webhook consumers must dedupe
by event_id/Idempotency-Key (delivery is at-least-once, not exactly-once).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import httpx
import numpy as np
import pandas as pd

from .filesystem import canonical, data_home, digest, write_json, writer_lock
from .noaa import normalize_time


def transition(previous, current):
    if current < 0:
        return None
    if previous is None:
        return "storm_observed" if current > 0 else None
    if previous == current:
        return None
    if previous == 0 and current > 0:
        return "storm_started"
    if previous > 0 and current == 0:
        return "storm_ended"
    return "intensified" if current > previous else "weakened"


def monitor(root=None, *, snapshot_id=None, notify=False, now=None):
    from .storage import read_dataset, repository
    snapshot_id = snapshot_id or repository(root).lookup_branch("main")
    ds = read_dataset(root, snapshot_id=snapshot_id)
    timestamp = pd.Timestamp(ds.time.values[-1])
    clock = pd.Timestamp(normalize_time(now or datetime.now(timezone.utc)))
    level = int(ds.storm_level.values[-1])
    if level < 0 or not np.isfinite(ds.kp.values[-1]) or clock - timestamp > pd.Timedelta(hours=6):
        return {"status": "missing_or_stale", "notified": False, "time": normalize_time(timestamp)}
    home = data_home(root) / "alerts"
    event = None
    with writer_lock(root):
        state_path = home / "state.json"
        state = json.loads(state_path.read_text()) if state_path.exists() else {}
        kind = transition(state.get("level"), level)
        # Never replay a rollback as a new current observation.
        if state.get("time") and normalize_time(timestamp) < state["time"]:
            return {"status": "older_snapshot_ignored", "notified": False}
        if kind:
            event = {"type": kind, "time": normalize_time(timestamp), "level": level,
                     "kp": float(ds.kp.values[-1]), "snapshot": snapshot_id,
                     "source_id": int(ds.source_id.values[-1]),
                     "notice": "Local observed-Kp threshold; not an official NOAA alert"}
            event_id = digest(canonical({"time": event["time"], "level": level}))
            event["event_id"] = event_id
            path = home / "outbox" / f"{event_id}.json"
            if not path.exists():
                write_json(path, {"event": event, "delivered": False})
        write_json(state_path, {"time": normalize_time(timestamp), "level": level})
    delivered = 0
    if notify:
        webhook = os.environ.get("KP_ALERT_WEBHOOK_URL")
        if not webhook or not webhook.startswith("https://"):
            raise ValueError("Set KP_ALERT_WEBHOOK_URL to an HTTPS webhook before enabling delivery")
        for path in sorted((home / "outbox").glob("*.json")):
            item = json.loads(path.read_text())
            if item["delivered"]:
                continue
            if clock - pd.Timestamp(item["event"]["time"]) > pd.Timedelta(hours=6):
                continue  # do not send stale backlogged storm messages
            response = httpx.post(webhook, json=item["event"], timeout=15,
                                  headers={"Idempotency-Key": item["event"]["event_id"]})
            response.raise_for_status()
            item["delivered"] = True
            write_json(path, item)
            delivered += 1
    return {"status": "current", "event": event, "delivered": delivered, "delivery_enabled": notify}


def alert_log(root=None):
    return [json.loads(path.read_text()) for path in sorted((data_home(root)/"alerts"/"outbox").glob("*.json"))]
