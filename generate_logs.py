"""
Synthetic data generator for the Application Incident & Log Analytics Pipeline.

Produces three feeds per batch (each batch = 1 hour of app activity):
  logs/         app_logs_<batch>.jsonl       -> semi-structured JSON lines
  incidents/    incidents_<batch>.csv        -> incident status updates (open -> investigating -> resolved)
  deployments/  deployments_<batch>.csv      -> releases, hotfixes, rollbacks

Data is intentionally messy so Bronze/Silver layers have real work to do:
  - ~2% duplicate events (same event_id)
  - ~0.5% malformed / truncated JSON lines
  - mixed timestamp formats (ISO-8601 Z, epoch millis, 'YYYY-MM-DD HH:MM:SS')
  - inconsistent level casing ('ERROR', 'error', 'warning', 'WARN')
  - ~3% late-arriving events (timestamped in the previous hour)
  - null user_id / latency_ms
  - schema drift: from batch 4 onward a new 'region' field appears

Error spikes are tied to "bad" deployments, and each spike opens an incident
that is resolved 1-2 batches later (with a rollback deployment) - so the Gold
layer can correlate deployments -> error spikes -> incidents -> MTTR.

Usage (local):
    python generate_logs.py --output-dir ./data --batches 6

Usage (Databricks notebook): copy this file into the workspace, then
    from generate_logs import generate
    generate("/Volumes/<catalog>/<schema>/raw", num_batches=6)

Re-running continues from where it left off (state is kept in _state.json),
which lets you simulate new data arriving for incremental loads.
"""

import argparse
import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

SERVICES = {
    "auth-service":         ["/login", "/logout", "/token/refresh", "/mfa/verify"],
    "payment-service":      ["/pay", "/refund", "/payment/status"],
    "order-service":        ["/orders", "/orders/{id}", "/cart/checkout"],
    "inventory-service":    ["/stock", "/stock/reserve", "/stock/release"],
    "notification-service": ["/notify/email", "/notify/sms", "/notify/push"],
}
TEAMS = {
    "auth-service": "identity-team", "payment-service": "payments-team",
    "order-service": "commerce-team", "inventory-service": "commerce-team",
    "notification-service": "platform-team",
}
ERRORS = [
    ("DB_TIMEOUT", "Database connection timed out after 30000ms"),
    ("NULL_POINTER", "NullPointerException in request handler"),
    ("UPSTREAM_503", "Upstream dependency returned 503 Service Unavailable"),
    ("AUTH_FAILED", "Token validation failed: signature mismatch"),
    ("POOL_EXHAUSTED", "Connection pool exhausted (max=50)"),
    ("SERIALIZATION", "Failed to deserialize request payload"),
]
WARNINGS = [
    "Slow query detected (>2000ms)",
    "Retrying request to upstream (attempt 2/3)",
    "Cache miss rate above threshold",
]
REGIONS = ["ap-south-1", "us-east-1", "eu-west-1"]
DEPLOYERS = ["ci-bot", "priya.k", "arun.m", "devops-pipeline"]

BASE_ERROR_RATE = 0.01
SPIKE_ERROR_RATE = 0.15
EVENTS_PER_SERVICE = 400


def _fmt_ts(ts: datetime) -> str:
    """Return the timestamp in one of three formats (mostly ISO)."""
    r = random.random()
    if r < 0.85:
        return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"
    if r < 0.93:
        return str(int(ts.timestamp() * 1000))  # epoch millis as string
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def _level(base: str) -> str:
    variants = {"ERROR": ["ERROR", "ERROR", "ERROR", "error"],
                "WARN": ["WARN", "WARN", "warning"],
                "INFO": ["INFO", "INFO", "INFO", "info"]}
    return random.choice(variants[base])


def _load_state(output_dir):
    path = os.path.join(output_dir, "_state.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    start = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(days=1)
    return {
        "next_batch": 1,
        "window_start": start.isoformat(),
        "versions": {s: "1.0.0" for s in SERVICES},
        "open_incidents": [],   # list of dicts
        "incident_seq": 1000,
        "deploy_seq": 5000,
    }


def _save_state(output_dir, state):
    with open(os.path.join(output_dir, "_state.json"), "w") as f:
        json.dump(state, f, indent=2)


def _bump(version: str, kind: str) -> str:
    major, minor, patch = (int(x) for x in version.split("."))
    return f"{major}.{minor}.{patch + 1}" if kind == "hotfix" else f"{major}.{minor + 1}.0"


def _write_batch(output_dir, state):
    b = state["next_batch"]
    w_start = datetime.fromisoformat(state["window_start"])
    w_end = w_start + timedelta(hours=1)
    tag = w_start.strftime("%Y%m%d_%H%M")

    deployments, incident_rows = [], []
    spiking = {}  # service -> (error_code, deployment_id)

    # 1) Resolve incidents from earlier batches (with a rollback deployment)
    still_open = []
    for inc in state["open_incidents"]:
        inc["age"] += 1
        if inc["age"] >= random.choice([1, 2]):
            svc = inc["service"]
            state["deploy_seq"] += 1
            rb_time = w_start + timedelta(minutes=random.randint(5, 25))
            deployments.append({
                "deployment_id": f"DEP-{state['deploy_seq']}", "service": svc,
                "version": inc["previous_version"], "deployed_at": rb_time.isoformat(),
                "deployed_by": random.choice(DEPLOYERS), "change_type": "rollback",
            })
            state["versions"][svc] = inc["previous_version"]
            resolved = rb_time + timedelta(minutes=random.randint(5, 20))
            incident_rows.append({**inc["row"], "status": "RESOLVED",
                                  "updated_at": resolved.isoformat(),
                                  "resolved_at": resolved.isoformat()})
        else:
            still_open.append(inc)
            # still bleeding errors while open
            spiking[inc["service"]] = (inc["error_code"], inc["row"]["related_deployment_id"])
    state["open_incidents"] = still_open

    # 2) New deployments this hour; some are bad and cause a spike + incident
    for svc in SERVICES:
        if svc in spiking or random.random() > 0.35:
            continue
        kind = random.choice(["release", "release", "hotfix"])
        prev = state["versions"][svc]
        new = _bump(prev, kind)
        state["deploy_seq"] += 1
        dep_id = f"DEP-{state['deploy_seq']}"
        dep_time = w_start + timedelta(minutes=random.randint(0, 20))
        deployments.append({
            "deployment_id": dep_id, "service": svc, "version": new,
            "deployed_at": dep_time.isoformat(), "deployed_by": random.choice(DEPLOYERS),
            "change_type": kind,
        })
        state["versions"][svc] = new

        if random.random() < 0.30:  # bad deployment
            code, msg = random.choice(ERRORS)
            spiking[svc] = (code, dep_id)
            state["incident_seq"] += 1
            opened = dep_time + timedelta(minutes=random.randint(8, 25))
            row = {
                "incident_id": f"INC-{state['incident_seq']}", "service": svc,
                "severity": random.choice(["SEV1", "SEV2", "SEV2", "SEV3"]),
                "status": "OPEN", "title": f"Elevated {code} errors on {svc}",
                "opened_at": opened.isoformat(), "updated_at": opened.isoformat(),
                "resolved_at": "", "assigned_team": TEAMS[svc],
                "related_deployment_id": dep_id,
            }
            incident_rows.append(dict(row))
            investigating = opened + timedelta(minutes=random.randint(3, 15))
            incident_rows.append({**row, "status": "INVESTIGATING",
                                  "updated_at": investigating.isoformat()})
            state["open_incidents"].append({
                "service": svc, "error_code": code, "previous_version": prev,
                "age": 0, "row": row,
            })

    # 3) Log events
    lines = []
    for svc, endpoints in SERVICES.items():
        err_rate = SPIKE_ERROR_RATE if svc in spiking else BASE_ERROR_RATE
        for _ in range(EVENTS_PER_SERVICE):
            ts = w_start + timedelta(seconds=random.uniform(0, 3600))
            if random.random() < 0.03:  # late-arriving
                ts -= timedelta(hours=1)
            r = random.random()
            event = {
                "event_id": str(uuid.uuid4()),
                "timestamp": _fmt_ts(ts),
                "service": svc,
                "host": f"{svc.split('-')[0]}-pod-{random.randint(1, 4)}",
                "endpoint": random.choice(endpoints),
                "trace_id": uuid.uuid4().hex[:16],
                "user_id": None if random.random() < 0.05 else f"U{random.randint(10000, 99999)}",
                "app_version": state["versions"][svc],
            }
            if r < err_rate:
                code, msg = (spiking[svc][0], dict(ERRORS)[spiking[svc][0]]) if svc in spiking \
                    else random.choice(ERRORS)
                event.update(level=_level("ERROR"), status_code=random.choice([500, 502, 503, 504]),
                             latency_ms=random.randint(3000, 30000), error_code=code, message=msg)
            elif r < err_rate + 0.05:
                event.update(level=_level("WARN"), status_code=200,
                             latency_ms=random.randint(1500, 4000), message=random.choice(WARNINGS))
            else:
                event.update(level=_level("INFO"), status_code=random.choice([200, 200, 200, 201, 204]),
                             latency_ms=random.randint(20, 600), message="Request completed")
            if random.random() < 0.02:
                event["latency_ms"] = None
            if b >= 4:  # schema drift
                event["region"] = random.choice(REGIONS)
            lines.append(json.dumps(event))
            if random.random() < 0.02:  # duplicate delivery
                lines.append(json.dumps(event))

    for i in random.sample(range(len(lines)), k=max(1, len(lines) // 200)):  # malformed lines
        lines[i] = lines[i][: random.randint(10, len(lines[i]) - 5)]
    random.shuffle(lines)

    # 4) Write files
    for sub in ("logs", "incidents", "deployments"):
        os.makedirs(os.path.join(output_dir, sub), exist_ok=True)
    with open(os.path.join(output_dir, "logs", f"app_logs_{tag}.jsonl"), "w") as f:
        f.write("\n".join(lines) + "\n")
    inc_cols = ["incident_id", "service", "severity", "status", "title", "opened_at",
                "updated_at", "resolved_at", "assigned_team", "related_deployment_id"]
    with open(os.path.join(output_dir, "incidents", f"incidents_{tag}.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=inc_cols)
        w.writeheader()
        w.writerows(incident_rows)
    dep_cols = ["deployment_id", "service", "version", "deployed_at", "deployed_by", "change_type"]
    with open(os.path.join(output_dir, "deployments", f"deployments_{tag}.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=dep_cols)
        w.writeheader()
        w.writerows(deployments)

    state["next_batch"] = b + 1
    state["window_start"] = w_end.isoformat()
    return tag, len(lines), len(incident_rows), len(deployments)


def generate(output_dir: str, num_batches: int = 6, seed: int | None = None):
    if seed is not None:
        random.seed(seed)
    os.makedirs(output_dir, exist_ok=True)
    state = _load_state(output_dir)
    for _ in range(num_batches):
        tag, n_logs, n_inc, n_dep = _write_batch(output_dir, state)
        print(f"batch {tag}: {n_logs} log lines, {n_inc} incident updates, {n_dep} deployments")
    _save_state(output_dir, state)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", default="./data")
    p.add_argument("--batches", type=int, default=6)
    p.add_argument("--seed", type=int, default=None)
    a = p.parse_args()
    generate(a.output_dir, a.batches, a.seed)
