"""Monitor heartbeat for generated continuous Lakeflow pipelines.

This script is intended for Databricks Jobs (spark_python_task).
It scans pipelines with the configured name prefix (default: p_) and checks:
1) pipeline is configured as continuous
2) latest update state is healthy
3) latest update heartbeat age is within heartbeat_interval_sec

If unhealthy pipelines are found, the script raises RuntimeError so job failure
notifications can deliver email alerts.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient


FAILED_STATES = {"FAILED", "CANCELED", "CANCELLED"}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_get(d: dict[str, Any], *keys: str):
    cur: Any = d
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _list_generated_pipelines(w: WorkspaceClient, name_prefix: str) -> list[dict[str, Any]]:
    payload = w.api_client.do("GET", "/api/2.0/pipelines")
    statuses = payload.get("statuses", []) if isinstance(payload, dict) else []
    return [p for p in statuses if str(p.get("name", "")).startswith(name_prefix)]


def _load_pipeline_names(path: str | None) -> list[str]:
    if not path:
        return []
    file = Path(path)
    if not file.exists():
        raise FileNotFoundError(f"pipeline names file not found: {path}")
    payload = json.loads(file.read_text(encoding="utf-8"))
    names = payload.get("pipelines", []) if isinstance(payload, dict) else []
    return [str(n) for n in names if str(n).strip()]


def _pipeline_health(
    w: WorkspaceClient, pipeline_id: str, heartbeat_interval_sec: int
) -> tuple[bool, str]:
    detail = w.api_client.do("GET", f"/api/2.0/pipelines/{pipeline_id}") or {}
    spec = detail.get("spec", {})
    continuous = bool(spec.get("continuous", False))
    if not continuous:
        return True, "non-continuous"

    state = detail.get("state", {}) or {}
    latest_update = state.get("latest_update", {}) or {}

    update_state = str(latest_update.get("state", "")).upper()
    if update_state in FAILED_STATES:
        return False, f"latest_update.state={update_state}"

    # Databricks APIs can expose timestamps under different keys depending on version.
    ts_candidates = [
        latest_update.get("creation_time"),
        latest_update.get("start_time"),
        latest_update.get("update_start_time"),
        latest_update.get("timestamp"),
        state.get("last_updated"),
    ]
    ts_values = [int(v) for v in ts_candidates if isinstance(v, (int, float))]
    if not ts_values:
        return False, "no heartbeat timestamp on latest_update/state"

    last_ms = max(ts_values)
    age_sec = (_now_ms() - last_ms) / 1000
    if age_sec > heartbeat_interval_sec:
        return False, f"heartbeat stale: {int(age_sec)}s > {heartbeat_interval_sec}s"

    return True, f"healthy heartbeat age={int(age_sec)}s"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name-prefix", default="p_")
    parser.add_argument("--heartbeat-interval-sec", type=int, default=900)
    parser.add_argument("--pipeline-names-file", default="")
    args = parser.parse_args()

    w = WorkspaceClient()
    configured_names = set(_load_pipeline_names(args.pipeline_names_file))
    pipelines = _list_generated_pipelines(w, args.name_prefix)
    if configured_names:
        pipelines = [p for p in pipelines if str(p.get("name", "")) in configured_names]
        print(
            f"Found {len(pipelines)} generated pipeline(s) from configured list "
            f"({len(configured_names)} names)"
        )
    else:
        print(f"Found {len(pipelines)} generated pipeline(s) with prefix '{args.name_prefix}'")

    unhealthy: list[str] = []
    for p in pipelines:
        pid = p.get("pipeline_id")
        name = p.get("name", pid)
        if not pid:
            continue
        ok, reason = _pipeline_health(w, pid, args.heartbeat_interval_sec)
        print(f"{name}: {reason}")
        if not ok:
            unhealthy.append(f"{name} ({pid}) -> {reason}")

    if unhealthy:
        joined = "\n".join(unhealthy)
        raise RuntimeError(f"Unhealthy continuous pipeline(s):\n{joined}")

    print("All monitored continuous pipelines are healthy.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
