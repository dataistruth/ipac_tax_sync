"""Restart failed generated continuous Lakeflow pipelines.

This script is intended for Databricks Jobs (spark_python_task).
It scans generated pipelines by prefix (default p_) and starts updates for
continuous pipelines whose latest update is in a failed state.
"""

from __future__ import annotations

import argparse
from typing import Any

from databricks.sdk import WorkspaceClient


FAILED_STATES = {"FAILED", "CANCELED", "CANCELLED"}


def _list_generated_pipelines(w: WorkspaceClient, name_prefix: str) -> list[dict[str, Any]]:
    payload = w.api_client.do("GET", "/api/2.0/pipelines")
    statuses = payload.get("statuses", []) if isinstance(payload, dict) else []
    return [p for p in statuses if str(p.get("name", "")).startswith(name_prefix)]


def _is_failed_continuous(detail: dict[str, Any]) -> tuple[bool, str]:
    spec = detail.get("spec", {}) or {}
    if not bool(spec.get("continuous", False)):
        return False, "non-continuous"
    latest_update = (detail.get("state", {}) or {}).get("latest_update", {}) or {}
    update_state = str(latest_update.get("state", "")).upper()
    if update_state in FAILED_STATES:
        return True, f"latest_update.state={update_state}"
    return False, f"latest_update.state={update_state or 'UNKNOWN'}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name-prefix", default="p_")
    parser.add_argument("--restart-limit", type=int, default=25)
    args = parser.parse_args()

    w = WorkspaceClient()
    pipelines = _list_generated_pipelines(w, args.name_prefix)
    restarted = 0

    for p in pipelines:
        if restarted >= args.restart_limit:
            break
        pid = p.get("pipeline_id")
        name = p.get("name", pid)
        if not pid:
            continue
        detail = w.api_client.do("GET", f"/api/2.0/pipelines/{pid}") or {}
        should_restart, reason = _is_failed_continuous(detail)
        print(f"{name}: {reason}")
        if not should_restart:
            continue
        w.api_client.do("POST", f"/api/2.0/pipelines/{pid}/updates", body={})
        restarted += 1
        print(f"Restart requested for {name} ({pid})")

    print(f"Restart requests submitted: {restarted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
