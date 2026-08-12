"""Restart failed generated continuous Lakeflow pipelines.

This script is intended for Databricks Jobs (spark_python_task).
It scans generated pipelines by prefix (default p_) and starts updates for
continuous pipelines whose latest update is in a failed state.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
from typing import Any


def _load_databricks_rest_client():
    """Load sibling module; spark_python_task does not add script dir to sys.path."""
    module_path = Path(__file__).resolve().with_name("databricks_rest.py")
    if not module_path.is_file():
        raise FileNotFoundError(
            f"Required module not deployed: {module_path}. "
            "Redeploy the bundle so src/common/ops/databricks_rest.py is synced."
        )
    spec = importlib.util.spec_from_file_location("databricks_rest", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module spec from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.DatabricksRestClient


DatabricksRestClient = _load_databricks_rest_client()


FAILED_STATES = {"FAILED", "CANCELED", "CANCELLED"}


def _list_generated_pipelines(client: DatabricksRestClient, name_prefix: str) -> list[dict[str, Any]]:
    payload = client.get("/api/2.0/pipelines")
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
    parser.add_argument("--pipeline-names-file", default="")
    args = parser.parse_args()

    client = DatabricksRestClient()
    configured_names = set(_load_pipeline_names(args.pipeline_names_file))
    pipelines = _list_generated_pipelines(client, args.name_prefix)
    if configured_names:
        pipelines = [p for p in pipelines if str(p.get("name", "")) in configured_names]
    restarted = 0

    for p in pipelines:
        if restarted >= args.restart_limit:
            break
        pid = p.get("pipeline_id")
        name = p.get("name", pid)
        if not pid:
            continue
        detail = client.get(f"/api/2.0/pipelines/{pid}") or {}
        should_restart, reason = _is_failed_continuous(detail)
        print(f"{name}: {reason}")
        if not should_restart:
            continue
        client.post(f"/api/2.0/pipelines/{pid}/updates")
        restarted += 1
        print(f"Restart requested for {name} ({pid})")

    print(f"Restart requests submitted: {restarted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
