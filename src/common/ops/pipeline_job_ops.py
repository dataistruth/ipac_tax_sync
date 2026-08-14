"""Self-contained pipeline monitor/restart ops for Databricks serverless jobs.

spark_python_task runs scripts via exec() without __file__ or sys.path setup,
so this module keeps all logic in one file (stdlib REST only, no pip deps).
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


FAILED_STATES = {"FAILED", "CANCELED", "CANCELLED"}
_dbutils: Any | None = None


def configure_dbutils(db: Any) -> None:
    """Inject notebook dbutils (avoids SparkSession in serverless notebooks)."""
    global _dbutils
    _dbutils = db


def _auth_from_dbutils() -> tuple[str, str]:
    db = _dbutils
    if db is not None:
        try:
            ctx = db.notebook.entry_point.getDbutils().notebook().getContext()
            host = str(ctx.apiUrl().get()).rstrip("/")
            token = str(ctx.apiToken().get())
            if host and token:
                return host, token
        except Exception:
            pass
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        host = str(ctx.apiUrl().get()).rstrip("/")
        token = str(ctx.apiToken().get())
        return host, token
    except Exception:
        return "", ""


class DatabricksRestClient:
    def __init__(self, host: str | None = None, token: str | None = None) -> None:
        self.host = (host or os.environ.get("DATABRICKS_HOST") or "").rstrip("/")
        self.token = token or os.environ.get("DATABRICKS_TOKEN")
        if not self.host or not self.token:
            host, token = _auth_from_dbutils()
            self.host = host or self.host
            self.token = token or self.token
        if not self.host or not self.token:
            raise RuntimeError(
                "Databricks credentials not found. Set DATABRICKS_HOST and DATABRICKS_TOKEN "
                "or run inside a Databricks job/cluster."
            )

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not path.startswith("/"):
            path = f"/{path}"
        url = f"{self.host}{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Databricks API {method} {path} failed ({exc.code}): {err_body}"
            ) from exc

    def get(self, path: str) -> dict[str, Any]:
        return self.request("GET", path)

    def post(self, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.request("POST", path, body=body or {})


def _now_ms() -> int:
    return int(time.time() * 1000)


def _strip_bundle_dev_prefix(name: str) -> str:
    """Remove bundle development-mode prefix, e.g. '[dev user] p_client_1' -> 'p_client_1'."""
    text = str(name).strip()
    if text.startswith("[") and "] " in text:
        return text.split("] ", 1)[1].strip()
    return text


def _list_generated_pipelines(client: DatabricksRestClient, name_prefix: str) -> list[dict[str, Any]]:
    payload = client.get("/api/2.0/pipelines")
    statuses = payload.get("statuses", []) if isinstance(payload, dict) else []
    prefix = name_prefix.casefold()
    matched: list[dict[str, Any]] = []
    for pipeline in statuses:
        logical_name = _strip_bundle_dev_prefix(str(pipeline.get("name", "")))
        if logical_name.casefold().startswith(prefix):
            matched.append(pipeline)
    return matched


def _load_pipeline_names(path: str | None) -> list[str]:
    if not path:
        return []
    file = Path(path)
    if not file.exists():
        raise FileNotFoundError(f"pipeline names file not found: {path}")
    payload = json.loads(file.read_text(encoding="utf-8"))
    names = payload.get("pipelines", []) if isinstance(payload, dict) else []
    return [str(n) for n in names if str(n).strip()]


def _filter_pipelines(
    pipelines: list[dict[str, Any]], configured_names: set[str]
) -> list[dict[str, Any]]:
    if not configured_names:
        return pipelines
    configured = {name.casefold() for name in configured_names}
    return [
        p
        for p in pipelines
        if _strip_bundle_dev_prefix(str(p.get("name", "")).casefold()) in configured
    ]


def _pipeline_health(
    client: DatabricksRestClient, pipeline_id: str, heartbeat_interval_sec: int
) -> tuple[bool, str]:
    detail = client.get(f"/api/2.0/pipelines/{pipeline_id}") or {}
    spec = detail.get("spec", {})
    continuous = bool(spec.get("continuous", False))
    if not continuous:
        return True, "non-continuous"

    state = detail.get("state", {}) or {}
    latest_update = state.get("latest_update", {}) or {}

    update_state = str(latest_update.get("state", "")).upper()
    if update_state in FAILED_STATES:
        return False, f"latest_update.state={update_state}"

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


def _is_failed_continuous(detail: dict[str, Any]) -> tuple[bool, str]:
    spec = detail.get("spec", {}) or {}
    if not bool(spec.get("continuous", False)):
        return False, "non-continuous"
    latest_update = (detail.get("state", {}) or {}).get("latest_update", {}) or {}
    update_state = str(latest_update.get("state", "")).upper()
    if update_state in FAILED_STATES:
        return True, f"latest_update.state={update_state}"
    return False, f"latest_update.state={update_state or 'UNKNOWN'}"


def run_monitor(
    name_prefix: str,
    heartbeat_interval_sec: int,
    pipeline_names_file: str,
) -> int:
    client = DatabricksRestClient()
    configured_names = set(_load_pipeline_names(pipeline_names_file))
    pipelines = _list_generated_pipelines(client, name_prefix)
    pipelines = _filter_pipelines(pipelines, configured_names)

    if configured_names:
        print(
            f"Found {len(pipelines)} generated pipeline(s) from configured list "
            f"({len(configured_names)} names)"
        )
        if not pipelines and configured_names:
            sample = _list_generated_pipelines(client, name_prefix)
            logical = [
                _strip_bundle_dev_prefix(str(p.get("name", ""))) for p in sample[:10]
            ]
            print(
                "No API pipelines matched pipeline_names.json after dev-prefix normalization. "
                f"Prefix '{name_prefix}' logical names (sample): {logical}"
            )
    else:
        print(f"Found {len(pipelines)} generated pipeline(s) with prefix '{name_prefix}'")

    unhealthy: list[str] = []
    for p in pipelines:
        pid = p.get("pipeline_id")
        name = p.get("name", pid)
        if not pid:
            continue
        ok, reason = _pipeline_health(client, pid, heartbeat_interval_sec)
        print(f"{name}: {reason}")
        if not ok:
            unhealthy.append(f"{name} ({pid}) -> {reason}")

    if unhealthy:
        joined = "\n".join(unhealthy)
        raise RuntimeError(f"Unhealthy continuous pipeline(s):\n{joined}")

    print("All monitored continuous pipelines are healthy.")
    return 0


def run_restart(
    name_prefix: str,
    restart_limit: int,
    pipeline_names_file: str,
) -> int:
    client = DatabricksRestClient()
    configured_names = set(_load_pipeline_names(pipeline_names_file))
    pipelines = _list_generated_pipelines(client, name_prefix)
    pipelines = _filter_pipelines(pipelines, configured_names)
    restarted = 0

    for p in pipelines:
        if restarted >= restart_limit:
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline heartbeat monitor and restart ops")
    parser.add_argument(
        "command",
        choices=["monitor", "restart"],
        help="monitor: check pipeline health; restart: restart failed continuous pipelines",
    )
    parser.add_argument("--name-prefix", default="p_")
    parser.add_argument("--heartbeat-interval-sec", type=int, default=900)
    parser.add_argument("--restart-limit", type=int, default=25)
    parser.add_argument("--pipeline-names-file", default="")
    args = parser.parse_args()

    if args.command == "monitor":
        return run_monitor(args.name_prefix, args.heartbeat_interval_sec, args.pipeline_names_file)
    return run_restart(args.name_prefix, args.restart_limit, args.pipeline_names_file)


if __name__ == "__main__":
    raise SystemExit(main())
