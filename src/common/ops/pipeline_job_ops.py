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
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from common.ops.pipeline_names import load_pipeline_names, normalize_pipeline_key


FAILED_STATES = {"FAILED", "CANCELED", "CANCELLED"}
ACTIVE_UPDATE_STATES = {
    "RUNNING",
    "INITIALIZING",
    "QUEUED",
    "SETTING_UP",
    "WAITING_FOR_RESOURCES",
    "RESTARTING",
}
_dbutils: Any | None = None


@dataclass
class PipelinePollSnapshot:
    """One pipeline status poll for monitoring and process_log."""

    display_name: str
    pipeline_id: str
    logical_name: str
    healthy: bool
    reason: str
    update_state: str
    continuous: bool
    heartbeat_age_sec: int | None
    heartbeat_threshold_sec: int
    start_tm_ms: int | None
    end_tm_ms: int | None
    poll_iteration: int = 0
    monitor_run_id: str = ""
    update_id: str = ""


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
    if text.startswith("[") and "]" in text:
        return text.split("]", 1)[1].strip()
    return text


def _logical_pipeline_name(raw_name: str) -> str:
    """Normalized pipeline name for matching (strip dev prefix, then casefold)."""
    return _strip_bundle_dev_prefix(str(raw_name)).casefold()


def _list_all_pipeline_statuses(
    client: DatabricksRestClient,
    filter_expr: str | None = None,
) -> list[dict[str, Any]]:
    """List pipelines with pagination (API default page size is 25)."""
    statuses: list[dict[str, Any]] = []
    page_token: str | None = None
    while True:
        query: dict[str, str] = {"max_results": "100"}
        if filter_expr:
            query["filter"] = filter_expr
        if page_token:
            query["page_token"] = page_token
        path = "/api/2.0/pipelines?" + urllib.parse.urlencode(query)
        payload = client.get(path)
        batch = payload.get("statuses", []) if isinstance(payload, dict) else []
        if isinstance(batch, list):
            statuses.extend(batch)
        page_token = payload.get("next_page_token") if isinstance(payload, dict) else None
        if not page_token:
            break
    return statuses


def _list_generated_pipelines(client: DatabricksRestClient, name_prefix: str) -> list[dict[str, Any]]:
    prefix = name_prefix.casefold()
    matched: list[dict[str, Any]] = []
    for pipeline in _list_all_pipeline_statuses(client):
        if _logical_pipeline_name(str(pipeline.get("name", ""))).startswith(prefix):
            matched.append(pipeline)
    return matched


def _load_pipeline_names(path: str | None) -> list[str]:
    return load_pipeline_names(path)


def _filter_pipelines(
    pipelines: list[dict[str, Any]], configured_names: set[str]
) -> list[dict[str, Any]]:
    if not configured_names:
        return pipelines
    configured = {
        _logical_pipeline_name(normalize_pipeline_key(name)) for name in configured_names
    }
    return [
        p
        for p in pipelines
        if _logical_pipeline_name(str(p.get("name", ""))) in configured
    ]


def _select_pipelines_for_ops(
    client: DatabricksRestClient,
    name_prefix: str,
    configured_names: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (selected pipelines, all prefix-matched pipelines for diagnostics)."""
    prefix_matches = _list_generated_pipelines(client, name_prefix)
    if not configured_names:
        return prefix_matches, prefix_matches
    return _filter_pipelines(prefix_matches, configured_names), prefix_matches


def _pipeline_match_failure_message(
    configured_names: set[str],
    prefix_matches: list[dict[str, Any]],
    name_prefix: str,
) -> str:
    configured = sorted(configured_names)
    available_logical = sorted(
        {_logical_pipeline_name(str(p.get("name", ""))) for p in prefix_matches}
    )
    raw_names = sorted({str(p.get("name", "")) for p in prefix_matches})
    return (
        f"No API pipelines matched pipeline_names.json ({len(configured)} configured name(s)). "
        f"Configured: {configured}. "
        f"Prefix '{name_prefix}' logical names ({len(available_logical)}): {available_logical}. "
        f"Raw API names (sample): {raw_names[:10]}. "
        "Regenerate with `ipac-delta-sync generate` and redeploy the bundle so "
        "generated/config/pipeline_names.json matches deployed pipeline names."
    )


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _pipeline_spec(detail: dict[str, Any]) -> dict[str, Any]:
    return _as_dict(detail.get("spec"))


def _pipeline_state_block(detail: dict[str, Any]) -> dict[str, Any]:
    """State object from GET pipeline; top-level state may be a string on some pipeline types."""
    state = detail.get("state")
    if isinstance(state, dict):
        return state
    return {}


def _update_timestamp_ms(update: dict[str, Any]) -> int:
    candidates = [
        update.get("end_time"),
        update.get("start_time"),
        update.get("update_start_time"),
        update.get("creation_time"),
        update.get("timestamp"),
    ]
    values = [int(v) for v in candidates if isinstance(v, (int, float))]
    return max(values) if values else 0


def _collect_update_blocks(detail: dict[str, Any]) -> list[dict[str, Any]]:
    """Gather update dicts from all API fields that may carry pipeline run state."""
    blocks: list[dict[str, Any]] = []
    state = _pipeline_state_block(detail)
    for candidate in (state.get("latest_update"), detail.get("latest_update")):
        if isinstance(candidate, dict):
            blocks.append(candidate)
    for updates in (state.get("latest_updates"), detail.get("latest_updates")):
        if isinstance(updates, list):
            for item in updates:
                if isinstance(item, dict):
                    blocks.append(item)
    return blocks


def _dedupe_updates(updates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for update in updates:
        update_id = str(update.get("update_id") or "").strip()
        key = update_id if update_id else f"__idx_{id(update)}"
        existing = by_key.get(key)
        if existing is None or _update_timestamp_ms(update) >= _update_timestamp_ms(existing):
            by_key[key] = update
    return list(by_key.values())


def _pick_relevant_update(updates: list[dict[str, Any]]) -> dict[str, Any]:
    """Prefer the newest in-progress update over older canceled/failed runs."""
    if not updates:
        return {}
    active = [
        u
        for u in updates
        if str(u.get("state", "")).upper() in ACTIVE_UPDATE_STATES
    ]
    if active:
        return max(active, key=_update_timestamp_ms)
    return max(updates, key=_update_timestamp_ms)


def _latest_update_block(detail: dict[str, Any]) -> dict[str, Any]:
    candidates = _dedupe_updates(_collect_update_blocks(detail))
    return _pick_relevant_update(candidates)


def _pipeline_state_label(detail: dict[str, Any]) -> str:
    state = detail.get("state")
    if isinstance(state, str):
        return state.strip().upper()
    block = _pipeline_state_block(detail)
    return str(block.get("state") or block.get("status") or "").strip().upper()


def _latest_update_timestamps_ms(detail: dict[str, Any]) -> list[int]:
    state = _pipeline_state_block(detail)
    latest_update = _latest_update_block(detail)
    ts_candidates = [
        latest_update.get("creation_time"),
        latest_update.get("start_time"),
        latest_update.get("update_start_time"),
        latest_update.get("timestamp"),
        state.get("last_updated"),
    ]
    return [int(v) for v in ts_candidates if isinstance(v, (int, float))]


def describe_pipeline_status(detail: dict[str, Any]) -> str:
    """Human-readable pipeline poll snapshot from GET /api/2.0/pipelines/{id}."""
    if not isinstance(detail, dict):
        return f"invalid detail type={type(detail).__name__}"
    spec = _pipeline_spec(detail)
    state = _pipeline_state_block(detail)
    latest_update = _latest_update_block(detail)
    update_state = str(latest_update.get("state", "")).upper() or "NONE"
    parts = [
        f"continuous={bool(spec.get('continuous', False))}",
        f"update_state={update_state}",
    ]
    if latest_update.get("update_id"):
        parts.append(f"update_id={latest_update['update_id']}")
    pipeline_state = _pipeline_state_label(detail)
    if pipeline_state:
        parts.append(f"pipeline_state={pipeline_state}")
    ts_values = _latest_update_timestamps_ms(detail)
    if ts_values:
        age_sec = int((_now_ms() - max(ts_values)) / 1000)
        parts.append(f"heartbeat_age_sec={age_sec}")
    return " | ".join(parts)


def _evaluate_pipeline_health(
    detail: dict[str, Any], heartbeat_interval_sec: int
) -> tuple[bool, str, int | None, str, bool]:
    """Return healthy, reason, heartbeat_age_sec, update_state, continuous."""
    if not isinstance(detail, dict):
        return False, f"UNHEALTHY | invalid pipeline detail type={type(detail).__name__}", None, "NONE", False
    status_line = describe_pipeline_status(detail)
    spec = _pipeline_spec(detail)
    continuous = bool(spec.get("continuous", False))
    latest_update = _latest_update_block(detail)
    update_state = str(latest_update.get("state", "")).upper() or "NONE"

    if not continuous:
        return True, f"non-continuous | {status_line}", None, update_state, continuous

    if update_state in FAILED_STATES:
        return False, f"UNHEALTHY | {status_line}", None, update_state, continuous

    ts_values = _latest_update_timestamps_ms(detail)
    if not ts_values:
        if update_state in ACTIVE_UPDATE_STATES:
            return True, f"healthy (active, no ts) | {status_line}", None, update_state, continuous
        return False, f"UNHEALTHY | no heartbeat timestamp | {status_line}", None, update_state, continuous

    last_ms = max(ts_values)
    age_sec = int((_now_ms() - last_ms) / 1000)
    if age_sec > heartbeat_interval_sec:
        return (
            False,
            f"UNHEALTHY | stale heartbeat {age_sec}s > {heartbeat_interval_sec}s | {status_line}",
            age_sec,
            update_state,
            continuous,
        )

    return (
        True,
        f"healthy | heartbeat_age={age_sec}s | {status_line}",
        age_sec,
        update_state,
        continuous,
    )


def _update_times_ms(detail: dict[str, Any]) -> tuple[int | None, int | None, str]:
    latest_update = _latest_update_block(detail)
    start_candidates = [
        latest_update.get("start_time"),
        latest_update.get("update_start_time"),
        latest_update.get("creation_time"),
    ]
    start_ms = next((int(v) for v in start_candidates if isinstance(v, (int, float))), None)
    end_raw = latest_update.get("end_time")
    end_ms = int(end_raw) if isinstance(end_raw, (int, float)) else None
    update_id = str(latest_update.get("update_id") or "").strip()
    return start_ms, end_ms, update_id


def poll_monitored_pipelines(
    name_prefix: str,
    heartbeat_interval_sec: int,
    pipeline_names_file: str,
    poll_iteration: int = 0,
    monitor_run_id: str = "",
) -> list[PipelinePollSnapshot]:
    client = DatabricksRestClient()
    configured_names = set(_load_pipeline_names(pipeline_names_file))
    pipelines, _ = _select_pipelines_for_ops(client, name_prefix, configured_names)

    snapshots: list[PipelinePollSnapshot] = []
    for p in pipelines:
        pid = p.get("pipeline_id")
        display_name = str(p.get("name", pid or ""))
        if not pid:
            continue
        detail = client.get(f"/api/2.0/pipelines/{pid}") or {}
        logical = _strip_bundle_dev_prefix(display_name)
        ok, reason, age_sec, update_state, continuous = _evaluate_pipeline_health(
            detail, heartbeat_interval_sec
        )
        start_ms, end_ms, update_id = _update_times_ms(detail)
        snapshots.append(
            PipelinePollSnapshot(
                display_name=display_name,
                pipeline_id=str(pid),
                logical_name=logical,
                healthy=ok,
                reason=reason,
                update_state=update_state,
                continuous=continuous,
                heartbeat_age_sec=age_sec,
                heartbeat_threshold_sec=heartbeat_interval_sec,
                start_tm_ms=start_ms,
                end_tm_ms=end_ms,
                poll_iteration=poll_iteration,
                monitor_run_id=monitor_run_id,
                update_id=update_id,
            )
        )
    return snapshots


def _pipeline_health(
    client: DatabricksRestClient, pipeline_id: str, heartbeat_interval_sec: int
) -> tuple[bool, str]:
    detail = client.get(f"/api/2.0/pipelines/{pipeline_id}") or {}
    ok, reason, _, _, _ = _evaluate_pipeline_health(detail, heartbeat_interval_sec)
    return ok, reason


def _is_failed_continuous(detail: dict[str, Any]) -> tuple[bool, str]:
    spec = _pipeline_spec(detail)
    if not bool(spec.get("continuous", False)):
        return False, "non-continuous"
    latest_update = _latest_update_block(detail)
    update_state = str(latest_update.get("state", "")).upper()
    if update_state in FAILED_STATES:
        return True, f"latest_update.state={update_state}"
    return False, f"latest_update.state={update_state or 'UNKNOWN'}"


def run_monitor(
    name_prefix: str,
    heartbeat_interval_sec: int,
    pipeline_names_file: str,
    poll_iteration: int = 0,
    monitor_run_id: str = "",
) -> tuple[int, list[PipelinePollSnapshot]]:
    snapshots = poll_monitored_pipelines(
        name_prefix,
        heartbeat_interval_sec,
        pipeline_names_file,
        poll_iteration=poll_iteration,
        monitor_run_id=monitor_run_id,
    )

    configured_names = set(_load_pipeline_names(pipeline_names_file))
    if snapshots:
        print(f"Polled {len(snapshots)} pipeline(s)")
    elif configured_names:
        client = DatabricksRestClient()
        _, prefix_matches = _select_pipelines_for_ops(client, name_prefix, configured_names)
        message = _pipeline_match_failure_message(configured_names, prefix_matches, name_prefix)
        print(message)
        raise RuntimeError(message)
    else:
        print(f"Found 0 generated pipeline(s) with prefix '{name_prefix}'")
        raise RuntimeError(
            f"No pipelines found with prefix '{name_prefix}'. "
            "Deploy generated pipelines or adjust name_prefix."
        )

    unhealthy: list[str] = []
    for snap in snapshots:
        print(f"{snap.display_name}: {snap.reason}")
        if not snap.healthy:
            unhealthy.append(f"{snap.display_name} ({snap.pipeline_id}) -> {snap.reason}")

    if unhealthy:
        joined = "\n".join(unhealthy)
        raise RuntimeError(f"Unhealthy continuous pipeline(s):\n{joined}")

    print("All monitored continuous pipelines are healthy.")
    return 0, snapshots


def run_monitor_loop(
    name_prefix: str,
    heartbeat_interval_sec: int,
    pipeline_names_file: str,
    poll_interval_sec: int | None = None,
    max_iterations: int | None = None,
    monitor_run_id: str = "",
    on_poll: Callable[[list[PipelinePollSnapshot]], None] | None = None,
) -> int:
    """Poll pipeline status every poll_interval_sec until unhealthy or max_iterations."""
    interval = poll_interval_sec if poll_interval_sec is not None else heartbeat_interval_sec
    if interval <= 0:
        raise ValueError("poll_interval_sec must be positive")

    iteration = 0
    print(
        f"Starting pipeline status poll loop: interval={interval}s, "
        f"stale_threshold={heartbeat_interval_sec}s"
    )
    while max_iterations is None or iteration < max_iterations:
        iteration += 1
        print(f"--- poll {iteration} ---")
        _, snapshots = run_monitor(
            name_prefix,
            heartbeat_interval_sec,
            pipeline_names_file,
            poll_iteration=iteration,
            monitor_run_id=monitor_run_id,
        )
        if on_poll:
            on_poll(snapshots)
        if max_iterations is not None and iteration >= max_iterations:
            break
        print(f"Sleeping {interval}s until next poll...")
        time.sleep(interval)
    return 0


def run_restart(
    name_prefix: str,
    restart_limit: int,
    pipeline_names_file: str,
) -> int:
    client = DatabricksRestClient()
    configured_names = set(_load_pipeline_names(pipeline_names_file))
    pipelines, prefix_matches = _select_pipelines_for_ops(client, name_prefix, configured_names)
    if configured_names and not pipelines:
        raise RuntimeError(
            _pipeline_match_failure_message(configured_names, prefix_matches, name_prefix)
        )
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
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Poll continuously every heartbeat-interval seconds (for long-running monitor jobs)",
    )
    parser.add_argument("--max-iterations", type=int, default=None)
    args = parser.parse_args()

    if args.command == "monitor":
        if args.loop:
            return run_monitor_loop(
                args.name_prefix,
                args.heartbeat_interval_sec,
                args.pipeline_names_file,
                max_iterations=args.max_iterations,
            )
        code, _ = run_monitor(
            args.name_prefix,
            args.heartbeat_interval_sec,
            args.pipeline_names_file,
        )
        return code
    return run_restart(args.name_prefix, args.restart_limit, args.pipeline_names_file)


if __name__ == "__main__":
    raise SystemExit(main())
