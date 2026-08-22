"""Orchestrate ingestion flow metrics reconciliation per pipeline."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from common.ops.pipeline_names import load_pipeline_names, normalize_pipeline_key

from common.ops.lakeflow_event_ops import (
    aggregate_flow_metrics,
    build_pipeline_recon_context,
    evaluate_recon,
    ingestion_recon_event_extract_sql,
    parse_flow_progress_event,
    resolve_table_from_flow_name,
    TableReconConfig,
)
from common.ops.pipeline_job_ops import (
    ACTIVE_UPDATE_STATES,
    DatabricksRestClient,
    FAILED_STATES,
    describe_pipeline_status,
    _latest_update_block,
    _pipeline_spec,
    _pipeline_state_label,
    _select_pipelines_for_ops,
)
from common.ops.process_log_store import (
    ARTIFACT_TYPE_PIPELINE,
    PROCESS_TYPE_INGEST,
    build_process_log_row,
    write_process_log_rows,
)
from common.ops.recon_store import (
    FlowMetricsRow,
    FlowSummaryRow,
    ReconReadyRow,
    ReconEventLogWatermark,
    qualified_table,
    resolve_uc_table_ref,
    is_streaming_uc_table,
    UcTableRef,
    read_recon_event_log_watermarks,
    RECON_READY_TABLE,
    write_flow_metrics_rows,
    write_flow_summary_rows,
    write_recon_ready_rows,
)
from common.ops.source_ct_ops import run_source_ct_count
from common.ops.sql_server_audit_store import (
    CtPendingCounts,
    PendingCtTable,
    complete_recon_run,
    discover_pending_ct_tables,
    fetch_change_tracking_current_version,
    fetch_sql_row_count,
    flush_recon_event_log_watermarks_sql,
    insert_recon_run,
    open_audit_connection,
    read_recon_event_log_watermarks_sql,
    record_recon_table_result,
    resolve_source_ct_for_recon,
    upsert_db_watermark,
    upsert_table_watermark,
    write_audit_log,
)


def table_configs_from_effective(
    client_nm: str,
    raw_schema: str,
    effective_tables: list[Any],
) -> list[TableReconConfig]:
    configs: list[TableReconConfig] = []
    for t in effective_tables:
        configs.append(
            TableReconConfig(
                table_nm=t.table_nm,
                recon_type=int(t.recon_type),
                destination_schema=raw_schema,
                destination_table=t.table_nm,
            )
        )
    return configs


def resolve_pipeline_id(pipeline_key: str, client: DatabricksRestClient | None = None) -> str:
    rest = client or DatabricksRestClient()
    configured = {pipeline_key}
    matches, _ = _select_pipelines_for_ops(rest, "p_", configured)
    for p in matches:
        name = str(p.get("name", "")).strip()
        if name.endswith(pipeline_key) or pipeline_key in name:
            return str(p.get("pipeline_id", "")).strip()
    if matches:
        return str(matches[0].get("pipeline_id", "")).strip()
    return ""


def pipeline_needs_event_log_poll(
    detail: dict[str, Any],
    watermark: ReconEventLogWatermark | None,
) -> tuple[bool, str]:
    """REST pre-filter: skip hidden event_log query when pipeline has no new activity."""
    latest = _latest_update_block(detail)
    update_id = str(latest.get("update_id") or "").strip()
    update_state = str(latest.get("state", "")).upper()
    pipeline_state = _pipeline_state_label(detail)

    if watermark is None:
        return True, "initial scan"

    if update_id and update_id != watermark.last_update_id:
        return True, f"new update_id={update_id}"

    if update_state in ACTIVE_UPDATE_STATES:
        return True, f"active update_state={update_state}"

    if update_state in FAILED_STATES:
        return True, f"failed update_state={update_state}"

    if pipeline_state == "RUNNING":
        return True, "pipeline_state=RUNNING"

    if update_state == "COMPLETED" and watermark.last_api_update_state != "COMPLETED":
        return True, "transition to COMPLETED"

    if watermark.last_update_id == update_id and update_state in ("COMPLETED", "IDLE", "STOPPED", ""):
        return False, f"steady terminal update_state={update_state or 'NONE'}"

    return True, f"update_state={update_state or 'UNKNOWN'}"


def pipeline_api_update_snapshot(detail: dict[str, Any]) -> dict[str, Any]:
    """Latest pipeline update block from GET /api/2.0/pipelines/{id}."""
    latest = _latest_update_block(detail) if detail else {}
    return {
        "update_id": str(latest.get("update_id") or "").strip(),
        "state": str(latest.get("state", "")).upper(),
        "creation_time": latest.get("creation_time"),
        "start_time": latest.get("start_time"),
        "end_time": latest.get("end_time"),
    }


def api_update_indicates_complete(
    detail: dict[str, Any],
    *,
    require_new_update: bool = False,
    watermark: ReconEventLogWatermark | None = None,
) -> bool:
    """True when REST API reports the latest pipeline update is COMPLETED."""
    snap = pipeline_api_update_snapshot(detail)
    if snap["state"] != "COMPLETED":
        return False
    if not require_new_update:
        return True
    if watermark is None:
        return True
    api_id = snap["update_id"]
    if not api_id:
        return watermark.last_api_update_state != "COMPLETED"
    return api_id != watermark.last_update_id or watermark.last_api_update_state != "COMPLETED"


def _default_flow_name_for_table(table_nm: str, src_schema: str = "dbo") -> str:
    return f"{src_schema}.{table_nm}_snapshot_flow"


def count_delta_table_rows(spark, catalog: str, schema: str, table_nm: str) -> int | None:
    """
    Logical row count for SCD1 recon.

    - Managed Delta: DeltaTable.detail / DESCRIBE DETAIL numRecords (metadata only).
    - Lakeflow Connect streaming targets (STREAMING_TABLE): COUNT_BIG only —
      DeltaTable.forName and DESCRIBE DETAIL are not supported.
    """
    ref = resolve_uc_table_ref(spark, catalog, schema, table_nm)
    if ref is None:
        return None

    streaming = is_streaming_uc_table(spark, ref)
    targets = [ref.name, ref.quoted_name]
    last_err: str | None = None

    def _row_count_from_detail_row(detail_row: Any) -> int | None:
        if detail_row is None:
            return None
        if hasattr(detail_row, "asDict"):
            data = detail_row.asDict()
            value = data.get("numRecords")
        elif isinstance(detail_row, dict):
            value = detail_row.get("numRecords")
        else:
            value = detail_row["numRecords"] if "numRecords" in detail_row else detail_row[0]
        if value is None:
            return None
        return int(value)

    if not streaming:
        for target in targets:
            try:
                from delta.tables import DeltaTable

                detail_row = DeltaTable.forName(spark, target).detail().select("numRecords").first()
                count = _row_count_from_detail_row(detail_row)
                if count is not None:
                    return count
            except Exception as exc:
                last_err = str(exc)
            try:
                detail_row = spark.sql(f"DESCRIBE DETAIL {target}").select("numRecords").first()
                count = _row_count_from_detail_row(detail_row)
                if count is not None:
                    return count
            except Exception as exc:
                last_err = str(exc)

    for target in targets:
        try:
            row = spark.sql(f"SELECT COUNT_BIG(*) AS cnt FROM {target}").collect()[0]
            return int(row["cnt"])
        except Exception as exc:
            last_err = str(exc)

    if last_err:
        label = "streaming" if streaming else "delta"
        print(f"[recon] WARN {label} row count failed for {ref.name}: {last_err}")
    return None


def _flow_metrics_for_table(
    pipeline_rows: list[dict[str, Any]],
    ctx: Any,
    table_nm: str,
) -> list[FlowMetricsRow]:
    metrics: list[FlowMetricsRow] = []
    target = table_nm.casefold()
    for row in pipeline_rows:
        parsed = parse_flow_progress_event(row, ctx)
        if not parsed:
            continue
        if not ctx.pipeline_id and parsed.pipeline_id:
            ctx.pipeline_id = parsed.pipeline_id
        resolved = (
            parsed.table_name
            or resolve_table_from_flow_name(parsed.flow_name, ctx.tables)
        )
        if (resolved or "").casefold() != target:
            continue
        if resolved and not parsed.table_name:
            parsed.table_name = resolved
        metrics.append(parsed)
    return metrics


def _log_event_log_table_hints(
    pipeline_rows: list[dict[str, Any]],
    ctx: Any,
    table_nm: str,
    pipeline_key: str,
) -> None:
    """One-line diagnostic when no metrics matched the configured table."""
    if not pipeline_rows:
        print(f"[recon] {pipeline_key} {table_nm}: event_log has no METRICS rows in window")
        return
    hints: list[str] = []
    for row in pipeline_rows[:8]:
        flow = str(row.get("flow_name") or "")
        evt = str(row.get("event_type") or "")
        hint_table = resolve_table_from_flow_name(flow, ctx.tables) or str(row.get("table_name") or "")
        ups = row.get("rows_upserted") or row.get("output_rows") or 0
        hints.append(f"{evt}:{flow}→{hint_table}(ups={ups})")
    print(
        f"[recon] {pipeline_key} {table_nm}: no metrics matched; "
        f"event_log sample: {'; '.join(hints)}"
    )

def _recon_type_for_table(ctx: Any, table_nm: str) -> int:
    target = table_nm.casefold()
    for cfg in ctx.tables:
        if cfg.table_nm.casefold() == target:
            return int(cfg.recon_type)
    return 1


def _destination_schema_for_table(ctx: Any, table_nm: str) -> str:
    target = table_nm.casefold()
    for cfg in ctx.tables:
        if cfg.table_nm.casefold() == target:
            return cfg.destination_schema
    return ""


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_refresh_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return _ensure_utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _spark_error_summary(exc: BaseException) -> str:
    text = str(exc).strip()
    if "PARSE_SYNTAX_ERROR" in text or "ParseException" in text:
        return "SQL syntax not supported on this runtime"
    first = text.split("\n", 1)[0]
    return first[:200] if len(first) > 200 else first


DELTA_DATA_WRITE_OPERATIONS = frozenset(
    {"MERGE", "WRITE", "UPDATE", "DELETE", "STREAMING UPDATE", "INSERT"}
)


def _extract_update_id_from_history_params(params: Any) -> str:
    if params is None:
        return ""
    if isinstance(params, dict):
        return str(params.get("updateId") or params.get("update_id") or "")
    text = str(params).strip()
    if not text:
        return ""
    if text.startswith("{") or text.startswith("["):
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return str(data.get("updateId") or data.get("update_id") or "")
        except json.JSONDecodeError:
            pass
    match = re.search(r"updateId[\"']?\s*[:=]\s*[\"']?([a-f0-9-]+)", text, re.I)
    return match.group(1) if match else ""


def _history_version_key(row: dict[str, Any]) -> int:
    try:
        return int(row.get("version"))
    except (TypeError, ValueError):
        return -1


def summarize_delta_history_refresh(
    history_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """
  From DESCRIBE HISTORY rows, pick latest MERGE (preferred) or other data-write op.

  Also captures latest DLT SETUP version/timestamp/updateId for pipeline correlation.
  """
    if not history_rows:
        return None

    merge_rows = [
        row
        for row in history_rows
        if str(row.get("operation") or "").upper() == "MERGE"
    ]
    write_rows = [
        row
        for row in history_rows
        if str(row.get("operation") or "").upper() in DELTA_DATA_WRITE_OPERATIONS
    ]
    dlt_rows = [
        row
        for row in history_rows
        if "DLT" in str(row.get("operation") or "").upper()
    ]

    latest_merge = (
        max(merge_rows, key=_history_version_key) if merge_rows else None
    )
    latest_write = (
        max(write_rows, key=_history_version_key) if write_rows else None
    )
    latest_dlt = max(dlt_rows, key=_history_version_key) if dlt_rows else None
    pick = latest_merge or latest_write
    if pick is None:
        return None

    last_at = _parse_refresh_timestamp(pick.get("timestamp"))
    if last_at is None:
        return None

    operation = str(pick.get("operation") or "")
    result: dict[str, Any] = {
        "table": "",
        "last_refreshed_at": last_at,
        "latest_refresh_status": operation,
        "last_refresh_type": operation.casefold(),
        "source": "delta_history",
        "delta_version": _history_version_key(pick),
    }
    if latest_merge is not None:
        result["last_merge_at"] = _parse_refresh_timestamp(
            latest_merge.get("timestamp")
        )
        result["last_merge_version"] = _history_version_key(latest_merge)
    if latest_dlt is not None:
        result["last_dlt_setup_at"] = _parse_refresh_timestamp(
            latest_dlt.get("timestamp")
        )
        result["last_dlt_setup_version"] = _history_version_key(latest_dlt)
        result["dlt_update_id"] = _extract_update_id_from_history_params(
            latest_dlt.get("operationParameters")
        )
    return result


def fetch_delta_history_refresh_info(
    spark,
    ref: UcTableRef,
    history_limit: int = 100,
) -> dict[str, Any] | None:
    """Read DESCRIBE HISTORY and return latest MERGE / write timestamp + version."""
    limit = max(10, min(int(history_limit), 500))
    rows: list[dict[str, Any]] = []
    try:
        history_df = spark.sql(
            f"DESCRIBE HISTORY {ref.quoted_name} LIMIT {limit}"
        )
        rows = [row.asDict() for row in history_df.collect()]
    except Exception as exc:
        try:
            history_df = spark.sql(f"DESCRIBE HISTORY {ref.quoted_name}")
            rows = [row.asDict() for row in history_df.collect()[:limit]]
        except Exception as exc2:
            print(
                f"[recon] WARN DESCRIBE HISTORY for {ref.name}: "
                f"{_spark_error_summary(exc2)}"
            )
            return None
        print(
            f"[recon] WARN DESCRIBE HISTORY LIMIT for {ref.name}: "
            f"{_spark_error_summary(exc)}; used unbounded history"
        )

    info = summarize_delta_history_refresh(rows)
    if info is None:
        return None
    info["table"] = ref.name
    return info


def fetch_streaming_table_refresh_info(
    spark,
    catalog: str,
    schema: str,
    table_nm: str,
    history_limit: int = 100,
) -> dict[str, Any] | None:
    """
    Per-table refresh metadata for UC streaming / materialized views.

    Prefers DESCRIBE HISTORY (latest MERGE timestamp + version), then UC metadata fallbacks.
    """
    ref = resolve_uc_table_ref(spark, catalog, schema, table_nm)
    if ref is None:
        print(
            f"[recon] WARN resolve_uc_table_ref failed "
            f"catalog={catalog} schema={schema} table={table_nm}"
        )
        return None

    history_info = fetch_delta_history_refresh_info(
        spark, ref, history_limit=history_limit
    )
    if history_info is not None:
        return history_info

    try:
        detail_row = spark.sql(f"DESCRIBE DETAIL {ref.quoted_name}").collect()[0]
        detail = detail_row.asDict() if hasattr(detail_row, "asDict") else {}
        last_at = _parse_refresh_timestamp(detail.get("lastModified"))
        if last_at is not None:
            return {
                "table": ref.name,
                "last_refreshed_at": last_at,
                "latest_refresh_status": "",
                "last_refresh_type": "lastModified",
                "source": "describe_detail",
            }
    except Exception as exc:
        print(
            f"[recon] WARN DESCRIBE DETAIL for {ref.name}: "
            f"{_spark_error_summary(exc)}"
        )

    try:
        esc_catalog = catalog.replace("`", "``")
        esc_schema = ref.schema.replace("`", "``")
        esc_table = ref.table.replace("`", "``")
        row = spark.sql(
            f"""
SELECT last_altered
FROM `{esc_catalog}`.information_schema.tables
WHERE table_schema = '{esc_schema.replace("'", "''")}'
  AND table_name = '{esc_table.replace("'", "''")}'
""".strip()
        ).first()
        if row is not None:
            last_at = _parse_refresh_timestamp(row.last_altered)
            if last_at is not None:
                return {
                    "table": ref.name,
                    "last_refreshed_at": last_at,
                    "latest_refresh_status": "",
                    "last_refresh_type": "last_altered",
                    "source": "information_schema",
                }
    except Exception as exc:
        print(
            f"[recon] WARN information_schema.tables for {ref.name}: "
            f"{_spark_error_summary(exc)}"
        )

    try:
        for row in spark.sql(f"DESCRIBE TABLE EXTENDED {ref.quoted_name}").collect():
            col_name = str(row.col_name).strip()
            if col_name in ("Last Modified", "last_modified", "last_refreshed_at"):
                last_at = _parse_refresh_timestamp(row.data_type)
                if last_at is not None:
                    return {
                        "table": ref.name,
                        "last_refreshed_at": last_at,
                        "latest_refresh_status": "",
                        "last_refresh_type": col_name,
                        "source": "describe_extended",
                    }
    except Exception as exc:
        print(
            f"[recon] WARN DESCRIBE TABLE EXTENDED for {ref.name}: "
            f"{_spark_error_summary(exc)}"
        )

    try:
        json_row = spark.sql(
            f"DESCRIBE TABLE EXTENDED {ref.quoted_name} AS JSON"
        ).collect()[0]
        json_text = json_row[0]
        data = json.loads(json_text) if isinstance(json_text, str) else {}
        refresh = data.get("refresh_information") or {}
        last_at = _parse_refresh_timestamp(refresh.get("last_refreshed_at"))
        if last_at is not None:
            return {
                "table": ref.name,
                "last_refreshed_at": last_at,
                "latest_refresh_status": str(refresh.get("latest_refresh_status") or ""),
                "last_refresh_type": str(refresh.get("last_refresh_type") or ""),
                "source": "refresh_information",
            }
    except Exception as exc:
        print(
            f"[recon] WARN refresh_information JSON for {ref.name}: "
            f"{_spark_error_summary(exc)}"
        )
    return None


def _flow_metrics_positive(metrics: list[FlowMetricsRow]) -> tuple[bool, int, int, int]:
    if not metrics:
        return False, 0, 0, 0
    upserted = sum(m.rows_upserted or 0 for m in metrics)
    deleted = sum(m.rows_deleted or 0 for m in metrics)
    output = sum(m.output_rows or 0 for m in metrics)
    changed = upserted + deleted
    return changed > 0 or output > 0, upserted, deleted, output


def evaluate_table_refresh_after_sql_ct(
    table_refresh: dict[str, Any] | None,
    sql_ct_reference_at: datetime | None,
    quiesce_sec: int = 15,
) -> tuple[str, str]:
    """
    Per-table gate: UC streaming target refreshed after SQL CT reference time + quiesce.
    """
    if sql_ct_reference_at is None:
        return "WAITING", "SQL CT reference timestamp unavailable"

    if table_refresh is None or table_refresh.get("last_refreshed_at") is None:
        return "WAITING", "no streaming table refresh metadata (last_refreshed_at)"

    refresh_dt = _ensure_utc(table_refresh["last_refreshed_at"])
    ref_dt = _ensure_utc(sql_ct_reference_at)
    deadline = ref_dt + timedelta(seconds=max(0, int(quiesce_sec)))

    if refresh_dt < deadline:
        return (
            "WAITING",
            f"delta last_refreshed_at={refresh_dt.isoformat()} before "
            f"sql_ct_reference+{quiesce_sec}s ({deadline.isoformat()})",
        )

    table_name = str(table_refresh.get("table") or "")
    refresh_status = str(table_refresh.get("latest_refresh_status") or "")
    delta_version = table_refresh.get("delta_version")
    version_note = (
        f" delta_version={delta_version}" if delta_version is not None else ""
    )
    return (
        "PASS",
        (
            f"delta refreshed after SQL CT: table={table_name} "
            f"last_refreshed_at={refresh_dt.isoformat()} "
            f"sql_ct_reference={ref_dt.isoformat()} "
            f"refresh_status={refresh_status or 'n/a'}{version_note}"
        ),
    )


def evaluate_ingest_quiesce_recon(
    metrics: list[FlowMetricsRow],
    pending: CtPendingCounts,
    recon_type: int,
    table_refresh: dict[str, Any] | None,
    sql_ct_reference_at: datetime | None,
    quiesce_sec: int = 15,
) -> tuple[str, str]:
    """
    PASS when flow_progress metrics are positive and the UC streaming target was
    refreshed after the SQL CT reference timestamp (+ quiesce buffer).
    """
    positive, upserted, deleted, output = _flow_metrics_positive(metrics)
    if not positive:
        return "WAITING", "no positive flow_progress metrics for table"

    status, message = evaluate_table_refresh_after_sql_ct(
        table_refresh, sql_ct_reference_at, quiesce_sec=quiesce_sec
    )
    if status != "PASS":
        return status, message

    source_metric = pending.metric_for_recon_type(recon_type)
    refresh_dt = _ensure_utc(table_refresh["last_refreshed_at"])
    ref_dt = _ensure_utc(sql_ct_reference_at)
    return (
        "PASS",
        (
            f"ingest_quiesce: flow upserted={upserted} deleted={deleted} output={output}, "
            f"CT pending={source_metric}, sql_ct_reference={ref_dt.isoformat()}, "
            f"delta last_refreshed_at={refresh_dt.isoformat()}"
        ),
    )


def evaluate_simple_recon(
    summary: FlowSummaryRow | None,
    pending: CtPendingCounts,
    recon_type: int,
    sql_row_count: int | None,
    delta_row_count: int | None,
    pass_rule: str,
    *,
    api_update_complete: bool = False,
) -> tuple[str, str]:
    """
    Simplified pass rules (first match wins for auto):
      flow_complete — COMPLETED flow in event log or pipeline API update COMPLETED
      row_count     — SQL COUNT_BIG == Delta COUNT_BIG
      ct_metrics    — ingest metrics vs CT (recon_type 2/3 rules)
      auto          — try all three in order
    Returns (status, message) where status is PASS | FAIL | WAITING.
    """
    event_log_complete = summary is not None and summary.final_flow_status == "COMPLETED"
    flow_complete = event_log_complete or api_update_complete

    rules = (
        ["flow_complete", "row_count", "ct_metrics"]
        if pass_rule == "auto"
        else [pass_rule]
    )

    for rule in rules:
        if rule == "flow_complete" and flow_complete:
            if event_log_complete:
                return "PASS", "flow_progress COMPLETED in event log"
            if api_update_complete:
                return "PASS", "pipeline API last update COMPLETED"
            return "PASS", "flow COMPLETED"
        if rule == "row_count" and sql_row_count is not None and delta_row_count is not None:
            if sql_row_count == delta_row_count:
                return "PASS", f"row_count match sql={sql_row_count} delta={delta_row_count}"
            if pass_rule == "row_count":
                return "FAIL", f"row_count mismatch sql={sql_row_count} delta={delta_row_count}"
            continue
        if rule == "ct_metrics" and summary is not None:
            source_metric = pending.metric_for_recon_type(recon_type)
            evaluated = evaluate_recon(summary, source_metric)
            return evaluated.recon_status, evaluated.recon_message

    if pending.total > 0 and not flow_complete:
        if pass_rule == "row_count" and sql_row_count is None and delta_row_count is None:
            return "WAITING", "waiting for flow COMPLETED before COUNT_BIG row_count"
        if pass_rule == "row_count" and (sql_row_count is None or delta_row_count is None):
            return (
                "WAITING",
                f"row_count unavailable sql={sql_row_count} delta={delta_row_count}",
            )
        return "WAITING", "CT pending; no COMPLETED flow in event log yet"

    return "FAIL", "no simple pass rule matched"


def run_simplified_pipeline_recon(
    spark,
    catalog: str,
    metadata_schema: str,
    ctx: Any,
    src_schema: str,
    pipeline_rows: list[dict[str, Any]],
    client: Any,
    dbutils: Any | None = None,
    pass_rule: str = "auto",
    *,
    recon_run_id: str | None = None,
    audit_conn: Any | None = None,
    row_count_only_on_flow_complete: bool = True,
    pipeline_detail: dict[str, Any] | None = None,
    use_api_update_complete: bool = True,
    event_log_watermark: ReconEventLogWatermark | None = None,
    ct_head_cache: dict[str, int] | None = None,
    table_quiesce_sec: int = 15,
) -> tuple[int, int, int]:
    """
  CT-driven recon for one pipeline: only tables with pending CT since watermark.
  Writes recon_ready (Delta) + SQL audit/watermarks on PASS. No flow_metrics/summary Delta writes.
  Returns (recon_ready_written, ct_pending_tables, waiting_tables).
    """
    if not ctx.pipeline_id:
        ctx.pipeline_id = resolve_pipeline_id(ctx.pipeline_key)

    active_tables = [cfg.table_nm for cfg in ctx.tables]
    api_snap = pipeline_api_update_snapshot(pipeline_detail or {})
    api_update_complete = use_api_update_complete and api_update_indicates_complete(
        pipeline_detail or {},
        watermark=event_log_watermark,
    )
    print(
        f"[recon] pipeline={ctx.pipeline_key} client={client.client_nm} "
        f"sql_db={client.src_db_nm} pipeline_id={ctx.pipeline_id or 'n/a'} "
        f"active_tables={len(active_tables)} pass_rule={pass_rule} "
        f"row_count_only_on_flow_complete={row_count_only_on_flow_complete} "
        f"use_api_update_complete={use_api_update_complete} "
        f"table_quiesce_sec={table_quiesce_sec}"
    )
    if pipeline_detail:
        print(f"[recon] {ctx.pipeline_key} {describe_pipeline_status(pipeline_detail)}")
    if api_snap.get("update_id") or api_snap.get("state"):
        print(
            f"[recon] {ctx.pipeline_key} API last_update: "
            f"update_id={api_snap['update_id'] or 'n/a'} "
            f"state={api_snap['state'] or 'NONE'} "
            f"end_time={api_snap.get('end_time') or 'n/a'}"
        )
    if event_log_watermark is not None:
        print(
            f"[recon] {ctx.pipeline_key} SQL watermark: "
            f"last_update_id={event_log_watermark.last_update_id or 'n/a'} "
            f"last_api_state={event_log_watermark.last_api_update_state or 'n/a'} "
            f"last_event_ts={event_log_watermark.last_event_ts or 'n/a'}"
        )
    is_continuous = bool(_pipeline_spec(pipeline_detail or {}).get("continuous"))
    if is_continuous and api_snap.get("state") == "RUNNING":
        print(
            f"[recon] {ctx.pipeline_key}: continuous pipeline — "
            "API update_state=RUNNING is normal (COMPLETED may not appear)"
        )

    head_cache = ct_head_cache if ct_head_cache is not None else {}

    conn = audit_conn
    owns_conn = False
    if conn is None:
        try:
            conn, _ = open_audit_connection(client, dbutils=dbutils)
            owns_conn = True
        except Exception as exc:
            print(f"[recon] WARN SQL connection failed for {ctx.pipeline_key}: {exc}")
            return 0, 0, 0

    pending_tables = discover_pending_ct_tables(conn, client, src_schema, active_tables)
    if not pending_tables:
        ct_head = None
        try:
            ct_head = fetch_change_tracking_current_version(conn)
        except Exception:
            pass
        print(
            f"[recon] {ctx.pipeline_key}: no pending CT on configured tables "
            f"(ct_head={ct_head})"
        )
        if owns_conn:
            conn.close()
        return 0, 0, 0

    print(
        f"[recon] {ctx.pipeline_key}: {len(pending_tables)} table(s) with CT activity "
        f"since watermark"
    )

    ready_written = 0
    waiting_count = 0
    delta_after_ct_pass = 0
    delta_after_ct_wait = 0
    run_id = recon_run_id
    if run_id is None:
        sample_update = str(pipeline_rows[0].get("update_id") or "") if pipeline_rows else ""
        try:
            run_id = insert_recon_run(
                conn,
                client_nm=client.client_nm,
                database_name=client.src_db_nm,
                pipeline_id=ctx.pipeline_id or "",
                update_id=sample_update,
                pipeline_key=ctx.pipeline_key,
            )
        except Exception as exc:
            print(f"[recon] WARN insert_recon_run failed: {exc}")
            run_id = None

    for probe in pending_tables:
        table_nm = probe.table_name
        recon_type = _recon_type_for_table(ctx, table_nm)
        dest_schema = _destination_schema_for_table(ctx, table_nm)
        print(
            f"[recon] {ctx.pipeline_key} table={table_nm} "
            f"ct_versions={probe.watermark_before}..{probe.ct_head_version} "
            f"pending I/U/D={probe.pending.inserts}/{probe.pending.updates}/{probe.pending.deletes} "
            f"recon_type={recon_type} "
            f"watermark_updated_at={probe.watermark_updated_at or 'n/a'} "
            f"sql_ct_reference_at={probe.sql_ct_reference_at or 'n/a'}"
        )

        metrics = _flow_metrics_for_table(pipeline_rows, ctx, table_nm)
        summaries = aggregate_flow_metrics(metrics, ctx.tables)
        summary = next(
            (s for s in summaries if s.table_name.casefold() == table_nm.casefold()),
            None,
        )
        flow_complete = (
            summary is not None and summary.final_flow_status == "COMPLETED"
        ) or api_update_complete

        stable_key = f"{client.src_db_nm}.{src_schema}.{table_nm}"
        prev_ct_head = head_cache.get(stable_key)
        ct_head_stable = (
            prev_ct_head is not None and prev_ct_head == probe.ct_head_version
        )
        head_cache[stable_key] = probe.ct_head_version

        sql_count: int | None = None
        delta_count: int | None = None

        sql_ct_ref = probe.sql_ct_reference_at or probe.watermark_updated_at

        if pass_rule in ("ingest_quiesce", "table_after_ct"):
            table_refresh = (
                fetch_streaming_table_refresh_info(spark, catalog, dest_schema, table_nm)
                if dest_schema
                else None
            )
            if table_refresh:
                dlt_id = table_refresh.get("dlt_update_id") or "n/a"
                print(
                    f"[recon] {ctx.pipeline_key} {table_nm}: delta refresh "
                    f"source={table_refresh.get('source') or 'n/a'} "
                    f"last_refreshed_at={table_refresh.get('last_refreshed_at')} "
                    f"op={table_refresh.get('latest_refresh_status') or 'n/a'} "
                    f"delta_version={table_refresh.get('delta_version') or 'n/a'} "
                    f"dlt_update_id={dlt_id}"
                )
            else:
                print(
                    f"[recon] WARN {ctx.pipeline_key} {table_nm}: "
                    "no delta table refresh metadata"
                )

        if pass_rule == "ingest_quiesce":
            metrics_positive, _, _, _ = _flow_metrics_positive(metrics)
            if not metrics_positive:
                _log_event_log_table_hints(pipeline_rows, ctx, table_nm, ctx.pipeline_key)
            status, message = evaluate_ingest_quiesce_recon(
                metrics,
                probe.pending,
                recon_type,
                table_refresh,
                sql_ct_ref,
                quiesce_sec=table_quiesce_sec,
            )
        elif pass_rule == "table_after_ct":
            status, message = evaluate_table_refresh_after_sql_ct(
                table_refresh,
                sql_ct_ref,
                quiesce_sec=table_quiesce_sec,
            )
        elif pass_rule in ("auto", "flow_complete") and flow_complete:
            if summary is not None and summary.final_flow_status == "COMPLETED":
                status, message = "PASS", "flow_progress COMPLETED in event log"
            elif api_update_complete:
                status, message = "PASS", "pipeline API last update COMPLETED"
            else:
                status, message = "PASS", "flow COMPLETED"
        else:
            if pass_rule in ("row_count", "auto"):
                defer_row_count = (
                    row_count_only_on_flow_complete
                    and not flow_complete
                    and not ct_head_stable
                )
                if defer_row_count:
                    print(
                        f"[recon] {ctx.pipeline_key} {table_nm}: "
                        "deferring COUNT_BIG until flow COMPLETED or CT head stable"
                    )
                elif (
                    row_count_only_on_flow_complete
                    and ct_head_stable
                    and not flow_complete
                ):
                    print(
                        f"[recon] {ctx.pipeline_key} {table_nm}: "
                        f"CT head stable at {probe.ct_head_version} — running row_count"
                    )
                if not defer_row_count:
                    sql_count = fetch_sql_row_count(conn, src_schema, table_nm)
                    uc_ref = resolve_uc_table_ref(spark, catalog, dest_schema, table_nm)
                    delta_count = (
                        count_delta_table_rows(spark, catalog, dest_schema, table_nm)
                        if dest_schema
                        else None
                    )
                    if sql_count is not None or delta_count is not None:
                        resolved = uc_ref.name if uc_ref else f"{catalog}.{dest_schema}.{table_nm}"
                        if uc_ref and uc_ref.name != qualified_table(catalog, dest_schema, table_nm):
                            print(
                                f"[recon] {ctx.pipeline_key} {table_nm}: "
                                f"resolved UC table {resolved}"
                            )
                        print(
                            f"[recon] {ctx.pipeline_key} {table_nm}: "
                            f"sql_count={sql_count} delta_count={delta_count} "
                            f"({'streaming COUNT_BIG' if uc_ref and is_streaming_uc_table(spark, uc_ref) else 'delta numRecords'}) "
                            f"{resolved}"
                        )
                    elif dest_schema:
                        print(
                            f"[recon] WARN {ctx.pipeline_key} {table_nm}: "
                            f"UC table not found {catalog}.{dest_schema}.{table_nm}"
                        )
            status, message = evaluate_simple_recon(
                summary,
                probe.pending,
                recon_type,
                sql_count,
                delta_count,
                pass_rule,
                api_update_complete=api_update_complete,
            )
        print(f"[recon] {ctx.pipeline_key} {table_nm}: {status} — {message}")

        if pass_rule in ("ingest_quiesce", "table_after_ct"):
            if status == "PASS":
                delta_after_ct_pass += 1
            elif status == "WAITING":
                delta_after_ct_wait += 1

        if status == "WAITING":
            waiting_count += 1
            continue

        update_id = summary.update_id if summary else (
            metrics[-1].update_id if metrics else api_snap.get("update_id", "")
        )
        flow_name = summary.flow_name if summary else _default_flow_name_for_table(table_nm, src_schema)
        pipeline_id = ctx.pipeline_id or (summary.pipeline_id if summary else "")

        if update_id and pipeline_id and flow_name:
            if recon_already_recorded(
                spark, catalog, metadata_schema, pipeline_id, update_id, flow_name
            ):
                print(f"[recon] {ctx.pipeline_key} {table_nm}: SKIP already in recon_ready")
                continue

        ingest_change = summary.total_change_rows if summary else (
            sum(m.rows_upserted or 0 for m in metrics) + sum(m.rows_deleted or 0 for m in metrics)
        )
        source_metric = probe.pending.metric_for_recon_type(recon_type)
        try:
            record_recon_table_result(
                conn,
                recon_run_id=run_id,
                client_nm=client.client_nm,
                database_name=client.src_db_nm,
                schema_name=src_schema,
                table_name=table_nm,
                pipeline_id=pipeline_id,
                update_id=update_id,
                flow_name=flow_name,
                recon_type=recon_type,
                watermark_before=probe.watermark_before,
                ct_head_version=probe.ct_head_version,
                pending=probe.pending,
                ingest_upserted=summary.total_upserted if summary else 0,
                ingest_deleted=summary.total_deleted if summary else 0,
                ingest_change_rows=ingest_change,
                sync_status=status,
                recon_message=message,
                watermark_advanced=status == "PASS",
            )
        except Exception as exc:
            print(f"[recon] WARN record_recon_table_result: {exc}")

        if status == "PASS":
            completed_at = summary.last_event_time if summary else datetime.now(timezone.utc)
            ready = ReconReadyRow(
                client_nm=client.client_nm,
                table_nm=table_nm,
                pipeline_id=pipeline_id,
                update_id=update_id,
                flow_name=flow_name or table_nm,
                recon_type=recon_type,
                ingest_change_rows=ingest_change,
                source_change_rows=source_metric,
                completed_at=completed_at,
                artifact_run_id=update_id,
                ready_for_calc=True,
            )
            ready_written += write_recon_ready_rows(spark, catalog, metadata_schema, [ready])
            upsert_table_watermark(
                conn,
                client.src_db_nm,
                src_schema,
                table_nm,
                probe.ct_head_version,
                client_nm=client.client_nm,
                pipeline_key=ctx.pipeline_key,
            )
            upsert_db_watermark(
                conn, client.src_db_nm, probe.ct_head_version, client_nm=client.client_nm
            )
            print(f"[recon] PASS {ctx.pipeline_key} {table_nm} → recon_ready written")

    if pass_rule in ("ingest_quiesce", "table_after_ct") and pending_tables:
        print(
            f"[recon] {ctx.pipeline_key}: per-table delta vs SQL CT — "
            f"checked={len(pending_tables)} "
            f"delta_after_ct_pass={delta_after_ct_pass} "
            f"delta_after_ct_wait={delta_after_ct_wait}"
        )

    if run_id:
        try:
            complete_recon_run(
                conn,
                run_id,
                run_status="PASS" if ready_written else "SKIPPED",
                run_message=f"ready={ready_written} waiting={waiting_count}",
            )
        except Exception as exc:
            print(f"[recon] WARN complete_recon_run: {exc}")

    if owns_conn:
        try:
            conn.close()
        except Exception:
            pass

    return ready_written, len(pending_tables), waiting_count


def fetch_flow_progress_rows(
    spark,
    pipeline_id: str,
    lookback_hours: int,
    since_timestamp: datetime | None = None,
) -> list[dict[str, Any]]:
    sql = ingestion_recon_event_extract_sql(
        pipeline_id,
        lookback_hours=lookback_hours,
        since_timestamp=since_timestamp,
    )
    try:
        return [row.asDict() for row in spark.sql(sql).collect()]
    except Exception as exc:
        print(f"WARN event_log query failed for pipeline_id={pipeline_id}: {exc}")
        return []


def _watermark_from_rows_and_api(
    pipeline_id: str,
    pipeline_key: str,
    pipeline_rows: list[dict[str, Any]],
    detail: dict[str, Any],
    previous: ReconEventLogWatermark | None,
    polled_at: datetime,
) -> ReconEventLogWatermark:
    latest = _latest_update_block(detail)
    update_id = str(latest.get("update_id") or "").strip()
    update_state = str(latest.get("state", "")).upper()

    last_event_ts = previous.last_event_ts if previous else None
    last_event_id = previous.last_event_id if previous else ""
    for row in pipeline_rows:
        event_ts = row.get("event_timestamp")
        if isinstance(event_ts, datetime):
            if last_event_ts is None or event_ts > last_event_ts:
                last_event_ts = event_ts
        event_id = str(row.get("event_id") or "").strip()
        if event_id:
            last_event_id = event_id

    if not update_id:
        for row in pipeline_rows:
            row_update = str(row.get("update_id") or "").strip()
            if row_update:
                update_id = row_update
                break

    return ReconEventLogWatermark(
        pipeline_id=pipeline_id,
        pipeline_key=pipeline_key,
        last_event_ts=last_event_ts,
        last_event_id=last_event_id,
        last_update_id=update_id,
        last_api_update_state=update_state,
        last_poll_at=polled_at,
    )


def recon_already_recorded(
    spark,
    catalog: str,
    metadata_schema: str,
    pipeline_id: str,
    update_id: str,
    flow_name: str,
) -> bool:
    target = qualified_table(catalog, metadata_schema, RECON_READY_TABLE)
    try:
        rows = spark.sql(
            f"""
            SELECT 1 FROM {target}
            WHERE pipeline_id = '{pipeline_id.replace("'", "''")}'
              AND update_id = '{update_id.replace("'", "''")}'
              AND flow_name = '{flow_name.replace("'", "''")}'
            LIMIT 1
            """
        ).collect()
        return len(rows) > 0
    except Exception:
        return False


def run_pipeline_recon(
    spark,
    catalog: str,
    metadata_schema: str,
    ctx: Any,
    src_catalog: str,
    src_schema: str,
    pipeline_rows: list[dict[str, Any]],
    rest_client: DatabricksRestClient | None = None,
    client: Any | None = None,
    dbutils: Any | None = None,
    use_sql_server_audit: bool = True,
) -> tuple[int, int, int]:
    """
    Run recon for one ingestion pipeline context from pre-fetched event_log rows.
    Returns (metrics_merged, summaries_written, recon_ready_written).
    """
    if not ctx.pipeline_id:
        ctx.pipeline_id = resolve_pipeline_id(ctx.pipeline_key, rest_client)

    metrics: list[FlowMetricsRow] = []
    for row in pipeline_rows:
        parsed = parse_flow_progress_event(row, ctx)
        if parsed:
            if not parsed.pipeline_id and ctx.pipeline_id:
                parsed.pipeline_id = ctx.pipeline_id
            metrics.append(parsed)

    if not metrics:
        return 0, 0, 0

    merged = write_flow_metrics_rows(spark, catalog, metadata_schema, metrics)
    summaries = aggregate_flow_metrics(metrics, ctx.tables)

    summaries_written = 0
    ready_written = 0
    process_rows: list[Any] = []

    audit_conn = None
    recon_run_id: str | None = None
    needs_sql_audit = (
        use_sql_server_audit
        and client is not None
        and any(s.recon_type in (2, 3) for s in summaries)
    )
    if needs_sql_audit:
        try:
            audit_conn, _ = open_audit_connection(client, dbutils=dbutils)
            sample_update = summaries[0].update_id if summaries else ""
            recon_run_id = insert_recon_run(
                audit_conn,
                client_nm=client.client_nm,
                database_name=client.src_db_nm,
                pipeline_id=ctx.pipeline_id,
                update_id=sample_update,
                pipeline_key=ctx.pipeline_key,
            )
        except Exception as exc:
            print(f"WARN SQL Server audit connection failed for {ctx.pipeline_key}: {exc}")
            audit_conn = None
            recon_run_id = None

    pass_count = 0
    fail_count = 0

    for summary in summaries:
        if recon_already_recorded(
            spark, catalog, metadata_schema, summary.pipeline_id, summary.update_id, summary.flow_name
        ):
            continue

        source_count: int | None = None
        pending = None
        watermark_before = 0
        ct_head = 0
        if summary.recon_type in (2, 3):
            if audit_conn is not None and client is not None:
                try:
                    source_count, pending, watermark_before, ct_head = resolve_source_ct_for_recon(
                        audit_conn,
                        client,
                        src_schema,
                        summary.table_name,
                        summary.recon_type,
                        pipeline_key=ctx.pipeline_key,
                    )
                except Exception as exc:
                    print(
                        f"WARN SQL audit CT count failed {ctx.pipeline_key} "
                        f"{summary.table_name}: {exc}"
                    )
                    source_count = None
            else:
                source_count = run_source_ct_count(
                    spark,
                    src_catalog,
                    src_schema,
                    summary.table_name,
                    watermark_before,
                    ct_head or None,
                    summary.recon_type,
                )

        evaluated = evaluate_recon(summary, source_count)
        summaries_written += write_flow_summary_rows(spark, catalog, metadata_schema, [evaluated])

        if audit_conn is not None and client is not None and summary.recon_type in (2, 3):
            sync_status = evaluated.recon_status
            if evaluated.recon_status == "PASS":
                pass_count += 1
            elif evaluated.recon_status == "FAIL":
                fail_count += 1
            watermark_advanced = evaluated.recon_status == "PASS" and ct_head > 0
            if watermark_advanced:
                upsert_table_watermark(
                    audit_conn,
                    client.src_db_nm,
                    src_schema,
                    summary.table_name,
                    ct_head,
                    client_nm=client.client_nm,
                    pipeline_key=ctx.pipeline_key,
                )
                upsert_db_watermark(audit_conn, client.src_db_nm, ct_head, client_nm=client.client_nm)
            try:
                record_recon_table_result(
                    audit_conn,
                    recon_run_id=recon_run_id,
                    client_nm=client.client_nm,
                    database_name=client.src_db_nm,
                    schema_name=src_schema,
                    table_name=summary.table_name,
                    pipeline_id=summary.pipeline_id,
                    update_id=summary.update_id,
                    flow_name=summary.flow_name,
                    recon_type=summary.recon_type,
                    watermark_before=watermark_before,
                    ct_head_version=ct_head,
                    pending=pending or CtPendingCounts(),
                    ingest_upserted=evaluated.total_upserted,
                    ingest_deleted=evaluated.total_deleted,
                    ingest_change_rows=evaluated.total_change_rows,
                    sync_status=sync_status,
                    recon_message=evaluated.recon_message,
                    watermark_advanced=watermark_advanced,
                )
            except Exception as exc:
                print(f"WARN record_recon_table_result failed: {exc}")

        if evaluated.recon_status == "PASS":
            ready = ReconReadyRow(
                client_nm=evaluated.client_nm,
                table_nm=evaluated.table_name,
                pipeline_id=evaluated.pipeline_id,
                update_id=evaluated.update_id,
                flow_name=evaluated.flow_name,
                recon_type=evaluated.recon_type,
                ingest_change_rows=evaluated.total_change_rows,
                source_change_rows=evaluated.source_change_rows,
                completed_at=evaluated.last_event_time,
                artifact_run_id=evaluated.update_id,
                ready_for_calc=True,
            )
            ready_written += write_recon_ready_rows(spark, catalog, metadata_schema, [ready])
            process_rows.append(
                build_process_log_row(
                    PROCESS_TYPE_INGEST,
                    ctx.pipeline_key,
                    "SUCCESS",
                    artifact_type=ARTIFACT_TYPE_PIPELINE,
                    artifact_id=evaluated.pipeline_id,
                    artifact_run_id=evaluated.update_id,
                    client_nm=evaluated.client_nm,
                    object_nm=evaluated.table_name,
                    start_tm=evaluated.first_event_time,
                    end_tm=evaluated.last_event_time,
                    detail_status=evaluated.final_flow_status,
                    rows_written=evaluated.total_upserted,
                    rows_deleted=evaluated.total_deleted,
                    log=evaluated.recon_message,
                    recorded_at=datetime.now(timezone.utc),
                )
            )
            print(f"PASS {ctx.pipeline_key} {evaluated.table_name} update={evaluated.update_id}")
        else:
            process_rows.append(
                build_process_log_row(
                    PROCESS_TYPE_INGEST,
                    ctx.pipeline_key,
                    "FAILED",
                    artifact_type=ARTIFACT_TYPE_PIPELINE,
                    artifact_id=evaluated.pipeline_id,
                    artifact_run_id=evaluated.update_id,
                    client_nm=evaluated.client_nm,
                    object_nm=evaluated.table_name,
                    start_tm=evaluated.first_event_time,
                    end_tm=evaluated.last_event_time,
                    detail_status=evaluated.final_flow_status,
                    rows_written=evaluated.total_upserted,
                    rows_deleted=evaluated.total_deleted,
                    log=evaluated.recon_message,
                    recorded_at=datetime.now(timezone.utc),
                )
            )
            print(f"FAIL {ctx.pipeline_key} {evaluated.table_name}: {evaluated.recon_message}")

    if process_rows:
        write_process_log_rows(spark, catalog, metadata_schema, process_rows)

    if audit_conn is not None and recon_run_id:
        run_status = "PASS" if fail_count == 0 and pass_count > 0 else "FAIL" if fail_count else "SKIPPED"
        run_message = f"pass={pass_count} fail={fail_count}"
        try:
            complete_recon_run(audit_conn, recon_run_id, run_status=run_status, run_message=run_message)
            write_audit_log(
                audit_conn,
                "RECON_PIPELINE_COMPLETE",
                client_nm=client.client_nm if client else "",
                database_name=client.src_db_nm if client else "",
                pipeline_id=ctx.pipeline_id,
                update_id=summaries[0].update_id if summaries else "",
                detail={"pipeline_key": ctx.pipeline_key, "pass": pass_count, "fail": fail_count},
            )
        except Exception as exc:
            print(f"WARN complete_recon_run failed: {exc}")
        try:
            audit_conn.close()
        except Exception:
            pass

    return merged, summaries_written, ready_written


def run_all_pipeline_recon(
    spark,
    catalog: str,
    metadata_schema: str,
    pipeline_contexts: list[tuple[Any, str, str, Any]],
    lookback_hours: int = 24,
    dbutils: Any | None = None,
    use_sql_server_audit: bool = True,
    simplified_recon: bool = False,
    simple_pass_rule: str = "auto",
    row_count_only_on_flow_complete: bool = True,
    use_api_update_complete: bool = True,
    table_quiesce_sec: int = 15,
) -> dict[str, int]:
    """Poll hidden event logs only for pipelines with activity; recon changed flows."""
    if simplified_recon and not use_sql_server_audit:
        print("WARN simplified_recon requires use_sql_server_audit=true; using full recon")
        simplified_recon = False

    totals = {
        "metrics": 0,
        "summaries": 0,
        "recon_ready": 0,
        "pipelines": 0,
        "polled": 0,
        "skipped": 0,
        "new_events": 0,
        "ct_pending_tables": 0,
        "waiting_tables": 0,
    }
    rest_client = DatabricksRestClient()
    polled_at = datetime.now(timezone.utc)

    pipeline_ids = [ctx.pipeline_id for ctx, _, _, _ in pipeline_contexts if ctx.pipeline_id]
    for ctx, _, _, _ in pipeline_contexts:
        if not ctx.pipeline_id:
            ctx.pipeline_id = resolve_pipeline_id(ctx.pipeline_key, rest_client)
        if ctx.pipeline_id:
            pipeline_ids.append(ctx.pipeline_id)
    unique_pipeline_ids = sorted(set(pipeline_ids))

    watermark_conn: Any | None = None
    watermark_conn_client: Any | None = None
    pending_event_log_watermarks: dict[str, ReconEventLogWatermark] = {}

    if use_sql_server_audit and pipeline_contexts:
        try:
            watermark_conn_client = pipeline_contexts[0][3]
            watermark_conn, _ = open_audit_connection(
                watermark_conn_client, dbutils=dbutils
            )
            watermarks = read_recon_event_log_watermarks_sql(
                watermark_conn, unique_pipeline_ids
            )
            print(
                f"[recon] loaded {len(watermarks)} event_log watermark(s) from SQL Server"
            )
        except Exception as exc:
            print(f"[recon] WARN SQL watermark read failed, using empty: {exc}")
            watermarks = {}
    else:
        watermarks = read_recon_event_log_watermarks(
            spark,
            catalog,
            metadata_schema,
            unique_pipeline_ids,
        )

    ct_head_cache: dict[str, int] = {}

    for ctx, src_catalog, src_schema, ipac_client in pipeline_contexts:
        totals["pipelines"] += 1
        if not ctx.pipeline_id:
            ctx.pipeline_id = resolve_pipeline_id(ctx.pipeline_key, rest_client)
        if not ctx.pipeline_id:
            print(f"{ctx.pipeline_key}: SKIP no pipeline_id")
            totals["skipped"] += 1
            continue

        detail = rest_client.get(f"/api/2.0/pipelines/{ctx.pipeline_id}") or {}
        watermark = watermarks.get(ctx.pipeline_id)
        needs_poll, reason = pipeline_needs_event_log_poll(detail, watermark)
        print(f"[recon] {ctx.pipeline_key} {describe_pipeline_status(detail)}")
        if watermark is not None:
            print(
                f"[recon] {ctx.pipeline_key} stored watermark: "
                f"last_update_id={watermark.last_update_id or 'n/a'} "
                f"last_api_state={watermark.last_api_update_state or 'n/a'} "
                f"last_event_ts={watermark.last_event_ts or 'n/a'}"
            )

        ct_pending_probe: list[PendingCtTable] = []
        if simplified_recon and use_sql_server_audit:
            try:
                probe_conn, _ = open_audit_connection(ipac_client, dbutils=dbutils)
                active_tables = [c.table_nm for c in ctx.tables]
                ct_pending_probe = discover_pending_ct_tables(
                    probe_conn, ipac_client, src_schema, active_tables
                )
                probe_conn.close()
            except Exception as exc:
                print(f"{ctx.pipeline_key}: WARN CT probe failed: {exc}")

            if not ct_pending_probe:
                print(f"{ctx.pipeline_key}: SKIP no pending CT on configured tables")
                totals["skipped"] += 1
                continue

            needs_poll = True
            reason = f"CT pending on {len(ct_pending_probe)} table(s)"

        if not needs_poll:
            pending_event_log_watermarks[ctx.pipeline_id] = _watermark_from_rows_and_api(
                ctx.pipeline_id,
                ctx.pipeline_key,
                [],
                detail,
                watermark,
                polled_at,
            )
            print(f"{ctx.pipeline_key}: SKIP no activity ({reason})")
            totals["skipped"] += 1
            continue

        since_ts = watermark.last_event_ts if watermark else None
        print(f"{ctx.pipeline_key}: polling hidden event_log ({reason})")
        pipeline_rows = fetch_flow_progress_rows(
            spark,
            ctx.pipeline_id,
            lookback_hours=lookback_hours,
            since_timestamp=since_ts,
        )
        totals["polled"] += 1
        totals["new_events"] += len(pipeline_rows)

        pending_event_log_watermarks[ctx.pipeline_id] = _watermark_from_rows_and_api(
            ctx.pipeline_id,
            ctx.pipeline_key,
            pipeline_rows,
            detail,
            watermark,
            polled_at,
        )

        if not pipeline_rows:
            print(f"{ctx.pipeline_key}: no new flow_progress events")
            if not simplified_recon:
                continue
            print(
                f"{ctx.pipeline_key}: continuing simplified recon "
                "(CT pending / API last_update tracking)"
            )

        if simplified_recon:
            r, ct_n, wait_n = run_simplified_pipeline_recon(
                spark,
                catalog,
                metadata_schema,
                ctx,
                src_schema,
                pipeline_rows=pipeline_rows,
                client=ipac_client,
                dbutils=dbutils,
                pass_rule=simple_pass_rule,
                row_count_only_on_flow_complete=row_count_only_on_flow_complete,
                pipeline_detail=detail,
                use_api_update_complete=use_api_update_complete,
                event_log_watermark=watermark,
                ct_head_cache=ct_head_cache,
                table_quiesce_sec=table_quiesce_sec,
            )
            totals["recon_ready"] += r
            totals["ct_pending_tables"] += ct_n
            totals["waiting_tables"] += wait_n
            print(
                f"{ctx.pipeline_key}: simplified recon_ready={r} "
                f"ct_pending={ct_n} waiting={wait_n}"
            )
        else:
            m, s, r = run_pipeline_recon(
                spark,
                catalog,
                metadata_schema,
                ctx,
                src_catalog,
                src_schema,
                pipeline_rows=pipeline_rows,
                rest_client=rest_client,
                client=ipac_client,
                dbutils=dbutils,
                use_sql_server_audit=use_sql_server_audit,
            )
            totals["metrics"] += m
            totals["summaries"] += s
            totals["recon_ready"] += r
            print(f"{ctx.pipeline_key}: metrics_merged={m} summaries={s} recon_ready={r}")

    if pending_event_log_watermarks and watermark_conn is not None:
        try:
            flushed = flush_recon_event_log_watermarks_sql(
                watermark_conn, pending_event_log_watermarks
            )
            print(f"[recon] flushed {flushed} event_log watermark(s) to SQL Server")
        except Exception as exc:
            print(f"[recon] WARN SQL watermark flush failed: {exc}")
    if watermark_conn is not None:
        try:
            watermark_conn.close()
        except Exception:
            pass

    return totals


def build_contexts_for_client(
    client: Any,
    effective_tables: list[Any],
    dest_schema_suffix: str,
    pipeline_keys: list[str],
) -> list[tuple[Any, str, str, Any]]:
    raw_schema = client.raw_schema(dest_schema_suffix)
    table_cfgs = table_configs_from_effective(client.client_nm, raw_schema, effective_tables)
    src_catalog = client.src_db_nm
    src_schema = client.src_db_schema or "dbo"
    out: list[tuple[Any, str, str, Any]] = []
    for key in pipeline_keys:
        pipeline_key = normalize_pipeline_key(key)
        if not pipeline_key.startswith("p_"):
            continue
        if client.client_nm not in pipeline_key:
            continue
        ctx = build_pipeline_recon_context(pipeline_key, table_cfgs)
        out.append((ctx, src_catalog, src_schema, client))
    return out
