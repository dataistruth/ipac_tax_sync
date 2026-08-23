"""Orchestrate ingestion flow metrics reconciliation per pipeline."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
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
    fetch_sql_row_counts_batch,
    flush_recon_event_log_watermarks_sql,
    insert_recon_run,
    open_audit_connection,
    read_db_watermark,
    read_recon_event_log_watermarks_sql,
    record_recon_table_result,
    read_recon_batch_detected_at,
    record_recon_batch_detected,
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
    - Lakeflow Connect streaming targets (STREAMING_TABLE): COUNT(1) —
      DeltaTable.forName and DESCRIBE DETAIL are not supported on UC streaming tables.
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
            row = spark.sql(f"SELECT COUNT(1) AS cnt FROM {target}").collect()[0]
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


RECON_TYPE_ROW_COUNT = 2


def _uses_row_count_validation(recon_type: int) -> bool:
    """recon_type 2 → SQL vs Delta COUNT; other types → delta history ts after SQL CT."""
    return recon_type == RECON_TYPE_ROW_COUNT


def split_pending_by_recon_strategy(
    pending_tables: list[PendingCtTable],
    ctx: Any,
) -> tuple[list[PendingCtTable], list[PendingCtTable]]:
    row_count_pending: list[PendingCtTable] = []
    delta_ts_pending: list[PendingCtTable] = []
    for probe in pending_tables:
        if _uses_row_count_validation(_recon_type_for_table(ctx, probe.table_name)):
            row_count_pending.append(probe)
        else:
            delta_ts_pending.append(probe)
    return row_count_pending, delta_ts_pending


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
    Per-table gate: Delta write (DESCRIBE HISTORY MERGE) after SQL CT reference + quiesce.
    """
    return _evaluate_table_refresh_after_reference(
        table_refresh,
        sql_ct_reference_at,
        quiesce_sec,
        reference_label="sql_ct_reference",
    )


def evaluate_table_refresh_after_ct_detected(
    table_refresh: dict[str, Any] | None,
    ct_detected_at: datetime | None,
    quiesce_sec: int = 10,
) -> tuple[str, str]:
    """
    Per-table gate: Delta last_write must be after CT batch first-detected time + quiesce.
    """
    return _evaluate_table_refresh_after_reference(
        table_refresh,
        ct_detected_at,
        quiesce_sec,
        reference_label="ct_detected",
    )


def _evaluate_table_refresh_after_reference(
    table_refresh: dict[str, Any] | None,
    reference_at: datetime | None,
    quiesce_sec: int,
    reference_label: str,
) -> tuple[str, str]:
    if reference_at is None:
        label = "SQL CT reference" if reference_label == "sql_ct_reference" else "CT detected"
        return "WAITING", f"{label} timestamp unavailable"

    if table_refresh is None or table_refresh.get("last_refreshed_at") is None:
        return "WAITING", "no delta history write timestamp (last_refreshed_at)"

    refresh_dt = _ensure_utc(table_refresh["last_refreshed_at"])
    ref_dt = _ensure_utc(reference_at)
    deadline = ref_dt + timedelta(seconds=max(0, int(quiesce_sec)))

    if refresh_dt < deadline:
        return (
            "WAITING",
            f"delta last_write={refresh_dt.isoformat()} before "
            f"{reference_label}+{quiesce_sec}s ({deadline.isoformat()})",
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
            f"delta write after {reference_label}: table={table_name} "
            f"last_write={refresh_dt.isoformat()} "
            f"{reference_label}={ref_dt.isoformat()} "
            f"operation={refresh_status or 'n/a'}{version_note}"
        ),
    )


def _row_count_verified_key(database_name: str, table_nm: str, ct_head_version: int) -> str:
    return f"{database_name.casefold()}|{table_nm.casefold()}|{ct_head_version}"


def _ct_batch_key(database_name: str, ct_head_version: int) -> str:
    return f"{database_name.casefold()}|{ct_head_version}"


def resolve_batch_detected_at(
    conn: Any,
    database_name: str,
    ct_head_version: int,
    batch_detected: dict[str, datetime],
) -> datetime | None:
    """In-memory cache first, then SQL audit (first poll that detected this ct_head)."""
    batch_key = _ct_batch_key(database_name, ct_head_version)
    cached = batch_detected.get(batch_key)
    if cached is not None:
        return cached
    sql_at = read_recon_batch_detected_at(conn, database_name, ct_head_version)
    if sql_at is not None:
        batch_detected[batch_key] = sql_at
    return sql_at


def mark_batch_detected(
    conn: Any,
    client: Any,
    ctx: Any,
    ct_head_version: int,
    batch_detected: dict[str, datetime],
) -> datetime | None:
    """Record first time this database+ct_head entered the recon queue."""
    batch_key = _ct_batch_key(client.src_db_nm, ct_head_version)
    if batch_key in batch_detected:
        return batch_detected[batch_key]
    detected_at = record_recon_batch_detected(
        conn,
        client.src_db_nm,
        ct_head_version,
        client_nm=client.client_nm,
        pipeline_id=ctx.pipeline_id or "",
    )
    batch_detected[batch_key] = detected_at
    return detected_at


def clear_recon_batch_state(
    conn: Any,
    client: Any,
    ctx: Any,
    ct_head_version: int,
    batch_detected: dict[str, datetime],
    verified_cache: dict[str, RowCountVerified],
    history_verified_cache: dict[str, DeltaHistoryVerified] | None = None,
) -> None:
    """
    After PASS: drop in-memory batch timer and prefetch caches for this ct_head.
    Next CT activity (new ct_head) starts a fresh detection via mark_batch_detected.
    """
    batch_key = _ct_batch_key(client.src_db_nm, ct_head_version)
    if batch_key in batch_detected:
        batch_detected.pop(batch_key, None)
    db_prefix = client.src_db_nm.casefold() + "|"
    head_suffix = f"|{int(ct_head_version)}"
    for key in list(verified_cache.keys()):
        if key.startswith(db_prefix) and key.endswith(head_suffix):
            verified_cache.pop(key, None)
    if history_verified_cache is not None:
        for key in list(history_verified_cache.keys()):
            if key.startswith(db_prefix) and key.endswith(head_suffix):
                history_verified_cache.pop(key, None)
    try:
        write_audit_log(
            conn,
            "RECON_BATCH_COMPLETED",
            client_nm=client.client_nm,
            database_name=client.src_db_nm,
            pipeline_id=ctx.pipeline_id or "",
            detail={"ct_head_version": int(ct_head_version)},
        )
    except Exception as exc:
        print(f"[recon] WARN RECON_BATCH_COMPLETED audit: {exc}")


@dataclass(frozen=True)
class RowCountVerified:
    sql_count: int
    delta_count: int
    ct_head_version: int


@dataclass(frozen=True)
class DeltaHistoryVerified:
    ct_head_version: int
    table_refresh: dict[str, Any]


def _is_row_count_verified(
    database_name: str,
    table_nm: str,
    ct_head_version: int,
    verified_cache: dict[str, RowCountVerified],
) -> bool:
    vk = _row_count_verified_key(database_name, table_nm, ct_head_version)
    entry = verified_cache.get(vk)
    return entry is not None and entry.ct_head_version == ct_head_version


def _is_delta_history_verified(
    database_name: str,
    table_nm: str,
    ct_head_version: int,
    verified_cache: dict[str, DeltaHistoryVerified],
) -> bool:
    vk = _row_count_verified_key(database_name, table_nm, ct_head_version)
    entry = verified_cache.get(vk)
    return entry is not None and entry.ct_head_version == ct_head_version


def _parallel_delta_row_count(
    spark,
    catalog: str,
    ctx: Any,
    table_nm: str,
) -> tuple[str, str, int | None]:
    dest_schema = _destination_schema_for_table(ctx, table_nm)
    if not dest_schema:
        return table_nm.casefold(), "delta", None
    try:
        return table_nm.casefold(), "delta", count_delta_table_rows(
            spark, catalog, dest_schema, table_nm
        )
    except Exception as exc:
        print(f"[recon] WARN parallel delta_count {table_nm}: {exc}")
        return table_nm.casefold(), "delta", None


def _parallel_table_refresh(
    spark,
    catalog: str,
    ctx: Any,
    table_nm: str,
) -> tuple[str, str, dict[str, Any] | None]:
    dest_schema = _destination_schema_for_table(ctx, table_nm)
    if not dest_schema:
        return table_nm.casefold(), "history", None
    try:
        return table_nm.casefold(), "history", fetch_streaming_table_refresh_info(
            spark, catalog, dest_schema, table_nm
        )
    except Exception as exc:
        print(f"[recon] WARN parallel delta_history {table_nm}: {exc}")
        return table_nm.casefold(), "history", None


def prefetch_ct_delta_parallel(
    spark,
    catalog: str,
    ctx: Any,
    history_probes: list[PendingCtTable],
    sample_names: list[str],
    conn: Any,
    src_schema: str,
    database_name: str,
    pending_by_table: dict[str, PendingCtTable],
    verified_cache: dict[str, RowCountVerified],
    *,
    max_workers: int = 10,
) -> tuple[dict[str, dict[str, Any] | None], dict[str, tuple[int | None, int | None]]]:
    """
    Prefetch delta history metadata and SQL/Delta row counts for recon samples.

    SQL row counts use one UNION batch; UC spark.sql calls run sequentially
    (Spark sessions are not thread-safe).
    """
    table_refresh_cache: dict[str, dict[str, Any] | None] = {}
    row_count_cache: dict[str, tuple[int | None, int | None]] = {}

    need_count_names: list[str] = []
    skipped_verified = 0
    for table_nm in sample_names:
        key = table_nm.casefold()
        probe = pending_by_table.get(key)
        if probe is None:
            need_count_names.append(table_nm)
            continue
        if _is_row_count_verified(
            database_name, table_nm, probe.ct_head_version, verified_cache
        ):
            entry = verified_cache[
                _row_count_verified_key(database_name, table_nm, probe.ct_head_version)
            ]
            row_count_cache[key] = (entry.sql_count, entry.delta_count)
            skipped_verified += 1
        else:
            need_count_names.append(table_nm)

    if skipped_verified:
        print(
            f"[recon] row_count cache hit: {skipped_verified}/{len(sample_names)} "
            "table(s) skip SQL+Delta recount"
        )

    sql_counts: dict[str, int | None] = {}
    if need_count_names:
        sql_counts = fetch_sql_row_counts_batch(conn, src_schema, need_count_names)

    task_count = len(history_probes) + len(need_count_names)
    if task_count == 0:
        return table_refresh_cache, row_count_cache

    # Spark sessions are not thread-safe; parallel spark.sql often stalls or returns
    # empty results. Run UC calls sequentially (SQL counts are already one batch).
    for probe in history_probes:
        key, _, value = _parallel_table_refresh(spark, catalog, ctx, probe.table_name)
        table_refresh_cache[key] = value
    for table_nm in need_count_names:
        key, _, delta_val = _parallel_delta_row_count(spark, catalog, ctx, table_nm)
        row_count_cache[key] = (sql_counts.get(key), delta_val)

    return table_refresh_cache, row_count_cache


def prefetch_row_count_samples(
    spark,
    catalog: str,
    ctx: Any,
    src_schema: str,
    table_names: list[str],
    conn: Any,
    pending_by_table: dict[str, PendingCtTable],
    database_name: str,
    verified_cache: dict[str, RowCountVerified] | None = None,
    *,
    max_delta_workers: int = 10,
) -> dict[str, tuple[int | None, int | None]]:
    """Row-count sample only (tests); production uses prefetch_ct_delta_parallel."""
    _, row_counts = prefetch_ct_delta_parallel(
        spark,
        catalog,
        ctx,
        [],
        table_names,
        conn,
        src_schema,
        database_name,
        pending_by_table,
        verified_cache if verified_cache is not None else {},
        max_workers=max_delta_workers,
    )
    return row_counts


def _select_prefetch_sample_tables(
    pending_tables: list[PendingCtTable],
    sample_size: int,
    database_name: str,
    verified_cache: dict[str, Any],
    is_verified: Any,
) -> set[str]:
    """Highest pending CT first; skip tables already verified for this ct_head."""
    if sample_size <= 0 or not pending_tables:
        return set()
    candidates = [
        p
        for p in pending_tables
        if not is_verified(
            database_name, p.table_name, p.ct_head_version, verified_cache
        )
    ]
    ranked = sorted(candidates, key=lambda p: p.pending.total, reverse=True)
    return {p.table_name.casefold() for p in ranked[:sample_size]}


def select_history_sample_tables(
    pending_tables: list[PendingCtTable],
    sample_size: int = 5,
    database_name: str = "",
    history_verified_cache: dict[str, DeltaHistoryVerified] | None = None,
) -> set[str]:
    """Up to sample_size tables needing DESCRIBE HISTORY this poll."""
    cache = history_verified_cache if history_verified_cache is not None else {}
    return _select_prefetch_sample_tables(
        pending_tables,
        sample_size,
        database_name,
        cache,
        _is_delta_history_verified,
    )


def select_row_count_sample_tables(
    pending_tables: list[PendingCtTable],
    sample_size: int = 5,
    database_name: str = "",
    verified_cache: dict[str, RowCountVerified] | None = None,
) -> set[str]:
    """Up to sample_size tables needing SQL vs Delta row count this poll."""
    cache = verified_cache if verified_cache is not None else {}
    return _select_prefetch_sample_tables(
        pending_tables,
        sample_size,
        database_name,
        cache,
        _is_row_count_verified,
    )


def select_disjoint_history_sample_tables(
    pending_tables: list[PendingCtTable],
    exclude: set[str],
    sample_size: int = 5,
    database_name: str = "",
    history_verified_cache: dict[str, DeltaHistoryVerified] | None = None,
) -> set[str]:
    """History sample from pending tables not already in row_count sample."""
    candidates = [
        p for p in pending_tables if p.table_name.casefold() not in exclude
    ]
    return select_history_sample_tables(
        candidates,
        sample_size,
        database_name,
        history_verified_cache,
    )


def log_changed_tables_for_recon(
    database_name: str,
    pending_tables: list[PendingCtTable],
) -> None:
    """Print each CT-changed table with pending counts for this database."""
    print(
        f"[recon] database={database_name}: {len(pending_tables)} table(s) changed"
    )
    for probe in pending_tables:
        print(
            f"[recon] database={database_name} table={probe.table_name} "
            f"pending I/U/D={probe.pending.inserts}/{probe.pending.updates}/{probe.pending.deletes} "
            f"ct_versions={probe.watermark_before}..{probe.ct_head_version}"
        )


def evaluate_ct_delta_timestamp_recon(
    table_refresh: dict[str, Any] | None,
    ct_detected_at: datetime | None,
    quiesce_sec: int,
    pending: CtPendingCounts,
) -> tuple[str, str]:
    """PASS when Delta last_write is after CT batch detected_at + quiesce_sec."""
    status, message = evaluate_table_refresh_after_ct_detected(
        table_refresh,
        ct_detected_at,
        quiesce_sec=quiesce_sec,
    )
    if status != "PASS":
        return status, f"ct_delta_ts: {message}"
    return (
        "PASS",
        f"ct_delta_ts: CT pending={pending.total}; {message}",
    )


def evaluate_ct_delta_ts_after_sql_ct(
    table_refresh: dict[str, Any] | None,
    sql_ct_reference_at: datetime | None,
    quiesce_sec: int,
    pending: CtPendingCounts,
) -> tuple[str, str]:
    """PASS when Delta last_write is after SQL CT version-change timestamp + quiesce."""
    status, message = evaluate_table_refresh_after_sql_ct(
        table_refresh,
        sql_ct_reference_at,
        quiesce_sec=quiesce_sec,
    )
    if status != "PASS":
        return status, f"ct_delta_ts: {message}"
    return (
        "PASS",
        f"ct_delta_ts: CT pending={pending.total}; {message}",
    )


def evaluate_ct_row_count_recon(
    pending: CtPendingCounts,
    sql_count: int | None,
    delta_count: int | None,
    *,
    require_row_count: bool = True,
) -> tuple[str, str]:
    """CT-changed tables only: PASS when SQL COUNT matches Delta COUNT."""
    if not require_row_count:
        return (
            "WAITING",
            f"ct_row_count: CT pending={pending.total}; "
            "row_count deferred (not in sample this poll)",
        )
    if sql_count is None or delta_count is None:
        return (
            "WAITING",
            f"row_count unavailable sql={sql_count} delta={delta_count}",
        )
    if sql_count != delta_count:
        return (
            "FAIL",
            f"row_count mismatch sql={sql_count} delta={delta_count}",
        )
    return (
        "PASS",
        f"ct_row_count: CT pending={pending.total} match sql={sql_count} delta={delta_count}",
    )


def evaluate_ct_delta_history_recon(
    table_refresh: dict[str, Any] | None,
    sql_ct_reference_at: datetime | None,
    quiesce_sec: int,
    pending: CtPendingCounts,
    recon_type: int,
    sql_row_count: int | None,
    delta_row_count: int | None,
    require_row_count: bool,
) -> tuple[str, str]:
    """
    CT-driven recon without flow_metrics / flow COMPLETED:
    1) Delta DESCRIBE HISTORY write after SQL CT version-change timestamp
    2) Optional row-count match for sampled tables
    """
    status, message = evaluate_table_refresh_after_sql_ct(
        table_refresh, sql_ct_reference_at, quiesce_sec=quiesce_sec
    )
    if status != "PASS":
        return status, message

    if require_row_count:
        if sql_row_count is None or delta_row_count is None:
            return "WAITING", "row_count sample unavailable"
        if sql_row_count != delta_row_count:
            return (
                "FAIL",
                f"row_count sample mismatch sql={sql_row_count} delta={delta_row_count}",
            )
        count_note = f" sample_match sql={sql_row_count} delta={delta_row_count}"
    else:
        count_note = ""

    source_metric = pending.metric_for_recon_type(recon_type)
    return (
        "PASS",
        (
            f"ct_delta_history: CT pending={source_metric}; "
            f"{message}{count_note}"
        ),
    )


def log_db_ct_recon_queue(
    conn: Any,
    client: Any,
    pending_tables: list[PendingCtTable],
) -> None:
    """Log DB-level CT watermark vs head; list changed tables on recon queue."""
    try:
        db_wm = read_db_watermark(conn, client.src_db_nm)
        ct_head = fetch_change_tracking_current_version(conn)
    except Exception as exc:
        print(f"[recon] WARN db CT watermark read failed: {exc}")
        return

    if db_wm is None:
        print(
            f"[recon] db={client.src_db_nm}: no ct_db_watermark row — "
            "baseline required before recon"
        )
    elif ct_head is not None:
        delta = ct_head - db_wm.last_version
        print(
            f"[recon] db={client.src_db_nm}: ct_db_watermark={db_wm.last_version} "
            f"ct_head={ct_head} version_delta={delta}"
        )
        if delta > 0:
            print(f"[recon] db={client.src_db_nm}: on recon queue (CT version advanced)")


PASS_RULES_DELTA_HISTORY = frozenset(
    {"ingest_quiesce", "ct_delta_history", "table_after_ct"}
)

DATABASE_RECON_FLOW_NAME = "__database_recon__"
DATABASE_RECON_TABLE_NM = "__database__"


@dataclass
class SimplifiedTableOutcome:
    table_nm: str
    schema_name: str
    recon_type: int
    probe: PendingCtTable
    status: str
    message: str
    ingest_change_rows: int = 0
    sql_count: int | None = None
    delta_count: int | None = None
    table_refresh: dict[str, Any] | None = None
    update_id: str = ""
    flow_name: str = ""


def _table_entry_for_json(outcome: SimplifiedTableOutcome) -> dict[str, Any]:
    probe = outcome.probe
    refresh = outcome.table_refresh or {}
    return {
        "schema_name": outcome.schema_name,
        "table_name": outcome.table_nm,
        "recon_type": outcome.recon_type,
        "watermark_before": probe.watermark_before,
        "ct_head_version": probe.ct_head_version,
        "pending_inserts": probe.pending.inserts,
        "pending_updates": probe.pending.updates,
        "pending_deletes": probe.pending.deletes,
        "pending_total": probe.pending.total,
        "sql_ct_reference_at": (
            probe.sql_ct_reference_at.isoformat()
            if probe.sql_ct_reference_at
            else None
        ),
        "sql_count": outcome.sql_count,
        "delta_count": outcome.delta_count,
        "delta_version": refresh.get("delta_version"),
        "last_write_at": (
            refresh.get("last_refreshed_at").isoformat()
            if refresh.get("last_refreshed_at") is not None
            and hasattr(refresh.get("last_refreshed_at"), "isoformat")
            else str(refresh.get("last_refreshed_at") or "")
        ),
        "delta_operation": refresh.get("latest_refresh_status"),
        "status": outcome.status,
        "message": outcome.message,
    }


def build_database_tables_json(outcomes: list[SimplifiedTableOutcome]) -> str:
    payload = {"tables": [_table_entry_for_json(o) for o in outcomes]}
    return json.dumps(payload, default=str)


def build_database_recon_ready_row(
    client: Any,
    ctx: Any,
    outcomes: list[SimplifiedTableOutcome],
    *,
    pipeline_id: str,
    update_id: str,
    ct_watermark_before: int | None,
    ct_head_version: int | None,
    completed_at: datetime,
    total_ingestion_sec: int | None = None,
) -> ReconReadyRow:
    source_total = sum(
        o.probe.pending.metric_for_recon_type(o.recon_type) for o in outcomes
    )
    ingest_total = sum(o.ingest_change_rows for o in outcomes)
    db_name = client.src_db_nm
    return ReconReadyRow(
        client_nm=client.client_nm,
        table_nm=DATABASE_RECON_TABLE_NM,
        database_name=db_name,
        pipeline_id=pipeline_id,
        update_id=update_id,
        flow_name=DATABASE_RECON_FLOW_NAME,
        recon_type=1,
        ingest_change_rows=ingest_total,
        source_change_rows=source_total,
        completed_at=completed_at,
        artifact_run_id=update_id,
        ready_for_calc=True,
        tables_json=build_database_tables_json(outcomes),
        ct_watermark_before=ct_watermark_before,
        ct_head_version=ct_head_version,
        total_ingestion_sec=total_ingestion_sec,
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
    row_count_verified_cache: dict[str, RowCountVerified] | None = None,
    delta_history_verified_cache: dict[str, DeltaHistoryVerified] | None = None,
    ct_batch_detected_at: dict[str, datetime] | None = None,
    table_quiesce_sec: int = 15,
    row_count_sample_size: int = 5,
    history_sample_size: int = 5,
    uc_parallel_workers: int = 10,
    pending_ct_tables: list[PendingCtTable] | None = None,
) -> tuple[int, int, int]:
    """
  CT-driven recon for one pipeline: only tables with pending CT since watermark.
  Writes recon_ready (Delta) + SQL audit/watermarks on PASS. No flow_metrics/summary Delta writes.
  Returns (recon_ready_written, ct_pending_tables, waiting_tables).
    """
    if not ctx.pipeline_id:
        ctx.pipeline_id = resolve_pipeline_id(ctx.pipeline_key)

    active_tables = [cfg.table_nm for cfg in ctx.tables]
    if pass_rule == "ct_row_count":
        api_snap = {}
        api_update_complete = False
        print(
            f"[recon] pipeline={ctx.pipeline_key} client={client.client_nm} "
            f"sql_db={client.src_db_nm} pipeline_id={ctx.pipeline_id or 'n/a'} "
            f"active_tables={len(active_tables)} pass_rule=ct_row_count "
            f"(recon_type 2=row_count, else=delta_ts after sql_ct_reference) "
            f"row_count_sample_size={row_count_sample_size} "
            f"quiesce_after_sql_ct_sec={table_quiesce_sec} "
            f"uc_parallel_workers={uc_parallel_workers}"
        )
    else:
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
            f"table_quiesce_sec={table_quiesce_sec} "
            f"row_count_sample_size={row_count_sample_size} "
            f"history_sample_size={history_sample_size} "
            f"uc_parallel_workers={uc_parallel_workers}"
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

    pending_tables = (
        list(pending_ct_tables)
        if pending_ct_tables is not None
        else discover_pending_ct_tables(conn, client, src_schema, active_tables)
    )
    if pending_ct_tables is not None:
        print(
            f"[recon] {ctx.pipeline_key}: reusing CT probe "
            f"({len(pending_tables)} pending table(s), skip rediscover)"
        )
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

    log_db_ct_recon_queue(conn, client, pending_tables)
    log_changed_tables_for_recon(client.src_db_nm, pending_tables)

    batch_detected = ct_batch_detected_at if ct_batch_detected_at is not None else {}
    ct_batch_detected_at_value: datetime | None = None
    pending_by_table_early: dict[str, PendingCtTable] = {}
    if pending_tables and pending_tables[0].ct_head_version is not None:
        pending_by_table_early = {p.table_name.casefold(): p for p in pending_tables}
        detected_at = mark_batch_detected(
            conn,
            client,
            ctx,
            pending_tables[0].ct_head_version,
            batch_detected,
        )
        if detected_at is not None:
            ct_batch_detected_at_value = detected_at
            print(
                f"[recon] {ctx.pipeline_key}: CT batch tracking "
                f"ct_head={pending_tables[0].ct_head_version} "
                f"detected_at={detected_at.isoformat()}"
            )

    verified_cache = (
        row_count_verified_cache if row_count_verified_cache is not None else {}
    )
    history_verified_cache = (
        delta_history_verified_cache if delta_history_verified_cache is not None else {}
    )
    pending_by_table = pending_by_table_early or {
        p.table_name.casefold(): p for p in pending_tables
    }

    row_count_sample: set[str] = set()
    history_sample: set[str] = set()
    row_count_pending: list[PendingCtTable] = []
    delta_ts_pending: list[PendingCtTable] = []
    if pass_rule == "ct_row_count":
        row_count_pending, delta_ts_pending = split_pending_by_recon_strategy(
            pending_tables, ctx
        )
        if row_count_sample_size > 0:
            row_count_sample = select_row_count_sample_tables(
                row_count_pending,
                row_count_sample_size,
                client.src_db_nm,
                verified_cache,
            )
        else:
            row_count_sample = {
                p.table_name.casefold() for p in row_count_pending
            }
        row_count_list = sorted(row_count_sample)
        delta_ts_list = sorted(p.table_name.casefold() for p in delta_ts_pending)
        print(
            f"[recon] database={client.src_db_nm}: recon by recon_type — "
            f"changed={len(pending_tables)} "
            f"recon_type_2_row_count={len(row_count_pending)} "
            f"({', '.join(row_count_list) or 'none'}) "
            f"recon_type_1_delta_ts={len(delta_ts_pending)} "
            f"({', '.join(delta_ts_list) or 'none'}) "
            f"quiesce_after_sql_ct_sec={table_quiesce_sec}"
        )
    elif pass_rule == "ct_delta_history":
        history_sample = select_history_sample_tables(
            pending_tables,
            history_sample_size,
            client.src_db_nm,
            history_verified_cache,
        )
        row_count_sample = select_row_count_sample_tables(
            pending_tables,
            row_count_sample_size,
            client.src_db_nm,
            verified_cache,
        )
        history_list = sorted(history_sample)
        sample_list = sorted(row_count_sample)
        print(
            f"[recon] {ctx.pipeline_key}: history sample "
            f"({len(history_list)}/{len(pending_tables)} tables): "
            f"{', '.join(history_list) or 'none'}"
        )
        print(
            f"[recon] {ctx.pipeline_key}: row_count sample "
            f"({len(sample_list)}/{len(pending_tables)} tables): "
            f"{', '.join(sample_list) or 'none'}"
        )

    row_count_cache: dict[str, tuple[int | None, int | None]] = {}
    table_refresh_cache: dict[str, dict[str, Any] | None] = {}
    if pass_rule == "ct_row_count" and pending_tables:
        sample_names = [
            p.table_name
            for p in row_count_pending
            if p.table_name.casefold() in row_count_sample
        ]
        history_probes = list(delta_ts_pending)
        prefetch_start = time.perf_counter()
        table_refresh_cache, row_count_cache = prefetch_ct_delta_parallel(
            spark,
            catalog,
            ctx,
            history_probes,
            sample_names,
            conn,
            src_schema,
            client.src_db_nm,
            pending_by_table,
            verified_cache,
            max_workers=uc_parallel_workers,
        )
        prefetch_elapsed = time.perf_counter() - prefetch_start
        print(
            f"[recon] {ctx.pipeline_key}: uc prefetch "
            f"row_count_tables={len(sample_names)} "
            f"delta_history_ts_tables={len(history_probes)} "
            f"elapsed={prefetch_elapsed:.1f}s (UC sequential)"
        )
        for table_nm in sample_names:
            key = table_nm.casefold()
            sql_count, delta_count = row_count_cache.get(key, (None, None))
            dest_schema = _destination_schema_for_table(ctx, table_nm)
            uc_ref = (
                resolve_uc_table_ref(spark, catalog, dest_schema, table_nm)
                if dest_schema
                else None
            )
            resolved = (
                uc_ref.name if uc_ref else f"{catalog}.{dest_schema}.{table_nm}"
            )
            print(
                f"[recon] {ctx.pipeline_key} {table_nm}: row_count "
                f"sql_count={sql_count} delta_count={delta_count} {resolved}"
            )
        for probe in history_probes:
            table_nm = probe.table_name
            table_refresh = table_refresh_cache.get(table_nm.casefold())
            if table_refresh:
                print(
                    f"[recon] {ctx.pipeline_key} {table_nm}: delta history ts "
                    f"source={table_refresh.get('source') or 'n/a'} "
                    f"last_write={table_refresh.get('last_refreshed_at')} "
                    f"op={table_refresh.get('latest_refresh_status') or 'n/a'} "
                    f"delta_version={table_refresh.get('delta_version') or 'n/a'}"
                )
            else:
                print(
                    f"[recon] WARN {ctx.pipeline_key} {table_nm}: "
                    "no delta history write metadata for ts check"
                )
    elif pass_rule == "ct_delta_history" and pending_tables:
        history_probes = [
            p for p in pending_tables if p.table_name.casefold() in history_sample
        ]
        sample_names = [
            p.table_name
            for p in pending_tables
            if p.table_name.casefold() in row_count_sample
        ]
        prefetch_start = time.perf_counter()
        table_refresh_cache, row_count_cache = prefetch_ct_delta_parallel(
            spark,
            catalog,
            ctx,
            history_probes,
            sample_names,
            conn,
            src_schema,
            client.src_db_nm,
            pending_by_table,
            verified_cache,
            max_workers=uc_parallel_workers,
        )
        prefetch_elapsed = time.perf_counter() - prefetch_start
        workers_used = max(
            1,
            min(
                uc_parallel_workers,
                len(history_probes) + len(sample_names),
            ),
        )
        print(
            f"[recon] {ctx.pipeline_key}: uc_parallel prefetch "
            f"workers={workers_used} history_tables={len(history_probes)} "
            f"row_count_sample={len(sample_names)} elapsed={prefetch_elapsed:.1f}s"
        )
        for table_nm in sample_names:
            key = table_nm.casefold()
            sql_count, delta_count = row_count_cache.get(key, (None, None))
            dest_schema = _destination_schema_for_table(ctx, table_nm)
            uc_ref = (
                resolve_uc_table_ref(spark, catalog, dest_schema, table_nm)
                if dest_schema
                else None
            )
            resolved = (
                uc_ref.name if uc_ref else f"{catalog}.{dest_schema}.{table_nm}"
            )
            print(
                f"[recon] {ctx.pipeline_key} {table_nm}: row_count sample "
                f"sql_count={sql_count} delta_count={delta_count} {resolved}"
            )

    ready_written = 0
    waiting_count = 0
    delta_after_ct_pass = 0
    delta_after_ct_wait = 0
    row_count_pass = 0
    row_count_wait = 0
    row_count_fail = 0
    history_ts_pass = 0
    history_ts_wait = 0
    history_ts_fail = 0
    run_id = recon_run_id
    if run_id is None:
        sample_update = (
            str(pipeline_rows[0].get("update_id") or "")
            if pipeline_rows
            else str(api_snap.get("update_id") or "")
        )
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

    db_watermark_before: int | None = None
    try:
        db_wm = read_db_watermark(conn, client.src_db_nm)
        if db_wm is not None:
            db_watermark_before = db_wm.last_version
    except Exception:
        pass

    table_outcomes: list[SimplifiedTableOutcome] = []

    for probe in pending_tables:
        table_nm = probe.table_name
        recon_type = _recon_type_for_table(ctx, table_nm)
        dest_schema = _destination_schema_for_table(ctx, table_nm)
        print(
            f"[recon] {ctx.pipeline_key} table={table_nm} "
            f"ct_versions={probe.watermark_before}..{probe.ct_head_version} "
            f"pending I/U/D={probe.pending.inserts}/{probe.pending.updates}/{probe.pending.deletes} "
            f"recon_type={recon_type} "
            f"recon_path={'row_count' if _uses_row_count_validation(recon_type) else 'delta_ts'} "
            f"watermark_updated_at={probe.watermark_updated_at or 'n/a'} "
            f"sql_ct_reference_at={probe.sql_ct_reference_at or 'n/a'}"
        )

        metrics = (
            []
            if pass_rule == "ct_row_count"
            else _flow_metrics_for_table(pipeline_rows, ctx, table_nm)
        )
        summaries = (
            []
            if pass_rule == "ct_row_count"
            else aggregate_flow_metrics(metrics, ctx.tables)
        )
        summary = (
            None
            if pass_rule == "ct_row_count"
            else next(
                (s for s in summaries if s.table_name.casefold() == table_nm.casefold()),
                None,
            )
        )
        flow_complete = (
            False
            if pass_rule == "ct_row_count"
            else (
                (summary is not None and summary.final_flow_status == "COMPLETED")
                or api_update_complete
            )
        )

        stable_key = f"{client.src_db_nm}.{src_schema}.{table_nm}"
        prev_ct_head = head_cache.get(stable_key)
        ct_head_stable = (
            prev_ct_head is not None and prev_ct_head == probe.ct_head_version
        )
        head_cache[stable_key] = probe.ct_head_version

        sql_count: int | None = None
        delta_count: int | None = None
        table_refresh: dict[str, Any] | None = None

        sql_ct_ref = probe.sql_ct_reference_at or probe.watermark_updated_at

        if pass_rule == "ct_row_count":
            hist_key = _row_count_verified_key(
                client.src_db_nm, table_nm, probe.ct_head_version
            )
            uses_row_count = _uses_row_count_validation(recon_type)
            in_row_sample = table_nm.casefold() in row_count_sample
            if uses_row_count:
                if _is_row_count_verified(
                    client.src_db_nm,
                    table_nm,
                    probe.ct_head_version,
                    verified_cache,
                ):
                    entry = verified_cache[hist_key]
                    sql_count, delta_count = entry.sql_count, entry.delta_count
                    status, message = (
                        "PASS",
                        (
                            f"ct_row_count: verified cache match "
                            f"sql={sql_count} delta={delta_count}"
                        ),
                    )
                    row_count_pass += 1
                elif in_row_sample:
                    cached = row_count_cache.get(table_nm.casefold())
                    if cached is not None:
                        sql_count, delta_count = cached
                    else:
                        sql_count = fetch_sql_row_counts_batch(
                            conn, src_schema, [table_nm]
                        ).get(table_nm.casefold())
                        delta_count = (
                            count_delta_table_rows(spark, catalog, dest_schema, table_nm)
                            if dest_schema
                            else None
                        )
                    status, message = evaluate_ct_row_count_recon(
                        probe.pending, sql_count, delta_count
                    )
                    if status == "PASS":
                        row_count_pass += 1
                    elif status == "WAITING":
                        row_count_wait += 1
                    else:
                        row_count_fail += 1
                    if (
                        sql_count is not None
                        and delta_count is not None
                        and sql_count == delta_count
                    ):
                        verified_cache[hist_key] = RowCountVerified(
                            sql_count, delta_count, probe.ct_head_version
                        )
                else:
                    status, message = evaluate_ct_row_count_recon(
                        probe.pending,
                        None,
                        None,
                        require_row_count=False,
                    )
                    row_count_wait += 1
            elif _is_delta_history_verified(
                client.src_db_nm,
                table_nm,
                probe.ct_head_version,
                history_verified_cache,
            ):
                hist_entry = history_verified_cache[hist_key]
                table_refresh = hist_entry.table_refresh
                last_write = table_refresh.get("last_refreshed_at")
                status, message = (
                    "PASS",
                    (
                        f"ct_delta_ts: verified cache "
                        f"last_write={last_write or 'n/a'}"
                    ),
                )
                history_ts_pass += 1
            else:
                table_refresh = table_refresh_cache.get(table_nm.casefold())
                hist_cached = history_verified_cache.get(hist_key)
                if table_refresh is None and hist_cached is not None:
                    table_refresh = hist_cached.table_refresh
                elif (
                    table_refresh is None
                    and dest_schema
                    and not _is_delta_history_verified(
                        client.src_db_nm,
                        table_nm,
                        probe.ct_head_version,
                        history_verified_cache,
                    )
                ):
                    table_refresh = fetch_streaming_table_refresh_info(
                        spark, catalog, dest_schema, table_nm
                    )
                status, message = evaluate_ct_delta_ts_after_sql_ct(
                    table_refresh,
                    sql_ct_ref,
                    table_quiesce_sec,
                    probe.pending,
                )
                if status == "PASS":
                    history_ts_pass += 1
                    if table_refresh is not None:
                        history_verified_cache[hist_key] = DeltaHistoryVerified(
                            probe.ct_head_version,
                            table_refresh,
                        )
                elif status == "WAITING":
                    history_ts_wait += 1
                else:
                    history_ts_fail += 1
        elif pass_rule in PASS_RULES_DELTA_HISTORY:
            in_history_sample = table_nm.casefold() in history_sample
            table_refresh = table_refresh_cache.get(table_nm.casefold())
            hist_key = _row_count_verified_key(
                client.src_db_nm, table_nm, probe.ct_head_version
            )
            hist_cached = history_verified_cache.get(hist_key)
            if table_refresh is None and hist_cached is not None:
                table_refresh = hist_cached.table_refresh
            elif (
                table_refresh is None
                and dest_schema
                and in_history_sample
                and not _is_delta_history_verified(
                    client.src_db_nm,
                    table_nm,
                    probe.ct_head_version,
                    history_verified_cache,
                )
            ):
                table_refresh = fetch_streaming_table_refresh_info(
                    spark, catalog, dest_schema, table_nm
                )
            if table_refresh:
                dlt_id = table_refresh.get("dlt_update_id") or "n/a"
                print(
                    f"[recon] {ctx.pipeline_key} {table_nm}: delta history "
                    f"source={table_refresh.get('source') or 'n/a'} "
                    f"last_write={table_refresh.get('last_refreshed_at')} "
                    f"op={table_refresh.get('latest_refresh_status') or 'n/a'} "
                    f"delta_version={table_refresh.get('delta_version') or 'n/a'} "
                    f"dlt_update_id={dlt_id}"
                )
            else:
                print(
                    f"[recon] WARN {ctx.pipeline_key} {table_nm}: "
                    "no delta history write metadata"
                )

        if pass_rule != "ct_row_count":
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
            elif pass_rule == "ct_delta_history":
                in_sample = table_nm.casefold() in row_count_sample
                if in_sample:
                    cached = row_count_cache.get(table_nm.casefold())
                    if cached is not None:
                        sql_count, delta_count = cached
                    else:
                        sql_count = fetch_sql_row_counts_batch(
                            conn, src_schema, [table_nm]
                        ).get(table_nm.casefold())
                        delta_count = (
                            count_delta_table_rows(spark, catalog, dest_schema, table_nm)
                            if dest_schema
                            else None
                        )
                        uc_ref = resolve_uc_table_ref(spark, catalog, dest_schema, table_nm)
                        resolved = uc_ref.name if uc_ref else f"{catalog}.{dest_schema}.{table_nm}"
                        print(
                            f"[recon] {ctx.pipeline_key} {table_nm}: row_count sample "
                            f"sql_count={sql_count} delta_count={delta_count} {resolved}"
                        )
                status, message = evaluate_ct_delta_history_recon(
                    table_refresh,
                    sql_ct_ref,
                    table_quiesce_sec,
                    probe.pending,
                    recon_type,
                    sql_count,
                    delta_count,
                    require_row_count=in_sample,
                )
                hist_status, _ = evaluate_table_refresh_after_sql_ct(
                    table_refresh,
                    sql_ct_ref,
                    quiesce_sec=table_quiesce_sec,
                )
                if (
                    hist_status == "PASS"
                    and table_refresh is not None
                    and not _is_delta_history_verified(
                        client.src_db_nm,
                        table_nm,
                        probe.ct_head_version,
                        history_verified_cache,
                    )
                ):
                    history_verified_cache[hist_key] = DeltaHistoryVerified(
                        probe.ct_head_version,
                        table_refresh,
                    )
                if in_sample and sql_count is not None and delta_count is not None:
                    if sql_count == delta_count:
                        verified_cache[hist_key] = RowCountVerified(
                            sql_count, delta_count, probe.ct_head_version
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
                        sql_count = fetch_sql_row_counts_batch(
                            conn, src_schema, [table_nm]
                        ).get(table_nm.casefold())
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
                                f"({'streaming COUNT(1)' if uc_ref and is_streaming_uc_table(spark, uc_ref) else 'delta numRecords'}) "
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

        if pass_rule in PASS_RULES_DELTA_HISTORY:
            if status == "PASS":
                delta_after_ct_pass += 1
            elif status == "WAITING":
                delta_after_ct_wait += 1

        row_update_id = (
            ""
            if pass_rule == "ct_row_count"
            else (
                summary.update_id if summary else (
                    metrics[-1].update_id if metrics else api_snap.get("update_id", "")
                )
            )
        )
        if pass_rule == "ct_delta_history" and table_refresh:
            dlt_uid = table_refresh.get("dlt_update_id")
            if dlt_uid:
                row_update_id = str(dlt_uid)
        flow_name = (
            DATABASE_RECON_FLOW_NAME
            if pass_rule == "ct_row_count"
            else (
                summary.flow_name if summary else _default_flow_name_for_table(table_nm, src_schema)
            )
        )
        pipeline_id = ctx.pipeline_id or (summary.pipeline_id if summary else "")
        ingest_change = (
            0
            if pass_rule == "ct_row_count"
            else (
                summary.total_change_rows if summary else (
                    sum(m.rows_upserted or 0 for m in metrics)
                    + sum(m.rows_deleted or 0 for m in metrics)
                )
            )
        )

        table_outcomes.append(
            SimplifiedTableOutcome(
                table_nm=table_nm,
                schema_name=src_schema,
                recon_type=recon_type,
                probe=probe,
                status=status,
                message=message,
                ingest_change_rows=ingest_change,
                sql_count=sql_count,
                delta_count=delta_count,
                table_refresh=table_refresh,
                update_id=row_update_id or "",
                flow_name=flow_name or "",
            )
        )

        if status == "WAITING":
            waiting_count += 1

    ct_head_version = (
        pending_tables[0].ct_head_version if pending_tables else None
    )
    batch_update_id = api_snap.get("update_id", "")
    for outcome in table_outcomes:
        if outcome.update_id:
            batch_update_id = outcome.update_id
            break
    pipeline_id = ctx.pipeline_id or ""

    batch_fail = [o for o in table_outcomes if o.status == "FAIL"]
    batch_waiting = [o for o in table_outcomes if o.status == "WAITING"]
    batch_pass = [o for o in table_outcomes if o.status == "PASS"]
    batch_all_pass = batch_pass and len(batch_pass) == len(table_outcomes)

    for outcome in table_outcomes:
        try:
            record_recon_table_result(
                conn,
                recon_run_id=run_id,
                client_nm=client.client_nm,
                database_name=client.src_db_nm,
                schema_name=outcome.schema_name,
                table_name=outcome.table_nm,
                pipeline_id=pipeline_id,
                update_id=batch_update_id or outcome.update_id,
                flow_name=outcome.flow_name or DATABASE_RECON_FLOW_NAME,
                recon_type=outcome.recon_type,
                watermark_before=outcome.probe.watermark_before,
                ct_head_version=outcome.probe.ct_head_version,
                pending=outcome.probe.pending,
                ingest_upserted=0,
                ingest_deleted=0,
                ingest_change_rows=outcome.ingest_change_rows,
                sync_status=outcome.status,
                recon_message=outcome.message,
                watermark_advanced=batch_all_pass and outcome.status == "PASS",
            )
        except Exception as exc:
            print(f"[recon] WARN record_recon_table_result: {exc}")

    if batch_fail:
        print(
            f"[recon] {ctx.pipeline_key}: database recon blocked — "
            f"{len(batch_fail)} table(s) FAIL"
        )
    elif batch_waiting:
        print(
            f"[recon] {ctx.pipeline_key}: database recon WAITING — "
            f"{len(batch_waiting)}/{len(table_outcomes)} table(s) not ready"
        )
    elif batch_pass and len(batch_pass) == len(table_outcomes):
        if (
            pipeline_id
            and ct_head_version is not None
            and recon_database_already_recorded(
                spark,
                catalog,
                metadata_schema,
                pipeline_id,
                client.src_db_nm,
                ct_head_version,
            )
        ):
            print(
                f"[recon] {ctx.pipeline_key}: SKIP database recon_ready already "
                f"recorded database={client.src_db_nm} ct_head={ct_head_version}"
            )
        else:
            completed_at = datetime.now(timezone.utc)
            detected_at = (
                resolve_batch_detected_at(
                    conn, client.src_db_nm, ct_head_version, batch_detected
                )
                if ct_head_version is not None
                else None
            )
            total_ingestion_sec: int | None = None
            if detected_at is not None:
                total_ingestion_sec = max(
                    0, int((completed_at - detected_at).total_seconds())
                )
            ready = build_database_recon_ready_row(
                client,
                ctx,
                table_outcomes,
                pipeline_id=pipeline_id,
                update_id=batch_update_id,
                ct_watermark_before=db_watermark_before,
                ct_head_version=ct_head_version,
                completed_at=completed_at,
                total_ingestion_sec=total_ingestion_sec,
            )
            try:
                written = write_recon_ready_rows(
                    spark, catalog, metadata_schema, [ready]
                )
                ready_written += written
            except Exception as exc:
                print(
                    f"[recon] ERROR write_recon_ready_rows database={client.src_db_nm} "
                    f"ct_head={ct_head_version}: {exc}"
                )
                written = 0
            if written:
                for outcome in batch_pass:
                    upsert_table_watermark(
                        conn,
                        client.src_db_nm,
                        outcome.schema_name,
                        outcome.table_nm,
                        outcome.probe.ct_head_version,
                        client_nm=client.client_nm,
                        pipeline_key=ctx.pipeline_key,
                    )
                if ct_head_version is not None:
                    upsert_db_watermark(
                        conn,
                        client.src_db_nm,
                        ct_head_version,
                        client_nm=client.client_nm,
                    )
                    clear_recon_batch_state(
                        conn,
                        client,
                        ctx,
                        ct_head_version,
                        batch_detected,
                        verified_cache,
                        history_verified_cache,
                    )
                print(
                    f"[recon] PASS {ctx.pipeline_key} database={client.src_db_nm} "
                    f"tables={len(batch_pass)} ct_head={ct_head_version} "
                    f"total_ingestion_sec={total_ingestion_sec} "
                    f"→ recon_ready written (1 row)"
                )

    if pass_rule == "ct_row_count" and pending_tables:
        print(
            f"[recon] {ctx.pipeline_key}: recon summary — "
            f"changed={len(pending_tables)} "
            f"row_count pass={row_count_pass} wait={row_count_wait} fail={row_count_fail} | "
            f"delta_history_ts pass={history_ts_pass} wait={history_ts_wait} "
            f"fail={history_ts_fail} "
            f"(recon_type 2=COUNT; else delta last_write > sql_ct_reference+{table_quiesce_sec}s)"
        )
    elif pass_rule in PASS_RULES_DELTA_HISTORY and pending_tables:
        print(
            f"[recon] {ctx.pipeline_key}: per-table delta vs SQL CT — "
            f"checked={len(pending_tables)} "
            f"delta_after_ct_pass={delta_after_ct_pass} "
            f"delta_after_ct_wait={delta_after_ct_wait}"
        )

    if run_id:
        try:
            if batch_all_pass and ready_written == 0 and ct_head_version is not None:
                run_message = (
                    f"ready=0 deduped ct_head={ct_head_version} waiting={waiting_count}"
                )
            else:
                run_message = f"ready={ready_written} waiting={waiting_count}"
            complete_recon_run(
                conn,
                run_id,
                run_status="PASS" if batch_all_pass else "SKIPPED",
                run_message=run_message,
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


def recon_database_already_recorded(
    spark,
    catalog: str,
    metadata_schema: str,
    pipeline_id: str,
    database_name: str,
    ct_head_version: int,
) -> bool:
    """
    Idempotency for one DB-level recon_ready per reconciled CT head.

    Continuous Lakeflow pipelines reuse the same update_id for hours; keying only
    on update_id blocks every subsequent batch after the first PASS.
    """
    target = qualified_table(catalog, metadata_schema, RECON_READY_TABLE)
    try:
        rows = spark.sql(
            f"""
            SELECT 1 FROM {target}
            WHERE pipeline_id = '{pipeline_id.replace("'", "''")}'
              AND database_name = '{database_name.replace("'", "''")}'
              AND ct_head_version = {int(ct_head_version)}
              AND flow_name = '{DATABASE_RECON_FLOW_NAME.replace("'", "''")}'
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
    row_count_sample_size: int = 5,
    history_sample_size: int = 5,
    row_count_parallel_workers: int = 10,
    ct_batch_detected_at: dict[str, datetime] | None = None,
    row_count_verified_cache: dict[str, RowCountVerified] | None = None,
    delta_history_verified_cache: dict[str, DeltaHistoryVerified] | None = None,
) -> dict[str, int]:
    """Poll pipelines; recon CT-changed tables (SQL CT vs Delta row count when ct_row_count)."""
    if simplified_recon and not use_sql_server_audit:
        print("WARN simplified_recon requires use_sql_server_audit=true; using full recon")
        simplified_recon = False

    ct_row_count_mode = simplified_recon and simple_pass_rule == "ct_row_count"

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
    rest_client = DatabricksRestClient() if not ct_row_count_mode else None
    polled_at = datetime.now(timezone.utc)

    pipeline_ids = [ctx.pipeline_id for ctx, _, _, _ in pipeline_contexts if ctx.pipeline_id]
    for ctx, _, _, _ in pipeline_contexts:
        if not ctx.pipeline_id and rest_client is not None:
            ctx.pipeline_id = resolve_pipeline_id(ctx.pipeline_key, rest_client)
        elif not ctx.pipeline_id:
            ctx.pipeline_id = resolve_pipeline_id(ctx.pipeline_key)
        if ctx.pipeline_id:
            pipeline_ids.append(ctx.pipeline_id)
    unique_pipeline_ids = sorted(set(pipeline_ids))

    watermark_conn: Any | None = None
    watermark_conn_client: Any | None = None
    pending_event_log_watermarks: dict[str, ReconEventLogWatermark] = {}

    watermarks: dict[str, ReconEventLogWatermark] = {}
    if (
        use_sql_server_audit
        and pipeline_contexts
        and not ct_row_count_mode
    ):
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
    elif not ct_row_count_mode:
        watermarks = read_recon_event_log_watermarks(
            spark,
            catalog,
            metadata_schema,
            unique_pipeline_ids,
        )

    ct_head_cache: dict[str, int] = {}
    if ct_batch_detected_at is None:
        ct_batch_detected_at = {}
    if row_count_verified_cache is None:
        row_count_verified_cache = {}
    if delta_history_verified_cache is None:
        delta_history_verified_cache = {}

    for ctx, src_catalog, src_schema, ipac_client in pipeline_contexts:
        totals["pipelines"] += 1
        if not ctx.pipeline_id:
            if rest_client is not None:
                ctx.pipeline_id = resolve_pipeline_id(ctx.pipeline_key, rest_client)
            else:
                ctx.pipeline_id = resolve_pipeline_id(ctx.pipeline_key)
        if not ctx.pipeline_id:
            print(f"{ctx.pipeline_key}: SKIP no pipeline_id")
            totals["skipped"] += 1
            continue

        detail: dict[str, Any] = {}
        watermark: ReconEventLogWatermark | None = None
        needs_poll = False
        reason = "n/a"
        skip_event_log = ct_row_count_mode

        if ct_row_count_mode:
            pass
        else:
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
        discover_start = time.perf_counter()
        if simplified_recon and use_sql_server_audit:
            try:
                probe_conn, _ = open_audit_connection(ipac_client, dbutils=dbutils)
                active_tables = [c.table_nm for c in ctx.tables]
                ct_pending_probe = discover_pending_ct_tables(
                    probe_conn, ipac_client, src_schema, active_tables
                )
                discover_elapsed = time.perf_counter() - discover_start
                print(
                    f"[recon] {ctx.pipeline_key}: CT discover batch "
                    f"configured={len(active_tables)} pending={len(ct_pending_probe)} "
                    f"elapsed={discover_elapsed:.1f}s "
                    f"(1 watermark IN + 1 CHANGETABLE UNION)"
                )
                if (
                    ct_pending_probe
                    and ct_pending_probe[0].ct_head_version is not None
                ):
                    detected = mark_batch_detected(
                        probe_conn,
                        ipac_client,
                        ctx,
                        ct_pending_probe[0].ct_head_version,
                        ct_batch_detected_at,
                    )
                    if detected is not None:
                        print(
                            f"[recon] {ctx.pipeline_key}: DB CT batch queue "
                            f"ct_head={ct_pending_probe[0].ct_head_version} "
                            f"first_detected={detected.isoformat()}"
                        )
                probe_conn.close()
            except Exception as exc:
                print(f"{ctx.pipeline_key}: WARN CT probe failed: {exc}")

            if not ct_pending_probe:
                print(f"{ctx.pipeline_key}: SKIP no pending CT on configured tables")
                totals["skipped"] += 1
                continue

            if ct_row_count_mode:
                needs_poll = True
                skip_event_log = True
                reason = (
                    f"CT pending on {len(ct_pending_probe)} table(s), "
                    "ct_row_count (SQL vs Delta, no pipeline API/event_log)"
                )
            elif simple_pass_rule == "ct_delta_history":
                needs_poll = True
                skip_event_log = True
                reason = (
                    f"CT pending on {len(ct_pending_probe)} table(s), "
                    "ct_delta_history (no event_log)"
                )
            else:
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

        if skip_event_log:
            print(f"{ctx.pipeline_key}: CT recon without event_log ({reason})")
            pipeline_rows: list[dict[str, Any]] = []
            totals["polled"] += 1
            pending_event_log_watermarks[ctx.pipeline_id] = _watermark_from_rows_and_api(
                ctx.pipeline_id,
                ctx.pipeline_key,
                pipeline_rows,
                detail,
                watermark,
                polled_at,
            )
        else:
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
                row_count_verified_cache=row_count_verified_cache,
                delta_history_verified_cache=delta_history_verified_cache,
                ct_batch_detected_at=ct_batch_detected_at,
                table_quiesce_sec=table_quiesce_sec,
                row_count_sample_size=row_count_sample_size,
                history_sample_size=history_sample_size,
                uc_parallel_workers=row_count_parallel_workers,
                pending_ct_tables=ct_pending_probe if ct_pending_probe else None,
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

    if pending_event_log_watermarks and watermark_conn is not None and not ct_row_count_mode:
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
