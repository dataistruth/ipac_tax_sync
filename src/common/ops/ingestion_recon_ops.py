"""Orchestrate ingestion flow metrics reconciliation per pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from common.ops.pipeline_names import load_pipeline_names, normalize_pipeline_key

from common.ops.lakeflow_event_ops import (
    aggregate_flow_metrics,
    build_pipeline_recon_context,
    evaluate_recon,
    flow_progress_extract_sql,
    parse_flow_progress_event,
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
        if (parsed.table_name or "").casefold() == target:
            metrics.append(parsed)
    return metrics


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
        f"use_api_update_complete={use_api_update_complete}"
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
            f"recon_type={recon_type}"
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

        if pass_rule in ("auto", "flow_complete") and flow_complete:
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

        if status == "WAITING":
            waiting_count += 1
            continue

        update_id = summary.update_id if summary else api_snap.get("update_id", "")
        flow_name = summary.flow_name if summary else _default_flow_name_for_table(table_nm, src_schema)
        pipeline_id = ctx.pipeline_id or (summary.pipeline_id if summary else "")

        if update_id and pipeline_id and flow_name:
            if recon_already_recorded(
                spark, catalog, metadata_schema, pipeline_id, update_id, flow_name
            ):
                print(f"[recon] {ctx.pipeline_key} {table_nm}: SKIP already in recon_ready")
                continue

        ingest_change = summary.total_change_rows if summary else 0
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
    sql = flow_progress_extract_sql(
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
