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
    _latest_update_block,
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
    read_recon_event_log_watermarks,
    RECON_READY_TABLE,
    upsert_recon_event_log_watermark,
    write_flow_metrics_rows,
    write_flow_summary_rows,
    write_recon_ready_rows,
)
from common.ops.source_ct_ops import run_source_ct_count
from common.ops.sql_server_audit_store import (
    CtPendingCounts,
    complete_recon_run,
    insert_recon_run,
    open_audit_connection,
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
) -> dict[str, int]:
    """Poll hidden event logs only for pipelines with activity; recon changed flows."""
    totals = {
        "metrics": 0,
        "summaries": 0,
        "recon_ready": 0,
        "pipelines": 0,
        "polled": 0,
        "skipped": 0,
        "new_events": 0,
    }
    rest_client = DatabricksRestClient()
    polled_at = datetime.now(timezone.utc)

    pipeline_ids = [ctx.pipeline_id for ctx, _, _, _ in pipeline_contexts if ctx.pipeline_id]
    for ctx, _, _, _ in pipeline_contexts:
        if not ctx.pipeline_id:
            ctx.pipeline_id = resolve_pipeline_id(ctx.pipeline_key, rest_client)
        if ctx.pipeline_id:
            pipeline_ids.append(ctx.pipeline_id)
    watermarks = read_recon_event_log_watermarks(
        spark,
        catalog,
        metadata_schema,
        sorted(set(pipeline_ids)),
    )

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

        if not needs_poll:
            upsert_recon_event_log_watermark(
                spark,
                catalog,
                metadata_schema,
                _watermark_from_rows_and_api(
                    ctx.pipeline_id,
                    ctx.pipeline_key,
                    [],
                    detail,
                    watermark,
                    polled_at,
                ),
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

        upsert_recon_event_log_watermark(
            spark,
            catalog,
            metadata_schema,
            _watermark_from_rows_and_api(
                ctx.pipeline_id,
                ctx.pipeline_key,
                pipeline_rows,
                detail,
                watermark,
                polled_at,
            ),
        )

        if not pipeline_rows:
            print(f"{ctx.pipeline_key}: no new flow_progress events")
            continue

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
