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
from common.ops.pipeline_job_ops import DatabricksRestClient, _select_pipelines_for_ops
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
    ingest_event_log_table_name,
    qualified_table,
    RECON_READY_TABLE,
    write_flow_metrics_rows,
    write_flow_summary_rows,
    write_recon_ready_rows,
)
from common.ops.source_ct_ops import run_source_ct_count


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


def event_log_qualified(catalog: str, metadata_schema: str, event_log_table: str) -> str:
    return qualified_table(catalog, metadata_schema, event_log_table)


def table_exists(spark, qualified_name: str) -> bool:
    try:
        spark.sql(f"SELECT 1 FROM {qualified_name} LIMIT 1")
        return True
    except Exception:
        return False


def fetch_flow_progress_rows(
    spark,
    qualified_event_log: str,
    lookback_hours: int,
) -> list[dict[str, Any]]:
    sql = flow_progress_extract_sql(qualified_event_log, lookback_hours)
    return [row.asDict() for row in spark.sql(sql).collect()]


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
    lookback_hours: int = 24,
    rest_client: DatabricksRestClient | None = None,
) -> tuple[int, int, int]:
    """
    Run recon for one ingestion pipeline context.
    Returns (metrics_merged, summaries_written, recon_ready_written).
    """
    if not ctx.pipeline_id:
        ctx.pipeline_id = resolve_pipeline_id(ctx.pipeline_key, rest_client)

    el_name = ctx.event_log_table or ingest_event_log_table_name(ctx.pipeline_key)
    qualified_el = event_log_qualified(catalog, metadata_schema, el_name)
    if not table_exists(spark, qualified_el):
        print(f"SKIP event log not found: {qualified_el}")
        return 0, 0, 0
    pipeline_rows = fetch_flow_progress_rows(spark, qualified_el, lookback_hours)

    metrics: list[FlowMetricsRow] = []
    for row in pipeline_rows:
        parsed = parse_flow_progress_event(row, ctx)
        if parsed:
            if not parsed.pipeline_id and ctx.pipeline_id:
                parsed.pipeline_id = ctx.pipeline_id
            metrics.append(parsed)

    merged = write_flow_metrics_rows(spark, catalog, metadata_schema, metrics)
    summaries = aggregate_flow_metrics(metrics, ctx.tables)

    summaries_written = 0
    ready_written = 0
    process_rows: list[Any] = []

    for summary in summaries:
        if recon_already_recorded(
            spark, catalog, metadata_schema, summary.pipeline_id, summary.update_id, summary.flow_name
        ):
            continue

        source_count: int | None = None
        if summary.recon_type in (2, 3):
            source_count = run_source_ct_count(
                spark,
                src_catalog,
                src_schema,
                summary.table_name,
                summary.first_event_time,
                summary.last_event_time,
                summary.recon_type,
            )

        evaluated = evaluate_recon(summary, source_count)
        summaries_written += write_flow_summary_rows(spark, catalog, metadata_schema, [evaluated])

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

    return merged, summaries_written, ready_written


def run_all_pipeline_recon(
    spark,
    catalog: str,
    metadata_schema: str,
    pipeline_contexts: list[tuple[Any, str, str]],
    lookback_hours: int = 24,
) -> dict[str, int]:
    """Run recon for each (PipelineReconContext, src_catalog, src_schema)."""
    totals = {"metrics": 0, "summaries": 0, "recon_ready": 0, "pipelines": 0}
    client = DatabricksRestClient()

    for ctx, src_catalog, src_schema in pipeline_contexts:
        if not ctx.pipeline_id:
            ctx.pipeline_id = resolve_pipeline_id(ctx.pipeline_key, client)
        m, s, r = run_pipeline_recon(
            spark,
            catalog,
            metadata_schema,
            ctx,
            src_catalog,
            src_schema,
            lookback_hours=lookback_hours,
            rest_client=client,
        )
        totals["metrics"] += m
        totals["summaries"] += s
        totals["recon_ready"] += r
        totals["pipelines"] += 1
        print(
            f"{ctx.pipeline_key}: metrics_merged={m} summaries={s} recon_ready={r}"
        )
    return totals


def build_contexts_for_client(
    client: Any,
    effective_tables: list[Any],
    dest_schema_suffix: str,
    pipeline_keys: list[str],
) -> list[tuple[Any, str, str]]:
    raw_schema = client.raw_schema(dest_schema_suffix)
    table_cfgs = table_configs_from_effective(client.client_nm, raw_schema, effective_tables)
    src_catalog = client.src_db_nm
    src_schema = client.src_db_schema or "dbo"
    out: list[tuple[Any, str, str]] = []
    for key in pipeline_keys:
        pipeline_key = normalize_pipeline_key(key)
        if not pipeline_key.startswith("p_"):
            continue
        if client.client_nm not in pipeline_key:
            continue
        ctx = build_pipeline_recon_context(pipeline_key, table_cfgs)
        out.append((ctx, src_catalog, src_schema))
    return out
