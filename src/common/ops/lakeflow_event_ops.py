"""Extract and aggregate MANAGED_INGESTION flow_progress metrics from event logs."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from common.ops.process_log_store import client_nm_from_ingest_pipeline
from common.ops.recon_store import FlowMetricsRow, FlowSummaryRow

FLOW_PROGRESS_EVENT = "flow_progress"
MANAGED_INGESTION = "MANAGED_INGESTION"
COMPLETED_STATUS = "COMPLETED"


@dataclass
class TableReconConfig:
    table_nm: str
    recon_type: int
    destination_schema: str
    destination_table: str


@dataclass
class PipelineReconContext:
    pipeline_key: str
    pipeline_id: str = ""
    pipeline_name: str = ""
    client_nm: str = ""
    tables: list[TableReconConfig] = field(default_factory=list)


def build_pipeline_recon_context(
    pipeline_key: str,
    table_configs: list[TableReconConfig],
    pipeline_id: str = "",
    pipeline_name: str = "",
) -> PipelineReconContext:
    client_nm = client_nm_from_ingest_pipeline(pipeline_key)
    return PipelineReconContext(
        pipeline_key=pipeline_key,
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name or pipeline_key,
        client_nm=client_nm,
        tables=table_configs,
    )


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def flow_progress_extract_sql(
    pipeline_id: str,
    lookback_hours: int = 24,
    since_timestamp: datetime | None = None,
) -> str:
    """SQL to read flow_progress METRICS from a pipeline hidden event_log TVF."""
    pid = _sql_escape(pipeline_id.strip())
    if since_timestamp is not None:
        ts = since_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        time_filter = f"AND timestamp > timestamp '{ts}'"
    else:
        time_filter = f"AND timestamp >= current_timestamp() - INTERVAL {int(lookback_hours)} HOURS"
    return f"""
SELECT
    id AS event_id,
    origin.pipeline_id AS pipeline_id,
    origin.pipeline_name AS pipeline_name,
    origin.update_id AS update_id,
    origin.flow_name AS flow_name,
    origin.dataset_name AS table_name,
    timestamp AS event_timestamp,
    details:flow_progress:status::STRING AS flow_status,
    TRY_CAST(details:flow_progress:metrics:num_output_rows AS BIGINT) AS output_rows,
    TRY_CAST(details:flow_progress:metrics:num_upserted_rows AS BIGINT) AS rows_upserted,
    TRY_CAST(details:flow_progress:metrics:num_deleted_rows AS BIGINT) AS rows_deleted,
    TRY_CAST(details:flow_progress:metrics:num_output_bytes AS BIGINT) AS output_bytes
FROM event_log('{pid}')
WHERE event_type = '{FLOW_PROGRESS_EVENT}'
  AND level = 'METRICS'
  AND origin.pipeline_type = '{MANAGED_INGESTION}'
  AND origin.flow_name IS NOT NULL
  {time_filter}
""".strip()


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_flow_progress_event(
    row: Mapping[str, Any],
    ctx: PipelineReconContext,
) -> FlowMetricsRow | None:
    event_id = str(row.get("event_id") or "").strip()
    flow_name = str(row.get("flow_name") or "").strip()
    update_id = str(row.get("update_id") or "").strip()
    if not event_id or not flow_name or not update_id:
        return None

    table_name = str(row.get("table_name") or "").strip()
    if not table_name:
        table_name = resolve_table_from_flow_name(flow_name, ctx.tables)

    dest_schema, dest_table, recon_type = _resolve_destination(table_name, ctx.tables)

    event_ts = _parse_timestamp(row.get("event_timestamp"))
    if event_ts is None:
        event_ts = datetime.now(timezone.utc)

    pipeline_id = str(row.get("pipeline_id") or ctx.pipeline_id or "").strip()
    pipeline_name = str(row.get("pipeline_name") or ctx.pipeline_name or ctx.pipeline_key).strip()

    return FlowMetricsRow(
        event_id=event_id,
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name,
        update_id=update_id,
        flow_name=flow_name,
        table_name=table_name or dest_table,
        event_timestamp=event_ts,
        flow_status=str(row.get("flow_status") or "").strip().upper(),
        output_rows=_safe_int(row.get("output_rows")),
        rows_upserted=_safe_int(row.get("rows_upserted")),
        rows_deleted=_safe_int(row.get("rows_deleted")),
        output_bytes=_safe_int(row.get("output_bytes")),
        client_nm=ctx.client_nm,
        destination_schema=dest_schema,
        destination_table=dest_table,
    )


def resolve_table_from_flow_name(flow_name: str, tables: list[TableReconConfig]) -> str:
    """Map flow_name to table_nm using suffix match or exact name."""
    flow = flow_name.strip()
    if not flow:
        return ""
    flow_lower = flow.casefold()
    for cfg in tables:
        if cfg.table_nm.casefold() == flow_lower:
            return cfg.table_nm
        if flow_lower.endswith(cfg.table_nm.casefold()):
            return cfg.table_nm
    # Common pattern: <schema>_<table>_flow or similar — last segment before _flow
    m = re.match(r"^(.+)_flow$", flow, re.IGNORECASE)
    if m:
        candidate = m.group(1)
        for cfg in tables:
            if candidate.casefold().endswith(cfg.table_nm.casefold()):
                return cfg.table_nm
    return ""


def _resolve_destination(table_name: str, tables: list[TableReconConfig]) -> tuple[str, str, int]:
    if not table_name:
        return "", "", 1
    for cfg in tables:
        if cfg.table_nm.casefold() == table_name.casefold():
            return cfg.destination_schema, cfg.destination_table, cfg.recon_type
    return "", table_name, 1


def aggregate_flow_metrics(
    metrics: list[FlowMetricsRow],
    table_configs: list[TableReconConfig] | None = None,
) -> list[FlowSummaryRow]:
    """Group metrics by pipeline_id, update_id, flow_name; require COMPLETED final status."""
    configs = table_configs or []
    groups: dict[tuple[str, str, str], list[FlowMetricsRow]] = {}
    for row in metrics:
        key = (row.pipeline_id, row.update_id, row.flow_name)
        groups.setdefault(key, []).append(row)

    summaries: list[FlowSummaryRow] = []
    for key, rows in groups.items():
        pipeline_id, update_id, flow_name = key
        if not rows:
            continue
        sorted_rows = sorted(rows, key=lambda r: r.event_timestamp)
        final_status = sorted_rows[-1].flow_status
        if final_status != COMPLETED_STATUS:
            continue

        first_ts = sorted_rows[0].event_timestamp
        last_ts = sorted_rows[-1].event_timestamp
        duration = (last_ts - first_ts).total_seconds()

        total_output = sum(r.output_rows or 0 for r in rows)
        total_upserted = sum(r.rows_upserted or 0 for r in rows)
        total_deleted = sum(r.rows_deleted or 0 for r in rows)
        total_bytes = sum(r.output_bytes or 0 for r in rows)
        sample = sorted_rows[-1]

        table_nm = sample.table_name or resolve_table_from_flow_name(flow_name, configs)
        _, dest_table, recon_type = _resolve_destination(table_nm, configs)
        dest_schema = sample.destination_schema
        if sample.destination_table:
            dest_table = sample.destination_table

        summaries.append(
            FlowSummaryRow(
                pipeline_id=pipeline_id,
                pipeline_name=sample.pipeline_name,
                update_id=update_id,
                flow_name=flow_name,
                table_name=table_nm or dest_table,
                client_nm=sample.client_nm,
                destination_schema=dest_schema,
                destination_table=dest_table,
                recon_type=recon_type,
                final_flow_status=final_status,
                total_output_rows=total_output,
                total_upserted=total_upserted,
                total_deleted=total_deleted,
                total_change_rows=total_upserted + total_deleted,
                total_output_bytes=total_bytes,
                first_event_time=first_ts,
                last_event_time=last_ts,
                metric_duration_sec=duration,
            )
        )
    return summaries


def enrich_summaries_with_recon_type(
    summaries: list[FlowSummaryRow],
    table_configs: list[TableReconConfig],
) -> list[FlowSummaryRow]:
    by_table = {c.table_nm.casefold(): c for c in table_configs}
    out: list[FlowSummaryRow] = []
    for s in summaries:
        cfg = by_table.get(s.table_name.casefold()) or by_table.get(s.destination_table.casefold())
        recon_type = cfg.recon_type if cfg else s.recon_type
        dest_schema = cfg.destination_schema if cfg else s.destination_schema
        dest_table = cfg.destination_table if cfg else s.destination_table
        out.append(
            FlowSummaryRow(
                pipeline_id=s.pipeline_id,
                pipeline_name=s.pipeline_name,
                update_id=s.update_id,
                flow_name=s.flow_name,
                table_name=s.table_name,
                client_nm=s.client_nm,
                destination_schema=dest_schema,
                destination_table=dest_table,
                recon_type=recon_type,
                final_flow_status=s.final_flow_status,
                total_output_rows=s.total_output_rows,
                total_upserted=s.total_upserted,
                total_deleted=s.total_deleted,
                total_change_rows=s.total_change_rows,
                total_output_bytes=s.total_output_bytes,
                first_event_time=s.first_event_time,
                last_event_time=s.last_event_time,
                metric_duration_sec=s.metric_duration_sec,
                recon_status=s.recon_status,
                source_change_rows=s.source_change_rows,
                recon_message=s.recon_message,
            )
        )
    return out


def evaluate_recon(
    summary: FlowSummaryRow,
    source_change_rows: int | None,
) -> FlowSummaryRow:
    """Apply recon_type rules; return summary with recon_status and message set."""
    if summary.recon_type == 1:
        return _copy_summary(
            summary,
            recon_status="PASS",
            source_change_rows=None,
            recon_message="recon_type=1 metrics-only PASS",
        )

    if source_change_rows is None:
        return _copy_summary(
            summary,
            recon_status="FAIL",
            source_change_rows=None,
            recon_message="source CT count unavailable for recon_type 2/3",
        )

    ingest_metric = summary.total_change_rows if summary.recon_type == 2 else summary.total_upserted
    label = "change_rows" if summary.recon_type == 2 else "upserted_rows"

    if ingest_metric == source_change_rows:
        return _copy_summary(
            summary,
            recon_status="PASS",
            source_change_rows=source_change_rows,
            recon_message=f"PASS {label}={ingest_metric} source={source_change_rows}",
        )

    return _copy_summary(
        summary,
        recon_status="FAIL",
        source_change_rows=source_change_rows,
        recon_message=f"FAIL {label}={ingest_metric} source={source_change_rows}",
    )


def _copy_summary(
    summary: FlowSummaryRow,
    *,
    recon_status: str,
    source_change_rows: int | None,
    recon_message: str,
) -> FlowSummaryRow:
    return FlowSummaryRow(
        pipeline_id=summary.pipeline_id,
        pipeline_name=summary.pipeline_name,
        update_id=summary.update_id,
        flow_name=summary.flow_name,
        table_name=summary.table_name,
        client_nm=summary.client_nm,
        destination_schema=summary.destination_schema,
        destination_table=summary.destination_table,
        recon_type=summary.recon_type,
        final_flow_status=summary.final_flow_status,
        total_output_rows=summary.total_output_rows,
        total_upserted=summary.total_upserted,
        total_deleted=summary.total_deleted,
        total_change_rows=summary.total_change_rows,
        total_output_bytes=summary.total_output_bytes,
        first_event_time=summary.first_event_time,
        last_event_time=summary.last_event_time,
        metric_duration_sec=summary.metric_duration_sec,
        recon_status=recon_status,
        source_change_rows=source_change_rows,
        recon_message=recon_message,
    )
