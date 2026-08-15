"""Unity Catalog Delta tables for Lakeflow ingestion flow metrics and recon gating."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

ReconStatus = Literal["PENDING", "PASS", "FAIL", "SKIPPED"]

FLOW_METRICS_TABLE = "lakeflow_flow_metrics"
FLOW_SUMMARY_TABLE = "lakeflow_flow_summary"
RECON_READY_TABLE = "recon_ready"


@dataclass
class FlowMetricsRow:
    event_id: str
    pipeline_id: str
    pipeline_name: str
    update_id: str
    flow_name: str
    table_name: str
    event_timestamp: datetime
    flow_status: str
    output_rows: int | None = None
    rows_upserted: int | None = None
    rows_deleted: int | None = None
    output_bytes: int | None = None
    client_nm: str = ""
    destination_schema: str = ""
    destination_table: str = ""
    captured_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        now = self.captured_at or datetime.now(timezone.utc)
        return {
            "event_id": self.event_id,
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "update_id": self.update_id,
            "flow_name": self.flow_name,
            "table_name": self.table_name,
            "event_timestamp": self.event_timestamp,
            "flow_status": self.flow_status,
            "output_rows": self.output_rows,
            "rows_upserted": self.rows_upserted,
            "rows_deleted": self.rows_deleted,
            "output_bytes": self.output_bytes,
            "client_nm": self.client_nm,
            "destination_schema": self.destination_schema,
            "destination_table": self.destination_table,
            "captured_at": now,
        }


@dataclass
class FlowSummaryRow:
    pipeline_id: str
    pipeline_name: str
    update_id: str
    flow_name: str
    table_name: str
    client_nm: str
    destination_schema: str
    destination_table: str
    recon_type: int
    final_flow_status: str
    total_output_rows: int
    total_upserted: int
    total_deleted: int
    total_change_rows: int
    total_output_bytes: int
    first_event_time: datetime
    last_event_time: datetime
    metric_duration_sec: float | None = None
    recon_status: str = "PENDING"
    source_change_rows: int | None = None
    recon_message: str = ""
    recorded_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        now = self.recorded_at or datetime.now(timezone.utc)
        return {
            "summary_id": str(uuid.uuid4()),
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "update_id": self.update_id,
            "flow_name": self.flow_name,
            "table_name": self.table_name,
            "client_nm": self.client_nm,
            "destination_schema": self.destination_schema,
            "destination_table": self.destination_table,
            "recon_type": self.recon_type,
            "final_flow_status": self.final_flow_status,
            "total_output_rows": self.total_output_rows,
            "total_upserted": self.total_upserted,
            "total_deleted": self.total_deleted,
            "total_change_rows": self.total_change_rows,
            "total_output_bytes": self.total_output_bytes,
            "first_event_time": self.first_event_time,
            "last_event_time": self.last_event_time,
            "metric_duration_sec": self.metric_duration_sec,
            "recon_status": self.recon_status,
            "source_change_rows": self.source_change_rows,
            "recon_message": self.recon_message,
            "recorded_at": now,
        }


@dataclass
class ReconReadyRow:
    client_nm: str
    table_nm: str
    pipeline_id: str
    update_id: str
    flow_name: str
    recon_type: int
    ingest_change_rows: int
    source_change_rows: int | None
    completed_at: datetime
    artifact_run_id: str
    ready_for_calc: bool = True
    recon_id: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "recon_id": self.recon_id or str(uuid.uuid4()),
            "client_nm": self.client_nm,
            "table_nm": self.table_nm,
            "pipeline_id": self.pipeline_id,
            "update_id": self.update_id,
            "flow_name": self.flow_name,
            "recon_type": self.recon_type,
            "ingest_change_rows": self.ingest_change_rows,
            "source_change_rows": self.source_change_rows,
            "completed_at": self.completed_at,
            "artifact_run_id": self.artifact_run_id,
            "ready_for_calc": self.ready_for_calc,
        }


def qualified_table(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def ingest_event_log_table_name(pipeline_key: str) -> str:
    """Published UC event log table name for a pipeline resource key."""
    return f"ingest_events_{pipeline_key}"


def lakeflow_flow_metrics_create_sql(catalog: str, schema: str) -> str:
    table = qualified_table(catalog, schema, FLOW_METRICS_TABLE)
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  event_id STRING NOT NULL COMMENT 'Lakeflow event log id — merge key',
  pipeline_id STRING NOT NULL,
  pipeline_name STRING,
  update_id STRING NOT NULL,
  flow_name STRING NOT NULL,
  table_name STRING COMMENT 'origin.dataset_name or resolved destination table',
  event_timestamp TIMESTAMP NOT NULL,
  flow_status STRING,
  output_rows BIGINT,
  rows_upserted BIGINT,
  rows_deleted BIGINT,
  output_bytes BIGINT,
  client_nm STRING,
  destination_schema STRING,
  destination_table STRING,
  captured_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Raw flow_progress metrics from MANAGED_INGESTION event logs'
""".strip()


def lakeflow_flow_summary_create_sql(catalog: str, schema: str) -> str:
    table = qualified_table(catalog, schema, FLOW_SUMMARY_TABLE)
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  summary_id STRING NOT NULL,
  pipeline_id STRING NOT NULL,
  pipeline_name STRING,
  update_id STRING NOT NULL,
  flow_name STRING NOT NULL,
  table_name STRING NOT NULL,
  client_nm STRING,
  destination_schema STRING,
  destination_table STRING,
  recon_type INT COMMENT '1=metrics only, 2=change rows vs CDC, 3=upserts vs CDC',
  final_flow_status STRING,
  total_output_rows BIGINT,
  total_upserted BIGINT,
  total_deleted BIGINT,
  total_change_rows BIGINT,
  total_output_bytes BIGINT,
  first_event_time TIMESTAMP,
  last_event_time TIMESTAMP,
  metric_duration_sec DOUBLE,
  recon_status STRING COMMENT 'PENDING | PASS | FAIL | SKIPPED',
  source_change_rows BIGINT COMMENT 'SQL Server CDC count when recon_type 2/3',
  recon_message STRING,
  recorded_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Aggregated per-flow metrics when flow reaches COMPLETED'
""".strip()


def recon_ready_create_sql(catalog: str, schema: str) -> str:
    table = qualified_table(catalog, schema, RECON_READY_TABLE)
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  recon_id STRING NOT NULL,
  client_nm STRING NOT NULL,
  table_nm STRING NOT NULL,
  pipeline_id STRING NOT NULL,
  update_id STRING NOT NULL,
  flow_name STRING NOT NULL,
  recon_type INT,
  ingest_change_rows BIGINT,
  source_change_rows BIGINT,
  completed_at TIMESTAMP NOT NULL,
  artifact_run_id STRING COMMENT 'Pipeline update_id for calc gating',
  ready_for_calc BOOLEAN NOT NULL
)
USING DELTA
COMMENT 'PASS rows only — gate for ipac-sdt-calc'
""".strip()


def all_recon_tables_create_sql(catalog: str, schema: str) -> str:
    return "\n\n".join(
        [
            lakeflow_flow_metrics_create_sql(catalog, schema),
            lakeflow_flow_summary_create_sql(catalog, schema),
            recon_ready_create_sql(catalog, schema),
        ]
    )


def ensure_recon_tables(spark, catalog: str, schema: str) -> None:
    spark.sql(lakeflow_flow_metrics_create_sql(catalog, schema))
    spark.sql(lakeflow_flow_summary_create_sql(catalog, schema))
    spark.sql(recon_ready_create_sql(catalog, schema))


def write_flow_metrics_rows(
    spark,
    catalog: str,
    schema: str,
    rows: list[FlowMetricsRow],
) -> int:
    if not rows:
        return 0
    ensure_recon_tables(spark, catalog, schema)
    target = qualified_table(catalog, schema, FLOW_METRICS_TABLE)
    df = spark.createDataFrame([row.as_dict() for row in rows])
    df.createOrReplaceTempView("new_flow_metrics")
    spark.sql(
        f"""
        MERGE INTO {target} AS target
        USING new_flow_metrics AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    return len(rows)


def write_flow_summary_rows(
    spark,
    catalog: str,
    schema: str,
    rows: list[FlowSummaryRow],
) -> int:
    if not rows:
        return 0
    ensure_recon_tables(spark, catalog, schema)
    target = qualified_table(catalog, schema, FLOW_SUMMARY_TABLE)
    df = spark.createDataFrame([row.as_dict() for row in rows])
    df.write.format("delta").mode("append").saveAsTable(target)
    return len(rows)


def write_recon_ready_rows(
    spark,
    catalog: str,
    schema: str,
    rows: list[ReconReadyRow],
) -> int:
    if not rows:
        return 0
    ensure_recon_tables(spark, catalog, schema)
    target = qualified_table(catalog, schema, RECON_READY_TABLE)
    df = spark.createDataFrame([row.as_dict() for row in rows])
    df.write.format("delta").mode("append").saveAsTable(target)
    return len(rows)
