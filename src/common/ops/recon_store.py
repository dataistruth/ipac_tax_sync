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
RECON_EVENT_LOG_WATERMARK_TABLE = "recon_event_log_watermark"


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
    database_name: str = ""
    tables_json: str = ""
    ct_watermark_before: int | None = None
    ct_head_version: int | None = None
    total_ingestion_sec: int | None = None
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
            "database_name": self.database_name or self.table_nm,
            "tables_json": self.tables_json or "",
            "ct_watermark_before": self.ct_watermark_before,
            "ct_head_version": self.ct_head_version,
            "total_ingestion_sec": self.total_ingestion_sec,
        }


def qualified_table(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def quote_ident(part: str) -> str:
    return f"`{part.replace('`', '``')}`"


def qualified_table_quoted(catalog: str, schema: str, table: str) -> str:
    return f"{quote_ident(catalog)}.{quote_ident(schema)}.{quote_ident(table)}"


@dataclass(frozen=True)
class UcTableRef:
    catalog: str
    schema: str
    table: str

    @property
    def name(self) -> str:
        return qualified_table(self.catalog, self.schema, self.table)

    @property
    def quoted_name(self) -> str:
        return qualified_table_quoted(self.catalog, self.schema, self.table)


def resolve_uc_table_ref(
    spark,
    catalog: str,
    schema: str,
    table: str,
) -> UcTableRef | None:
    """
    Resolve a UC three-part table name with case-insensitive schema/table matching.

    Config often uses mixed-case client_nm while UC stores lowercase schema names.
    """
    if not catalog or not schema or not table:
        return None

    exact = qualified_table(catalog, schema, table)
    try:
        if spark.catalog.tableExists(exact):
            return UcTableRef(catalog=catalog, schema=schema, table=table)
    except Exception:
        pass

    sch_cf = schema.casefold()
    tbl_cf = table.casefold()

    schema_rows: list[Any] = []
    for stmt in (
        f"SHOW SCHEMAS IN {quote_ident(catalog)}",
        f"SHOW SCHEMAS IN {catalog}",
    ):
        try:
            schema_rows = spark.sql(stmt).collect()
            if schema_rows:
                break
        except Exception:
            continue
    if not schema_rows:
        return None

    actual_schema = None
    for row in schema_rows:
        name = str(row[0])
        if name.casefold() == sch_cf:
            actual_schema = name
            break
    if not actual_schema:
        return None

    table_rows: list[Any] = []
    for stmt in (
        f"SHOW TABLES IN {quote_ident(catalog)}.{quote_ident(actual_schema)}",
        f"SHOW TABLES IN {catalog}.{actual_schema}",
    ):
        try:
            table_rows = spark.sql(stmt).collect()
            if table_rows:
                break
        except Exception:
            continue
    if not table_rows:
        return None

    actual_table = None
    for row in table_rows:
        name = str(row.tableName)
        if name.casefold() == tbl_cf:
            actual_table = name
            break
    if not actual_table:
        return None

    return UcTableRef(catalog=catalog, schema=actual_schema, table=actual_table)


def is_streaming_uc_table(spark, ref: UcTableRef) -> bool:
    """Lakeflow Connect MANAGED_INGESTION targets are UC STREAMING_TABLEs."""
    try:
        rows = spark.sql(
            f"""
            SELECT table_type
            FROM {quote_ident(ref.catalog)}.information_schema.tables
            WHERE table_schema = '{ref.schema.replace("'", "''")}'
              AND table_name = '{ref.table.replace("'", "''")}'
            """
        ).collect()
        if rows and "STREAMING" in str(rows[0][0]).upper():
            return True
    except Exception:
        pass
    try:
        for row in spark.sql(f"DESCRIBE TABLE EXTENDED {ref.quoted_name}").collect():
            col_name = str(row.col_name).strip()
            data_type = str(row.data_type).strip()
            if col_name == "Type" and "STREAMING" in data_type.upper():
                return True
    except Exception:
        pass
    return False
@dataclass
class ReconEventLogWatermark:
    pipeline_id: str
    pipeline_key: str = ""
    last_event_ts: datetime | None = None
    last_event_id: str = ""
    last_update_id: str = ""
    last_api_update_state: str = ""
    last_poll_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "pipeline_id": self.pipeline_id,
            "pipeline_key": self.pipeline_key,
            "last_event_ts": self.last_event_ts,
            "last_event_id": self.last_event_id,
            "last_update_id": self.last_update_id,
            "last_api_update_state": self.last_api_update_state,
            "last_poll_at": self.last_poll_at,
        }


def recon_event_log_watermark_create_sql(catalog: str, schema: str) -> str:
    table = qualified_table(catalog, schema, RECON_EVENT_LOG_WATERMARK_TABLE)
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  pipeline_id STRING NOT NULL COMMENT 'Databricks pipeline UUID',
  pipeline_key STRING COMMENT 'Logical bundle key p_<client>_<n>',
  last_event_ts TIMESTAMP COMMENT 'Max event_timestamp captured from hidden event_log',
  last_event_id STRING COMMENT 'Latest flow_progress event id seen',
  last_update_id STRING COMMENT 'Latest pipeline update_id from API or events',
  last_api_update_state STRING COMMENT 'latest_update.state from last REST poll',
  last_poll_at TIMESTAMP COMMENT 'When recon monitor last evaluated this pipeline'
)
USING DELTA
COMMENT 'Per-pipeline watermark for hidden event_log polling'
""".strip()


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
  recon_type INT COMMENT '1=metrics only, 2=change rows vs CT, 3=upserts vs CT',
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
  source_change_rows BIGINT COMMENT 'SQL Server CT count when recon_type 2/3',
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
  table_nm STRING NOT NULL COMMENT 'UC table name for per-table rows; __database__ for DB-level batch',
  pipeline_id STRING NOT NULL,
  update_id STRING NOT NULL,
  flow_name STRING NOT NULL,
  recon_type INT,
  ingest_change_rows BIGINT,
  source_change_rows BIGINT,
  completed_at TIMESTAMP NOT NULL,
  artifact_run_id STRING COMMENT 'Pipeline update_id for calc gating',
  ready_for_calc BOOLEAN NOT NULL,
  database_name STRING COMMENT 'SQL Server database name',
  tables_json STRING COMMENT 'JSON array of reconciled tables in this batch',
  ct_watermark_before BIGINT COMMENT 'ct_db_watermark at recon start',
  ct_head_version BIGINT COMMENT 'CHANGE_TRACKING_CURRENT_VERSION at PASS',
  total_ingestion_sec BIGINT COMMENT 'Seconds from CT batch detected to recon_ready PASS'
)
USING DELTA
COMMENT 'PASS rows only — gate for ipac-sdt-calc'
""".strip()


def all_recon_tables_create_sql(catalog: str, schema: str) -> str:
    return "\n\n".join(
        [
            recon_event_log_watermark_create_sql(catalog, schema),
            lakeflow_flow_metrics_create_sql(catalog, schema),
            lakeflow_flow_summary_create_sql(catalog, schema),
            recon_ready_create_sql(catalog, schema),
        ]
    )


def ensure_recon_tables(spark, catalog: str, schema: str) -> None:
    from common.ops.uc_schema_ops import ensure_uc_schema

    ensure_uc_schema(spark, catalog, schema)
    spark.sql(recon_event_log_watermark_create_sql(catalog, schema))
    spark.sql(lakeflow_flow_metrics_create_sql(catalog, schema))
    spark.sql(lakeflow_flow_summary_create_sql(catalog, schema))
    spark.sql(recon_ready_create_sql(catalog, schema))


def ensure_recon_ready_table(spark, catalog: str, schema: str) -> None:
    """Create only recon_ready in Delta — no MERGE metadata tables."""
    from common.ops.uc_schema_ops import ensure_uc_schema

    ensure_uc_schema(spark, catalog, schema)
    spark.sql(recon_ready_create_sql(catalog, schema))
    _evolve_recon_ready_table(spark, catalog, schema)


def _flow_metrics_spark_schema():
    from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("pipeline_id", StringType(), False),
            StructField("pipeline_name", StringType(), True),
            StructField("update_id", StringType(), False),
            StructField("flow_name", StringType(), False),
            StructField("table_name", StringType(), True),
            StructField("event_timestamp", TimestampType(), False),
            StructField("flow_status", StringType(), True),
            StructField("output_rows", LongType(), True),
            StructField("rows_upserted", LongType(), True),
            StructField("rows_deleted", LongType(), True),
            StructField("output_bytes", LongType(), True),
            StructField("client_nm", StringType(), True),
            StructField("destination_schema", StringType(), True),
            StructField("destination_table", StringType(), True),
            StructField("captured_at", TimestampType(), False),
        ]
    )


def _flow_summary_spark_schema():
    from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("summary_id", StringType(), False),
            StructField("pipeline_id", StringType(), False),
            StructField("pipeline_name", StringType(), True),
            StructField("update_id", StringType(), False),
            StructField("flow_name", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("client_nm", StringType(), True),
            StructField("destination_schema", StringType(), True),
            StructField("destination_table", StringType(), True),
            StructField("recon_type", IntegerType(), True),
            StructField("final_flow_status", StringType(), True),
            StructField("total_output_rows", LongType(), True),
            StructField("total_upserted", LongType(), True),
            StructField("total_deleted", LongType(), True),
            StructField("total_change_rows", LongType(), True),
            StructField("total_output_bytes", LongType(), True),
            StructField("first_event_time", TimestampType(), True),
            StructField("last_event_time", TimestampType(), True),
            StructField("metric_duration_sec", DoubleType(), True),
            StructField("recon_status", StringType(), True),
            StructField("source_change_rows", LongType(), True),
            StructField("recon_message", StringType(), True),
            StructField("recorded_at", TimestampType(), False),
        ]
    )


def _recon_ready_spark_schema():
    from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("recon_id", StringType(), False),
            StructField("client_nm", StringType(), False),
            StructField("table_nm", StringType(), False),
            StructField("pipeline_id", StringType(), False),
            StructField("update_id", StringType(), False),
            StructField("flow_name", StringType(), False),
            StructField("recon_type", IntegerType(), True),
            StructField("ingest_change_rows", LongType(), True),
            StructField("source_change_rows", LongType(), True),
            StructField("completed_at", TimestampType(), False),
            StructField("artifact_run_id", StringType(), True),
            StructField("ready_for_calc", BooleanType(), False),
            StructField("database_name", StringType(), True),
            StructField("tables_json", StringType(), True),
            StructField("ct_watermark_before", LongType(), True),
            StructField("ct_head_version", LongType(), True),
            StructField("total_ingestion_sec", LongType(), True),
        ]
    )


def _evolve_recon_ready_table(spark, catalog: str, schema: str) -> None:
    """Add DB-level columns to existing recon_ready tables."""
    table = qualified_table(catalog, schema, RECON_READY_TABLE)
    for col_def in (
        "database_name STRING",
        "tables_json STRING",
        "ct_watermark_before BIGINT",
        "ct_head_version BIGINT",
        "total_ingestion_sec BIGINT",
    ):
        try:
            spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({col_def})")
        except Exception:
            pass


def _create_typed_dataframe(spark, rows: list[dict[str, Any]], schema) -> Any:
    if not rows:
        return spark.createDataFrame([], schema)
    return spark.createDataFrame(rows, schema=schema)


def read_recon_event_log_watermarks(
    spark,
    catalog: str,
    schema: str,
    pipeline_ids: list[str] | None = None,
) -> dict[str, ReconEventLogWatermark]:
    ensure_recon_tables(spark, catalog, schema)
    target = qualified_table(catalog, schema, RECON_EVENT_LOG_WATERMARK_TABLE)
    try:
        df = spark.table(target)
        if pipeline_ids:
            ids = [pid.replace("'", "''") for pid in pipeline_ids if pid]
            if ids:
                in_list = ", ".join(f"'{pid}'" for pid in ids)
                df = df.filter(f"pipeline_id IN ({in_list})")
        rows = df.collect()
    except Exception:
        return {}

    out: dict[str, ReconEventLogWatermark] = {}
    for row in rows:
        data = row.asDict()
        pid = str(data.get("pipeline_id") or "").strip()
        if not pid:
            continue
        out[pid] = ReconEventLogWatermark(
            pipeline_id=pid,
            pipeline_key=str(data.get("pipeline_key") or "").strip(),
            last_event_ts=data.get("last_event_ts"),
            last_event_id=str(data.get("last_event_id") or "").strip(),
            last_update_id=str(data.get("last_update_id") or "").strip(),
            last_api_update_state=str(data.get("last_api_update_state") or "").strip().upper(),
            last_poll_at=data.get("last_poll_at"),
        )
    return out


def upsert_recon_event_log_watermark(
    spark,
    catalog: str,
    schema: str,
    watermark: ReconEventLogWatermark,
) -> None:
    ensure_recon_tables(spark, catalog, schema)
    target = qualified_table(catalog, schema, RECON_EVENT_LOG_WATERMARK_TABLE)
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    schema_def = StructType(
        [
            StructField("pipeline_id", StringType(), False),
            StructField("pipeline_key", StringType(), True),
            StructField("last_event_ts", TimestampType(), True),
            StructField("last_event_id", StringType(), True),
            StructField("last_update_id", StringType(), True),
            StructField("last_api_update_state", StringType(), True),
            StructField("last_poll_at", TimestampType(), True),
        ]
    )
    df = spark.createDataFrame([watermark.as_dict()], schema=schema_def)
    df.createOrReplaceTempView("new_recon_watermark")
    spark.sql(
        f"""
        MERGE INTO {target} AS target
        USING new_recon_watermark AS source
        ON target.pipeline_id = source.pipeline_id
        WHEN MATCHED THEN UPDATE SET
          pipeline_key = source.pipeline_key,
          last_event_ts = source.last_event_ts,
          last_event_id = source.last_event_id,
          last_update_id = source.last_update_id,
          last_api_update_state = source.last_api_update_state,
          last_poll_at = source.last_poll_at
        WHEN NOT MATCHED THEN INSERT (
          pipeline_id,
          pipeline_key,
          last_event_ts,
          last_event_id,
          last_update_id,
          last_api_update_state,
          last_poll_at
        )
        VALUES (
          source.pipeline_id,
          source.pipeline_key,
          source.last_event_ts,
          source.last_event_id,
          source.last_update_id,
          source.last_api_update_state,
          source.last_poll_at
        )
        """
    )


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
    spark_schema = _flow_metrics_spark_schema()
    df = _create_typed_dataframe(spark, [row.as_dict() for row in rows], spark_schema)
    df.createOrReplaceTempView("new_flow_metrics")
    spark.sql(
        f"""
        MERGE INTO {target} AS target
        USING new_flow_metrics AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN INSERT (
          event_id,
          pipeline_id,
          pipeline_name,
          update_id,
          flow_name,
          table_name,
          event_timestamp,
          flow_status,
          output_rows,
          rows_upserted,
          rows_deleted,
          output_bytes,
          client_nm,
          destination_schema,
          destination_table,
          captured_at
        )
        VALUES (
          source.event_id,
          source.pipeline_id,
          source.pipeline_name,
          source.update_id,
          source.flow_name,
          source.table_name,
          source.event_timestamp,
          source.flow_status,
          source.output_rows,
          source.rows_upserted,
          source.rows_deleted,
          source.output_bytes,
          source.client_nm,
          source.destination_schema,
          source.destination_table,
          source.captured_at
        )
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
    spark_schema = _flow_summary_spark_schema()
    df = _create_typed_dataframe(spark, [row.as_dict() for row in rows], spark_schema)
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
    ensure_recon_ready_table(spark, catalog, schema)
    target = qualified_table(catalog, schema, RECON_READY_TABLE)
    spark_schema = _recon_ready_spark_schema()
    df = _create_typed_dataframe(spark, [row.as_dict() for row in rows], spark_schema)
    df.write.format("delta").mode("append").saveAsTable(target)
    return len(rows)
