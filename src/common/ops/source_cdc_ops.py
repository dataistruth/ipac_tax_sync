"""SQL Server CDC change counts for ingestion reconciliation (recon_type 2/3)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

# SQL Server CDC __$operation: 1=delete, 2=insert, 3=update before, 4=update after
CDC_OPS_CHANGE_ROWS = (1, 2, 4)
CDC_OPS_UPSERT_ROWS = (2, 4)


def default_capture_instance(src_schema: str, table_nm: str) -> str:
    """Default SQL Server CDC capture instance: {schema}_{table}."""
    schema = src_schema.strip() or "dbo"
    return f"{schema}_{table_nm}"


def _format_sql_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _ops_for_recon_type(recon_type: int) -> tuple[int, ...]:
    if recon_type == 2:
        return CDC_OPS_CHANGE_ROWS
    if recon_type == 3:
        return CDC_OPS_UPSERT_ROWS
    return CDC_OPS_CHANGE_ROWS


def build_cdc_count_sql(
    src_schema: str,
    table_nm: str,
    capture_instance: str | None,
    start_time: datetime,
    end_time: datetime,
    recon_type: int,
) -> str:
    """Build SQL to count CDC rows in a time window (for UC federated SQL Server catalog)."""
    instance = capture_instance or default_capture_instance(src_schema, table_nm)
    ops = _ops_for_recon_type(recon_type)
    ops_sql = ", ".join(str(o) for o in ops)
    start_s = _format_sql_datetime(start_time)
    end_s = _format_sql_datetime(end_time)
    cdc_table = f"cdc.{instance}_CT"
    return f"""
SELECT COUNT_BIG(*) AS change_rows
FROM {cdc_table}
WHERE __$operation IN ({ops_sql})
  AND __$start_lsn >= sys.fn_cdc_map_time_to_lsn(
        'smallest greater than or equal', CAST('{start_s}' AS DATETIME2), NULL
      )
  AND __$start_lsn <= sys.fn_cdc_map_time_to_lsn(
        'largest less than or equal', CAST('{end_s}' AS DATETIME2), NULL
      )
""".strip()


def build_federated_cdc_count_sql(
    src_catalog: str,
    src_schema: str,
    table_nm: str,
    capture_instance: str | None,
    start_time: datetime,
    end_time: datetime,
    recon_type: int,
) -> str:
    """Wrap CDC count for Unity Catalog three-part name against foreign SQL catalog."""
    instance = capture_instance or default_capture_instance(src_schema, table_nm)
    inner = build_cdc_count_sql(src_schema, table_nm, instance, start_time, end_time, recon_type)
    # Replace cdc.{instance}_CT with qualified federated name
    qualified = f"{src_catalog}.cdc.{instance}_CT"
    return inner.replace(f"cdc.{instance}_CT", qualified)


def run_source_cdc_count(
    spark,
    src_catalog: str,
    src_schema: str,
    table_nm: str,
    start_time: datetime,
    end_time: datetime,
    recon_type: int,
    capture_instance: str | None = None,
) -> int | None:
    """Execute CDC count via Spark SQL (UC federated catalog). Returns None on failure."""
    sql = build_federated_cdc_count_sql(
        src_catalog,
        src_schema,
        table_nm,
        capture_instance,
        start_time,
        end_time,
        recon_type,
    )
    try:
        row = spark.sql(sql).collect()[0]
        value = row["change_rows"]
        return int(value) if value is not None else 0
    except Exception:
        return None


def ingest_metric_for_recon_type(summary: Any, recon_type: int) -> int:
    if recon_type == 3:
        return int(summary.total_upserted)
    return int(summary.total_change_rows)
