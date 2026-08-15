"""SQL Server Change Tracking (CT) counts for ingestion reconciliation (recon_type 2/3).

PK tables use ENABLE CHANGE_TRACKING (see *_enable_ct.sql). This module queries
CHANGETABLE + sys.dm_tran_commit_time — not CDC change tables.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

# CHANGETABLE SYS_CHANGE_OPERATION: I=insert, U=update, D=delete
CT_OPS_CHANGE_ROWS = ("I", "U", "D")
CT_OPS_UPSERT_ROWS = ("I", "U")


def _format_sql_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _ops_for_recon_type(recon_type: int) -> tuple[str, ...]:
    if recon_type == 2:
        return CT_OPS_CHANGE_ROWS
    if recon_type == 3:
        return CT_OPS_UPSERT_ROWS
    return CT_OPS_CHANGE_ROWS


def _qualified_table(src_schema: str, table_nm: str) -> str:
    schema = src_schema.strip() or "dbo"
    return f"{schema}.{table_nm}"


def _federated_table(src_catalog: str, src_schema: str, table_nm: str) -> str:
    schema = src_schema.strip() or "dbo"
    return f"{src_catalog}.{schema}.{table_nm}"


def build_ct_count_sql(
    src_schema: str,
    table_nm: str,
    start_time: datetime,
    end_time: datetime,
    recon_type: int,
) -> str:
    """Count CT changes in [start_time, end_time] via CHANGETABLE(CHANGES ..., 0)."""
    qualified = _qualified_table(src_schema, table_nm)
    ops = _ops_for_recon_type(recon_type)
    ops_sql = ", ".join(f"'{op}'" for op in ops)
    start_s = _format_sql_datetime(start_time)
    end_s = _format_sql_datetime(end_time)
    return f"""
SELECT COUNT_BIG(*) AS change_rows
FROM CHANGETABLE(CHANGES {qualified}, 0) AS ct
INNER JOIN sys.dm_tran_commit_time AS txn ON ct.sys_change_version = txn.version
WHERE txn.commit_time >= CAST('{start_s}' AS DATETIME2)
  AND txn.commit_time <= CAST('{end_s}' AS DATETIME2)
  AND ct.SYS_CHANGE_OPERATION IN ({ops_sql})
""".strip()


def build_federated_ct_count_sql(
    src_catalog: str,
    src_schema: str,
    table_nm: str,
    start_time: datetime,
    end_time: datetime,
    recon_type: int,
) -> str:
    """CT count SQL for Unity Catalog federated SQL Server catalog (three-part name)."""
    qualified = _federated_table(src_catalog, src_schema, table_nm)
    ops = _ops_for_recon_type(recon_type)
    ops_sql = ", ".join(f"'{op}'" for op in ops)
    start_s = _format_sql_datetime(start_time)
    end_s = _format_sql_datetime(end_time)
    return f"""
SELECT COUNT_BIG(*) AS change_rows
FROM CHANGETABLE(CHANGES {qualified}, 0) AS ct
INNER JOIN sys.dm_tran_commit_time AS txn ON ct.sys_change_version = txn.version
WHERE txn.commit_time >= CAST('{start_s}' AS DATETIME2)
  AND txn.commit_time <= CAST('{end_s}' AS DATETIME2)
  AND ct.SYS_CHANGE_OPERATION IN ({ops_sql})
""".strip()


def run_source_ct_count(
    spark,
    src_catalog: str,
    src_schema: str,
    table_nm: str,
    start_time: datetime,
    end_time: datetime,
    recon_type: int,
) -> int | None:
    """Execute CT change count via Spark SQL (UC federated catalog). Returns None on failure."""
    sql = build_federated_ct_count_sql(
        src_catalog,
        src_schema,
        table_nm,
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
    """Backward-compatible alias — source uses Change Tracking, not CDC."""
    _ = capture_instance
    return run_source_ct_count(
        spark,
        src_catalog,
        src_schema,
        table_nm,
        start_time,
        end_time,
        recon_type,
    )


def ingest_metric_for_recon_type(summary: Any, recon_type: int) -> int:
    if recon_type == 3:
        return int(summary.total_upserted)
    return int(summary.total_change_rows)
