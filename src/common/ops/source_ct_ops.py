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


def build_ct_connection_probe_sql(src_catalog: str, src_schema: str, table_nm: str) -> str:
    """Lightweight read against federated source table — verifies UC catalog connectivity."""
    qualified = _federated_table(src_catalog, src_schema, table_nm)
    return f"SELECT 1 AS connection_ok FROM {qualified} LIMIT 1"


def build_ct_max_version_sql(src_catalog: str, src_schema: str, table_nm: str) -> str:
    """Highest sys_change_version currently visible in CHANGETABLE for the table."""
    qualified = _federated_table(src_catalog, src_schema, table_nm)
    return f"""
SELECT MAX(ct.sys_change_version) AS max_change_version
FROM CHANGETABLE(CHANGES {qualified}, 0) AS ct
""".strip()


def build_ct_current_version_sql(src_catalog: str) -> str:
    """Database-level CHANGE_TRACKING_CURRENT_VERSION() (SQL Server)."""
    catalog = src_catalog.strip().replace("`", "")
    return f"SELECT CHANGE_TRACKING_CURRENT_VERSION() AS current_ct_version FROM `{catalog}`.INFORMATION_SCHEMA.TABLES LIMIT 1"


def _collect_scalar(spark, sql: str, column: str) -> tuple[bool, int | None, str | None]:
    try:
        row = spark.sql(sql).collect()[0]
        value = row[column]
        if value is None:
            return True, None, None
        return True, int(value), None
    except Exception as exc:
        return False, None, str(exc)


def probe_source_ct_connection(
    spark,
    src_catalog: str,
    src_schema: str,
    table_nm: str,
    *,
    recon_type: int = 2,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    print_results: bool = True,
) -> dict[str, Any]:
    """
  Run connectivity + Change Tracking version probes against federated SQL Server.
  Returns a dict suitable for logging; prints when print_results=True.
  """
    qualified = _federated_table(src_catalog, src_schema, table_nm)
    result: dict[str, Any] = {
        "src_catalog": src_catalog,
        "src_schema": src_schema,
        "table_nm": table_nm,
        "qualified_table": qualified,
        "connection_ok": False,
        "max_change_version": None,
        "current_ct_version": None,
        "window_change_rows": None,
        "errors": [],
    }

    def _log(message: str) -> None:
        if print_results:
            print(message)

    _log(f"[CT probe] federated table: {qualified}")
    _log(f"[CT probe] uc_conn uses catalog name from client.src_db_nm (same as ingest source_catalog)")

    conn_sql = build_ct_connection_probe_sql(src_catalog, src_schema, table_nm)
    _log(f"[CT probe] connection SQL: {conn_sql}")
    try:
        spark.sql(conn_sql).collect()
        result["connection_ok"] = True
        _log("[CT probe] connection OK (read from federated table succeeded)")
    except Exception as exc:
        result["errors"].append(f"connection: {exc}")
        _log(f"[CT probe] connection FAILED: {exc}")

    max_sql = build_ct_max_version_sql(src_catalog, src_schema, table_nm)
    _log(f"[CT probe] max version SQL: {max_sql}")
    ok, max_ver, err = _collect_scalar(spark, max_sql, "max_change_version")
    if ok:
        result["max_change_version"] = max_ver
        _log(f"[CT probe] max sys_change_version in CHANGETABLE: {max_ver}")
    else:
        result["errors"].append(f"max_version: {err}")
        _log(f"[CT probe] max version query FAILED: {err}")

    cur_sql = build_ct_current_version_sql(src_catalog)
    _log(f"[CT probe] current version SQL: {cur_sql}")
    ok, cur_ver, err = _collect_scalar(spark, cur_sql, "current_ct_version")
    if ok:
        result["current_ct_version"] = cur_ver
        _log(f"[CT probe] CHANGE_TRACKING_CURRENT_VERSION(): {cur_ver}")
    else:
        result["errors"].append(f"current_version: {err}")
        _log(f"[CT probe] current version query FAILED (optional): {err}")

    if start_time is not None and end_time is not None:
        count_sql = build_federated_ct_count_sql(
            src_catalog, src_schema, table_nm, start_time, end_time, recon_type
        )
        _log(f"[CT probe] window count SQL (recon_type={recon_type}): {count_sql}")
        ok, window_count, err = _collect_scalar(spark, count_sql, "change_rows")
        if ok:
            result["window_change_rows"] = window_count
            _log(
                f"[CT probe] change rows in [{start_time} .. {end_time}]: {window_count}"
            )
        else:
            result["errors"].append(f"window_count: {err}")
            _log(f"[CT probe] window count FAILED: {err}")
    else:
        _log("[CT probe] window count skipped (no start_time/end_time provided)")

    return result


def run_source_ct_count(
    spark,
    src_catalog: str,
    src_schema: str,
    table_nm: str,
    start_time: datetime,
    end_time: datetime,
    recon_type: int,
    verbose: bool = False,
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
    if verbose:
        print(
            f"[CT recon] catalog={src_catalog} schema={src_schema} table={table_nm} "
            f"recon_type={recon_type} window={start_time} .. {end_time}"
        )
        print(f"[CT recon] SQL: {sql}")
    try:
        row = spark.sql(sql).collect()[0]
        value = row["change_rows"]
        count = int(value) if value is not None else 0
        if verbose:
            print(f"[CT recon] change_rows={count}")
        return count
    except Exception as exc:
        if verbose:
            print(f"[CT recon] FAILED: {exc}")
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
