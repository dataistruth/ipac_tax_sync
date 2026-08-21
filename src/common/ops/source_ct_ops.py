"""SQL Server Change Tracking (CT) counts for ingestion reconciliation (recon_type 2/3).

Uses version-based CHANGETABLE queries (no sys.dm_tran_commit_time). Direct execution
Direct SQL Server CT counts via mssql-python live in source_ct_direct.py; Spark federation helpers remain for optional probes.
"""

from __future__ import annotations

from typing import Any

# CHANGETABLE SYS_CHANGE_OPERATION: I=insert, U=update, D=delete
CT_OPS_CHANGE_ROWS = ("I", "U", "D")
CT_OPS_UPSERT_ROWS = ("I", "U")


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


def build_version_ct_count_sql(
    src_schema: str,
    table_nm: str,
    version_before: int,
    version_after: int | None,
    recon_type: int,
) -> str:
    """Count CT rows with sys_change_version in (version_before, version_after]."""
    qualified = _qualified_table(src_schema, table_nm)
    ops = _ops_for_recon_type(recon_type)
    ops_sql = ", ".join(f"'{op}'" for op in ops)
    upper_filter = ""
    if version_after is not None:
        upper_filter = f"\n  AND ct.sys_change_version <= {int(version_after)}"
    return f"""
SELECT COUNT_BIG(*) AS change_rows
FROM CHANGETABLE(CHANGES {qualified}, {int(version_before)}) AS ct
WHERE ct.SYS_CHANGE_OPERATION IN ({ops_sql}){upper_filter}
""".strip()


def build_ct_count_sql(
    src_schema: str,
    table_nm: str,
    version_before: int,
    version_after: int | None,
    recon_type: int,
) -> str:
    """Alias for build_version_ct_count_sql (native two-part table name)."""
    return build_version_ct_count_sql(
        src_schema,
        table_nm,
        version_before,
        version_after,
        recon_type,
    )


def build_federated_ct_count_sql(
    src_catalog: str,
    src_schema: str,
    table_nm: str,
    version_before: int,
    version_after: int | None,
    recon_type: int,
) -> str:
    """Version-based CT count for UC federated catalog (three-part name)."""
    qualified = _federated_table(src_catalog, src_schema, table_nm)
    ops = _ops_for_recon_type(recon_type)
    ops_sql = ", ".join(f"'{op}'" for op in ops)
    upper_filter = ""
    if version_after is not None:
        upper_filter = f"\n  AND ct.sys_change_version <= {int(version_after)}"
    return f"""
SELECT COUNT_BIG(*) AS change_rows
FROM CHANGETABLE(CHANGES {qualified}, {int(version_before)}) AS ct
WHERE ct.SYS_CHANGE_OPERATION IN ({ops_sql}){upper_filter}
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
    version_before: int | None = None,
    version_after: int | None = None,
    print_results: bool = True,
) -> dict[str, Any]:
    """Run connectivity + CT version probes against federated SQL Server (Spark SQL)."""
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

    conn_sql = build_ct_connection_probe_sql(src_catalog, src_schema, table_nm)
    try:
        spark.sql(conn_sql).collect()
        result["connection_ok"] = True
        _log("[CT probe] connection OK (read from federated table succeeded)")
    except Exception as exc:
        result["errors"].append(f"connection: {exc}")
        _log(f"[CT probe] connection FAILED: {exc}")

    max_sql = build_ct_max_version_sql(src_catalog, src_schema, table_nm)
    ok, max_ver, err = _collect_scalar(spark, max_sql, "max_change_version")
    if ok:
        result["max_change_version"] = max_ver
        _log(f"[CT probe] max sys_change_version in CHANGETABLE: {max_ver}")
    elif err:
        result["errors"].append(f"max_version: {err}")

    cur_sql = build_ct_current_version_sql(src_catalog)
    ok, cur_ver, err = _collect_scalar(spark, cur_sql, "current_ct_version")
    if ok:
        result["current_ct_version"] = cur_ver
        _log(f"[CT probe] CHANGE_TRACKING_CURRENT_VERSION(): {cur_ver}")
    elif err:
        result["errors"].append(f"current_version: {err}")

    if version_before is not None:
        count_sql = build_federated_ct_count_sql(
            src_catalog,
            src_schema,
            table_nm,
            version_before,
            version_after,
            recon_type,
        )
        ok, window_count, err = _collect_scalar(spark, count_sql, "change_rows")
        if ok:
            result["window_change_rows"] = window_count
            _log(f"[CT probe] change rows versions {version_before}..{version_after}: {window_count}")
        elif err:
            result["errors"].append(f"window_count: {err}")

    return result


def run_source_ct_count(
    spark,
    src_catalog: str,
    src_schema: str,
    table_nm: str,
    version_before: int,
    version_after: int | None,
    recon_type: int,
    verbose: bool = False,
) -> int | None:
    """Execute CT change count via Spark SQL (UC federated catalog). Returns None on failure."""
    sql = build_federated_ct_count_sql(
        src_catalog,
        src_schema,
        table_nm,
        version_before,
        version_after,
        recon_type,
    )
    if verbose:
        print(
            f"[CT recon federated] catalog={src_catalog} schema={src_schema} table={table_nm} "
            f"versions={version_before}..{version_after} recon_type={recon_type}"
        )
        print(f"[CT recon federated] SQL: {sql}")
    try:
        row = spark.sql(sql).collect()[0]
        value = row["change_rows"]
        count = int(value) if value is not None else 0
        if verbose:
            print(f"[CT recon federated] change_rows={count}")
        return count
    except Exception as exc:
        if verbose:
            print(f"[CT recon federated] FAILED: {exc}")
        return None


def run_source_cdc_count(
    spark,
    src_catalog: str,
    src_schema: str,
    table_nm: str,
    version_before: int,
    version_after: int | None,
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
        version_before,
        version_after,
        recon_type,
    )


def ingest_metric_for_recon_type(summary: Any, recon_type: int) -> int:
    if recon_type == 3:
        return int(summary.total_upserted)
    return int(summary.total_change_rows)
