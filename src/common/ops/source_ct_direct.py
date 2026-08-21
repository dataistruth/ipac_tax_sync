"""Direct SQL Server access for Change Tracking recon via mssql-python."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from common.ops.source_ct_ops import build_version_ct_count_sql

try:
    from mssql_python import connect as mssql_python_connect
except ImportError:  # pragma: no cover
    mssql_python_connect = None  # type: ignore[assignment,misc]


class _DbutilsSecrets(Protocol):
    def get(self, scope: str, key: str) -> str: ...


@dataclass(frozen=True)
class SqlServerDirectConfig:
    host: str
    port: int
    database: str
    username: str
    password: str


def build_mssql_python_connection_string(config: SqlServerDirectConfig) -> str:
    return (
        f"Server={config.host},{config.port};"
        f"Database={config.database};"
        f"UID={config.username};"
        f"PWD={config.password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )


def resolve_sql_server_config(
    client: Any,
    *,
    dbutils: Any | None = None,
    host_override: str = "",
    port_override: int | None = None,
    username_override: str = "",
    password_override: str = "",
    secret_scope_override: str = "",
    username_secret_key: str = "",
    password_secret_key: str = "",
    host_secret_key: str = "",
) -> SqlServerDirectConfig:
    """Build connection settings from client.json + optional notebook overrides/secrets."""
    scope = (secret_scope_override or getattr(client, "sql_audit_secret_scope", "") or "").strip()
    host_key = (
        host_secret_key
        or getattr(client, "sql_host_secret_key", "")
        or "SQL_SERVER_HOST"
    )

    host = (host_override or getattr(client, "sql_host", "") or "").strip()
    if not host and scope and dbutils is not None:
        try:
            host = (dbutils.secrets.get(scope=scope, key=host_key) or "").strip()
        except Exception:
            host = ""
    if not host:
        raise ValueError(
            f"sql_host is required for direct CT recon (client={client.client_nm}). "
            "Set config/common/client.json sql_host, secret SQL_SERVER_HOST, or notebook widget sql_host."
        )

    port = port_override if port_override is not None else int(getattr(client, "sql_port", 1433) or 1433)
    database = (getattr(client, "src_db_nm", "") or "").strip()
    if not database:
        raise ValueError(f"src_db_nm is required for client {client.client_nm}")

    user_key = (
        username_secret_key
        or getattr(client, "sql_audit_username_secret_key", "")
        or getattr(client, "sql_username_secret_key", "")
        or "SQL_SERVER_AUDIT_USERNAME"
    )
    pass_key = (
        password_secret_key
        or getattr(client, "sql_audit_password_secret_key", "")
        or getattr(client, "sql_password_secret_key", "")
        or "SQL_SERVER_AUDIT_PASSWORD"
    )

    if scope and dbutils is not None:
        username = dbutils.secrets.get(scope=scope, key=user_key)
        password = dbutils.secrets.get(scope=scope, key=pass_key)
    else:
        username = (username_override or getattr(client, "sql_username", "") or "").strip()
        password = password_override or getattr(client, "sql_password", "") or ""
        if not username:
            raise ValueError(
                f"SQL credentials required for client {client.client_nm}. "
                "Set sql_audit_secret_scope + dbutils secrets or sql_username/sql_password overrides."
            )

    return SqlServerDirectConfig(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
    )


def open_sql_server_connection(config: SqlServerDirectConfig) -> Any:
    """Open a SQL connection using mssql-python (install via %pip in notebook)."""
    if mssql_python_connect is None:
        raise RuntimeError(
            "mssql-python is not installed. Run %pip install mssql-python in the notebook first."
        )
    conn = mssql_python_connect(build_mssql_python_connection_string(config))
    if hasattr(conn, "autocommit"):
        conn.autocommit = True
    return conn


def cursor_column_names(cursor: Any) -> list[str]:
    if not cursor.description:
        return []
    return [str(col[0]) for col in cursor.description]


def row_as_dict(row: Any, column_names: list[str]) -> dict[str, Any]:
    return {name: row[idx] for idx, name in enumerate(column_names)}


def fetch_one_as_dict(conn: Any, sql: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = cursor_column_names(cur)
        row = cur.fetchone()
        if row is None:
            return None
        return row_as_dict(row, cols)


def fetch_all_as_dict(conn: Any, sql: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = cursor_column_names(cur)
        return [row_as_dict(row, cols) for row in cur.fetchall()]


def fetch_scalar(conn: Any, sql: str, column: str) -> int | None:
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if row is None:
            return None
        value = row[0] if not hasattr(row, column) else row[column]
        if value is None:
            return None
        return int(value)


def fetch_change_tracking_current_version(conn: Any) -> int | None:
    sql = "SELECT CHANGE_TRACKING_CURRENT_VERSION() AS current_ct_version;"
    return fetch_scalar(conn, sql, "current_ct_version")


def fetch_min_valid_ct_version(conn: Any, src_schema: str, table_nm: str) -> int | None:
    schema = (src_schema or "dbo").replace("'", "''")
    table = table_nm.replace("'", "''")
    sql = f"""
SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('{schema}.{table}')) AS min_valid_version;
""".strip()
    return fetch_scalar(conn, sql, "min_valid_version")


def run_source_ct_count_direct(
    conn: Any,
    src_schema: str,
    table_nm: str,
    version_before: int,
    version_after: int | None,
    recon_type: int,
    *,
    verbose: bool = False,
) -> int | None:
    sql = build_version_ct_count_sql(
        src_schema,
        table_nm,
        version_before,
        version_after,
        recon_type,
    )
    if verbose:
        print(
            f"[CT recon direct] schema={src_schema} table={table_nm} "
            f"versions={version_before}..{version_after} recon_type={recon_type}"
        )
        print(f"[CT recon direct] SQL: {sql}")
    try:
        count = fetch_scalar(conn, sql, "change_rows")
        if verbose:
            print(f"[CT recon direct] change_rows={count}")
        return 0 if count is None else count
    except Exception as exc:
        if verbose:
            print(f"[CT recon direct] FAILED: {exc}")
        return None


def probe_source_ct_connection_direct(
    conn: Any,
    src_schema: str,
    table_nm: str,
    *,
    version_before: int | None = None,
    version_after: int | None = None,
    recon_type: int = 2,
    print_results: bool = True,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "src_schema": src_schema,
        "table_nm": table_nm,
        "connection_ok": False,
        "current_ct_version": None,
        "min_valid_version": None,
        "window_change_rows": None,
        "errors": [],
    }

    def _log(message: str) -> None:
        if print_results:
            print(message)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS connection_ok;")
            cur.fetchone()
        result["connection_ok"] = True
        _log("[CT probe direct] connection OK")
    except Exception as exc:
        result["errors"].append(f"connection: {exc}")
        _log(f"[CT probe direct] connection FAILED: {exc}")
        return result

    try:
        result["current_ct_version"] = fetch_change_tracking_current_version(conn)
        _log(f"[CT probe direct] CHANGE_TRACKING_CURRENT_VERSION(): {result['current_ct_version']}")
    except Exception as exc:
        result["errors"].append(f"current_version: {exc}")

    try:
        result["min_valid_version"] = fetch_min_valid_ct_version(conn, src_schema, table_nm)
        _log(f"[CT probe direct] min valid version: {result['min_valid_version']}")
    except Exception as exc:
        result["errors"].append(f"min_valid_version: {exc}")

    if version_before is not None:
        try:
            result["window_change_rows"] = run_source_ct_count_direct(
                conn,
                src_schema,
                table_nm,
                version_before,
                version_after,
                recon_type,
                verbose=print_results,
            )
        except Exception as exc:
            result["errors"].append(f"window_count: {exc}")

    return result
