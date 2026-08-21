"""Direct SQL Server access for Change Tracking recon counts (pyodbc preferred on Databricks)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from common.ops.source_ct_ops import build_version_ct_count_sql

try:
    import pyodbc
except ImportError:  # pragma: no cover
    pyodbc = None  # type: ignore[assignment]

try:
    import pymssql
except ImportError:  # pragma: no cover
    pymssql = None  # type: ignore[assignment]


class _DbutilsSecrets(Protocol):
    def get(self, scope: str, key: str) -> str: ...


@dataclass(frozen=True)
class SqlServerDirectConfig:
    host: str
    port: int
    database: str
    username: str
    password: str


def _pick_odbc_driver() -> str:
    if pyodbc is None:
        raise RuntimeError("pyodbc is not installed")
    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    for preferred in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"):
        if preferred in drivers:
            return preferred
    if not drivers:
        raise RuntimeError(
            "No SQL Server ODBC driver found. Run cluster init script install_sql_recon_dependencies.sh "
            "or install msodbcsql18 on the cluster."
        )
    return drivers[-1]


def build_pyodbc_connection_string(config: SqlServerDirectConfig, *, driver: str | None = None) -> str:
    odbc_driver = driver or _pick_odbc_driver()
    return (
        f"DRIVER={{{odbc_driver}}};"
        f"SERVER={config.host},{config.port};"
        f"DATABASE={config.database};"
        f"UID={config.username};"
        f"PWD={config.password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
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


def open_sql_server_connection(config: SqlServerDirectConfig, *, prefer: str = "pyodbc") -> Any:
    """Open a SQL connection. Prefer pyodbc on Databricks (pymssql can crash the kernel)."""
    errors: list[str] = []
    if prefer != "pymssql" and pyodbc is not None:
        try:
            conn_str = build_pyodbc_connection_string(config)
            return pyodbc.connect(conn_str, autocommit=True)
        except Exception as exc:
            errors.append(f"pyodbc: {exc}")

    if pymssql is not None:
        try:
            return pymssql.connect(
                server=config.host,
                port=config.port,
                user=config.username,
                password=config.password,
                database=config.database,
                login_timeout=30,
                timeout=300,
                tds_version="7.4",
            )
        except Exception as exc:
            errors.append(f"pymssql: {exc}")

    if pyodbc is None and pymssql is None:
        raise RuntimeError(
            "Neither pyodbc nor pymssql is installed. Attach to cluster ipac_sql_recon_shared "
            "or run install_sql_recon_dependencies.sh init script."
        )
    raise RuntimeError("Failed to connect to SQL Server: " + "; ".join(errors))


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
