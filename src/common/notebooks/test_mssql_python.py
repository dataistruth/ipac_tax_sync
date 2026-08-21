# Databricks notebook source
# MAGIC %md
# MAGIC # Test `mssql-python` → SQL Server
# MAGIC
# MAGIC Microsoft's official driver — bundles ODBC via `mssql-python-odbc` wheel (no cluster init script).

# COMMAND ----------

# MAGIC %pip install -q "mssql-python>=1.13.0"
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("auth_mode", "secrets", ["secrets", "widgets"], "Credential source")
dbutils.widgets.text("sql_host", "", "SQL host")
dbutils.widgets.text("sql_port", "1433", "SQL port")
dbutils.widgets.text("sql_database", "", "Database name")
dbutils.widgets.text("sql_username", "", "Username (widgets mode)")
dbutils.widgets.text("sql_password", "", "Password (widgets mode)")
dbutils.widgets.text("secret_scope", "scope_ipacs_audit", "Secret scope")
dbutils.widgets.text("secret_host_key", "SQL_SERVER_HOST", "Secret key: host")
dbutils.widgets.text("secret_user_key", "SQL_SERVER_AUDIT_USERNAME", "Secret key: username")
dbutils.widgets.text("secret_pass_key", "SQL_SERVER_AUDIT_PASSWORD", "Secret key: password")

auth_mode = dbutils.widgets.get("auth_mode").strip().lower()
sql_host = dbutils.widgets.get("sql_host").strip()
sql_port = dbutils.widgets.get("sql_port").strip() or "1433"
sql_database = dbutils.widgets.get("sql_database").strip()
secret_scope = dbutils.widgets.get("secret_scope").strip()
secret_host_key = dbutils.widgets.get("secret_host_key").strip() or "SQL_SERVER_HOST"
secret_user_key = dbutils.widgets.get("secret_user_key").strip() or "SQL_SERVER_AUDIT_USERNAME"
secret_pass_key = dbutils.widgets.get("secret_pass_key").strip() or "SQL_SERVER_AUDIT_PASSWORD"

if auth_mode == "secrets":
    if not secret_scope:
        raise ValueError("secret_scope is required when auth_mode=secrets")
    sql_host = sql_host or dbutils.secrets.get(scope=secret_scope, key=secret_host_key)
    sql_username = dbutils.secrets.get(scope=secret_scope, key=secret_user_key)
    sql_password = dbutils.secrets.get(scope=secret_scope, key=secret_pass_key)
else:
    sql_username = dbutils.widgets.get("sql_username").strip()
    sql_password = dbutils.widgets.get("sql_password")

if not sql_host:
    raise ValueError("sql_host is required (widget or secret)")
if not sql_database:
    raise ValueError("sql_database is required")
if not sql_username:
    raise ValueError("sql_username is required")

print(f"host={sql_host} port={sql_port} database={sql_database} user={sql_username}")

# COMMAND ----------

from mssql_python import connect

conn_str = (
    f"Server={sql_host},{sql_port};"
    f"Database={sql_database};"
    f"UID={sql_username};"
    f"PWD={sql_password};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

print("Connecting with mssql-python...")
with connect(conn_str) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT DB_NAME() AS db_name, SUSER_SNAME() AS login_name, @@VERSION AS version")
        row = cursor.fetchone()
        print("db_name   :", row[0])
        print("login_name:", row[1])
        print("version   :", row[2][:120], "...")

        cursor.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION() AS ct_version")
        ct_row = cursor.fetchone()
        print("CT version:", ct_row[0])

print("SUCCESS — mssql-python works on this cluster.")
