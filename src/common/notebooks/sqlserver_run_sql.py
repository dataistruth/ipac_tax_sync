# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server — connect with username / password and run SQL
# MAGIC
# MAGIC Use this notebook to:
# MAGIC - Test connectivity to Azure SQL / SQL Server from Databricks
# MAGIC - Run ad-hoc SQL (SELECT, TRUNCATE, etc.)
# MAGIC - Bulk load scale data into `dbo.K1Input_Snapshot` for Lakeflow perf tests
# MAGIC
# MAGIC **Credentials:** widget values below, or Databricks secrets (optional).
# MAGIC
# MAGIC **Cluster:** needs outbound network access to SQL Server (1433).

# COMMAND ----------

# MAGIC %pip install -q pymssql

# COMMAND ----------

dbutils.widgets.dropdown("auth_mode", "widgets", ["widgets", "secrets"], "Credential source")
dbutils.widgets.text("sql_host", "", "SQL host (e.g. myserver.database.windows.net)")
dbutils.widgets.text("sql_port", "1433", "SQL port")
dbutils.widgets.text("sql_database", "iPC_2025_Dev7_15347", "Database name")
dbutils.widgets.text("sql_username", "", "Username")
dbutils.widgets.text("sql_password", "", "Password")
dbutils.widgets.text("secret_scope", "", "Secret scope (auth_mode=secrets)")
dbutils.widgets.text("secret_user_key", "sql-username", "Secret key: username")
dbutils.widgets.text("secret_pass_key", "sql-password", "Secret key: password")

dbutils.widgets.dropdown("action", "test_connection", [
    "test_connection",
    "run_custom_sql",
    "count_k1input_snapshot",
    "truncate_k1input_snapshot",
    "load_k1input_snapshot",
], "Action")

dbutils.widgets.text("target_rows", "10000", "load_k1input_snapshot: row count")
dbutils.widgets.text("batch_size", "50000", "load_k1input_snapshot: batch size")
dbutils.widgets.text("client_id", "15347", "load_k1input_snapshot: ClientID column")
dbutils.widgets.text("tax_period_id", "2025", "load_k1input_snapshot: TaxPeriodID column")

dbutils.widgets.text("custom_sql", "SELECT DB_NAME() AS db_name, @@VERSION AS version;", "run_custom_sql: T-SQL")

auth_mode = dbutils.widgets.get("auth_mode").strip().lower()
sql_host = dbutils.widgets.get("sql_host").strip()
sql_port = int(dbutils.widgets.get("sql_port").strip() or "1433")
sql_database = dbutils.widgets.get("sql_database").strip()
sql_username = dbutils.widgets.get("sql_username").strip()
sql_password = dbutils.widgets.get("sql_password")
secret_scope = dbutils.widgets.get("secret_scope").strip()
secret_user_key = dbutils.widgets.get("secret_user_key").strip() or "sql-username"
secret_pass_key = dbutils.widgets.get("secret_pass_key").strip() or "sql-password"

action = dbutils.widgets.get("action").strip()
target_rows = int(dbutils.widgets.get("target_rows").strip() or "10000")
batch_size = int(dbutils.widgets.get("batch_size").strip() or "50000")
client_id = int(dbutils.widgets.get("client_id").strip() or "15347")
tax_period_id = int(dbutils.widgets.get("tax_period_id").strip() or "2025")
custom_sql = dbutils.widgets.get("custom_sql").strip()

print(f"auth_mode    : {auth_mode}")
print(f"sql_host     : {sql_host}")
print(f"sql_port     : {sql_port}")
print(f"sql_database : {sql_database}")
print(f"action       : {action}")
if action == "load_k1input_snapshot":
    print(f"target_rows  : {target_rows}")
    print(f"batch_size   : {batch_size}")

# COMMAND ----------

import time
from typing import Any

import pymssql


def _resolve_credentials() -> tuple[str, str]:
    if auth_mode == "secrets":
        if not secret_scope:
            raise ValueError("secret_scope is required when auth_mode=secrets")
        user = dbutils.secrets.get(scope=secret_scope, key=secret_user_key)
        password = dbutils.secrets.get(scope=secret_scope, key=secret_pass_key)
        return user, password
    if not sql_username:
        raise ValueError("sql_username is required when auth_mode=widgets")
    return sql_username, sql_password


def open_sql_connection() -> pymssql.Connection:
    if not sql_host:
        raise ValueError("sql_host is required")
    if not sql_database:
        raise ValueError("sql_database is required")

    username, password = _resolve_credentials()
    return pymssql.connect(
        server=sql_host,
        port=sql_port,
        user=username,
        password=password,
        database=sql_database,
        login_timeout=30,
        timeout=0,
        tds_version="7.4",
        as_dict=False,
    )


def run_query(sql: str, fetch: bool = True) -> list[tuple[Any, ...]] | None:
    sql = sql.strip()
    if not sql:
        raise ValueError("SQL is empty")
    with open_sql_connection() as conn:
        conn.autocommit(True)
        with conn.cursor() as cur:
            cur.execute(sql)
            if fetch and cur.description is not None:
                return cur.fetchall()
    return None


def run_many_statements(sql: str) -> None:
    """Run semicolon-separated batches (simple split — avoid semicolons in strings)."""
    for batch in [part.strip() for part in sql.split(";") if part.strip()]:
        print(f"--- executing batch ({len(batch)} chars) ---")
        rows = run_query(batch, fetch=True)
        if rows is not None:
            for row in rows[:50]:
                print(row)
            if len(rows) > 50:
                print(f"... ({len(rows) - 50} more rows)")


def load_k1input_snapshot(total_rows: int, batch: int, cid: int, tpid: int) -> dict[str, Any]:
    if total_rows <= 0:
        raise ValueError("target_rows must be > 0")
    if batch <= 0:
        raise ValueError("batch_size must be > 0")

    inserted = 0
    started = time.time()

    insert_template = """
INSERT INTO dbo.K1Input_Snapshot (
    WorkflowID,
    K1PackageID,
    LineID,
    Amount,
    Adjustment,
    TextValue,
    TotalAmount,
    ClientID,
    TaxPeriodID
)
SELECT
    (({batch_start} + rn) % 1000) + 1,
    (({batch_start} + rn) % 500) + 1,
    (({batch_start} + rn) % 10000) + 1,
    CAST(({batch_start} + rn) % 100000 AS float) / 100.0,
    CAST(({batch_start} + rn) % 1000 AS float) / 10.0,
    LEFT(N'load_' + CAST({batch_start} + rn AS varchar(20)), 60),
    CAST(({batch_start} + rn) % 200000 AS float) / 100.0,
    {client_id},
    {tax_period_id}
FROM (
    SELECT TOP ({this_batch})
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM sys.all_objects AS a
    CROSS JOIN sys.all_objects AS b
) AS tally;
"""

    with open_sql_connection() as conn:
        conn.autocommit(True)
        with conn.cursor() as cur:
            while inserted < total_rows:
                this_batch = min(batch, total_rows - inserted)
                batch_start = inserted
                sql = insert_template.format(
                    batch_start=batch_start,
                    this_batch=this_batch,
                    client_id=cid,
                    tax_period_id=tpid,
                )
                cur.execute(sql)
                inserted += this_batch
                if inserted % (batch * 2) == 0 or inserted == total_rows:
                    elapsed = int(time.time() - started)
                    print(f"Inserted {inserted:,} / {total_rows:,} ({elapsed}s elapsed)")

            cur.execute("SELECT COUNT_BIG(*) FROM dbo.K1Input_Snapshot;")
            row_count = cur.fetchone()[0]

    elapsed_sec = int(time.time() - started)
    rate = round(inserted / elapsed_sec, 1) if elapsed_sec else inserted
    summary = {
        "table": "dbo.K1Input_Snapshot",
        "database": sql_database,
        "inserted": inserted,
        "table_row_count": int(row_count),
        "elapsed_sec": elapsed_sec,
        "rows_per_sec": rate,
    }
    print("Load complete:", summary)
    return summary

# COMMAND ----------

if action == "test_connection":
    rows = run_query(
        "SELECT DB_NAME() AS db_name, SUSER_SNAME() AS login_name, @@VERSION AS version;"
    )
    for row in rows or []:
        print(row)

elif action == "run_custom_sql":
    run_many_statements(custom_sql)

elif action == "count_k1input_snapshot":
    rows = run_query("SELECT COUNT_BIG(*) AS row_count FROM dbo.K1Input_Snapshot;")
    print(rows)

elif action == "truncate_k1input_snapshot":
    run_query("TRUNCATE TABLE dbo.K1Input_Snapshot;", fetch=False)
    print("Truncated dbo.K1Input_Snapshot")

elif action == "load_k1input_snapshot":
    result = load_k1input_snapshot(target_rows, batch_size, client_id, tax_period_id)
    display(spark.createDataFrame([result]))  # noqa: F821 — Databricks display

else:
    raise ValueError(f"Unknown action: {action}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional — enable Change Tracking (for Lakeflow CDC ingest test)
# MAGIC
# MAGIC Run with **action = run_custom_sql** and paste (adjust if already enabled):

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Not UC SQL — use widget custom_sql in previous cell, example:
# MAGIC -- ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 7, AUTO_CLEANUP = ON);
# MAGIC -- ALTER TABLE dbo.K1Input_Snapshot ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
