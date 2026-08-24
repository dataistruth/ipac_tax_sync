# Databricks notebook source
# MAGIC %md
# MAGIC # Load snapshot test data (SQL Server)
# MAGIC
# MAGIC Standalone notebook — no project `common.ops` imports. Installs **`mssql-python`** (same as recon).
# MAGIC
# MAGIC **Secrets** (scope `scope_ipacs_audit` by default):
# MAGIC - `SQL_SERVER_HOST`
# MAGIC - `SQL_SERVER_AUDIT_USERNAME`
# MAGIC - `SQL_SERVER_AUDIT_PASSWORD`
# MAGIC
# MAGIC **Widgets:** target database, row count per table, how many tables to load (1–9).
# MAGIC Inserts run in **one transaction**; the final cell commits all tables.

# COMMAND ----------

# MAGIC %pip install -q "mssql-python>=1.13.0"
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("sql_database", "", "Database name")
dbutils.widgets.text("record_count", "1000", "Rows per table")
dbutils.widgets.text("num_tables", "5", "Number of tables to insert (max 9)")
dbutils.widgets.text("secret_scope", "scope_ipacs_audit", "Secret scope")
dbutils.widgets.text("secret_host_key", "SQL_SERVER_HOST", "Secret key: server/host")
dbutils.widgets.text("secret_user_key", "SQL_SERVER_AUDIT_USERNAME", "Secret key: username")
dbutils.widgets.text("secret_pass_key", "SQL_SERVER_AUDIT_PASSWORD", "Secret key: password")
dbutils.widgets.text("sql_port", "1433", "SQL port")
dbutils.widgets.text("client_id", "15347", "ClientID / CLIENTID values")
dbutils.widgets.text("tax_period_id", "2025", "TaxPeriodID / TAXPERIODID values")

sql_database = dbutils.widgets.get("sql_database").strip()
record_count = int(dbutils.widgets.get("record_count").strip() or "1000")
num_tables = int(dbutils.widgets.get("num_tables").strip() or "5")
secret_scope = dbutils.widgets.get("secret_scope").strip()
secret_host_key = dbutils.widgets.get("secret_host_key").strip() or "SQL_SERVER_HOST"
secret_user_key = dbutils.widgets.get("secret_user_key").strip() or "SQL_SERVER_AUDIT_USERNAME"
secret_pass_key = dbutils.widgets.get("secret_pass_key").strip() or "SQL_SERVER_AUDIT_PASSWORD"
sql_port = dbutils.widgets.get("sql_port").strip() or "1433"
client_id = int(dbutils.widgets.get("client_id").strip() or "15347")
tax_period_id = int(dbutils.widgets.get("tax_period_id").strip() or "2025")

if not sql_database:
    raise ValueError("sql_database widget is required")
if not secret_scope:
    raise ValueError("secret_scope is required")
if record_count <= 0:
    raise ValueError("record_count must be > 0")
if num_tables <= 0 or num_tables > 9:
    raise ValueError("num_tables must be between 1 and 9")

sql_host = dbutils.secrets.get(scope=secret_scope, key=secret_host_key)
sql_username = dbutils.secrets.get(scope=secret_scope, key=secret_user_key)
sql_password = dbutils.secrets.get(scope=secret_scope, key=secret_pass_key)

print(f"server       : {sql_host}:{sql_port}")
print(f"database     : {sql_database}")
print(f"record_count : {record_count}")
print(f"num_tables   : {num_tables}")
print(f"client_id    : {client_id}")
print(f"tax_period_id: {tax_period_id}")

# COMMAND ----------

from mssql_python import connect

insert_plan: list[tuple[int, str, str]] = []


def tally_subquery(row_count: int) -> str:
    return f"""
FROM (
    SELECT TOP ({row_count})
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM sys.all_objects AS a
    CROSS JOIN sys.all_objects AS b
) AS tally"""


def add_insert(table_index: int, qualified_name: str, insert_sql: str) -> None:
    if table_index <= num_tables:
        insert_plan.append((table_index, qualified_name, insert_sql))
        print(f"Queued table {table_index}: {qualified_name} ({record_count} rows)")


def open_connection():
    conn_str = (
        f"Server={sql_host},{sql_port};"
        f"Database={sql_database};"
        f"UID={sql_username};"
        f"PWD={sql_password};"
        "Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )
    conn = connect(conn_str)
    conn.autocommit = False
    return conn

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — `dbo.K1Input_Snapshot`

# COMMAND ----------

add_insert(
    1,
    "dbo.K1Input_Snapshot",
    f"""
INSERT INTO dbo.K1Input_Snapshot (
    WorkflowID, K1PackageID, LineID, Amount, Adjustment,
    TextValue, TotalAmount, ClientID, TaxPeriodID
)
SELECT
    ((rn) % 1000) + 1,
    ((rn) % 500) + 1,
    ((rn) % 10000) + 1,
    CAST((rn) % 100000 AS float) / 100.0,
    CAST((rn) % 1000 AS float) / 10.0,
    LEFT(N'k1_' + CAST(rn AS varchar(20)), 60),
    CAST((rn) % 200000 AS float) / 100.0,
    {client_id},
    {tax_period_id}
{tally_subquery(record_count)};
""",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — `dbo.K1UBTI_SNAPSHOT`

# COMMAND ----------

add_insert(
    2,
    "dbo.K1UBTI_SNAPSHOT",
    f"""
INSERT INTO dbo.K1UBTI_SNAPSHOT (
    WORKFLOWID, K1PACKAGEID, LINEID, PERCENT, AMOUNT, TOTAL,
    CLIENTID, TAXPERIODID, UBTITYPE
)
SELECT
    ((rn) % 1000) + 1,
    ((rn) % 500) + 1,
    ((rn) % 10000) + 1,
    CAST((rn) % 100 AS float) / 100.0,
    CAST((rn) % 100000 AS float) / 100.0,
    CAST((rn) % 200000 AS float) / 100.0,
    {client_id},
    {tax_period_id},
    LEFT(N'UBTI_' + CAST(rn AS varchar(20)), 50)
{tally_subquery(record_count)};
""",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — `dbo.LookthroughAdjustments_Snapshot`

# COMMAND ----------

add_insert(
    3,
    "dbo.LookthroughAdjustments_Snapshot",
    f"""
INSERT INTO dbo.LookthroughAdjustments_Snapshot (
    WorkflowID, TransactionID, EntityID, ClientID, TaxPeriodID,
    UnderlyingEntityID, SourceID, FootNoteID, LineID,
    LookthroughAmount, AdjustmentDescription, AdjustmentAmount,
    TrackingKey, ParentEntityID, SuperParentEntityID, Tag,
    SourceEntityID, BoxJKLBox
)
SELECT
    ((rn) % 1000) + 1,
    rn,
    ((rn) % 500) + 1,
    {client_id},
    {tax_period_id},
    ((rn) % 300) + 1,
    ((rn) % 50) + 1,
    ((rn) % 20) + 1,
    ((rn) % 10000) + 1,
    CAST((rn) % 10000 AS varchar(200)),
    LEFT(N'adj_' + CAST(rn AS varchar(20)), 200),
    CAST((rn) % 5000 AS varchar(200)),
    LEFT(N'trk_' + CAST(rn AS varchar(20)), 4000),
    ((rn) % 400) + 1,
    ((rn) % 200) + 1,
    LEFT(N'tag_' + CAST(rn AS varchar(20)), 5000),
    ((rn) % 350) + 1,
    CHAR(65 + (rn % 3))
{tally_subquery(record_count)};
""",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — `dbo.LookthroughReclassFNTieringBlocker_Snapshot`

# COMMAND ----------

add_insert(
    4,
    "dbo.LookthroughReclassFNTieringBlocker_Snapshot",
    f"""
INSERT INTO dbo.LookthroughReclassFNTieringBlocker_Snapshot (
    WorkflowID, TransactionID, ClientID, TaxPeriodID, EntityID,
    UnderlyingEntityID, SourceID, FootNoteID, TrackingKey,
    ParentEntityID, SuperParentEntityID, SourceEntityID
)
SELECT
    ((rn) % 1000) + 1,
    rn,
    {client_id},
    {tax_period_id},
    ((rn) % 500) + 1,
    ((rn) % 300) + 1,
    ((rn) % 50) + 1,
    ((rn) % 20) + 1,
    LEFT(N'trk_' + CAST(rn AS varchar(20)), 4000),
    ((rn) % 400) + 1,
    ((rn) % 200) + 1,
    ((rn) % 350) + 1
{tally_subquery(record_count)};
""",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — `dbo.M1Adjustments_Snapshot`

# COMMAND ----------

add_insert(
    5,
    "dbo.M1Adjustments_Snapshot",
    f"""
INSERT INTO dbo.M1Adjustments_Snapshot (
    WorkflowID, TransactionID, K1PackageID, EntityID, ClientID,
    TaxPeriodID, K1LineID, WorkpaperCode, PeriodID, Amount
)
SELECT
    ((rn) % 1000) + 1,
    rn,
    ((rn) % 500) + 1,
    ((rn) % 400) + 1,
    {client_id},
    {tax_period_id},
    ((rn) % 10000) + 1,
    LEFT(N'WP' + CAST((rn % 100) AS varchar(10)), 10),
    ((rn) % 12) + 1,
    CAST((rn) % 100000 AS float) / 100.0
{tally_subquery(record_count)};
""",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — `dbo.PFICFootnoteInput_Snapshot`

# COMMAND ----------

add_insert(
    6,
    "dbo.PFICFootnoteInput_Snapshot",
    f"""
INSERT INTO dbo.PFICFootnoteInput_Snapshot (
    WorkflowID, PFICFootnoteID, LineID, Amount, TextValue,
    ClientID, TaxPeriodID
)
SELECT
    ((rn) % 1000) + 1,
    ((rn) % 500) + 1,
    ((rn) % 10000) + 1,
    CAST((rn) % 100000 AS float) / 100.0,
    LEFT(N'pfic_' + CAST(rn AS varchar(20)), 100),
    {client_id},
    {tax_period_id}
{tally_subquery(record_count)};
""",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — `dbo.SM_PartnerWithholding_Snapshot`

# COMMAND ----------

add_insert(
    7,
    "dbo.SM_PartnerWithholding_Snapshot",
    f"""
INSERT INTO dbo.SM_PartnerWithholding_Snapshot (
    WorkflowID, EntityID, PartnerNumber, StateID,
    ClientID, TaxPeriodID, TransactionID
)
SELECT
    ((rn) % 1000) + 1,
    ((rn) % 500) + 1,
    LEFT(N'P' + CAST(rn AS varchar(20)), 50),
    ((rn) % 60) + 1,
    {client_id},
    {tax_period_id},
    CAST(rn AS bigint)
{tally_subquery(record_count)};
""",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 — `dbo.UBTIDFPercent_Snapshot`

# COMMAND ----------

add_insert(
    8,
    "dbo.UBTIDFPercent_Snapshot",
    f"""
INSERT INTO dbo.UBTIDFPercent_Snapshot (
    WorkFlowID, TransactionID, ClientID, TaxPeriodID,
    UBTIDFPercentID, EntityId, DealID, UBTIDFPercent
)
SELECT
    ((rn) % 1000) + 1,
    rn,
    {client_id},
    {tax_period_id},
    rn + 1000000,
    ((rn) % 400) + 1,
    ((rn) % 200) + 1,
    CAST((rn) % 100 AS float) / 100.0
{tally_subquery(record_count)};
""",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 — `dbo.TransfersInput_Snapshot`

# COMMAND ----------

add_insert(
    9,
    "dbo.TransfersInput_Snapshot",
    f"""
INSERT INTO dbo.TransfersInput_Snapshot (
    TransferID, WorkflowID, TransactionID, ClientID, TaxPeriodID,
    PartnerTo, PartnerToShareClass, PercentValue, TaxAmount,
    [704bAmount], GAAPAmount
)
SELECT
    rn + 2000000,
    ((rn) % 1000) + 1,
    rn,
    {client_id},
    {tax_period_id},
    LEFT(N'partner_' + CAST(rn AS varchar(20)), 50),
    LEFT(N'share_' + CAST(rn AS varchar(20)), 200),
    CAST((rn) % 100 AS float) / 100.0,
    CAST((rn) % 100000 AS float) / 100.0,
    CAST((rn) % 80000 AS float) / 100.0,
    CAST((rn) % 90000 AS float) / 100.0
{tally_subquery(record_count)};
""",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Commit batch — database `sql_database`
# MAGIC
# MAGIC Runs all queued inserts in **one transaction** and commits.

# COMMAND ----------

import time

if not insert_plan:
    raise ValueError("No tables queued — increase num_tables or check prior cells")

print(f"Committing {len(insert_plan)} table(s) on database [{sql_database}] ...")
started = time.time()

results: list[dict] = []

try:
    with open_connection() as conn:
        with conn.cursor() as cur:
            for table_index, qualified_name, insert_sql in insert_plan:
                step_start = time.time()
                cur.execute(insert_sql)
                rowcount = cur.rowcount
                elapsed = round(time.time() - step_start, 2)
                results.append(
                    {
                        "table_index": table_index,
                        "table": qualified_name,
                        "rows_inserted": rowcount,
                        "elapsed_sec": elapsed,
                    }
                )
                print(f"  [{table_index}] {qualified_name}: inserted {rowcount} rows ({elapsed}s)")
        conn.commit()
        print(f"COMMIT OK — database={sql_database}")
except Exception as exc:
    print(f"ROLLBACK — {exc}")
    raise

total_elapsed = round(time.time() - started, 2)
print(f"Done in {total_elapsed}s")

# COMMAND ----------

# Verify row counts for loaded tables
with open_connection() as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
        for table_index, qualified_name, _ in insert_plan:
            cur.execute(f"SELECT COUNT_BIG(*) FROM {qualified_name}")
            count = cur.fetchone()[0]
            print(f"{qualified_name}: {count:,} rows")

display(spark.createDataFrame(results))  # noqa: F821 — Databricks display
