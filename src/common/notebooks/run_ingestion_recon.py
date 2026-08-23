# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion reconciliation (CT + recon_type routing)
# MAGIC
# MAGIC Polls SQL Server Change Tracking for **CT-changed tables only**:
# MAGIC - **recon_type 2** (snapshot tables) → SQL `COUNT_BIG` vs Delta row count
# MAGIC - **recon_type 1** (and other) → Delta `last_write` after `sql_ct_reference_at + quiesce_sec`
# MAGIC
# MAGIC Writes **`recon_ready`** when all changed tables pass. No pipeline API / event_log.

# COMMAND ----------

# MAGIC %pip install -q "mssql-python>=1.13.0" "pydantic>=2.0.0"
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "dev7", "UC catalog")
dbutils.widgets.text("ipac_metadata_schema", "ipac_metadata", "Metadata schema")
dbutils.widgets.text("pipeline_names_file", "", "pipeline_names.json path")
dbutils.widgets.text("dest_schema_suffix", "_poc1", "Destination schema suffix")
dbutils.widgets.text("poll_interval_sec", "30", "Poll interval (seconds)")
dbutils.widgets.text("table_quiesce_sec", "10", "Seconds after sql_ct_reference before delta last_write gate")
dbutils.widgets.text("row_count_sample_size", "5", "Max recon_type 2 tables for COUNT per poll (0=all)")
dbutils.widgets.text("uc_parallel_workers", "1", "UC row_count threads (1=sequential; Spark not thread-safe)")

uc_catalog = dbutils.widgets.get("uc_catalog").strip() or "dev7"
metadata_schema = dbutils.widgets.get("ipac_metadata_schema").strip() or "ipac_metadata"
pipeline_names_file = dbutils.widgets.get("pipeline_names_file").strip()
dest_schema_suffix = dbutils.widgets.get("dest_schema_suffix").strip() or "_poc1"
poll_interval_sec = int(dbutils.widgets.get("poll_interval_sec").strip() or "30")
table_quiesce_sec = int(dbutils.widgets.get("table_quiesce_sec").strip() or "10")
row_count_sample_size = int(dbutils.widgets.get("row_count_sample_size").strip() or "5")
uc_parallel_workers = int(
    dbutils.widgets.get("uc_parallel_workers").strip() or "10"
)

LOOKBACK_HOURS = 24
USE_SQL_SERVER_AUDIT = True
SIMPLIFIED_RECON = True
SIMPLE_PASS_RULE = "ct_row_count"

print(f"uc_catalog                   : {uc_catalog}")
print(f"metadata_schema              : {metadata_schema}")
print(f"pipeline_names_file          : {pipeline_names_file or '(required)'}")
print(f"dest_schema_suffix           : {dest_schema_suffix}")
print(f"poll_interval_sec            : {poll_interval_sec}")
print(f"table_quiesce_sec            : {table_quiesce_sec}")
print(f"row_count_sample_size        : {row_count_sample_size}")
print(f"uc_parallel_workers         : {uc_parallel_workers}")
print(f"pass_rule (fixed)            : {SIMPLE_PASS_RULE}")

# COMMAND ----------

import sys
import time
from datetime import datetime

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from util.config_loader import get_client, load_client_overrides, load_common_tables
from util.resolver import resolve_effective_tables
from common.ops.ingestion_recon_ops import (
    build_contexts_for_client,
    load_pipeline_names,
    run_all_pipeline_recon,
    RowCountVerified,
    DeltaHistoryVerified,
)
from common.ops.process_log_store import client_nm_from_ingest_pipeline
from common.ops.recon_store import ensure_recon_ready_table

ensure_recon_ready_table(spark, uc_catalog, metadata_schema)

pipeline_keys = load_pipeline_names(pipeline_names_file) if pipeline_names_file else []
if not pipeline_keys:
    raise ValueError("pipeline_names_file is required for ingestion recon")

print(f"pipeline_keys ({len(pipeline_keys)}): {pipeline_keys}")

client_names = sorted(
    {client_nm_from_ingest_pipeline(k) for k in pipeline_keys if client_nm_from_ingest_pipeline(k)}
)
if not client_names:
    raise ValueError(f"No clients resolved from pipeline keys: {pipeline_keys}")

catalog = load_common_tables()
contexts: list = []
for client_nm in client_names:
    client = get_client(client_nm)
    overrides = load_client_overrides(client_nm)
    tables = resolve_effective_tables(client, catalog, overrides)
    keys_for_client = [k for k in pipeline_keys if client_nm in k]
    contexts.extend(build_contexts_for_client(client, tables, dest_schema_suffix, keys_for_client))

print(f"Monitoring {len(contexts)} pipeline(s) for {len(client_names)} client(s)")

# COMMAND ----------

iteration = 0
ct_batch_detected_at: dict[str, datetime] = {}
row_count_verified_cache: dict[str, RowCountVerified] = {}
delta_history_verified_cache: dict[str, DeltaHistoryVerified] = {}

while True:
    iteration += 1
    print(f"--- recon poll {iteration} ---")
    poll_start = time.perf_counter()
    totals = run_all_pipeline_recon(
        spark,
        uc_catalog,
        metadata_schema,
        contexts,
        lookback_hours=LOOKBACK_HOURS,
        dbutils=dbutils,
        use_sql_server_audit=USE_SQL_SERVER_AUDIT,
        simplified_recon=SIMPLIFIED_RECON,
        simple_pass_rule=SIMPLE_PASS_RULE,
        table_quiesce_sec=table_quiesce_sec,
        row_count_sample_size=row_count_sample_size,
        row_count_parallel_workers=uc_parallel_workers,
        ct_batch_detected_at=ct_batch_detected_at,
        row_count_verified_cache=row_count_verified_cache,
        delta_history_verified_cache=delta_history_verified_cache,
    )
    poll_elapsed_sec = time.perf_counter() - poll_start
    print(
        f"poll {iteration} complete in {poll_elapsed_sec:.1f}s: "
        f"pipelines={totals['pipelines']} "
        f"polled={totals['polled']} skipped={totals['skipped']} "
        f"ct_pending_tables={totals['ct_pending_tables']} "
        f"waiting_tables={totals['waiting_tables']} "
        f"recon_ready={totals['recon_ready']}"
    )
    print(f"Sleeping {poll_interval_sec}s...")
    time.sleep(poll_interval_sec)
