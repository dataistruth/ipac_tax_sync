# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion flow metrics reconciliation
# MAGIC
# MAGIC Polls hidden `event_log(pipeline_id)` for pipelines with activity,
# MAGIC aggregates per-table `flow_progress` metrics when status = COMPLETED,
# MAGIC compares SQL Server Change Tracking (ipac_metadata.dbo watermarks) for `recon_type` 2/3,
# MAGIC writes `recon_ready` + `process_log` on PASS.
# MAGIC
# MAGIC Continuous loop: poll ingestion event log → compare CT watermarks / counts (recon_type 2/3) → write `recon_ready`.
# MAGIC
# MAGIC **Dependencies:** `mssql-python` and `pydantic>=2` via `%pip` in cell 1 (runtime ships Pydantic v1).

# COMMAND ----------

# MAGIC %pip install -q "mssql-python>=1.13.0" "pydantic>=2.0.0"
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "ipac_tax_synch", "UC catalog")
dbutils.widgets.text("ipac_metadata_schema", "ipac_metadata", "Metadata schema")
dbutils.widgets.text("pipeline_names_file", "", "pipeline_names.json path")
dbutils.widgets.text("dest_schema_suffix", "_poc1", "Destination schema suffix")
dbutils.widgets.text("poll_interval_sec", "300", "Poll interval seconds")
dbutils.widgets.text("lookback_hours", "24", "Event log lookback hours")
dbutils.widgets.dropdown("run_ct_probe", "true", ["true", "false"], "Run SQL Server CT probe at startup")
dbutils.widgets.text("ct_probe_table_nm", "", "Table to probe (blank = first active common table)")
dbutils.widgets.text("sql_host", "", "SQL host override (blank = client.json sql_host)")
dbutils.widgets.text("sql_audit_secret_scope", "scope_ipacs_audit", "Databricks secret scope for audit SQL login")
dbutils.widgets.dropdown("use_sql_server_audit", "true", ["true", "false"], "Use ipac_metadata.dbo CT watermarks")

uc_catalog = dbutils.widgets.get("uc_catalog").strip() or "ipac_tax_synch"
metadata_schema = dbutils.widgets.get("ipac_metadata_schema").strip() or "ipac_metadata"
pipeline_names_file = dbutils.widgets.get("pipeline_names_file").strip()
dest_schema_suffix = dbutils.widgets.get("dest_schema_suffix").strip() or "_poc1"
poll_interval_sec = int(dbutils.widgets.get("poll_interval_sec").strip() or "300")
lookback_hours = int(dbutils.widgets.get("lookback_hours").strip() or "24")
run_ct_probe = dbutils.widgets.get("run_ct_probe").strip().lower() == "true"
ct_probe_table_nm = dbutils.widgets.get("ct_probe_table_nm").strip()
sql_host_override = dbutils.widgets.get("sql_host").strip()
sql_audit_secret_scope = dbutils.widgets.get("sql_audit_secret_scope").strip() or "scope_ipacs_audit"
use_sql_server_audit = dbutils.widgets.get("use_sql_server_audit").strip().lower() == "true"

print(f"uc_catalog              : {uc_catalog}")
print(f"metadata_schema         : {metadata_schema}")
print(f"pipeline_names_file     : {pipeline_names_file or '(none)'}")
print(f"dest_schema_suffix      : {dest_schema_suffix}")
print(f"poll_interval_sec       : {poll_interval_sec}")
print(f"lookback_hours          : {lookback_hours}")
print(f"run_ct_probe            : {run_ct_probe}")
print(f"ct_probe_table_nm       : {ct_probe_table_nm or '(first active table)'}")
print(f"sql_host_override       : {sql_host_override or '(from client.json)'}")
print(f"sql_audit_secret_scope  : {sql_audit_secret_scope}")
print(f"use_sql_server_audit    : {use_sql_server_audit}")

# COMMAND ----------

import sys
import time

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from util.config_loader import get_client, list_active_clients, load_client_overrides, load_common_tables
from util.resolver import resolve_effective_tables
from common.ops.ingestion_recon_ops import (
    build_contexts_for_client,
    load_pipeline_names,
    run_all_pipeline_recon,
)
from common.ops.process_log_store import client_nm_from_ingest_pipeline
from common.ops.recon_store import ensure_recon_tables
from common.ops.source_ct_direct import probe_source_ct_connection_direct
from common.ops.sql_server_audit_store import open_audit_connection, resolve_source_ct_for_recon

ensure_recon_tables(spark, uc_catalog, metadata_schema)

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
    if sql_host_override:
        client = client.model_copy(update={"sql_host": sql_host_override})
    if sql_audit_secret_scope:
        client = client.model_copy(update={"sql_audit_secret_scope": sql_audit_secret_scope})
    overrides = load_client_overrides(client_nm)
    tables = resolve_effective_tables(client, catalog, overrides)
    keys_for_client = [k for k in pipeline_keys if client_nm in k]
    contexts.extend(build_contexts_for_client(client, tables, dest_schema_suffix, keys_for_client))

print(f"Monitoring {len(contexts)} pipeline(s) for {len(client_names)} client(s)")

# COMMAND ----------

if run_ct_probe:
    common_catalog = load_common_tables()
    for client_nm in client_names:
        client = get_client(client_nm)
        if sql_host_override:
            client = client.model_copy(update={"sql_host": sql_host_override})
        if sql_audit_secret_scope:
            client = client.model_copy(update={"sql_audit_secret_scope": sql_audit_secret_scope})
        overrides = load_client_overrides(client_nm)
        effective = resolve_effective_tables(client, common_catalog, overrides)
        probe_table = ct_probe_table_nm
        if not probe_table and effective:
            probe_table = effective[0].table_nm
        if not probe_table:
            print(f"[CT probe] SKIP {client_nm}: no table available for probe")
            continue
        print(f"========== CT probe: client={client_nm} table={probe_table} ==========")
        print(f"[CT probe] src_db_nm: {client.src_db_nm}")
        print(f"[CT probe] secret scope: {client.sql_audit_secret_scope}")
        try:
            conn, cfg = open_audit_connection(client, dbutils=dbutils, host_override=sql_host_override)
            probe_source_ct_connection_direct(
                conn,
                client.src_db_schema or "dbo",
                probe_table,
                print_results=True,
            )
            metric, pending, wm, head = resolve_source_ct_for_recon(
                conn,
                client,
                client.src_db_schema or "dbo",
                probe_table,
                recon_type=2,
                verbose=True,
            )
            print(f"[CT probe] watermark={wm} head={head} metric={metric} pending={pending}")
            conn.close()
        except Exception as exc:
            print(f"[CT probe] FAILED for {client_nm}: {exc}")

# COMMAND ----------

iteration = 0
while True:
    iteration += 1
    print(f"--- recon poll {iteration} ---")
    poll_start = time.perf_counter()
    totals = run_all_pipeline_recon(
        spark,
        uc_catalog,
        metadata_schema,
        contexts,
        lookback_hours=lookback_hours,
        dbutils=dbutils,
        use_sql_server_audit=use_sql_server_audit,
    )
    poll_elapsed_sec = time.perf_counter() - poll_start
    print(
        f"poll {iteration} complete in {poll_elapsed_sec:.1f}s: "
        f"pipelines={totals['pipelines']} "
        f"polled={totals['polled']} skipped={totals['skipped']} "
        f"new_events={totals['new_events']} metrics={totals['metrics']} "
        f"summaries={totals['summaries']} recon_ready={totals['recon_ready']}"
    )
    print(f"Sleeping {poll_interval_sec}s...")
    time.sleep(poll_interval_sec)
