# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion flow metrics reconciliation
# MAGIC
# MAGIC Polls shared ingest event log (`ingest_events`),
# MAGIC aggregates per-table `flow_progress` metrics when status = COMPLETED,
# MAGIC optionally compares SQL Server Change Tracking for `recon_type` 2/3,
# MAGIC writes `recon_ready` + `process_log` on PASS.

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "ipac_tax_synch", "UC catalog")
dbutils.widgets.text("ipac_metadata_schema", "ipac_metadata", "Metadata schema")
dbutils.widgets.text("pipeline_names_file", "", "pipeline_names.json path")
dbutils.widgets.text("dest_schema_suffix", "_poc1", "Destination schema suffix")
dbutils.widgets.text("poll_interval_sec", "300", "Poll interval seconds")
dbutils.widgets.text("lookback_hours", "24", "Event log lookback hours")
dbutils.widgets.text("ingest_event_log_name", "ingest_events", "Shared ingest event log table name")
dbutils.widgets.dropdown("run_ct_probe", "true", ["true", "false"], "Run SQL Server CT connection probe at startup")
dbutils.widgets.text("ct_probe_table_nm", "", "Table to probe (blank = first active common table)")

uc_catalog = dbutils.widgets.get("uc_catalog").strip() or "ipac_tax_synch"
metadata_schema = dbutils.widgets.get("ipac_metadata_schema").strip() or "ipac_metadata"
pipeline_names_file = dbutils.widgets.get("pipeline_names_file").strip()
dest_schema_suffix = dbutils.widgets.get("dest_schema_suffix").strip() or "_poc1"
poll_interval_sec = int(dbutils.widgets.get("poll_interval_sec").strip() or "300")
lookback_hours = int(dbutils.widgets.get("lookback_hours").strip() or "24")
ingest_event_log_name = dbutils.widgets.get("ingest_event_log_name").strip() or "ingest_events"
run_ct_probe = dbutils.widgets.get("run_ct_probe").strip().lower() == "true"
ct_probe_table_nm = dbutils.widgets.get("ct_probe_table_nm").strip()

print(f"uc_catalog           : {uc_catalog}")
print(f"metadata_schema      : {metadata_schema}")
print(f"pipeline_names_file  : {pipeline_names_file or '(none)'}")
print(f"dest_schema_suffix   : {dest_schema_suffix}")
print(f"poll_interval_sec    : {poll_interval_sec}")
print(f"lookback_hours       : {lookback_hours}")
print(f"ingest_event_log_name: {ingest_event_log_name}")
print(f"run_ct_probe          : {run_ct_probe}")
print(f"ct_probe_table_nm     : {ct_probe_table_nm or '(first active table)'}")

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
from common.ops.source_ct_ops import probe_source_ct_connection

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
    overrides = load_client_overrides(client_nm)
    tables = resolve_effective_tables(client, catalog, overrides)
    keys_for_client = [k for k in pipeline_keys if client_nm in k]
    contexts.extend(build_contexts_for_client(client, tables, dest_schema_suffix, keys_for_client))

print(f"Monitoring {len(contexts)} pipeline(s) for {len(client_names)} client(s)")

# COMMAND ----------

if run_ct_probe:
    from datetime import datetime, timedelta, timezone

    probe_end = datetime.now(timezone.utc)
    probe_start = probe_end - timedelta(hours=lookback_hours)
    common_catalog = load_common_tables()
    for client_nm in client_names:
        client = get_client(client_nm)
        overrides = load_client_overrides(client_nm)
        effective = resolve_effective_tables(client, common_catalog, overrides)
        probe_table = ct_probe_table_nm
        if not probe_table and effective:
            probe_table = effective[0].table_nm
        if not probe_table:
            print(f"[CT probe] SKIP {client_nm}: no table available for probe")
            continue
        print(f"========== CT probe: client={client_nm} table={probe_table} ==========")
        print(f"[CT probe] src_db_nm (federated catalog): {client.src_db_nm}")
        print(f"[CT probe] src_db_schema: {client.src_db_schema or 'dbo'}")
        print(f"[CT probe] uc_conn_nm (Lakeflow connection): {client.uc_conn_nm}")
        probe_source_ct_connection(
            spark,
            client.src_db_nm,
            client.src_db_schema or "dbo",
            probe_table,
            recon_type=2,
            start_time=probe_start,
            end_time=probe_end,
            print_results=True,
        )

# COMMAND ----------

iteration = 0
while True:
    iteration += 1
    print(f"--- recon poll {iteration} ---")
    totals = run_all_pipeline_recon(
        spark,
        uc_catalog,
        metadata_schema,
        contexts,
        lookback_hours=lookback_hours,
        event_log_table=ingest_event_log_name,
    )
    print(
        f"poll {iteration} complete: pipelines={totals['pipelines']} "
        f"metrics={totals['metrics']} summaries={totals['summaries']} "
        f"recon_ready={totals['recon_ready']}"
    )
    print(f"Sleeping {poll_interval_sec}s...")
    time.sleep(poll_interval_sec)
