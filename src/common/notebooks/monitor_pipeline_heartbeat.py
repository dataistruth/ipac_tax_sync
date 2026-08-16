# Databricks notebook source
# MAGIC %md
# MAGIC # Monitor continuous Lakeflow pipeline heartbeats
# MAGIC
# MAGIC Continuous job: polls `p_*` pipeline status every `heartbeat_interval_sec`,
# MAGIC appends ingest poll rows to `{uc_catalog}.{ipac_metadata_schema}.process_log`
# MAGIC (shared table for ingest, calc, transfer, and other process types).
# MAGIC Alerts on unhealthy pipelines but does not fail the job — restart job handles FAILED pipelines.

# COMMAND ----------

dbutils.widgets.text("name_prefix", "p_", "Pipeline name prefix")
dbutils.widgets.text("heartbeat_interval_sec", "900", "Poll interval + stale threshold (seconds)")
dbutils.widgets.text("pipeline_names_file", "", "Generated pipeline_names.json path")
dbutils.widgets.text("uc_catalog", "ipac_tax_synch", "UC catalog for process_log")
dbutils.widgets.text("ipac_metadata_schema", "ipac_metadata", "Metadata schema for process_log")
dbutils.widgets.text("heartbeat_job_alert_mail", "", "Alert email for unhealthy pipeline detection")

name_prefix = dbutils.widgets.get("name_prefix").strip() or "p_"
heartbeat_interval_sec = int(dbutils.widgets.get("heartbeat_interval_sec").strip() or "900")
pipeline_names_file = dbutils.widgets.get("pipeline_names_file").strip()
uc_catalog = dbutils.widgets.get("uc_catalog").strip() or "ipac_tax_synch"
metadata_schema = dbutils.widgets.get("ipac_metadata_schema").strip() or "ipac_metadata"
alert_email = dbutils.widgets.get("heartbeat_job_alert_mail").strip()

try:
    monitor_run_id = (
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("jobRunId")
        or ""
    )
except Exception:
    monitor_run_id = ""

print(f"name_prefix            : {name_prefix}")
print(f"heartbeat_interval_sec : {heartbeat_interval_sec}")
print(f"pipeline_names_file    : {pipeline_names_file or '(prefix scan only)'}")
print(f"process_log table      : {uc_catalog}.{metadata_schema}.process_log")
print(f"monitor_run_id         : {monitor_run_id}")
print(f"heartbeat_job_alert_mail: {alert_email or '(not set — logs only)'}")

# COMMAND ----------

import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.ops.alert_ops import configure_smtp_from_dbutils
from common.ops.pipeline_job_ops import configure_dbutils, run_monitor_loop
from common.ops.pipeline_names import load_pipeline_names
from common.ops.process_log_store import (
    ingest_log_rows_from_poll_snapshots,
    write_process_log_rows,
)

configure_dbutils(dbutils)
configure_smtp_from_dbutils(dbutils)

if pipeline_names_file:
    monitored = load_pipeline_names(pipeline_names_file)
    print(f"monitored pipeline keys ({len(monitored)}): {monitored}")
else:
    print("monitored pipeline keys: (prefix scan only, no pipeline_names_file)")


def _log_poll_to_delta(snapshots):
    rows = ingest_log_rows_from_poll_snapshots(snapshots)
    written = write_process_log_rows(spark, uc_catalog, metadata_schema, rows)
    print(f"process_log: appended {written} row(s)")


run_monitor_loop(
    name_prefix,
    heartbeat_interval_sec,
    pipeline_names_file,
    monitor_run_id=str(monitor_run_id),
    on_poll=_log_poll_to_delta,
    alert_email=alert_email,
)
