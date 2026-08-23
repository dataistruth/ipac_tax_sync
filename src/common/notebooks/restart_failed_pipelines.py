# Databricks notebook source
# MAGIC %md
# MAGIC # Restart failed continuous Lakeflow pipelines
# MAGIC
# MAGIC Continuous job: reads latest `process_log` ingest rows per **individual** `p_*` pipeline.
# MAGIC When status is FAILED and no active pipeline update is running, sends an alert email
# MAGIC to `heartbeat_job_alert_mail` and requests a pipeline restart.

# COMMAND ----------

dbutils.widgets.text("name_prefix", "p_", "Pipeline name prefix")
dbutils.widgets.text("restart_limit", "25", "Max restart requests per poll")
dbutils.widgets.text("poll_interval_sec", "900", "Seconds between restart polls")
dbutils.widgets.text("pipeline_names_file", "", "Generated pipeline_names.json path")
dbutils.widgets.text("uc_catalog", "ipac_tax_synch", "UC catalog for process_log")
dbutils.widgets.text("ipac_metadata_schema", "ipac_metadata", "Metadata schema for process_log")
dbutils.widgets.text("heartbeat_job_alert_mail", "", "Alert email for failed pipeline restarts")

name_prefix = dbutils.widgets.get("name_prefix").strip() or "p_"
restart_limit = int(dbutils.widgets.get("restart_limit").strip() or "25")
poll_interval_sec = int(dbutils.widgets.get("poll_interval_sec").strip() or "900")
pipeline_names_file = dbutils.widgets.get("pipeline_names_file").strip()
uc_catalog = dbutils.widgets.get("uc_catalog").strip() or "ipac_tax_synch"
metadata_schema = dbutils.widgets.get("ipac_metadata_schema").strip() or "ipac_metadata"
alert_email = dbutils.widgets.get("heartbeat_job_alert_mail").strip()

print(f"name_prefix            : {name_prefix}")
print(f"restart_limit          : {restart_limit}")
print(f"poll_interval_sec      : {poll_interval_sec}")
print(f"pipeline_names_file    : {pipeline_names_file or '(prefix scan only)'}")
print(f"process_log table      : {uc_catalog}.{metadata_schema}.process_log")
print(f"heartbeat_job_alert_mail: {alert_email or '(not set — logs only)'}")

# COMMAND ----------

import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.ops.alert_ops import configure_smtp_from_dbutils
from common.ops.pipeline_job_ops import configure_dbutils, run_restart_loop
from common.ops.pipeline_names import load_pipeline_names

configure_dbutils(dbutils)
configure_smtp_from_dbutils(dbutils)

if pipeline_names_file:
    monitored = load_pipeline_names(pipeline_names_file)
    print(f"monitored pipeline keys ({len(monitored)}): {monitored}")
else:
    print("monitored pipeline keys: (prefix scan only, no pipeline_names_file)")

run_restart_loop(
    name_prefix,
    restart_limit,
    pipeline_names_file,
    poll_interval_sec,
    spark=spark,
    uc_catalog=uc_catalog,
    metadata_schema=metadata_schema,
    alert_email=alert_email,
)
