# Databricks notebook source
# MAGIC %md
# MAGIC # Monitor continuous Lakeflow pipeline heartbeats
# MAGIC
# MAGIC Serverless notebook job: checks generated `p_*` pipelines for failed/stale heartbeats.
# MAGIC Fails the job (email on_failure) when any monitored continuous pipeline is unhealthy.
# MAGIC
# MAGIC **Parameters:** `name_prefix`, `heartbeat_interval_sec`, `pipeline_names_file`

# COMMAND ----------

dbutils.widgets.text("name_prefix", "p_", "Pipeline name prefix")
dbutils.widgets.text("heartbeat_interval_sec", "900", "Max heartbeat age (seconds)")
dbutils.widgets.text("pipeline_names_file", "", "Generated pipeline_names.json path")

name_prefix = dbutils.widgets.get("name_prefix").strip() or "p_"
heartbeat_interval_sec = int(dbutils.widgets.get("heartbeat_interval_sec").strip() or "900")
pipeline_names_file = dbutils.widgets.get("pipeline_names_file").strip()

print(f"name_prefix            : {name_prefix}")
print(f"heartbeat_interval_sec : {heartbeat_interval_sec}")
print(f"pipeline_names_file    : {pipeline_names_file or '(prefix scan only)'}")

# COMMAND ----------

import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.ops.pipeline_job_ops import configure_dbutils, run_monitor

configure_dbutils(dbutils)
run_monitor(name_prefix, heartbeat_interval_sec, pipeline_names_file)
