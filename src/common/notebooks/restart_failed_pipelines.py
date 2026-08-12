# Databricks notebook source
# MAGIC %md
# MAGIC # Restart failed continuous Lakeflow pipelines
# MAGIC
# MAGIC Serverless notebook job: restarts generated `p_*` continuous pipelines whose latest
# MAGIC update is in a failed state.
# MAGIC
# MAGIC **Parameters:** `name_prefix`, `restart_limit`, `pipeline_names_file`

# COMMAND ----------

dbutils.widgets.text("name_prefix", "p_", "Pipeline name prefix")
dbutils.widgets.text("restart_limit", "25", "Max restart requests per run")
dbutils.widgets.text("pipeline_names_file", "", "Generated pipeline_names.json path")

name_prefix = dbutils.widgets.get("name_prefix").strip() or "p_"
restart_limit = int(dbutils.widgets.get("restart_limit").strip() or "25")
pipeline_names_file = dbutils.widgets.get("pipeline_names_file").strip()

print(f"name_prefix         : {name_prefix}")
print(f"restart_limit       : {restart_limit}")
print(f"pipeline_names_file : {pipeline_names_file or '(prefix scan only)'}")

# COMMAND ----------

import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.ops.pipeline_job_ops import configure_dbutils, run_restart

configure_dbutils(dbutils)
run_restart(name_prefix, restart_limit, pipeline_names_file)
