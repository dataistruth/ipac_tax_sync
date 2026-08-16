# Databricks notebook source
# MAGIC %md
# MAGIC # Ensure Unity Catalog schemas exist
# MAGIC
# MAGIC Creates metadata + per-client raw schemas with `CREATE SCHEMA IF NOT EXISTS`
# MAGIC before pipeline deploy when bundle validation reports missing `ipac_metadata`.
# MAGIC
# MAGIC Run once per workspace/catalog (or after adding clients):
# MAGIC `databricks bundle run ensure_uc_schemas` after deploying this job.

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "ipac_tax_synch", "UC catalog")
dbutils.widgets.text("ipac_metadata_schema", "ipac_metadata", "Metadata schema")
dbutils.widgets.text("dest_schema_suffix", "poc_1", "Client raw schema suffix")

uc_catalog = dbutils.widgets.get("uc_catalog").strip()
metadata_schema = dbutils.widgets.get("ipac_metadata_schema").strip() or "ipac_metadata"
dest_schema_suffix = dbutils.widgets.get("dest_schema_suffix").strip() or "poc_1"

print(f"uc_catalog           : {uc_catalog}")
print(f"ipac_metadata_schema : {metadata_schema}")
print(f"dest_schema_suffix   : {dest_schema_suffix}")

# COMMAND ----------

import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from util.config_loader import list_active_clients
from common.ops.uc_schema_ops import ensure_uc_schema

if not uc_catalog:
    raise ValueError("uc_catalog widget is required")

created: list[str] = []

print(f"Ensuring metadata schema {uc_catalog}.{metadata_schema} ...")
ensure_uc_schema(spark, uc_catalog, metadata_schema)
created.append(f"{uc_catalog}.{metadata_schema}")

clients = list_active_clients()
print(f"Ensuring raw schemas for {len(clients)} active client(s)...")
for client in clients:
    raw_schema = client.raw_schema(dest_schema_suffix)
    qualified = f"{uc_catalog}.{raw_schema}"
    print(f"  {qualified}")
    ensure_uc_schema(spark, uc_catalog, raw_schema)
    created.append(qualified)

print(f"Done. Ensured {len(created)} schema(s):")
for name in created:
    print(f"  - {name}")
