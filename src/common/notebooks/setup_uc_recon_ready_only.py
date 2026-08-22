# Databricks notebook source
# MAGIC %md
# MAGIC # UC `ipac_metadata` — `recon_ready` + `process_log`
# MAGIC
# MAGIC Drops the UC metadata schema (optional) and creates:
# MAGIC - `recon_ready` — calc gate (simplified recon)
# MAGIC - `process_log` — heartbeat monitor + restart job (unchanged)
# MAGIC
# MAGIC SQL Server `ipac_metadata.dbo` holds CT watermarks and recon audit.

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "dev7", "UC catalog")
dbutils.widgets.text("ipac_metadata_schema", "ipac_metadata", "Metadata schema")
dbutils.widgets.dropdown("drop_schema_first", "false", ["true", "false"], "DROP SCHEMA CASCADE before create")

uc_catalog = dbutils.widgets.get("uc_catalog").strip()
metadata_schema = dbutils.widgets.get("ipac_metadata_schema").strip() or "ipac_metadata"
drop_schema_first = dbutils.widgets.get("drop_schema_first").strip().lower() == "true"

print(f"catalog : {uc_catalog}")
print(f"schema  : {metadata_schema}")
print(f"drop    : {drop_schema_first}")

# COMMAND ----------

from common.ops.process_log_store import ensure_process_log_table
from common.ops.recon_store import ensure_recon_ready_table, recon_ready_create_sql

if drop_schema_first:
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{metadata_schema}` CASCADE")
    print(f"Dropped {uc_catalog}.{metadata_schema}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{metadata_schema}`")
spark.sql(recon_ready_create_sql(uc_catalog, metadata_schema))
ensure_recon_ready_table(spark, uc_catalog, metadata_schema)
ensure_process_log_table(spark, uc_catalog, metadata_schema)

print(f"Ready: {uc_catalog}.{metadata_schema}.recon_ready")
print(f"Ready: {uc_catalog}.{metadata_schema}.process_log (heartbeat/restart)")

# COMMAND ----------

spark.sql(f"DESCRIBE TABLE EXTENDED `{uc_catalog}`.`{metadata_schema}`.`recon_ready`").show(truncate=False)
