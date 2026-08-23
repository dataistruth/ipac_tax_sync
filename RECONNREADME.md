# Ingestion flow metrics reconciliation

Standalone guide for downstream Lakeflow **MANAGED_INGESTION** reconciliation in `ipac_delta_sync`. This covers per-table `flow_progress` metrics from published pipeline event logs, optional SQL Server **Change Tracking (CT)** compare, and calc gating via `recon_ready`.

Lakeflow pipelines use `connector_type: CDC` in YAML — that is the Lakeflow Connect connector name. **Source SQL Server tables use Change Tracking (CT)** on PK tables (`*_enable_ct.sql`), not CDC change tables.

For general repo setup, see [README.md](README.md).

## Scope

| In scope | Out of scope |
|----------|----------------|
| Downstream ingestion pipeline event logs | Gateway / `INGESTION_GATEWAY` event logs |
| Per-table flow when `flow_progress.status = COMPLETED` | Comparing flow deltas to `COUNT(*)` on destination |
| `recon_type` 1 = metrics-only; 2/3 = SQL **CT** compare | Calc job implementation (`ipac-sdt-calc`) |
| `recon_ready` + `process_log` as producers | Native table-update triggers |

## Architecture

```
MANAGED_INGESTION pipeline (continuous)
        │
        ▼
Published UC event log  ingest_events_p_<client>_<n>
        │
        ▼
j_ipac_delta_sync_ingestion_recon_monitor job (poll every recon_poll_interval_sec)
        │
        ├── MERGE → lakeflow_flow_metrics
        ├── APPEND → lakeflow_flow_summary
        ├── On PASS → recon_ready + process_log (ingest SUCCESS)
        └── On FAIL → process_log (ingest FAILED) + job alert
        │
        ▼
ipac-sdt-calc reads recon_ready / process_log
```

**Separate from heartbeat:** `j_ipac_delta_sync_pipeline_heartbeat_monitor` checks pipeline health (RUNNING, stale heartbeat). Recon checks **per-table flow completion and row metrics**.

## SQL Server `ipac_metadata` database (CT watermarks + audit)

Low-latency CT state lives in the dedicated **`ipac_metadata`** database on SQL Server (`dbo` schema), not in `master`. Databricks recon uses **`mssql-python`** (`%pip install` in notebook) + secret scope `scope_ipacs_audit`.

### Setup (SSMS + Databricks CLI)

1. Run SQL scripts in order from `src/common/sql_server/sql/`:
   - `001_create_database.sql`
   - `002_ct_recon_tables.sql`
   - `002_ct_recon_tables.sql`
   - `004_grants.sql` (edit `@AuditLogin`)
   - Skip `003_process_log_table.sql` (`process_log` is UC only)
   - `004_grants.sql` (replace `YOUR_ADMIN_SQL_LOGIN`)
   - Optional: `006_baseline_ct_watermarks.sql`

2. Create Databricks secrets:

```bash
databricks secrets create-scope scope_ipacs_audit
databricks secrets put-secret scope_ipacs_audit SQL_SERVER_AUDIT_USERNAME
databricks secrets put-secret scope_ipacs_audit SQL_SERVER_AUDIT_PASSWORD
```

Or: `./src/common/sql_server/setup_audit_secrets.sh --profile <profile>`

3. Set `sql_host` in `config/common/client.json` for each client.

### SQL Server tables

| Table | Purpose |
|-------|---------|
| `ipac_metadata.dbo.ct_db_watermark` | DB-level CT head snapshot |
| `ipac_metadata.dbo.ct_table_watermark` | Per-table last reconciled CT version |
| `ipac_metadata.dbo.recon_run` | One row per pipeline recon batch |
| `ipac_metadata.dbo.recon_table_result` | CT pending I/U/D vs ingestion metrics |
| `ipac_metadata.dbo.ingestion_audit_log` | General audit events |

On **PASS**, recon advances `ct_table_watermark` to `CHANGE_TRACKING_CURRENT_VERSION()`.

See [src/common/sql_server/README.md](src/common/sql_server/README.md).


| Value | Meaning | PASS condition |
|-------|---------|----------------|
| `1` | Metrics only | Flow reaches `COMPLETED`; no SQL compare |
| `2` | Full change rows | `total_upserted + total_deleted` = CT **I + U + D** since SQL watermark |
| `3` | Upserts only | `total_upserted` = CT **I + U** since SQL watermark |

Override per table in `config/common/client_overrides/<client_nm>.json` (`extra` / same fields as common tables).

**Do not** compare CT/ingest deltas to current table row count. Compare like-for-like change metrics only.

## Recon batch boundary

One recon unit = one **table flow** (`flow_name`) inside a pipeline `update_id` when:

- `event_type = flow_progress`
- `level = METRICS`
- `origin.pipeline_type = MANAGED_INGESTION`
- Final aggregated `flow_status = COMPLETED`

Continuous pipelines do **not** wait for the whole pipeline update to finish—only the per-table flow.

Metrics in the event log are **deltas** (reset each emission). The recon job **sums** upserted/deleted/output rows across all events for that `(update_id, flow_name)` window.

## Where data lives (UC vs SQL Server)

| Store | Location | Contents |
|-------|----------|----------|
| **Unity Catalog Delta** | `{uc_catalog}.{ipac_metadata_schema}` | **`recon_ready`** (calc gate), **`process_log`** (heartbeat + restart) |
| **Unity Catalog Delta** | same schema | **`ingest_events_p_*`** (published pipeline event logs) |
| **SQL Server** | `ipac_metadata.dbo` | CT watermarks, recon audit, event-log poll state |

SQL scripts: `src/common/sql_server/sql/` (`001` → `006`).

Simplified recon (`simplified_recon=true`) writes **only** `recon_ready` to UC; everything else goes to SQL Server.

### Reset UC metadata schema (recon_ready only)

**Option A — notebook** `src/common/notebooks/setup_uc_recon_ready_only.py`  
Set widgets `drop_schema_first=true`, run once.

**Option B — SQL + notebook**

```sql
-- Drops ALL tables in the UC metadata schema (including old lakeflow_flow_* if present)
DROP SCHEMA IF EXISTS dev7.ipac_metadata CASCADE;
CREATE SCHEMA dev7.ipac_metadata;
```

Then in a cluster notebook:

```python
from common.ops.recon_store import ensure_recon_ready_table
ensure_recon_ready_table(spark, "dev7", "ipac_metadata")
```

**Do not drop** SQL Server `ipac_metadata` when resetting UC — watermarks and audit history stay there.

## Unity Catalog — `recon_ready`

Only operational Delta tables in `{uc_catalog}.{ipac_metadata_schema}` for recon/ops:

| Table | Used by |
|-------|---------|
| `recon_ready` | Simplified recon → calc gate |
| `process_log` | Heartbeat monitor, restart-failed-pipelines (unchanged) |

Legacy tables (`lakeflow_flow_metrics`, `lakeflow_flow_summary`, UC `recon_event_log_watermark`) are **not used** by simplified recon. Keep **`process_log`** on UC for heartbeat/restart jobs.

### Published event logs (per pipeline)

Still in UC (separate from recon_ready):

```yaml
event_log:
  catalog: ${var.uc_catalog}
  schema: ${var.ipac_metadata_schema}
  name: ingest_events_p_<client_nm>_<serial>
```

Example: `dev7.ipac_metadata.ingest_events_p_iPC_2025_Dev7_15347_1`

### `recon_ready`

**PASS rows only** — calc gate. Simplified recon publishes **one row per database** when all pending tables PASS.

| Column | Description |
|--------|-------------|
| `recon_id` | UUID |
| `client_nm` | Client |
| `table_nm` | `__database__` for DB-level rows; actual UC table name for legacy per-table rows |
| `database_name` | SQL Server database reconciled |
| `tables_json` | JSON list of all tables in the batch (names, pending CT, counts, delta version) |
| `ct_watermark_before` | `ct_db_watermark.last_version` at recon start |
| `ct_head_version` | `CHANGE_TRACKING_CURRENT_VERSION()` at PASS |
| `total_ingestion_sec` | Seconds from first poll that detected this `ct_head` until `recon_ready`; resets after PASS when a new `ct_head` is detected |
| `pipeline_id`, `update_id` | Pipeline update idempotency |
| `flow_name` | `__database_recon__` for DB-level rows |
| `ingest_change_rows`, `source_change_rows` | Summed across tables in batch |
| `completed_at` | Batch completion time |
| `artifact_run_id` | Pipeline `update_id` |
| `ready_for_calc` | `true` |

Duplicate `(pipeline_id, update_id, flow_name=__database_recon__)` are skipped.

Example query:

```sql
SELECT
  database_name,
  ct_watermark_before,
  ct_head_version,
  update_id,
  completed_at,
  tables_json
FROM dev7.ipac_metadata.recon_ready
WHERE client_nm = 'iPC_2025_DEV7_15447'
ORDER BY completed_at DESC;
```

Audit / watermarks remain in SQL Server (`recon_table_result`, `ct_table_watermark`, etc.).

## Bundle configuration (`databricks.yml`)

| Variable | Default | Purpose |
|----------|---------|---------|
| `uc_catalog` | `ipac_tax_synch` | Catalog for metadata + event logs |
| `ipac_metadata_schema` | `ipac_metadata` | Metadata schema |
| `dest_schema_suffix` | `poc_1` | Client raw schema suffix for table config resolution |
| `recon_poll_interval_sec` | `300` | Recon job sleep between polls |
| `recon_lookback_hours` | `24` | Event log scan window per poll |
| `heartbeat_job_alert_mail` | (email) | Recon job failure notifications |

DDL is generated on `generate` (UC recon_ready only):

```bash
./ipac-delta-sync generate
# → generated/schema/ipac_metadata_recon_ready.sql
```

SQL Server DDL: run scripts in `src/common/sql_server/sql/` on the instance.

## Recon job

**Job name:** `j_ipac_delta_sync_ingestion_recon_monitor`  
**Definition:** `resources/jobs/ingestion_recon_jobs.yml`  
**Notebook:** `src/common/notebooks/run_ingestion_recon.py`

Continuous job (`pause_status: UNPAUSED`). Notebook widgets (8):

| Widget | Default | Description |
|--------|---------|-------------|
| `uc_catalog` | `dev7` | UC catalog for `recon_ready` / `process_log` |
| `ipac_metadata_schema` | `ipac_metadata` | Metadata schema |
| `pipeline_names_file` | `generated/config/pipeline_names.json` | Monitored pipelines (required) |
| `dest_schema_suffix` | `_poc1` | Suffix for UC destination schema resolution |
| `poll_interval_sec` | `300` | Poll loop interval |
| `table_quiesce_sec` | `15` | Buffer after SQL CT change before Delta write must occur |
| `row_count_sample_size` | `5` | Up to N highest-pending tables get SQL vs UC row-count check per poll |
| `history_sample_size` | `5` | Up to N highest-pending tables get DESCRIBE HISTORY per poll |
| `uc_parallel_workers` | `10` | Parallel threads for history sample + Delta `COUNT(1)` sample; SQL stays one UNION |

**Fixed in notebook (not widgets):** `simple_pass_rule=ct_delta_history`, `simplified_recon=true`, `use_sql_server_audit=true`. SQL host and secret scope come from `client.json`. CT connectivity probe removed — use `test_mssql_python.py` if needed.

**Delta write timestamp:** Recon reads `DESCRIBE HISTORY` on the UC target and uses the latest **MERGE** timestamp (fallback: WRITE/UPDATE/DELETE). DLT SETUP rows supply `updateId` for logging only.

**`ct_delta_history`:** Does not use `flow_progress` or event_log. When watermarks lag CT head, every pending table must eventually show a Delta write after the SQL CT reference time (up to `history_sample_size` checked per poll; tables that already pass are skipped on later polls). Up to `row_count_sample_size` tables per poll (highest pending first) also require SQL vs Delta row-count match; matching counts are cached and skipped on later polls until batch PASS.

Deploy:

```bash
databricks bundle deploy
```

Unpause or confirm continuous trigger for `j_ipac_delta_sync_ingestion_recon_monitor`.

## Source code map

| Path | Role |
|------|------|
| `src/common/ops/recon_store.py` | DDL, dataclasses, Delta writes |
| `src/common/ops/lakeflow_event_ops.py` | Event extract SQL, aggregation, `evaluate_recon` |
| `src/common/ops/source_ct_ops.py` | SQL Server CT count SQL (`recon_type` 2/3) |
| `src/common/ops/ingestion_recon_ops.py` | Per-pipeline orchestration |
| `src/common/notebooks/run_ingestion_recon.py` | Continuous recon notebook |
| `src/util/pipeline_generator.py` | `event_log` block on generated pipelines |
| `src/util/metadata_table_generator.py` | `write_recon_tables_sql()` |
| `tests/test_lakeflow_recon.py` | Unit tests |

## SQL Server Change Tracking compare (`recon_type` 2/3)

Counts rows from `CHANGETABLE(CHANGES schema.table, 0)` joined to `sys.dm_tran_commit_time`, filtered to the flow `[first_event_time, last_event_time]`:

- **Type 2:** `SYS_CHANGE_OPERATION IN ('I', 'U', 'D')`
- **Type 3:** `SYS_CHANGE_OPERATION IN ('I', 'U')`

Queries run via Spark SQL against the UC federated SQL catalog (`client.src_db_nm`). Requires CT enabled (`*_enable_ct.sql`) and grants (`*_grant_ct_access.sql`).

If CT count cannot be read, recon **FAILs** for types 2/3.

## Example event log query (manual)

Against a published table (replace catalog/schema/name):

```sql
SELECT
    id AS event_id,
    origin.update_id,
    origin.flow_name,
    timestamp AS event_timestamp,
    details:flow_progress:status::STRING AS flow_status,
    TRY_CAST(details:flow_progress:metrics:num_upserted_rows AS BIGINT) AS rows_upserted,
    TRY_CAST(details:flow_progress:metrics:num_deleted_rows AS BIGINT) AS rows_deleted
FROM ipac_tax_synch.ipac_metadata.ingest_events_p_iPC_2025_Dev7_15347_1
WHERE event_type = 'flow_progress'
  AND level = 'METRICS'
  AND origin.pipeline_type = 'MANAGED_INGESTION'
  AND origin.flow_name IS NOT NULL
ORDER BY event_timestamp DESC;
```

## Calc gating (consumer contract)

`ipac-sdt-calc` should gate on `recon_ready` (and optionally `process_log`):

```text
last_recon_ready.completed_at > last_calc.completed_at
AND recon_ready.ready_for_calc = true
AND (recon_type = 1 OR source_change_rows = ingest_change_rows)
```

See `docs/IPAC_SDT_THREE_REPO_ARCHITECTURE.md` in the deloitte repo for cross-repo coordination.

### Bundle validate errors

If `databricks bundle validate` reports `reference does not exist: ${var.recon_lookback_hours}` (or `recon_poll_interval_sec`), ensure `databricks.yml` defines those variables under **both** root `variables:` and your active target (`dev` / `prod`):

```yaml
variables:
  recon_poll_interval_sec:
    default: 300
  recon_lookback_hours:
    default: 24

targets:
  dev:
    variables:
      recon_poll_interval_sec: 300
      recon_lookback_hours: 24
```

Warnings about unknown fields (`pipeline_type`, `spark_version`, `data_security_mode` on pipeline clusters) are usually benign — the Databricks CLI schema may lag Lakeflow Connect pipeline fields. Upgrade the Databricks CLI if deploy fails on those fields.

|---------|--------------|--------|
| `SKIP event log not found` | Pipeline not redeployed with `event_log` | `generate` + `bundle deploy` |
| No `recon_ready` rows | No flows reached `COMPLETED` yet | Check event log for `flow_progress` |
| FAIL `source CT count unavailable` | Federated SQL / CT grants | Run `*_enable_ct.sql` + `*_grant_ct_access.sql`; verify `src_db_nm` catalog |
| Duplicate calc triggers | Should not occur | Recon skips existing `(pipeline_id, update_id, flow_name)` in `recon_ready` |
| Metrics look low | Summing deltas in window only | Expected; do not compare to table `COUNT(*)` |

## Tests

```bash
uv sync --group dev
export PYTHONPATH=src
uv run pytest tests/test_lakeflow_recon.py -q
```

Full suite: `uv run pytest tests/ -q`
