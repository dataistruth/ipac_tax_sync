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
   - `003_process_log_table.sql` (optional SQL ops log)
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

## Unity Catalog tables

All tables live in `{uc_catalog}.{ipac_metadata_schema}` (default `ipac_tax_synch.ipac_metadata`).

### Published event logs (per pipeline)

Generated pipelines include:

```yaml
event_log:
  catalog: ${var.uc_catalog}
  schema: ${var.ipac_metadata_schema}
  name: ingest_events_p_<client_nm>_<serial>
```

Example: `ipac_tax_synch.ipac_metadata.ingest_events_p_iPC_2025_Dev7_15347_1`

Regenerate and redeploy pipelines after enabling event log publishing:

```bash
./ipac-delta-sync generate
databricks bundle deploy
```

### `lakeflow_flow_metrics`

Raw `flow_progress` rows. Merge key: `event_id`.

| Column | Description |
|--------|-------------|
| `event_id` | Lakeflow event log id |
| `pipeline_id`, `pipeline_name`, `update_id`, `flow_name` | Origin |
| `table_name` | `origin.dataset_name` or resolved config name |
| `event_timestamp`, `flow_status` | Event time and status |
| `output_rows`, `rows_upserted`, `rows_deleted`, `output_bytes` | Metrics |
| `client_nm`, `destination_schema`, `destination_table` | Resolved from config |
| `captured_at` | When recon job captured the row |

### `lakeflow_flow_summary`

One row per completed flow aggregate.

| Column | Description |
|--------|-------------|
| `summary_id` | UUID |
| `pipeline_id`, `update_id`, `flow_name`, `table_name` | Grain |
| `recon_type` | From table config |
| `total_output_rows`, `total_upserted`, `total_deleted`, `total_change_rows` | Summed deltas |
| `first_event_time`, `last_event_time`, `metric_duration_sec` | Window |
| `final_flow_status` | Must be `COMPLETED` |
| `recon_status` | `PENDING`, `PASS`, `FAIL`, `SKIPPED` |
| `source_change_rows` | SQL Server CT count when `recon_type` 2/3 |
| `recon_message` | PASS/FAIL detail |

### `recon_ready`

**PASS rows only** — calc gate.

| Column | Description |
|--------|-------------|
| `recon_id` | UUID |
| `client_nm`, `table_nm` | Client and table |
| `pipeline_id`, `update_id`, `flow_name` | Idempotency key with flow |
| `recon_type` | 1, 2, or 3 |
| `ingest_change_rows`, `source_change_rows` | Compared metrics |
| `completed_at` | Flow `last_event_time` |
| `artifact_run_id` | Pipeline `update_id` |
| `ready_for_calc` | `true` |

Duplicate `(pipeline_id, update_id, flow_name)` are skipped.

### `process_log`

On PASS or FAIL, recon also appends ingest rows to shared `process_log` (see main README). PASS uses `current_status = SUCCESS`; FAIL uses `FAILED`.

## Bundle configuration (`databricks.yml`)

| Variable | Default | Purpose |
|----------|---------|---------|
| `uc_catalog` | `ipac_tax_synch` | Catalog for metadata + event logs |
| `ipac_metadata_schema` | `ipac_metadata` | Metadata schema |
| `dest_schema_suffix` | `poc_1` | Client raw schema suffix for table config resolution |
| `recon_poll_interval_sec` | `300` | Recon job sleep between polls |
| `recon_lookback_hours` | `24` | Event log scan window per poll |
| `heartbeat_job_alert_mail` | (email) | Recon job failure notifications |

DDL is generated on `generate`:

```bash
./ipac-delta-sync generate
# → generated/schema/ipac_metadata_recon_tables.sql
# → generated/schema/ipac_metadata_process_log.sql
```

## Recon job

**Job name:** `j_ipac_delta_sync_ingestion_recon_monitor`  
**Definition:** `resources/jobs/ingestion_recon_jobs.yml`  
**Notebook:** `src/common/notebooks/run_ingestion_recon.py`

Continuous job (`pause_status: UNPAUSED`). Widgets:

| Widget | Default | Description |
|--------|---------|-------------|
| `uc_catalog` | `ipac_tax_synch` | UC catalog |
| `ipac_metadata_schema` | `ipac_metadata` | Metadata schema |
| `pipeline_names_file` | `generated/config/pipeline_names.json` | Monitored pipelines |
| `dest_schema_suffix` | `poc_1` | Schema suffix for table resolution |
| `poll_interval_sec` | `300` | Poll loop interval |
| `lookback_hours` | `24` | Event log lookback |
| `simplified_recon` | `true` | CT-driven simplified path (`recon_ready` only) |
| `simple_pass_rule` | `ct_delta_history` | SQL CT version change + all changed tables: Delta MERGE after CT timestamp; sample COUNT_BIG |
| `table_after_ct` | (option) | Per-table only: Delta history write after SQL CT reference (+ quiesce) |
| `table_quiesce_sec` | `15` | Buffer after SQL CT change timestamp before Delta write must occur |
| `row_count_sample_size` | `5` | For `ct_delta_history`: up to N highest-pending tables get COUNT_BIG validation |

**Delta write timestamp:** For `ct_delta_history` / `table_after_ct` / `ingest_quiesce`, recon reads `DESCRIBE HISTORY` on the UC target and uses the latest **MERGE** version/timestamp (fallback: WRITE/UPDATE/DELETE). DLT SETUP rows supply `updateId` for logging only. UC metadata fallbacks apply when history is empty.

**`ct_delta_history` (recommended):** Does not use `flow_progress` metrics or flow COMPLETED. When `ct_db_watermark` / table watermarks lag CT head, the DB is on the recon queue; all configured tables with pending CT must show a Delta write after the SQL CT reference time; up to `row_count_sample_size` tables (highest pending first) also require SQL vs Delta row-count match.
| `row_count_only_on_flow_complete` | `true` | Skip SQL/Delta `COUNT_BIG` until flow/API COMPLETED |
| `use_api_update_complete` | `true` | Use GET pipeline `latest_update.state=COMPLETED` when event log has no `flow_progress` |

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
