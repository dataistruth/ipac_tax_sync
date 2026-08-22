# SQL Server `ipac_metadata` database

Low-latency CT watermarks, recon audit, and event-log poll state live in the dedicated **`ipac_metadata`** database on SQL Server (`dbo` schema).

**`process_log` is NOT on SQL Server** — heartbeat and restart use UC Delta only (`{catalog}.ipac_metadata.process_log`).

## Run order (SSMS)

Execute on the **SQL Server instance**:

| Order | Script | Purpose |
|-------|--------|---------|
| 1 | `001_create_database.sql` | Create database `ipac_metadata` |
| 2 | `002_ct_recon_tables.sql` | All CT/recon tables + monitoring views |
| 3 | `003_process_log_table.sql` | **Skip** — deprecated; `process_log` is UC only |
| 4 | `004_grants.sql` | Set `@AuditLogin` + client DB CT grants |
| opt | `006_baseline_ct_watermarks.sql` | Baseline CT heads for all CT DBs |
| opt | `005_poll_changed_tables.sql` | Ad-hoc CT poll for one client DB |

## Tables (`ipac_metadata.dbo`)

### CT recon (`002_ct_recon_tables.sql`)

| Table | Purpose |
|-------|---------|
| `ct_db_watermark` | DB-level CT head |
| `ct_table_watermark` | Per-table reconciled CT version |
| `recon_event_log_watermark` | Pipeline event_log poll state |
| `recon_run` | One row per recon batch |
| `recon_table_result` | Per-table recon outcome |
| `ingestion_audit_log` | General audit events |

Databricks recon appends **`recon_ready`** in Unity Catalog Delta. **`process_log`** is UC only (heartbeat/restart).

## Databricks secrets

```bash
databricks secrets create-scope scope_ipacs_audit
databricks secrets put-secret scope_ipacs_audit SQL_SERVER_AUDIT_USERNAME
databricks secrets put-secret scope_ipacs_audit SQL_SERVER_AUDIT_PASSWORD
```

See `config/common/secrets/` for setup scripts.

## Python

- `common.ops.sql_server_audit_store` — SQL read/write via **mssql-python**
- `common.ops.ingestion_recon_ops` — simplified CT-driven recon
