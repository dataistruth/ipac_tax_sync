# SQL Server `ipac_metadata` (master)

Low-latency CT watermarks and recon audit state live on the SQL Server instance in
`master.ipac_metadata`, separate from Unity Catalog Delta tables.

## Run order (SSMS)

Execute against the **SQL Server instance** (connect to `master`):

1. `sql/001_create_schema.sql`
2. `sql/002_watermark_tables.sql`
3. `sql/003_recon_audit_tables.sql`
4. `sql/004_grants.sql` — set `@AuditLogin` to your **existing admin SQL user** (skip if already `db_owner` on master + client DBs)
5. Optional: `sql/005_poll_changed_tables.sql` — ad-hoc poll for one client database
6. Optional: `sql/006_recon_views.sql`

## Databricks secret scope

Create scope and secrets with the Databricks CLI (run locally; you will be prompted for values):

```bash
databricks secrets create-scope scope_ipacs_audit

databricks secrets put-secret scope_ipacs_audit SQL_SERVER_AUDIT_USERNAME
databricks secrets put-secret scope_ipacs_audit SQL_SERVER_AUDIT_PASSWORD
```

Or use scripts in `config/common/secrets/`:

```powershell
.\config\common\secrets\setup_scope_ipacs_audit.ps1
```

See `config/common/secrets/databricks_secrets_commands.txt` for raw CLI commands.

## Python wiring

- `common.ops.sql_server_audit_store` — read/write watermarks and recon rows via pymssql
- `common.ops.ingestion_recon_ops` — uses SQL Server CT watermarks for `recon_type` 2/3
- `config/common/client.json` — set `sql_host` per client; defaults use `scope_ipacs_audit`

## Tables

| Table | Purpose |
|-------|---------|
| `ct_db_watermark` | DB-level `CHANGE_TRACKING_CURRENT_VERSION()` snapshot |
| `ct_table_watermark` | Per-table last reconciled CT version |
| `recon_run` | One row per ingestion pipeline recon attempt |
| `recon_table_result` | Per-table CT vs ingestion comparison |
| `ingestion_audit_log` | General audit trail |
