# ipac_delta_sync

JSON-config-driven scaffolding for **continuous Lakeflow Connect** CDC pipelines. One active row in `client.json` → one pipeline; table sets come from `common_tables.json` + per-client overrides.

## Config layout (`config/common/`)

```
config/common/
  client.json              # array: who gets a pipeline (is_active drives count)
  common_tables.json       # shared SQL Server calc tables
  cluster_config.json      # tiers s1/s2/s3 (serverless) + j1/j2/j3 (job clusters)
  client_overrides/
    client_a.json          # ignore (is_active false) + extra tables
    client_b.json
```

### `client.json` (list of dicts)

| Field | Purpose |
|-------|---------|
| `client_nm` | Client id; UC schema = `{client_nm}{dest_schema_suffix}`; drives `src/<client_nm>/` |
| `desc` | Description (comment in generated YAML) |
| `volume` | Optional volume name inside `{client_nm}_raw`; empty = platform creates staging volume in raw schema |
| `priority` | Client priority metadata |
| `is_active` | **Decides whether a pipeline is created** |
| `src_db_nm` | SQL Server source catalog / database name |
| `src_db_schema` | SQL Server schema (usually `dbo`) |
| `uc_conn_nm` | Unity Catalog connection for Lakeflow |
| `client_size` | One of `small`, `medium`, `large` — **drives job cluster tier** |
| `cluster_tier` | Must match `client_size`: `small→j1`, `medium→j2`, `large→j3` (see `cluster_config.json`) |

### Cluster tiers (`cluster_config.json`)

Six named profiles in `config/common/cluster_config.json`:

| Tier | Type | Workers (autoscale) | Used for |
|------|------|---------------------|----------|
| `s1` | Serverless small | 1–2 | Reserved (not Lakeflow Connect CDC) |
| `s2` | Serverless medium | 2–4 | Reserved (not Lakeflow Connect CDC) |
| `s3` | Serverless large | 4–8 | Reserved (not Lakeflow Connect CDC) |
| `j1` | Job cluster small | 1–2 | `client_size: small` |
| `j2` | Job cluster medium | 2–4 | `client_size: medium` |
| `j3` | Job cluster large | 4–8 | `client_size: large` |

Lakeflow Connect CDC pipelines use **job clusters** (`j1`/`j2`/`j3`). Generated YAML sets `serverless: false` and a `clusters` block:

```yaml
clusters:
  - label: default
    node_type_id: ${var.pipeline_cluster_node_type}
    spark_version: ${var.pipeline_spark_version}
    data_security_mode: USER_ISOLATION
    autoscale:
      min_workers: 2   # from j2 in cluster_config.json
      max_workers: 4
```

Set node type and Spark version in `databricks.yml` (`variables.pipeline_cluster_node_type`, `variables.pipeline_spark_version`). Classic compute for ingestion requires the bundle **direct deployment engine** (`bundle.engine: direct` in `databricks.yml`).

> `cluster_tier` in `client.json` must agree with `client_size` (validate fails if e.g. `small` + `j2`).

**UC catalog** is common for all clients — set in `databricks.yml` as `variables.uc_catalog`.
**Destination schema suffix** is optional — set `variables.dest_schema_suffix` in `databricks.yml` (empty means schema is exactly `client_nm`).
**Pipeline heartbeat** threshold is configured in `variables.heartbeat_interval_sec`.
**Metadata schema** for operational tables is `variables.ipac_metadata_schema`.
**Access grant group** is configured in `variables.grant_group` and applied as `CAN_MANAGE` on generated pipelines/jobs.
**Heartbeat alert email** is configured in `variables.heartbeat_job_alert_mail`.

**Tables per pipeline** — `variables.num_of_tables_in_pipeline` (default `5`). A client with 10 tables generates `p_client_a_1` and `p_client_a_2` (5 tables each); 12 tables → three pipelines with 5, 5, and 2 tables.

Raw and staging share one schema per client: `{uc_catalog}.{client_nm}{dest_schema_suffix}`.

### `common_tables.json`

| Field | Purpose |
|-------|---------|
| `table_nm` | SQL Server / UC table name |
| `lq_key` | Lakeflow clustering columns; **empty = no cluster by** |
| `is_active` | Include in default client set when true |
| `select_cols` | Column list; empty = select all (`*`) |
| `scd_type` | `1` (default overwrite) or `2` (history / merge) |
| `recon_type` | Ingestion reconciliation mode: `1` = metrics-only PASS on flow COMPLETED (no SQL compare); `2` = compare `upserted+deleted` vs SQL Server CDC changes; `3` = compare `upserted` vs SQL Server CDC inserts+updates |

### `client_overrides/<client_nm>.json`

| Section | Purpose |
|---------|---------|
| `include_common` | Optional `false` — skip all common tables (extra-only client) |
| `ignore` | `{ table_nm, is_active: false }` — drop specific common tables |
| `extra` | Client-only tables (same shape as common table entries) |

## Src layout (generated / synced)

```
src/
  util/                    # CLI, config loading, pipeline generation
  common/                  # shared client code
  client_a/
    pipelines/             # generated <client>_lakeflow.yml
    transform/
```

## CLI (run from repo root — no package install)

### macOS / Linux (bash)

```bash
uv sync --group dev
chmod +x ipac-delta-sync   # once

./ipac-delta-sync list-clients --active-only
./ipac-delta-sync validate
./ipac-delta-sync resolve --client client_a
./ipac-delta-sync sync-src              # folders only
./ipac-delta-sync generate              # sync-src + pipeline YAML
```

Equivalent without the wrapper script:

```bash
export PYTHONPATH=src
uv run python -m util.cli validate
uv run python -m util.cli generate
```

### Windows (PowerShell)

From the repo root (e.g. `C:\Users\you\PycharmProjects\ipac_delta_sync`):

```powershell
cd C:\path\to\ipac_delta_sync

# One-time: install dev dependencies
uv sync --group dev

# Required each PowerShell session (same as export PYTHONPATH=src on bash)
$env:PYTHONPATH = "$PWD\src"

# Validate all active clients (or one client)
uv run python -m util.cli validate
uv run python -m util.cli validate --client iPC_2025_Dev7_15447

# Generate pipelines, SQL, bundle artifacts (skip deleting old YAML with --no-clean)
uv run python -m util.cli generate
uv run python -m util.cli generate --no-clean
uv run python -m util.cli generate --client iPC_2025_Dev7_15447 --no-clean

# Other useful commands
uv run python -m util.cli list-clients --active-only
uv run python -m util.cli resolve --client iPC_2025_Dev7_15447
```

Persist `PYTHONPATH` for every new PowerShell window (optional):

```powershell
[System.Environment]::SetEnvironmentVariable(
  "PYTHONPATH",
  "C:\path\to\ipac_delta_sync\src",
  "User"
)
```

Then reopen the terminal, or in the current session:

```powershell
$env:PYTHONPATH = "C:\path\to\ipac_delta_sync\src"
```

**Notes**

- The `ipac-delta-sync` shell script is bash-only. On Windows use `uv run python -m util.cli …` as above, or run the script from **Git Bash**.
- Use forward slashes or quoted paths if your repo path contains spaces.
- If `uv` is not found, install [uv](https://docs.astral.sh/uv/) and ensure it is on your `PATH`.

`generate` writes:

- `generated/bundle/<client_nm>_pipeline.yml` — all `p_<client_nm>_<n>` pipelines for bundle deploy
- `generated/schema/<client_nm>_schema.yml` — schema resource per client destination schema
- `generated/schema/ipac_metadata_schema.yml` — metadata schema resource
- `generated/schema/ipac_metadata_process_log.sql` — Delta `process_log` DDL

### `ipac_metadata.process_log` (Delta)

Shared operational log at `{uc_catalog}.{ipac_metadata_schema}.process_log` for **all** workloads:

| Column | Purpose |
|--------|---------|
| `process_type` | `ingest`, `calc`, `transfer`, `transform`, `egress` |
| `process_nm` | Pipeline name, calc job, transfer batch, etc. |
| `artifact_type` | `pipeline`, `job`, `notebook` |
| `artifact_id` | Stable resource: `pipeline_id`, job definition id, notebook path |
| `artifact_run_id` | **Each run**: pipeline `update_id`, `job_run_id`, `task_run_id` |
| `process_id` | Legacy column — prefers `artifact_run_id`, else `artifact_id` |
| `client_nm` | Client when applicable |
| `object_nm` | Table, calc module, file set |
| `start_tm` / `end_tm` | Process window |
| `current_status` | `RUNNING`, `SUCCESS`, `FAILED`, `HEALTHY`, `UNHEALTHY`, `IDLE`, `SKIPPED` |
| `detail_status` | Sub-status (e.g. pipeline `update_state`) |
| `heartbeat_age_sec` | Ingest monitor staleness |
| `rows_read` / `rows_written` / `rows_deleted` | Calc / transfer metrics |
| `log` | Detail text (truncated to 2000 chars) |

Heartbeat monitor writes `process_type=ingest` rows each poll. Calc / transfer jobs should use `common.ops.process_log_store.build_process_log_row()` + `write_process_log_rows()`.
- `generated/config/pipeline_names.json` — list of generated pipeline names for heartbeat/restart jobs
- `src/<client_nm>/pipelines/p_<client_nm>_<n>.yml` — one file per pipeline batch
- `src/<client_nm>/sql/<client_nm>_enable_ct.sql` — enable CT on PK tables (skips non-PK; CDC not used)
- `src/<client_nm>/sql/<client_nm>_grant_ct_access.sql` — CT grants for PK tables (`<KEEP_USER_ID>` placeholder; creates DB user from server login if needed)
- `src/<client_nm>/sql/<client_nm>_grant_cdc_access.sql` — CDC grants for non-PK tables (`<KEEP_USER_ID>` placeholder; creates DB user from server login if needed)

Deploy (example — client_a with 10 tables, batch size 5):

```bash
./ipac-delta-sync generate
databricks bundle deploy --select pipelines.p_client_a_1,pipelines.p_client_a_2
```

### Heartbeat + restart jobs

Bundle also defines two jobs under `resources/jobs/pipeline_heartbeat_jobs.yml`:

- `pipeline_heartbeat_monitor` — **continuous** job (UNPAUSED) that polls `p_*` pipeline status every `heartbeat_interval_sec` and fails (email alert) when unhealthy.
- `pipeline_failed_restart` — restarts failed continuous generated pipelines.

Both jobs run as **serverless notebook tasks** (no Spark session, no pip dependencies).
Notebooks: `src/common/notebooks/monitor_pipeline_heartbeat.py` and `restart_failed_pipelines.py`.
Shared REST logic: `src/common/ops/pipeline_job_ops.py` (stdlib only, auth via `dbutils`).
Monitor polls `GET /api/2.0/pipelines/{id}` for each configured pipeline and logs `update_state`, heartbeat age, and continuous flag. Uses `variables.heartbeat_interval_sec` as both poll sleep interval and stale threshold.

### Ingestion flow metrics reconciliation

Job `ingestion_recon_monitor` in `resources/jobs/ingestion_recon_jobs.yml` polls published MANAGED_INGESTION event logs (`ingest_events_p_*` in `{uc_catalog}.{ipac_metadata_schema}`), aggregates per-table `flow_progress` when status = `COMPLETED`, and writes:

- `lakeflow_flow_metrics` — raw event metrics (merge by `event_id`)
- `lakeflow_flow_summary` — per `(update_id, flow_name)` aggregates
- `recon_ready` — PASS rows only (calc gate)
- `process_log` — ingest SUCCESS/FAILED per reconciled table flow

Poll interval: `variables.recon_poll_interval_sec` (default 300s). Lookback: `variables.recon_lookback_hours`.

Notebook: `src/common/notebooks/run_ingestion_recon.py`. Logic: `src/common/ops/lakeflow_event_ops.py`, `ingestion_recon_ops.py`, `source_cdc_ops.py`, `recon_store.py`.

Generated pipelines include `event_log` publishing — regenerate and redeploy after upgrading:

```bash
./ipac-delta-sync generate
databricks bundle deploy
```


1. Add a row to `config/common/client.json` with `is_active: true`
2. Optionally add `config/common/client_overrides/<client_nm>.json`
3. Validate and generate (bash or PowerShell — see CLI section above)

   ```bash
   ./ipac-delta-sync validate --client <client_nm>
   ./ipac-delta-sync generate --client <client_nm>
   ```

   ```powershell
   $env:PYTHONPATH = "$PWD\src"
   uv run python -m util.cli validate --client <client_nm>
   uv run python -m util.cli generate --client <client_nm> --no-clean
   ```

## Tests

### macOS / Linux

```bash
uv sync --group dev
uv run pytest
```

### Windows (PowerShell)

```powershell
$env:PYTHONPATH = "$PWD\src"
uv run pytest
```
