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
| `client_size` | One of `small`, `medium`, `large` |
| `cluster_tier` | One of `s1`, `s2`, `s3`, `j1`, `j2`, `j3` |

> Lakeflow Connect pipelines must use job-cluster tiers (`j1/j2/j3`), and generated YAML always sets `serverless: false`.

**UC catalog** is common for all clients — set in `databricks.yml` as `variables.uc_catalog`.
**Destination schema suffix** is optional — set `variables.dest_schema_suffix` in `databricks.yml` (empty means schema is exactly `client_nm`).
**Pipeline heartbeat** threshold is configured in `variables.heartbeat_interval_sec`.
**Metadata schema** for operational tables is `variables.ipac_metadata_schema`.

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
| `recon_type` | Reconciliation type: `1`, `2`, or `3` |

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
```

`generate` writes:

- `generated/bundle/<client_nm>_pipeline.yml` — all `p_<client_nm>_<n>` pipelines for bundle deploy
- `generated/schema/<client_nm>_schema.yml` — schema resource per client destination schema
- `generated/schema/ipac_metadata_schema.yml` — metadata schema resource
- `src/<client_nm>/pipelines/p_<client_nm>_<n>.yml` — one file per pipeline batch

Deploy (example — client_a with 10 tables, batch size 5):

```bash
./ipac-delta-sync generate
databricks bundle deploy --select pipelines.p_client_a_1,pipelines.p_client_a_2
```

### Heartbeat + restart jobs

Bundle also defines two jobs under `resources/jobs/pipeline_heartbeat_jobs.yml`:

- `pipeline_heartbeat_monitor` — checks generated continuous pipelines (`p_*`) heartbeat/state and fails (email alert) when unhealthy.
- `pipeline_failed_restart` — restarts failed continuous generated pipelines.

Monitor uses `variables.heartbeat_interval_sec` to decide staleness.

## Onboard a new client

1. Add a row to `config/common/client.json` with `is_active: true`
2. Optionally add `config/common/client_overrides/<client_nm>.json`
3. `./ipac-delta-sync validate --client <client_nm>`
4. `./ipac-delta-sync generate --client <client_nm>`

## Tests

```bash
uv sync --group dev
uv run pytest
```
