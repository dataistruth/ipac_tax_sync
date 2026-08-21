# SQL/recon cluster init script troubleshooting

## Symptom: `INIT_SCRIPT_FAILURE` on cluster start

The allowlist passed (script path accepted) but the **bash script exited with an error** during cluster startup.

### Get the real error (do this first)

1. On the pink banner click **Show details** (full stderr is there).
2. **Event log** → `INIT_SCRIPT_FAILURE` → **JSON** tab → full `databricks_error_message`.
3. **Driver logs** → search `ipac-sql-recon-init`.
4. On the node: `/var/log/ipac-sql-recon-init.log`

### Isolate volume vs apt failure

Upload `install_sql_recon_smoke_test.sh` to the volume and **temporarily** set cluster init script to that file (same folder). Restart.

| Result | Meaning |
|--------|---------|
| Cluster **starts** | Volume + allowlist OK → problem is **apt/msodbcsql** inside the main script |
| Cluster **still fails** | Volume path, permissions, or file corrupt (CRLF / empty file) |

```powershell
databricks fs cp src/common/cluster/init/install_sql_recon_smoke_test.sh `
  dbfs:/Volumes/dev7/ipac_metadata/cluster_init/install_sql_recon_smoke_test.sh --overwrite
```

Point init script to `/Volumes/dev7/ipac_metadata/cluster_init/install_sql_recon_smoke_test.sh` and restart.

## 2. Verify the volume file

Catalog Explorer → `dev7` → `ipac_metadata` → `cluster_init`

| Check | Expected |
|-------|----------|
| File exists | `install_sql_recon_dependencies.sh` |
| Size | ~2 KB (not 0 bytes) |
| Re-upload after edits | Use **overwrite** |

Re-upload from repo (Windows):

```powershell
databricks fs cp `
  src/common/cluster/init/install_sql_recon_dependencies.sh `
  dbfs:/Volumes/dev7/ipac_metadata/cluster_init/install_sql_recon_dependencies.sh `
  --overwrite
```

## 3. Common causes

| Cause | Fix |
|-------|-----|
| **Empty / wrong file uploaded** | Re-upload script from repo |
| **Windows CRLF line endings** | Re-upload from git; or save as LF in editor |
| **Wrong Ubuntu repo (22.04 hardcoded)** | Use updated script (auto-detects `VERSION_ID`) |
| **`apt-get` blocked** | Common on locked-down Azure workspaces (EDR kills apt) — ask admin or **custom Docker image** with ODBC baked in |
| **No outbound network** | Allow packages.microsoft.com + pypi from cluster nodes |
| **Volume READ permission** | Cluster owner needs `READ_VOLUME` on `cluster_init` |

## 4. Cluster init script path

Must match exactly:

```
/Volumes/dev7/ipac_metadata/cluster_init/install_sql_recon_dependencies.sh
```

## 5. After fix

1. Re-upload script (if changed)
2. **Restart** cluster (init scripts only run at start)
3. Test on cluster:

```python
import pyodbc
print(pyodbc.drivers())
```

Expected: `ODBC Driver 18 for SQL Server` (or 17).
