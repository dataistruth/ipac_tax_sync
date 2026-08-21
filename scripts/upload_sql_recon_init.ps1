# Upload ODBC/pyodbc init script to the ipac_metadata cluster_init UC volume.
# Usage (from repo root):
#   .\scripts\upload_sql_recon_init.ps1
#   .\scripts\upload_sql_recon_init.ps1 -Catalog dev7 -Schema ipac_metadata

param(
    [string]$Catalog = "dev7",
    [string]$Schema = "ipac_metadata",
    [string]$VolumeName = "cluster_init",
    [string]$Src = "src/common/cluster/init/install_sql_recon_dependencies.sh"
)

$ErrorActionPreference = "Stop"
if (-not (Test-Path $Src)) {
    throw "Missing $Src (run from bundle root)"
}

$Dest = "/Volumes/$Catalog/$Schema/$VolumeName/install_sql_recon_dependencies.sh"
Write-Host "Uploading $Src -> dbfs:$Dest"
databricks fs cp $Src "dbfs:$Dest" --overwrite
if ($LASTEXITCODE -ne 0) { throw "databricks fs cp failed" }
Write-Host "Done. Run allowlist script, then restart ipac_sql_recon_shared."
