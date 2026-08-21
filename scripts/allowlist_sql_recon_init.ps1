# Add SQL/recon cluster init script directory to UC INIT_SCRIPT artifact allowlist.
# Requires MANAGE ALLOWLIST on the metastore (metastore admin).
# Usage (from repo root):
#   .\scripts\allowlist_sql_recon_init.ps1
#   .\scripts\allowlist_sql_recon_init.ps1 -Catalog dev7 -Schema ipac_metadata

param(
    [string]$Catalog = "dev7",
    [string]$Schema = "ipac_metadata",
    [string]$VolumeName = "cluster_init"
)

$ErrorActionPreference = "Stop"
$AllowlistPath = "/Volumes/$Catalog/$Schema/$VolumeName/"
if (-not $AllowlistPath.EndsWith("/")) { $AllowlistPath += "/" }

Write-Host "Fetching current INIT_SCRIPT allowlist..."
$get = databricks artifact-allowlists get INIT_SCRIPT -o json 2>$null
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($get)) {
    $current = @{ artifact_matchers = @() }
} else {
    $current = $get | ConvertFrom-Json
}

$matchers = @()
if ($null -ne $current.artifact_matchers) {
    $matchers = @($current.artifact_matchers)
}

$exists = $false
foreach ($m in $matchers) {
    if ($m.artifact -eq $AllowlistPath) { $exists = $true; break }
}

if (-not $exists) {
    $matchers += @{ artifact = $AllowlistPath; match_type = "PREFIX_MATCH" }
    Write-Host "Adding allowlist entry: $AllowlistPath"
} else {
    Write-Host "Allowlist entry already present: $AllowlistPath"
}

$payload = @{ artifact_matchers = $matchers } | ConvertTo-Json -Depth 5 -Compress
databricks artifact-allowlists update INIT_SCRIPT --json $payload
if ($LASTEXITCODE -ne 0) { throw "artifact-allowlists update failed" }

Write-Host "Done. Restart ipac_sql_recon_shared."
