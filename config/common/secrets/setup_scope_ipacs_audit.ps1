# Create Databricks secret scope + SQL Server audit credentials for ipac recon.
# Run in PowerShell from repo root (or any directory with databricks CLI on PATH).
#
# Usage:
#   .\config\common\secrets\setup_scope_ipacs_audit.ps1
#   .\config\common\secrets\setup_scope_ipacs_audit.ps1 -Profile dev
#   .\config\common\secrets\setup_scope_ipacs_audit.ps1 -Username "admin" -Password "secret"

param(
    [string]$Scope = "scope_ipacs_audit",
    [string]$Profile = "",
    [string]$Username = "",
    [string]$Password = ""
)

$ErrorActionPreference = "Stop"

function Invoke-Databricks {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Args)
    if ($Profile) {
        & databricks @Args --profile $Profile
    } else {
        & databricks @Args
    }
}

Write-Host "Scope: $Scope"
if ($Profile) { Write-Host "Profile: $Profile" }

$scopes = Invoke-Databricks secrets list-scopes 2>$null
if ($scopes -match [regex]::Escape($Scope)) {
    Write-Host "Scope '$Scope' already exists — skipping create-scope"
} else {
    Write-Host "Creating scope '$Scope'..."
    Invoke-Databricks secrets create-scope $Scope
}

if ($Username -and $Password) {
    Write-Host "Putting SQL_SERVER_AUDIT_USERNAME (from parameter)..."
    if ($Profile) {
        databricks secrets put-secret $Scope SQL_SERVER_AUDIT_USERNAME --string-value $Username --profile $Profile
    } else {
        databricks secrets put-secret $Scope SQL_SERVER_AUDIT_USERNAME --string-value $Username
    }
    Write-Host "Putting SQL_SERVER_AUDIT_PASSWORD (from parameter)..."
    if ($Profile) {
        databricks secrets put-secret $Scope SQL_SERVER_AUDIT_PASSWORD --string-value $Password --profile $Profile
    } else {
        databricks secrets put-secret $Scope SQL_SERVER_AUDIT_PASSWORD --string-value $Password
    }
} else {
    Write-Host "Putting SQL_SERVER_AUDIT_USERNAME (interactive prompt)..."
    Invoke-Databricks secrets put-secret $Scope SQL_SERVER_AUDIT_USERNAME
    Write-Host "Putting SQL_SERVER_AUDIT_PASSWORD (interactive prompt)..."
    Invoke-Databricks secrets put-secret $Scope SQL_SERVER_AUDIT_PASSWORD
}

Write-Host "Done. Secrets in scope '$Scope':"
Invoke-Databricks secrets list-secrets $Scope
