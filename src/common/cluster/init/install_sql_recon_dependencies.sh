#!/bin/bash
# Installs Microsoft ODBC Driver for SQL Server + pyodbc on Databricks classic clusters.
# Used by resources/clusters/ipac_sql_recon_cluster.yml init_scripts.
set -euo pipefail

log() { echo "[ipac-sql-recon-init] $*"; }

install_odbc_driver() {
  if odbcinst -q -d 2>/dev/null | grep -qi "ODBC Driver 1[78] for SQL Server"; then
    log "SQL Server ODBC driver already installed"
    odbcinst -q -d || true
    return 0
  fi

  log "Installing unixODBC + msodbcsql18..."
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y curl gnupg unixodbc unixodbc-dev

  if [ ! -f /usr/share/keyrings/microsoft-prod.gpg ]; then
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg
    curl -fsSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
  fi

  apt-get update
  ACCEPT_EULA=Y apt-get install -y msodbcsql18 || {
    log "msodbcsql18 failed; trying msodbcsql17..."
    ACCEPT_EULA=Y apt-get install -y msodbcsql17
  }

  odbcinst -q -d || true
}

install_pyodbc() {
  if /databricks/python3/bin/python3 -c "import pyodbc" 2>/dev/null; then
    log "pyodbc already available on Databricks Python"
    return 0
  fi
  log "Installing pyodbc via pip..."
  /databricks/python3/bin/pip install --quiet "pyodbc==5.2.0"
  /databricks/python3/bin/python3 -c "import pyodbc; print('pyodbc', pyodbc.version); print(pyodbc.drivers())"
}

install_odbc_driver
install_pyodbc
log "Done."
