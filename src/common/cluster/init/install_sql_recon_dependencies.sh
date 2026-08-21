#!/bin/bash
# Installs Microsoft ODBC Driver for SQL Server + pyodbc on Databricks classic clusters.
# Logs: /var/log/ipac-sql-recon-init.log (also echoed to init-script stdout)
set -e

LOG="/var/log/ipac-sql-recon-init.log"
exec > >(tee -a "$LOG") 2>&1

log() { echo "[ipac-sql-recon-init] $(date -Iseconds) $*"; }

log "Starting init script (pid=$$)"
log "OS: $(cat /etc/os-release 2>/dev/null | tr '\n' ' ')"

install_odbc_driver() {
  if command -v odbcinst >/dev/null 2>&1 && odbcinst -q -d 2>/dev/null | grep -qi "ODBC Driver 1[78] for SQL Server"; then
    log "SQL Server ODBC driver already installed"
    odbcinst -q -d || true
    return 0
  fi

  log "Installing unixODBC + msodbcsql18..."
  export DEBIAN_FRONTEND=noninteractive

  apt-get update -y
  apt-get install -y curl gnupg ca-certificates unixodbc unixodbc-dev apt-transport-https

  UBUNTU_VERSION="${VERSION_ID:-22.04}"
  if [ -f /etc/os-release ]; then
    # shellcheck disable=SC1091
    . /etc/os-release
    UBUNTU_VERSION="${VERSION_ID:-22.04}"
  fi
  log "Using Microsoft repo for Ubuntu ${UBUNTU_VERSION}"

  install -d /usr/share/keyrings
  curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg
  curl -fsSL "https://packages.microsoft.com/config/ubuntu/${UBUNTU_VERSION}/prod.list" \
    > /etc/apt/sources.list.d/mssql-release.list

  apt-get update -y
  if ! ACCEPT_EULA=Y apt-get install -y msodbcsql18; then
    log "msodbcsql18 failed; trying msodbcsql17..."
    ACCEPT_EULA=Y apt-get install -y msodbcsql17
  fi

  odbcinst -q -d || true
  log "ODBC drivers after install:"
  odbcinst -q -d || true
}

install_pyodbc() {
  PY="/databricks/python3/bin/python3"
  PIP="/databricks/python3/bin/pip"
  if [ ! -x "$PY" ]; then
    log "WARN: $PY not found; skipping pyodbc pip install"
    return 0
  fi
  if "$PY" -c "import pyodbc" 2>/dev/null; then
    log "pyodbc already installed"
  else
    log "Installing pyodbc via pip..."
    "$PIP" install --quiet "pyodbc==5.2.0"
  fi
  "$PY" -c "import pyodbc; print('pyodbc', pyodbc.version); print(pyodbc.drivers())"
}

install_odbc_driver
install_pyodbc
log "Done."
