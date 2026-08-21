#!/bin/bash
# Installs Microsoft ODBC Driver for SQL Server + pyodbc on Databricks classic clusters.
# Logs: /var/log/ipac-sql-recon-init.log
LOG="/var/log/ipac-sql-recon-init.log"
exec > >(tee -a "$LOG") 2>&1

log() { echo "[ipac-sql-recon-init] $(date -Iseconds) $*"; }

fail() {
  log "FAILED: $*"
  log "--- tail of $LOG ---"
  tail -n 40 "$LOG" 2>/dev/null || true
  exit 1
}

log "Starting (pid=$$)"
if [ -f /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  log "OS VERSION_ID=${VERSION_ID:-unknown}"
fi

export DEBIAN_FRONTEND=noninteractive
export ACCEPT_EULA=Y

install_odbc_driver() {
  if command -v odbcinst >/dev/null 2>&1 && odbcinst -q -d 2>/dev/null | grep -qi "ODBC Driver 1[78] for SQL Server"; then
    log "ODBC driver already present"
    odbcinst -q -d || true
    return 0
  fi

  log "apt-get update..."
  if ! apt-get update -y; then
    fail "apt-get update failed (workspace may block apt — ask admin or use custom Docker image)"
  fi

  log "Installing base packages..."
  if ! apt-get install -y curl gnupg ca-certificates apt-transport-https unixodbc unixodbc-dev; then
    fail "apt-get install base packages failed"
  fi

  UBUNTU_VERSION="${VERSION_ID:-22.04}"
  log "Microsoft repo for Ubuntu ${UBUNTU_VERSION}"
  install -d /usr/share/keyrings
  if ! curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg; then
    fail "Could not fetch Microsoft signing key (outbound network?)"
  fi
  if ! curl -fsSL "https://packages.microsoft.com/config/ubuntu/${UBUNTU_VERSION}/prod.list" \
    > /etc/apt/sources.list.d/mssql-release.list; then
    fail "Could not fetch Microsoft prod.list for Ubuntu ${UBUNTU_VERSION}"
  fi

  log "apt-get update (microsoft repo)..."
  if ! apt-get update -y; then
    fail "apt-get update after adding Microsoft repo failed"
  fi

  log "Installing msodbcsql18..."
  if ! ACCEPT_EULA=Y apt-get install -y msodbcsql18; then
    log "msodbcsql18 failed; trying msodbcsql17..."
    if ! ACCEPT_EULA=Y apt-get install -y msodbcsql17; then
      fail "msodbcsql18 and msodbcsql17 install both failed"
    fi
  fi

  # Required so unixODBC / pyodbc can load the driver (Databricks KB)
  for libdir in /opt/microsoft/msodbcsql18/lib64 /opt/microsoft/msodbcsql17/lib64; do
    if [ -d "$libdir" ]; then
      export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:+$LD_LIBRARY_PATH:}$libdir"
      if ! grep -q "$libdir" /etc/environment 2>/dev/null; then
        echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:$libdir" >> /etc/environment
      fi
      log "Added LD_LIBRARY_PATH: $libdir"
    fi
  done

  odbcinst -q -d || true
}

install_pyodbc() {
  PY="/databricks/python3/bin/python3"
  PIP="/databricks/python3/bin/pip"
  if [ ! -x "$PY" ]; then
    log "WARN: $PY not found; skipping pyodbc"
    return 0
  fi
  if ! "$PY" -c "import pyodbc" 2>/dev/null; then
    log "pip install pyodbc..."
    if ! "$PIP" install --quiet "pyodbc==5.2.0"; then
      fail "pip install pyodbc failed"
    fi
  fi
  "$PY" -c "import pyodbc; print('pyodbc', pyodbc.version); print(pyodbc.drivers())"
}

install_odbc_driver
install_pyodbc

if ! odbcinst -q -d 2>/dev/null | grep -qi "ODBC Driver 1[78] for SQL Server"; then
  fail "ODBC driver not registered after install — check $LOG"
fi

log "Success."
exit 0
