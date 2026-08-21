#!/bin/bash
# Smoke test only — confirms UC volume init script path works (no apt-get).
# Temporarily point cluster init script to this file to isolate allowlist/volume issues.
echo "[ipac-sql-recon-smoke] OK $(date -Iseconds)" | tee /var/log/ipac-sql-recon-smoke.log
exit 0
