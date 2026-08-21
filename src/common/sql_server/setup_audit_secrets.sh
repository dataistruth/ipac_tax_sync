#!/usr/bin/env bash
# Create Databricks secret scope + SQL Server audit credentials for ipac recon.
set -euo pipefail

SCOPE="${SCOPE:-scope_ipacs_audit}"
PROFILE="${PROFILE:-}"

profile_args=()
if [[ -n "${PROFILE}" ]]; then
  profile_args=(--profile "${PROFILE}")
fi

echo "Creating secret scope: ${SCOPE}"
if databricks secrets list-scopes "${profile_args[@]}" 2>/dev/null | grep -q "${SCOPE}"; then
  echo "Scope ${SCOPE} already exists — skipping create-scope"
else
  databricks secrets create-scope "${SCOPE}" "${profile_args[@]}"
fi

echo "Put secret SQL_SERVER_AUDIT_USERNAME (interactive prompt)"
databricks secrets put-secret "${SCOPE}" SQL_SERVER_AUDIT_USERNAME "${profile_args[@]}"

echo "Put secret SQL_SERVER_AUDIT_PASSWORD (interactive prompt)"
databricks secrets put-secret "${SCOPE}" SQL_SERVER_AUDIT_PASSWORD "${profile_args[@]}"

echo "Done. Verify:"
databricks secrets list-secrets "${SCOPE}" "${profile_args[@]}"
