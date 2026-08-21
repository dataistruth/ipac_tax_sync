"""Tests for direct SQL Server connection helpers."""

from __future__ import annotations

from common.ops.source_ct_direct import SqlServerDirectConfig, build_mssql_python_connection_string


def test_build_mssql_python_connection_string():
    config = SqlServerDirectConfig(
        host="sql.example.com",
        port=1433,
        database="iPC_2025_DEV7_15447",
        username="audit_user",
        password="secret",
    )
    conn_str = build_mssql_python_connection_string(config)
    assert "Server=sql.example.com,1433" in conn_str
    assert "Database=iPC_2025_DEV7_15447" in conn_str
    assert "UID=audit_user" in conn_str
    assert "TrustServerCertificate=yes" in conn_str
