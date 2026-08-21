"""Tests for direct SQL Server connection helpers."""

from __future__ import annotations

import sys
import types

from common.ops.source_ct_direct import SqlServerDirectConfig, build_pyodbc_connection_string


def test_build_pyodbc_connection_string_uses_driver_18(monkeypatch):
    fake_pyodbc = types.SimpleNamespace(
        drivers=lambda: ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
    )
    monkeypatch.setitem(sys.modules, "pyodbc", fake_pyodbc)

    config = SqlServerDirectConfig(
        host="sql.example.com",
        port=1433,
        database="iPC_2025_DEV7_15447",
        username="audit_user",
        password="secret",
    )
    conn_str = build_pyodbc_connection_string(config)
    assert "DRIVER={ODBC Driver 18 for SQL Server}" in conn_str
    assert "SERVER=sql.example.com,1433" in conn_str
    assert "DATABASE=iPC_2025_DEV7_15447" in conn_str
    assert "TrustServerCertificate=yes" in conn_str
