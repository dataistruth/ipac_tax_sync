"""Tests for direct SQL Server connection helpers."""

from __future__ import annotations

from common.ops.source_ct_direct import (
    SqlServerDirectConfig,
    build_mssql_python_connection_string,
    cursor_column_names,
    fetch_one_as_dict,
    row_as_dict,
)


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


def test_row_as_dict_from_tuple_row():
    row = ("dbo", "Entity", 65229)
    cols = ["schema_name", "table_name", "last_version"]
    assert row_as_dict(row, cols) == {
        "schema_name": "dbo",
        "table_name": "Entity",
        "last_version": 65229,
    }


class _Cursor:
    description = [("op",), ("cnt",)]

    def __init__(self, rows):
        self._rows = rows
        self.executed: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql: str):
        self.executed.append(sql)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _Conn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _Cursor(self._rows)


def test_fetch_one_as_dict():
    conn = _Conn([("iPC_2025_DEV7_15447", 65229)])
    _Cursor.description = [("database_name",), ("last_version",)]
    row = fetch_one_as_dict(conn, "SELECT 1")
    assert row == {"database_name": "iPC_2025_DEV7_15447", "last_version": 65229}


def test_cursor_column_names():
    cur = _Cursor([])
    assert cursor_column_names(cur) == ["op", "cnt"]
