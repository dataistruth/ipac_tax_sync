"""Tests for SQL Server Change Tracking probe and count SQL builders."""

from datetime import datetime, timezone

from common.ops.source_ct_ops import (
    build_ct_connection_probe_sql,
    build_ct_current_version_sql,
    build_ct_max_version_sql,
    build_federated_ct_count_sql,
    probe_source_ct_connection,
)


def _ts() -> datetime:
    return datetime(2026, 8, 16, 10, 0, 0, tzinfo=timezone.utc)


def test_build_ct_connection_probe_sql():
    sql = build_ct_connection_probe_sql("src_cat", "dbo", "Entity")
    assert "src_cat.dbo.Entity" in sql
    assert "LIMIT 1" in sql


def test_build_ct_max_version_sql():
    sql = build_ct_max_version_sql("src_cat", "dbo", "Entity")
    assert "CHANGETABLE(CHANGES src_cat.dbo.Entity, 0)" in sql
    assert "MAX(ct.sys_change_version)" in sql


def test_build_ct_current_version_sql():
    sql = build_ct_current_version_sql("src_cat")
    assert "CHANGE_TRACKING_CURRENT_VERSION()" in sql
    assert "src_cat" in sql


def test_probe_source_ct_connection_with_mock_spark():
    class _Row:
        def __init__(self, data: dict) -> None:
            self._data = data

        def __getitem__(self, key: str):
            return self._data[key]

    class _Spark:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def sql(self, query: str):
            self.calls.append(query)
            if "LIMIT 1" in query and "CHANGE_TRACKING" not in query:
                if "connection_ok" in query:
                    return type("_DF", (), {"collect": lambda self: [_Row({"connection_ok": 1})]})()
            if "MAX(ct.sys_change_version)" in query:
                return type("_DF", (), {"collect": lambda self: [_Row({"max_change_version": 42})]})()
            if "CHANGE_TRACKING_CURRENT_VERSION()" in query:
                return type("_DF", (), {"collect": lambda self: [_Row({"current_ct_version": 99})]})()
            if "COUNT_BIG" in query:
                return type("_DF", (), {"collect": lambda self: [_Row({"change_rows": 7})]})()
            raise AssertionError(query)

    spark = _Spark()
    end = _ts()
    start = datetime(2026, 8, 16, 9, 0, 0, tzinfo=timezone.utc)
    result = probe_source_ct_connection(
        spark,
        "src_cat",
        "dbo",
        "Entity",
        recon_type=2,
        start_time=start,
        end_time=end,
        print_results=False,
    )
    assert result["connection_ok"] is True
    assert result["max_change_version"] == 42
    assert result["current_ct_version"] == 99
    assert result["window_change_rows"] == 7
    assert len(spark.calls) == 4


def test_federated_ct_count_sql_operations_by_recon_type():
    start = _ts()
    end = datetime(2026, 8, 16, 10, 5, 0, tzinfo=timezone.utc)
    sql2 = build_federated_ct_count_sql("c", "dbo", "T", start, end, 2)
    sql3 = build_federated_ct_count_sql("c", "dbo", "T", start, end, 3)
    assert "('I', 'U', 'D')" in sql2
    assert "('I', 'U')" in sql3
