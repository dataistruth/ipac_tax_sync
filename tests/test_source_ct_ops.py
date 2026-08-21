"""Tests for SQL Server Change Tracking probe and count SQL builders."""

from common.ops.source_ct_ops import (
    build_ct_connection_probe_sql,
    build_ct_current_version_sql,
    build_ct_count_sql,
    build_ct_max_version_sql,
    build_federated_ct_count_sql,
    probe_source_ct_connection,
)


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
    result = probe_source_ct_connection(
        spark,
        "src_cat",
        "dbo",
        "Entity",
        recon_type=2,
        version_before=10,
        version_after=99,
        print_results=False,
    )
    assert result["connection_ok"] is True
    assert result["max_change_version"] == 42
    assert result["current_ct_version"] == 99
    assert result["window_change_rows"] == 7
    assert len(spark.calls) == 4


def test_federated_ct_count_sql_operations_by_recon_type():
    sql2 = build_federated_ct_count_sql("c", "dbo", "T", 100, 200, 2)
    sql3 = build_federated_ct_count_sql("c", "dbo", "T", 100, 200, 3)
    assert "CHANGETABLE(CHANGES c.dbo.T, 100)" in sql2
    assert "sys_change_version <= 200" in sql2
    assert "('I', 'U', 'D')" in sql2
    assert "('I', 'U')" in sql3


def test_native_ct_count_sql_operations_by_recon_type():
    sql2 = build_ct_count_sql("dbo", "Entity", 8800, 8842, recon_type=2)
    sql3 = build_ct_count_sql("dbo", "Entity", 8800, 8842, recon_type=3)
    assert "CHANGETABLE(CHANGES dbo.Entity, 8800)" in sql2
    assert "SYS_CHANGE_VERSION <= 8842" in sql2
    assert "('I', 'U', 'D')" in sql2
    assert "('I', 'U')" in sql3
