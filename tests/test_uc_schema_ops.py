"""Tests for UC schema ensure helper."""

from common.ops.uc_schema_ops import ensure_uc_schema


def test_ensure_uc_schema_executes_create_if_not_exists():
    class _Row:
        def __init__(self, catalog: str) -> None:
            self.catalog = catalog

    class _Df:
        def __init__(self, rows: list[_Row]) -> None:
            self._rows = rows

        def collect(self):
            return self._rows

    class _Spark:
        def __init__(self) -> None:
            self.sql_calls: list[str] = []

        def sql(self, statement: str):
            self.sql_calls.append(statement)
            if statement.strip().upper().startswith("SHOW CATALOGS"):
                return _Df([_Row("dev7")])
            return self

    spark = _Spark()
    ensure_uc_schema(spark, "dev7", "ipac_metadata")
    assert spark.sql_calls[0].startswith("SHOW CATALOGS")
    assert spark.sql_calls[1] == "USE CATALOG `dev7`"
    assert "CREATE SCHEMA IF NOT EXISTS" in spark.sql_calls[2]
    assert "`ipac_metadata`" in spark.sql_calls[2]
