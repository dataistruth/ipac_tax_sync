"""Tests for UC schema ensure helper."""

from common.ops.uc_schema_ops import ensure_uc_schema


def test_ensure_uc_schema_executes_create_if_not_exists():
    class _Spark:
        def __init__(self) -> None:
            self.sql_calls: list[str] = []

        def sql(self, statement: str):
            self.sql_calls.append(statement)
            return self

    spark = _Spark()
    ensure_uc_schema(spark, "dev7", "ipac_metadata")
    assert len(spark.sql_calls) == 1
    assert "CREATE SCHEMA IF NOT EXISTS" in spark.sql_calls[0]
    assert "`dev7`" in spark.sql_calls[0]
    assert "`ipac_metadata`" in spark.sql_calls[0]
