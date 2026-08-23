"""Unity Catalog schema helpers — create metadata/raw schemas when missing."""

from __future__ import annotations


def _quote_ident(value: str) -> str:
    return "`" + str(value).replace("`", "``") + "`"


def ensure_uc_schema(spark, catalog: str, schema: str) -> None:
    """Create UC schema if missing (USE CATALOG + single-part schema name)."""
    cat = str(catalog).strip()
    sch = str(schema).strip()
    if not cat or not sch:
        raise ValueError("catalog and schema are required")
    spark.sql(f"USE CATALOG {_quote_ident(cat)}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(sch)}")
