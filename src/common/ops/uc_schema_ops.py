"""Unity Catalog schema helpers — create metadata/raw schemas when missing."""

from __future__ import annotations


def _quote_ident(value: str) -> str:
    return "`" + str(value).replace("`", "``") + "`"


def _list_catalog_names(spark) -> set[str]:
    try:
        rows = spark.sql("SHOW CATALOGS").collect()
        names: set[str] = set()
        for row in rows:
            if hasattr(row, "catalog"):
                names.add(str(row.catalog))
            elif len(row) > 0:
                names.add(str(row[0]))
        if names:
            return names
    except Exception:
        pass
    return {c.name for c in spark.catalog.listCatalogs()}


def ensure_uc_schema(spark, catalog: str, schema: str) -> None:
    """Create UC schema if missing. Requires UC-enabled compute (not hive-only cluster)."""
    cat = str(catalog).strip()
    sch = str(schema).strip()
    if not cat or not sch:
        raise ValueError("catalog and schema are required")

    available = _list_catalog_names(spark)
    if cat not in available:
        raise ValueError(
            f"UC catalog '{cat}' not visible on this compute. "
            f"Available catalogs: {sorted(available)}. "
            "Set job cluster data_security_mode to SINGLE_USER and deploy bundle schemas, "
            "or pass uc_catalog=${var.uc_catalog} from databricks.yml."
        )

    spark.sql(f"USE CATALOG {_quote_ident(cat)}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(sch)}")
