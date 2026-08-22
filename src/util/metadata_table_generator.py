"""Generate ipac_metadata Delta table DDL."""

from __future__ import annotations

from pathlib import Path

from common.ops.process_log_store import process_log_create_sql
from common.ops.recon_store import recon_ready_create_sql


def write_process_log_table_sql(
    catalog: str,
    metadata_schema: str,
    output_dir: Path | str,
) -> str:
    """UC process_log — heartbeat monitor and restart job (UC only)."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "ipac_metadata_process_log.sql"
    header = (
        f"-- Heartbeat + restart: UC process_log only\n"
        f"-- Target: {catalog}.{metadata_schema}.process_log\n\n"
    )
    out_file.write_text(header + process_log_create_sql(catalog, metadata_schema) + "\n", encoding="utf-8")
    return str(out_file)


def write_recon_ready_table_sql(
    catalog: str,
    metadata_schema: str,
    output_dir: Path | str,
) -> str:
    """UC ipac_metadata holds only recon_ready; all other metadata is SQL Server."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "ipac_metadata_recon_ready.sql"
    header = (
        f"-- Calc gate: recon_ready in Unity Catalog\n"
        f"-- Target: {catalog}.{metadata_schema}.recon_ready\n"
        f"-- CT watermarks, recon audit → SQL Server ipac_metadata.dbo\n"
        f"-- process_log stays on UC for heartbeat/restart jobs\n\n"
    )
    out_file.write_text(header + recon_ready_create_sql(catalog, metadata_schema) + "\n", encoding="utf-8")
    return str(out_file)


def write_recon_tables_sql(
    catalog: str,
    metadata_schema: str,
    output_dir: Path | str,
) -> str:
    """Alias for recon_ready-only UC DDL (legacy generator name)."""
    return write_recon_ready_table_sql(catalog, metadata_schema, output_dir)
