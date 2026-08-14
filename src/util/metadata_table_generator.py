"""Generate ipac_metadata Delta table DDL."""

from __future__ import annotations

from pathlib import Path

from common.ops.process_log_store import process_log_create_sql


def write_process_log_table_sql(
    catalog: str,
    metadata_schema: str,
    output_dir: Path | str,
) -> str:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "ipac_metadata_process_log.sql"
    header = (
        f"-- iPAC operational process_log (ingest, calc, transfer, ...)\n"
        f"-- Target: {catalog}.{metadata_schema}.process_log\n"
        f"-- Created by generate; heartbeat monitor also creates if missing.\n\n"
    )
    out_file.write_text(header + process_log_create_sql(catalog, metadata_schema) + "\n", encoding="utf-8")
    return str(out_file)
