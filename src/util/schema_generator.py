"""Generate Unity Catalog schema resources for bundle deployment."""

from __future__ import annotations

from pathlib import Path

from util.models import ClientEntry


def schema_resource_key(schema_name: str) -> str:
    key = schema_name.lower().replace("-", "_")
    return f"schema_{key}"


def _schema_resource_key(schema_name: str) -> str:
    return schema_resource_key(schema_name)


def generate_schema_resource_yaml(
    schema_name: str,
    uc_catalog_ref: str,
    comment: str,
) -> str:
    key = _schema_resource_key(schema_name)
    lines = [
        "resources:",
        "  schemas:",
        f"    {key}:",
        f"      name: {schema_name}",
        f"      catalog_name: {uc_catalog_ref}",
        f"      comment: \"{comment}\"",
    ]
    return "\n".join(lines) + "\n"


def write_client_schema_resource_yaml(
    client: ClientEntry,
    dest_schema_suffix: str,
    output_dir: Path | str,
    uc_catalog_ref: str,
) -> str:
    schema_name = client.raw_schema(dest_schema_suffix)
    comment = f"iPAC delta sync schema for client {client.client_nm} (raw + staging)"
    content = generate_schema_resource_yaml(schema_name, uc_catalog_ref, comment)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{client.client_nm}_schema.yml"
    out_file.write_text(content, encoding="utf-8")
    return str(out_file)


def write_metadata_schema_resource_yaml(
    metadata_schema: str,
    output_dir: Path | str,
    uc_catalog_ref: str,
) -> str:
    comment = "iPAC metadata schema for process logs and pipeline operations"
    content = generate_schema_resource_yaml(metadata_schema, uc_catalog_ref, comment)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "ipac_metadata_schema.yml"
    out_file.write_text(content, encoding="utf-8")
    return str(out_file)
