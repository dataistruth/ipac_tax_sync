"""Unity Catalog schema resource helpers for bundle pipeline YAML."""

from __future__ import annotations


def schema_resource_key(schema_name: str) -> str:
    key = schema_name.lower().replace("-", "_")
    return f"schema_{key}"


def format_schema_resource_lines(
    schema_name: str,
    uc_catalog_ref: str,
    comment: str,
) -> list[str]:
    key = schema_resource_key(schema_name)
    return [
        f"    {key}:",
        f"      name: {schema_name}",
        f"      catalog_name: {uc_catalog_ref}",
        f"      comment: \"{comment}\"",
    ]


def generate_schema_resource_yaml(
    schema_name: str,
    uc_catalog_ref: str,
    comment: str,
) -> str:
    lines = [
        "resources:",
        "  schemas:",
    ]
    lines.extend(format_schema_resource_lines(schema_name, uc_catalog_ref, comment))
    return "\n".join(lines) + "\n"
