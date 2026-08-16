"""Collect Databricks bundle schema resource selectors for deploy."""

from __future__ import annotations

from pathlib import Path


def schema_resource_keys_from_yaml(path: Path) -> list[str]:
    """Parse `resources.schemas.<key>:` entries from a bundle YAML fragment."""
    keys: list[str] = []
    in_schemas = False
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped == "schemas:":
            in_schemas = True
            continue
        if not in_schemas:
            continue
        if line.startswith("  ") and not line.startswith("    "):
            break
        if line.startswith("    ") and not line.startswith("      ") and stripped.endswith(":"):
            keys.append(stripped[:-1])
    return keys


def collect_schema_deploy_selectors(
    generated_schema_dir: Path,
    static_schema_dir: Path,
) -> list[str]:
    """Build `schemas.<key>` selectors for `databricks bundle deploy --select`."""
    keys: list[str] = []
    for directory in (static_schema_dir, generated_schema_dir):
        if not directory.is_dir():
            continue
        for yml in sorted(directory.glob("*.yml")):
            keys.extend(schema_resource_keys_from_yaml(yml))
    # stable unique order
    seen: set[str] = set()
    selectors: list[str] = []
    for key in keys:
        if key in seen:
            continue
        seen.add(key)
        selectors.append(f"schemas.{key}")
    return selectors
