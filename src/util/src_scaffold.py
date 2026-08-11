"""Create src/common/per-client folders and clean generated artifacts."""

from __future__ import annotations

import shutil

from util.models import ClientEntry
from util.paths import (
    client_pipelines_dir,
    client_sql_dir,
    client_transform_dir,
    generated_bundle_dir,
    generated_schema_dir,
    src_common_dir,
    src_dir,
)

COMMON_INIT = '''"""Shared code for ipac_delta_sync client pipelines and transforms."""
'''

TRANSFORM_PLACEHOLDER = '''"""Client-specific transform notebooks and modules.

Add silver/gold transforms for this client here.
"""
'''

PIPELINES_PLACEHOLDER = '''"""Lakeflow Connect pipeline definitions for this client.

Pipeline YAML is generated here by: ipac-delta-sync generate --client <client_nm>
"""
'''


def scaffold_src_tree(clients: list[ClientEntry], force_placeholders: bool = False) -> list[str]:
    """Ensure src/common and src/<client_nm>/{pipelines,transform,sql} exist."""
    created: list[str] = []

    common = src_common_dir()
    common.mkdir(parents=True, exist_ok=True)
    common_init = common / "__init__.py"
    if not common_init.exists() or force_placeholders:
        common_init.write_text(COMMON_INIT, encoding="utf-8")
        created.append(str(common_init))

    for client in clients:
        if not client.is_active:
            continue

        pipelines = client_pipelines_dir(client.client_nm)
        transform = client_transform_dir(client.client_nm)
        sql = client_sql_dir(client.client_nm)
        pipelines.mkdir(parents=True, exist_ok=True)
        transform.mkdir(parents=True, exist_ok=True)
        sql.mkdir(parents=True, exist_ok=True)

        pipelines_init = pipelines / "__init__.py"
        transform_init = transform / "__init__.py"
        if not pipelines_init.exists() or force_placeholders:
            pipelines_init.write_text(PIPELINES_PLACEHOLDER, encoding="utf-8")
            created.append(str(pipelines_init))
        if not transform_init.exists() or force_placeholders:
            transform_init.write_text(TRANSFORM_PLACEHOLDER, encoding="utf-8")
            created.append(str(transform_init))

    return created


def remove_stale_client_dirs(active_names: set[str]) -> list[str]:
    """Remove src/<client_nm> folders not in active client list."""
    removed: list[str] = []
    if not src_dir().exists():
        return removed
    for child in src_dir().iterdir():
        if not child.is_dir() or child.name in ("common", "util"):
            continue
        if child.name in active_names:
            continue
        shutil.rmtree(child)
        removed.append(str(child))
    return removed


def remove_generated_pipeline_artifacts() -> list[str]:
    """Delete previously generated YAML under generated/ and src/*/pipelines."""
    removed: list[str] = []

    bundle_dir = generated_bundle_dir()
    if bundle_dir.exists():
        for path in bundle_dir.glob("*.yml"):
            path.unlink()
            removed.append(str(path))

    schema_dir = generated_schema_dir()
    if schema_dir.exists():
        for path in schema_dir.glob("*.yml"):
            path.unlink()
            removed.append(str(path))

    root = src_dir()
    if root.exists():
        for child in root.iterdir():
            if not child.is_dir() or child.name in ("common", "util"):
                continue
            pipelines_dir = child / "pipelines"
            if not pipelines_dir.exists():
                continue
            for path in pipelines_dir.glob("*.yml"):
                path.unlink()
                removed.append(str(path))

    return removed
