"""Load and validate JSON configs from config/common."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import ValidationError

from util.models import (
    ClientEntry,
    ClientOverrides,
    ClientRegistry,
    ClusterConfig,
    CommonTablesCatalog,
)
from util.paths import client_overrides_dir, common_config_dir


def _read_json(path: Path):
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def load_client_registry(path: Path | None = None) -> ClientRegistry:
    file_path = path or (common_config_dir() / "client.json")
    if not file_path.exists():
        raise FileNotFoundError(f"client.json not found: {file_path}")
    raw = _read_json(file_path)
    return ClientRegistry(clients=raw)


def load_common_tables(path: Path | None = None) -> CommonTablesCatalog:
    file_path = path or (common_config_dir() / "common_tables.json")
    if not file_path.exists():
        raise FileNotFoundError(f"common_tables.json not found: {file_path}")
    return CommonTablesCatalog.model_validate(_read_json(file_path))


def load_cluster_config(path: Path | None = None) -> ClusterConfig:
    file_path = path or (common_config_dir() / "cluster_config.json")
    if not file_path.exists():
        raise FileNotFoundError(f"cluster_config.json not found: {file_path}")
    return ClusterConfig.model_validate(_read_json(file_path))


def client_override_path(client_nm: str) -> Path:
    return client_overrides_dir() / f"{client_nm}.json"


def load_client_overrides(client_nm: str, path: Path | None = None) -> ClientOverrides | None:
    file_path = path or client_override_path(client_nm)
    if not file_path.exists():
        return None
    overrides = ClientOverrides.model_validate(_read_json(file_path))
    if overrides.client_nm != client_nm:
        raise ValueError(
            f"client_nm mismatch: file '{client_nm}' vs JSON client_nm '{overrides.client_nm}'"
        )
    return overrides


def get_client(client_nm: str, registry: ClientRegistry | None = None) -> ClientEntry:
    reg = registry or load_client_registry()
    for client in reg.clients:
        if client.client_nm == client_nm:
            return client
    raise ValueError(f"Client not found in client.json: {client_nm}")


def list_active_clients(registry: ClientRegistry | None = None) -> list[ClientEntry]:
    reg = registry or load_client_registry()
    return [c for c in reg.clients if c.is_active]


def list_client_names(active_only: bool = False) -> list[str]:
    reg = load_client_registry()
    clients = list_active_clients(reg) if active_only else reg.clients
    return [c.client_nm for c in clients]


def validate_all(client_nm: str | None = None) -> list[ClientEntry]:
    """Validate registry, catalog, overrides, and effective table resolution."""
    from util.resolver import resolve_effective_tables

    registry = load_client_registry()
    catalog = load_common_tables()
    cluster_cfg = load_cluster_config()

    targets = (
        [get_client(client_nm, registry)]
        if client_nm
        else [c for c in registry.clients if c.is_active]
    )

    for client in targets:
        tier_key = str(client.cluster_tier)
        if tier_key not in cluster_cfg.tiers:
            raise ValueError(f"cluster_tier {client.cluster_tier} not defined in cluster_config.json")
        resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))

    return targets


def format_validation_error(exc: ValidationError) -> str:
    lines = ["Validation failed:"]
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"])
        lines.append(f"  - {loc}: {err['msg']}")
    return "\n".join(lines)
