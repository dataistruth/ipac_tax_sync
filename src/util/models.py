"""Pydantic models for config/common JSON files."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator


ScdType = Literal[1, 2]
ReconType = Literal[1, 2, 3]
ClientSize = Literal["small", "medium", "large"]
ClusterTierName = Literal["s1", "s2", "s3", "j1", "j2", "j3"]

RECON_TYPE_ROW_COUNT = 2


def recon_type_for_table_name(table_nm: str, recon_type: ReconType) -> ReconType:
    """Snapshot tables use recon_type 2 (row count vs CT); others keep configured type."""
    if "snapshot" in table_nm.casefold():
        return RECON_TYPE_ROW_COUNT
    return recon_type


class CommonTable(BaseModel):
    table_nm: str
    lq_key: str = ""
    is_active: bool = True
    select_cols: str = ""
    scd_type: ScdType = 1
    recon_type: ReconType = 1

    @field_validator("recon_type", mode="before")
    @classmethod
    def _apply_snapshot_recon_type(cls, value: int, info) -> int:
        table_nm = info.data.get("table_nm") or ""
        return recon_type_for_table_name(str(table_nm), int(value))

    @property
    def resolved_select_cols(self) -> str:
        return self.select_cols.strip() if self.select_cols.strip() else "*"

    @property
    def has_cluster_by(self) -> bool:
        return bool(self.lq_key.strip())


class CommonTablesCatalog(BaseModel):
    tables: list[CommonTable]


class ClientEntry(BaseModel):
    """One row in config/common/client.json — one continuous Lakeflow pipeline when active."""

    client_nm: str
    desc: str = ""
    volume: str = ""
    priority: int = 100
    is_active: bool = True
    src_db_nm: str
    src_db_schema: str = "dbo"
    uc_conn_nm: str
    client_size: ClientSize = "medium"
    cluster_tier: ClusterTierName = "j2"
    sql_host: str = ""
    sql_port: int = 1433
    sql_host_secret_key: str = "SQL_SERVER_HOST"
    sql_audit_secret_scope: str = "scope_ipacs_audit"
    sql_audit_username_secret_key: str = "SQL_SERVER_AUDIT_USERNAME"
    sql_audit_password_secret_key: str = "SQL_SERVER_AUDIT_PASSWORD"

    def raw_schema(self, suffix: str = "") -> str:
        """UC schema for raw tables and staging — {client_nm}{suffix}."""
        return f"{self.client_nm}{suffix}"

    def staging_schema(self, suffix: str = "") -> str:
        """Same as raw_schema; staging volume lives inside the raw schema."""
        return self.raw_schema(suffix)

    def resolved_volume_name(self) -> str | None:
        """Optional explicit volume in raw schema; empty → platform creates/uses default in raw."""
        name = self.volume.strip()
        return name or None


class ClientRegistry(BaseModel):
    clients: list[ClientEntry]

    @field_validator("clients", mode="before")
    @classmethod
    def _accept_root_array(cls, value: list | dict) -> list:
        if isinstance(value, list):
            return value
        if isinstance(value, dict) and "clients" in value:
            return value["clients"]
        raise ValueError("client.json must be a JSON array or object with 'clients' key")


class TableIgnore(BaseModel):
    table_nm: str
    is_active: bool = False


class ClientTableOverride(BaseModel):
    table_nm: str
    lq_key: str = ""
    is_active: bool = True
    select_cols: str = ""
    scd_type: ScdType = 1
    recon_type: ReconType = 1

    @field_validator("recon_type", mode="before")
    @classmethod
    def _apply_snapshot_recon_type(cls, value: int, info) -> int:
        table_nm = info.data.get("table_nm") or ""
        return recon_type_for_table_name(str(table_nm), int(value))

    @property
    def resolved_select_cols(self) -> str:
        return self.select_cols.strip() if self.select_cols.strip() else "*"

    @property
    def has_cluster_by(self) -> bool:
        return bool(self.lq_key.strip())


class ClientOverrides(BaseModel):
    client_nm: str
    include_common: bool = True
    ignore: list[TableIgnore] = Field(default_factory=list)
    extra: list[ClientTableOverride] = Field(default_factory=list)


class ClusterTier(BaseModel):
    label: str = ""
    description: str = ""
    serverless: bool = True
    min_workers: int = 1
    max_workers: int = 2
    node_type_id: str = ""
    local_cores: int = 0
    driver_node_type_id: str = ""
    policy_id: str = ""


class ClusterConfig(BaseModel):
    tiers: dict[str, ClusterTier]


class EffectiveTable(BaseModel):
    table_nm: str
    source: Literal["common", "extra"]
    src_schema: str
    lq_key: str = ""
    select_cols: str
    scd_type: ScdType = 1
    recon_type: ReconType = 1
    is_active: bool = True

    @property
    def has_cluster_by(self) -> bool:
        return bool(self.lq_key.strip())
