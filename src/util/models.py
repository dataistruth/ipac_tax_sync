"""Pydantic models for config/common JSON files."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator


ScdType = Literal[1, 2]


class CommonTable(BaseModel):
    table_nm: str
    lq_key: str = ""
    is_active: bool = True
    select_cols: str = ""
    scd_type: ScdType = 1

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
    cluster_tier: int = Field(default=3, ge=1, le=5)

    def raw_schema(self) -> str:
        """UC schema for raw tables and staging — always {client_nm}_raw."""
        return f"{self.client_nm}_raw"

    def staging_schema(self) -> str:
        """Same as raw_schema; staging volume lives inside the raw schema."""
        return self.raw_schema()

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


class ClusterConfig(BaseModel):
    tiers: dict[str, ClusterTier]


class EffectiveTable(BaseModel):
    table_nm: str
    source: Literal["common", "extra"]
    src_schema: str
    lq_key: str = ""
    select_cols: str
    scd_type: ScdType = 1
    is_active: bool = True

    @property
    def has_cluster_by(self) -> bool:
        return bool(self.lq_key.strip())
