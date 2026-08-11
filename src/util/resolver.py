"""Resolve effective tables from common catalog + per-client overrides."""

from __future__ import annotations

from util.models import (
    ClientEntry,
    ClientOverrides,
    ClientTableOverride,
    CommonTable,
    CommonTablesCatalog,
    EffectiveTable,
)


def _common_by_name(catalog: CommonTablesCatalog) -> dict[str, CommonTable]:
    return {t.table_nm: t for t in catalog.tables}


def _from_common(table: CommonTable, client: ClientEntry) -> EffectiveTable:
    return EffectiveTable(
        table_nm=table.table_nm,
        source="common",
        src_schema=client.src_db_schema,
        lq_key=table.lq_key,
        select_cols=table.resolved_select_cols,
        scd_type=table.scd_type,
        recon_type=table.recon_type,
        is_active=table.is_active,
    )


def _from_extra(table: ClientTableOverride, client: ClientEntry) -> EffectiveTable:
    return EffectiveTable(
        table_nm=table.table_nm,
        source="extra",
        src_schema=client.src_db_schema,
        lq_key=table.lq_key,
        select_cols=table.resolved_select_cols,
        scd_type=table.scd_type,
        recon_type=table.recon_type,
        is_active=table.is_active,
    )


def resolve_effective_tables(
    client: ClientEntry,
    catalog: CommonTablesCatalog,
    overrides: ClientOverrides | None = None,
) -> list[EffectiveTable]:
    """Merge common tables with client_overrides ignore/extra lists."""
    common_map = _common_by_name(catalog)
    effective: dict[str, EffectiveTable] = {}

    use_common = overrides is None or overrides.include_common

    ignored = set()
    if overrides:
        for item in overrides.ignore:
            if not item.is_active:
                ignored.add(item.table_nm)

    for table_nm, common_table in common_map.items():
        if not use_common:
            continue
        if table_nm in ignored:
            continue
        if not common_table.is_active:
            continue
        effective[table_nm] = _from_common(common_table, client)

    if overrides:
        for extra in overrides.extra:
            if not extra.is_active:
                effective.pop(extra.table_nm, None)
                continue
            if extra.table_nm in common_map and extra.table_nm not in ignored:
                raise ValueError(
                    f"extra table '{extra.table_nm}' collides with common catalog; "
                    "use ignore with is_active false instead"
                )
            effective[extra.table_nm] = _from_extra(extra, client)

    if not effective:
        raise ValueError(f"No effective tables for client '{client.client_nm}'")

    return sorted(effective.values(), key=lambda t: t.table_nm)
