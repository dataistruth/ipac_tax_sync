"""Read/write ipac_metadata.dbo CT watermarks and recon audit rows via mssql-python."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any

from common.ops.recon_store import ReconEventLogWatermark

from common.ops.source_ct_direct import (
    SqlServerDirectConfig,
    fetch_all_as_dict,
    fetch_change_tracking_current_version,
    fetch_one_as_dict,
    fetch_scalar,
    fetch_scalar_value,
    open_sql_server_connection,
    resolve_sql_server_config,
)

DEFAULT_AUDIT_SECRET_SCOPE = "scope_ipacs_audit"
DEFAULT_AUDIT_USERNAME_KEY = "SQL_SERVER_AUDIT_USERNAME"
DEFAULT_AUDIT_PASSWORD_KEY = "SQL_SERVER_AUDIT_PASSWORD"
METADATA_DATABASE = "ipac_metadata"
METADATA_SCHEMA = "dbo"
METADATA_TABLE_PREFIX = f"{METADATA_DATABASE}.{METADATA_SCHEMA}"


@dataclass(frozen=True)
class TableWatermark:
    database_name: str
    schema_name: str
    table_name: str
    last_version: int
    client_nm: str = ""
    pipeline_key: str = ""
    updated_at: datetime | None = None


@dataclass
class CtPendingCounts:
    inserts: int = 0
    updates: int = 0
    deletes: int = 0

    @property
    def total(self) -> int:
        return self.inserts + self.updates + self.deletes

    def metric_for_recon_type(self, recon_type: int) -> int:
        if recon_type == 3:
            return self.inserts + self.updates
        return self.total


@dataclass(frozen=True)
class PendingCtTable:
    schema_name: str
    table_name: str
    watermark_before: int
    ct_head_version: int
    pending: CtPendingCounts
    watermark_updated_at: datetime | None = None
    sql_ct_reference_at: datetime | None = None

    @property
    def version_delta(self) -> int:
        return self.ct_head_version - self.watermark_before


def fetch_sql_row_count(conn: Any, src_schema: str, table_name: str) -> int | None:
    schema = (src_schema or "dbo").replace("'", "''")
    table = table_name.replace("'", "''")
    sql = f"SELECT COUNT_BIG(*) AS cnt FROM {schema}.{table};"
    return fetch_scalar(conn, sql, "cnt")


def fetch_latest_pending_ct_commit_time(
    conn: Any,
    src_schema: str,
    table_name: str,
    watermark_before: int,
    ct_head_version: int | None = None,
) -> datetime | None:
    """Latest SQL commit time for CT changes since watermark_before."""
    schema = (src_schema or "dbo").replace("'", "''")
    table = table_name.replace("'", "''")
    upper = (
        f"AND chg.SYS_CHANGE_VERSION <= {int(ct_head_version)}"
        if ct_head_version is not None
        else ""
    )
    sql = f"""
SELECT MAX(ct.commit_time) AS latest_ct_commit_at
FROM CHANGETABLE(CHANGES {schema}.{table}, {int(watermark_before)}) AS chg
INNER JOIN sys.dm_tran_commit_table AS ct ON ct.commit_ts = chg.SYS_CHANGE_VERSION
WHERE chg.SYS_CHANGE_OPERATION IN ('I', 'U', 'D'){upper};
""".strip()
    value = fetch_scalar_value(conn, sql, "latest_ct_commit_at")
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    except ValueError:
        return None


def _sql_ct_reference_timestamp(
    watermark_updated_at: datetime | None,
    latest_ct_commit_at: datetime | None,
) -> datetime | None:
    """Reference instant on SQL side: latest pending CT commit, else watermark row time."""
    candidates: list[datetime] = []
    for ts in (latest_ct_commit_at, watermark_updated_at):
        if ts is None:
            continue
        if ts.tzinfo is None:
            candidates.append(ts.replace(tzinfo=timezone.utc))
        else:
            candidates.append(ts)
    if not candidates:
        return None
    return max(candidates)


def discover_pending_ct_tables(
    conn: Any,
    client: Any,
    src_schema: str,
    table_names: list[str],
) -> list[PendingCtTable]:
    """List configured tables with CT activity since stored watermarks (pending I/U/D > 0)."""
    database_name = client.src_db_nm
    ct_head = fetch_change_tracking_current_version(conn)
    if ct_head is None:
        return []

    pending_tables: list[PendingCtTable] = []
    for table_nm in table_names:
        if not table_nm:
            continue
        watermark = read_table_watermark(conn, database_name, src_schema, table_nm)
        watermark_before = watermark.last_version if watermark else 0
        watermark_updated_at = watermark.updated_at if watermark else None
        latest_ct_commit_at: datetime | None = None
        try:
            latest_ct_commit_at = fetch_latest_pending_ct_commit_time(
                conn, src_schema, table_nm, watermark_before, ct_head
            )
        except Exception as exc:
            print(
                f"[recon] WARN latest CT commit time failed for {table_nm}: {exc}"
            )
        try:
            pending = fetch_pending_ct_counts(
                conn, src_schema, table_nm, watermark_before, ct_head
            )
        except Exception:
            continue
        if pending.total <= 0:
            continue
        if latest_ct_commit_at is None and watermark_before > 0:
            print(
                f"[recon] WARN {table_nm}: no CT commit_time from CHANGETABLE "
                f"(watermark_before={watermark_before}); using watermark updated_at"
            )
        sql_ct_reference_at = _sql_ct_reference_timestamp(
            watermark_updated_at, latest_ct_commit_at
        )
        pending_tables.append(
            PendingCtTable(
                schema_name=src_schema or "dbo",
                table_name=table_nm,
                watermark_before=watermark_before,
                ct_head_version=ct_head,
                pending=pending,
                watermark_updated_at=watermark_updated_at,
                sql_ct_reference_at=sql_ct_reference_at,
            )
        )
    return pending_tables


def resolve_audit_sql_config(
    client: Any,
    *,
    dbutils: Any | None = None,
    host_override: str = "",
    port_override: int | None = None,
    secret_scope_override: str = "",
) -> SqlServerDirectConfig:
    scope = (
        secret_scope_override
        or getattr(client, "sql_audit_secret_scope", "")
        or DEFAULT_AUDIT_SECRET_SCOPE
    ).strip()
    return resolve_sql_server_config(
        client,
        dbutils=dbutils,
        host_override=host_override,
        port_override=port_override,
        secret_scope_override=scope,
        username_secret_key=getattr(client, "sql_audit_username_secret_key", "")
        or DEFAULT_AUDIT_USERNAME_KEY,
        password_secret_key=getattr(client, "sql_audit_password_secret_key", "")
        or DEFAULT_AUDIT_PASSWORD_KEY,
    )


def open_audit_connection(
    client: Any,
    *,
    dbutils: Any | None = None,
    host_override: str = "",
    port_override: int | None = None,
    secret_scope_override: str = "",
) -> tuple[Any, SqlServerDirectConfig]:
    config = resolve_audit_sql_config(
        client,
        dbutils=dbutils,
        host_override=host_override,
        port_override=port_override,
        secret_scope_override=secret_scope_override,
    )
    return open_sql_server_connection(config), config


def read_table_watermark(
    conn: Any,
    database_name: str,
    schema_name: str,
    table_name: str,
) -> TableWatermark | None:
    db = database_name.replace("'", "''")
    schema = (schema_name or "dbo").replace("'", "''")
    table = table_name.replace("'", "''")
    sql = f"""
SELECT database_name, schema_name, table_name, client_nm, pipeline_key, last_version, updated_at
FROM {METADATA_TABLE_PREFIX}.ct_table_watermark
WHERE database_name = '{db}'
  AND schema_name = '{schema}'
  AND table_name = '{table}';
""".strip()
    row = fetch_one_as_dict(conn, sql)
    if not row:
        return None
    updated_at = row.get("updated_at")
    if isinstance(updated_at, datetime) and updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    return TableWatermark(
        database_name=str(row["database_name"]),
        schema_name=str(row["schema_name"]),
        table_name=str(row["table_name"]),
        last_version=int(row["last_version"]),
        client_nm=str(row.get("client_nm") or ""),
        pipeline_key=str(row.get("pipeline_key") or ""),
        updated_at=updated_at if isinstance(updated_at, datetime) else None,
    )


def upsert_db_watermark(
    conn: Any,
    database_name: str,
    last_version: int,
    *,
    client_nm: str = "",
) -> None:
    db = database_name.replace("'", "''")
    client = client_nm.replace("'", "''")
    sql = f"""
MERGE {METADATA_TABLE_PREFIX}.ct_db_watermark AS target
USING (SELECT '{db}' AS database_name, {int(last_version)} AS last_version, '{client}' AS client_nm) AS source
ON target.database_name = source.database_name
WHEN MATCHED THEN
    UPDATE SET
        last_version = source.last_version,
        client_nm = NULLIF(source.client_nm, ''),
        checked_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (database_name, client_nm, last_version)
    VALUES (source.database_name, NULLIF(source.client_nm, ''), source.last_version);
""".strip()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def upsert_table_watermark(
    conn: Any,
    database_name: str,
    schema_name: str,
    table_name: str,
    last_version: int,
    *,
    client_nm: str = "",
    pipeline_key: str = "",
) -> None:
    db = database_name.replace("'", "''")
    schema = (schema_name or "dbo").replace("'", "''")
    table = table_name.replace("'", "''")
    client = client_nm.replace("'", "''")
    pipeline = pipeline_key.replace("'", "''")
    sql = f"""
MERGE {METADATA_TABLE_PREFIX}.ct_table_watermark AS target
USING (
    SELECT
        '{db}' AS database_name,
        '{schema}' AS schema_name,
        '{table}' AS table_name,
        '{client}' AS client_nm,
        '{pipeline}' AS pipeline_key,
        {int(last_version)} AS last_version
) AS source
ON target.database_name = source.database_name
 AND target.schema_name = source.schema_name
 AND target.table_name = source.table_name
WHEN MATCHED THEN
    UPDATE SET
        last_version = source.last_version,
        client_nm = NULLIF(source.client_nm, ''),
        pipeline_key = NULLIF(source.pipeline_key, ''),
        updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (database_name, schema_name, table_name, client_nm, pipeline_key, last_version)
    VALUES (
        source.database_name,
        source.schema_name,
        source.table_name,
        NULLIF(source.client_nm, ''),
        NULLIF(source.pipeline_key, ''),
        source.last_version
    );
""".strip()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def fetch_pending_ct_counts(
    conn: Any,
    src_schema: str,
    table_name: str,
    version_before: int,
    version_after: int | None,
) -> CtPendingCounts:
    schema = (src_schema or "dbo").replace("'", "''")
    table = table_name.replace("'", "''")
    upper = f"AND ct.SYS_CHANGE_VERSION <= {int(version_after)}" if version_after is not None else ""
    sql = f"""
SELECT ct.SYS_CHANGE_OPERATION AS op, COUNT_BIG(*) AS cnt
FROM CHANGETABLE(CHANGES {schema}.{table}, {int(version_before)}) AS ct
WHERE ct.SYS_CHANGE_OPERATION IN ('I', 'U', 'D'){upper}
GROUP BY ct.SYS_CHANGE_OPERATION;
""".strip()
    counts = CtPendingCounts()
    for row in fetch_all_as_dict(conn, sql):
        op = str(row["op"]).strip().upper()
        cnt = int(row["cnt"])
        if op == "I":
            counts.inserts = cnt
        elif op == "U":
            counts.updates = cnt
        elif op == "D":
            counts.deletes = cnt
    return counts


def run_source_ct_recon_direct(
    conn: Any,
    src_schema: str,
    table_name: str,
    version_before: int,
    version_after: int | None,
    recon_type: int,
    *,
    verbose: bool = False,
) -> tuple[int | None, CtPendingCounts | None]:
    pending = fetch_pending_ct_counts(conn, src_schema, table_name, version_before, version_after)
    metric = pending.metric_for_recon_type(recon_type)
    if verbose:
        print(
            f"[SQL audit CT] table={table_name} versions={version_before}..{version_after} "
            f"pending I/U/D={pending.inserts}/{pending.updates}/{pending.deletes} metric={metric}"
        )
    return metric, pending


def baseline_table_watermark_if_missing(
    conn: Any,
    client: Any,
    src_schema: str,
    table_name: str,
    *,
    pipeline_key: str = "",
) -> TableWatermark:
    database_name = client.src_db_nm
    existing = read_table_watermark(conn, database_name, src_schema, table_name)
    if existing:
        return existing
    head = fetch_change_tracking_current_version(conn)
    if head is None:
        head = 0
    upsert_table_watermark(
        conn,
        database_name,
        src_schema,
        table_name,
        head,
        client_nm=client.client_nm,
        pipeline_key=pipeline_key,
    )
    upsert_db_watermark(conn, database_name, head, client_nm=client.client_nm)
    return TableWatermark(
        database_name=database_name,
        schema_name=src_schema,
        table_name=table_name,
        last_version=head,
        client_nm=client.client_nm,
        pipeline_key=pipeline_key,
    )


def insert_recon_run(
    conn: Any,
    *,
    client_nm: str,
    database_name: str,
    pipeline_id: str,
    update_id: str,
    pipeline_key: str = "",
    ct_head_version: int | None = None,
    run_status: str = "RUNNING",
    run_message: str = "",
) -> str:
    run_id = str(uuid.uuid4())
    client = client_nm.replace("'", "''")
    db = database_name.replace("'", "''")
    pipeline = pipeline_id.replace("'", "''")
    update = update_id.replace("'", "''")
    pkey = pipeline_key.replace("'", "''")
    message = run_message.replace("'", "''")
    head_sql = str(int(ct_head_version)) if ct_head_version is not None else "NULL"
    sql = f"""
INSERT INTO {METADATA_TABLE_PREFIX}.recon_run (
    recon_run_id, client_nm, database_name, pipeline_id, pipeline_key,
    update_id, ct_head_version, run_status, run_message
)
VALUES (
    '{run_id}', '{client}', '{db}', '{pipeline}', NULLIF('{pkey}', ''),
    '{update}', {head_sql}, '{run_status.replace("'", "''")}', NULLIF('{message}', '')
);
""".strip()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    return run_id


def complete_recon_run(
    conn: Any,
    recon_run_id: str,
    *,
    run_status: str,
    run_message: str = "",
) -> None:
    rid = recon_run_id.replace("'", "''")
    message = run_message.replace("'", "''")
    sql = f"""
UPDATE {METADATA_TABLE_PREFIX}.recon_run
SET
    completed_at = SYSUTCDATETIME(),
    run_status = '{run_status.replace("'", "''")}',
    run_message = NULLIF('{message}', '')
WHERE recon_run_id = '{rid}';
""".strip()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def record_recon_table_result(
    conn: Any,
    *,
    recon_run_id: str | None,
    client_nm: str,
    database_name: str,
    schema_name: str,
    table_name: str,
    pipeline_id: str,
    update_id: str,
    flow_name: str,
    recon_type: int,
    watermark_before: int,
    ct_head_version: int,
    pending: CtPendingCounts,
    ingest_upserted: int,
    ingest_deleted: int,
    ingest_change_rows: int,
    sync_status: str,
    recon_message: str,
    watermark_advanced: bool,
) -> None:
    rid_sql = "NULL"
    if recon_run_id:
        rid_sql = "'" + recon_run_id.replace("'", "''") + "'"
    sql = f"""
INSERT INTO {METADATA_TABLE_PREFIX}.recon_table_result (
    recon_run_id,
    client_nm,
    database_name,
    schema_name,
    table_name,
    pipeline_id,
    update_id,
    flow_name,
    recon_type,
    watermark_before,
    ct_head_version,
    pending_inserts,
    pending_updates,
    pending_deletes,
    pending_total,
    ingest_upserted,
    ingest_deleted,
    ingest_change_rows,
    sync_status,
    recon_message,
    watermark_advanced
)
VALUES (
    {rid_sql},
    '{client_nm.replace("'", "''")}',
    '{database_name.replace("'", "''")}',
    '{(schema_name or "dbo").replace("'", "''")}',
    '{table_name.replace("'", "''")}',
    '{pipeline_id.replace("'", "''")}',
    '{update_id.replace("'", "''")}',
    '{flow_name.replace("'", "''")}',
    {int(recon_type)},
    {int(watermark_before)},
    {int(ct_head_version)},
    {int(pending.inserts)},
    {int(pending.updates)},
    {int(pending.deletes)},
    {int(pending.total)},
    {int(ingest_upserted)},
    {int(ingest_deleted)},
    {int(ingest_change_rows)},
    '{sync_status.replace("'", "''")}',
    '{recon_message.replace("'", "''")}',
    {1 if watermark_advanced else 0}
);
""".strip()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _sql_datetime_literal(value: datetime | None) -> str:
    if value is None:
        return "NULL"
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"


def read_recon_event_log_watermarks_sql(
    conn: Any,
    pipeline_ids: list[str] | None = None,
) -> dict[str, ReconEventLogWatermark]:
    sql = f"""
SELECT
    pipeline_id,
    pipeline_key,
    last_event_ts,
    last_event_id,
    last_update_id,
    last_api_update_state,
    last_poll_at
FROM {METADATA_TABLE_PREFIX}.recon_event_log_watermark
""".strip()
    if pipeline_ids:
        ids = [pid.replace("'", "''") for pid in pipeline_ids if pid]
        if ids:
            in_list = ", ".join(f"'{pid}'" for pid in ids)
            sql += f"\nWHERE pipeline_id IN ({in_list})"

    out: dict[str, ReconEventLogWatermark] = {}
    try:
        rows = fetch_all_as_dict(conn, sql)
    except Exception:
        return out

    for row in rows:
        pid = str(row.get("pipeline_id") or "").strip()
        if not pid:
            continue
        out[pid] = ReconEventLogWatermark(
            pipeline_id=pid,
            pipeline_key=str(row.get("pipeline_key") or "").strip(),
            last_event_ts=row.get("last_event_ts"),
            last_event_id=str(row.get("last_event_id") or "").strip(),
            last_update_id=str(row.get("last_update_id") or "").strip(),
            last_api_update_state=str(row.get("last_api_update_state") or "").strip().upper(),
            last_poll_at=row.get("last_poll_at"),
        )
    return out


def upsert_recon_event_log_watermark_sql(conn: Any, watermark: ReconEventLogWatermark) -> None:
    pid = watermark.pipeline_id.replace("'", "''")
    pkey = watermark.pipeline_key.replace("'", "''")
    event_id = watermark.last_event_id.replace("'", "''")
    update_id = watermark.last_update_id.replace("'", "''")
    api_state = watermark.last_api_update_state.replace("'", "''")
    sql = f"""
MERGE {METADATA_TABLE_PREFIX}.recon_event_log_watermark AS target
USING (
    SELECT
        '{pid}' AS pipeline_id,
        NULLIF('{pkey}', '') AS pipeline_key,
        {_sql_datetime_literal(watermark.last_event_ts)} AS last_event_ts,
        NULLIF('{event_id}', '') AS last_event_id,
        NULLIF('{update_id}', '') AS last_update_id,
        NULLIF('{api_state}', '') AS last_api_update_state,
        {_sql_datetime_literal(watermark.last_poll_at)} AS last_poll_at
) AS source
ON target.pipeline_id = source.pipeline_id
WHEN MATCHED THEN
    UPDATE SET
        pipeline_key = source.pipeline_key,
        last_event_ts = source.last_event_ts,
        last_event_id = source.last_event_id,
        last_update_id = source.last_update_id,
        last_api_update_state = source.last_api_update_state,
        last_poll_at = source.last_poll_at
WHEN NOT MATCHED THEN
    INSERT (
        pipeline_id,
        pipeline_key,
        last_event_ts,
        last_event_id,
        last_update_id,
        last_api_update_state,
        last_poll_at
    )
    VALUES (
        source.pipeline_id,
        source.pipeline_key,
        source.last_event_ts,
        source.last_event_id,
        source.last_update_id,
        source.last_api_update_state,
        source.last_poll_at
    );
""".strip()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def flush_recon_event_log_watermarks_sql(
    conn: Any,
    watermarks: dict[str, ReconEventLogWatermark],
) -> int:
    """Upsert in-memory watermark dict to SQL Server (no Spark)."""
    written = 0
    for watermark in watermarks.values():
        upsert_recon_event_log_watermark_sql(conn, watermark)
        written += 1
    return written


def write_audit_log(
    conn: Any,
    event_type: str,
    *,
    client_nm: str = "",
    database_name: str = "",
    object_name: str = "",
    pipeline_id: str = "",
    update_id: str = "",
    detail: dict[str, Any] | None = None,
) -> None:
    payload = json.dumps(detail or {}, default=str).replace("'", "''")
    sql = f"""
INSERT INTO {METADATA_TABLE_PREFIX}.ingestion_audit_log (
    event_type, client_nm, database_name, object_name, pipeline_id, update_id, detail_json
)
VALUES (
    '{event_type.replace("'", "''")}',
    NULLIF('{client_nm.replace("'", "''")}', ''),
    NULLIF('{database_name.replace("'", "''")}', ''),
    NULLIF('{object_name.replace("'", "''")}', ''),
    NULLIF('{pipeline_id.replace("'", "''")}', ''),
    NULLIF('{update_id.replace("'", "''")}', ''),
    NULLIF('{payload}', '')
);
""".strip()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def resolve_source_ct_for_recon(
    conn: Any,
    client: Any,
    src_schema: str,
    table_name: str,
    recon_type: int,
    *,
    pipeline_key: str = "",
    verbose: bool = False,
) -> tuple[int | None, CtPendingCounts | None, int, int]:
    """
    Read watermark from ipac_metadata.dbo, count pending CT to current head.
    Returns (source_metric, pending_counts, watermark_before, ct_head).
    """
    watermark = baseline_table_watermark_if_missing(
        conn,
        client,
        src_schema,
        table_name,
        pipeline_key=pipeline_key,
    )
    ct_head = fetch_change_tracking_current_version(conn)
    if ct_head is None:
        return None, None, watermark.last_version, watermark.last_version

    metric, pending = run_source_ct_recon_direct(
        conn,
        src_schema,
        table_name,
        watermark.last_version,
        ct_head,
        recon_type,
        verbose=verbose,
    )
    return metric, pending, watermark.last_version, ct_head
