"""Read/write master.ipac_metadata CT watermarks and recon audit rows via pymssql."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

from common.ops.source_ct_direct import (
    SqlServerDirectConfig,
    fetch_change_tracking_current_version,
    open_sql_server_connection,
    resolve_sql_server_config,
)

DEFAULT_AUDIT_SECRET_SCOPE = "scope_ipacs_audit"
DEFAULT_AUDIT_USERNAME_KEY = "SQL_SERVER_AUDIT_USERNAME"
DEFAULT_AUDIT_PASSWORD_KEY = "SQL_SERVER_AUDIT_PASSWORD"
METADATA_SCHEMA = "ipac_metadata"


@dataclass(frozen=True)
class TableWatermark:
    database_name: str
    schema_name: str
    table_name: str
    last_version: int
    client_nm: str = ""
    pipeline_key: str = ""


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
SELECT database_name, schema_name, table_name, client_nm, pipeline_key, last_version
FROM master.{METADATA_SCHEMA}.ct_table_watermark
WHERE database_name = '{db}'
  AND schema_name = '{schema}'
  AND table_name = '{table}';
""".strip()
    with conn.cursor(as_dict=True) as cur:
        cur.execute(sql)
        row = cur.fetchone()
    if not row:
        return None
    return TableWatermark(
        database_name=str(row["database_name"]),
        schema_name=str(row["schema_name"]),
        table_name=str(row["table_name"]),
        last_version=int(row["last_version"]),
        client_nm=str(row.get("client_nm") or ""),
        pipeline_key=str(row.get("pipeline_key") or ""),
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
MERGE master.{METADATA_SCHEMA}.ct_db_watermark AS target
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
MERGE master.{METADATA_SCHEMA}.ct_table_watermark AS target
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
    with conn.cursor(as_dict=True) as cur:
        cur.execute(sql)
        for row in cur.fetchall():
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
INSERT INTO master.{METADATA_SCHEMA}.recon_run (
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
UPDATE master.{METADATA_SCHEMA}.recon_run
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
INSERT INTO master.{METADATA_SCHEMA}.recon_table_result (
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
INSERT INTO master.{METADATA_SCHEMA}.ingestion_audit_log (
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
    Read watermark from master.ipac_metadata, count pending CT to current head.
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
