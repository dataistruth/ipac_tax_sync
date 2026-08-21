"""Tests for SQL Server ipac_metadata audit store helpers."""

from common.ops.sql_server_audit_store import (
    CtPendingCounts,
    fetch_pending_ct_counts,
    upsert_table_watermark,
)


class _Cursor:
    def __init__(self, rows=None) -> None:
        self.rows = rows or []
        self.executed: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql: str):
        self.executed.append(sql)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class _Conn:
    def __init__(self) -> None:
        self.commits = 0
        self.last_cursor: _Cursor | None = None

    def cursor(self, as_dict=False):
        self.last_cursor = _Cursor()
        return self.last_cursor

    def commit(self):
        self.commits += 1


def test_ct_pending_counts_metric_recon_type():
    pending = CtPendingCounts(inserts=3, updates=2, deletes=1)
    assert pending.total == 6
    assert pending.metric_for_recon_type(2) == 6
    assert pending.metric_for_recon_type(3) == 5


def test_upsert_table_watermark_executes_merge():
    conn = _Conn()
    upsert_table_watermark(
        conn,
        "iPC_2025_DEV7_15447",
        "dbo",
        "Entity",
        65229,
        client_nm="iPC_2025_Dev7_15447",
        pipeline_key="p_iPC_2025_Dev7_15447_1",
    )
    assert conn.commits == 1
    assert conn.last_cursor is not None
    assert "MERGE master.ipac_metadata.ct_table_watermark" in conn.last_cursor.executed[0]
