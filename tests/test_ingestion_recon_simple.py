"""Tests for simplified CT-driven recon helpers."""

from unittest.mock import MagicMock

from common.ops.ingestion_recon_ops import count_delta_table_rows, evaluate_simple_recon
from common.ops.lakeflow_event_ops import FlowSummaryRow
from common.ops.sql_server_audit_store import CtPendingCounts
from datetime import datetime, timezone


def _summary(upserted: int = 1, deleted: int = 0) -> FlowSummaryRow:
    now = datetime.now(timezone.utc)
    return FlowSummaryRow(
        pipeline_id="pid",
        pipeline_name="p_test",
        update_id="upd",
        flow_name="dbo.Table_snapshot_flow",
        table_name="Table",
        client_nm="client",
        destination_schema="client_poc1",
        destination_table="Table",
        recon_type=1,
        final_flow_status="COMPLETED",
        total_output_rows=upserted,
        total_upserted=upserted,
        total_deleted=deleted,
        total_change_rows=upserted + deleted,
        total_output_bytes=0,
        first_event_time=now,
        last_event_time=now,
    )


def test_evaluate_simple_flow_complete():
    status, msg = evaluate_simple_recon(
        _summary(),
        CtPendingCounts(inserts=5),
        recon_type=1,
        sql_row_count=100,
        delta_row_count=99,
        pass_rule="auto",
    )
    assert status == "PASS"
    assert "COMPLETED" in msg


def test_evaluate_simple_row_count_match():
    status, msg = evaluate_simple_recon(
        None,
        CtPendingCounts(inserts=1),
        recon_type=1,
        sql_row_count=100,
        delta_row_count=100,
        pass_rule="row_count",
    )
    assert status == "PASS"
    assert "row_count match" in msg


def test_evaluate_simple_waiting():
    status, msg = evaluate_simple_recon(
        None,
        CtPendingCounts(inserts=3),
        recon_type=1,
        None,
        None,
        pass_rule="flow_complete",
    )
    assert status == "WAITING"
    assert "CT pending" in msg


def test_count_delta_table_rows_uses_describe_detail():
    spark = MagicMock()
    spark.sql.return_value.select.return_value.first.return_value = {"numRecords": 42000}
    assert count_delta_table_rows(spark, "cat", "schema", "Table") == 42000
    spark.sql.assert_called_once()
    assert "DESCRIBE DETAIL" in spark.sql.call_args[0][0]
