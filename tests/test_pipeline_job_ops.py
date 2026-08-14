"""Tests for pipeline heartbeat monitor matching logic."""

from common.ops.pipeline_job_ops import (
    _filter_pipelines,
    _list_generated_pipelines,
    _pipeline_health,
    _strip_bundle_dev_prefix,
    describe_pipeline_status,
    run_monitor_loop,
)


class _FakeClient:
    def __init__(self, statuses: list[dict]) -> None:
        self._statuses = statuses

    def get(self, path: str) -> dict:
        if path.startswith("/api/2.0/pipelines"):
            return {"statuses": self._statuses}
        raise AssertionError(path)


def test_strip_bundle_dev_prefix():
    assert _strip_bundle_dev_prefix("[dev ipacs_dev_oauth_spn] p_client_1") == "p_client_1"
    assert _strip_bundle_dev_prefix("[dev user]p_client_1") == "p_client_1"
    assert _strip_bundle_dev_prefix("p_client_1") == "p_client_1"


def test_list_generated_pipelines_paginates():
    class _PagingClient:
        def get(self, path: str) -> dict:
            if "page_token=page2" in path:
                return {
                    "statuses": [
                        {"name": "p_page_2", "pipeline_id": "2"},
                    ],
                }
            if path.startswith("/api/2.0/pipelines"):
                return {
                    "statuses": [
                        {"name": "other", "pipeline_id": "0"},
                        {"name": "p_page_1", "pipeline_id": "1"},
                    ],
                    "next_page_token": "page2",
                }
            raise AssertionError(path)

    found = _list_generated_pipelines(_PagingClient(), "p_")
    assert {p["pipeline_id"] for p in found} == {"1", "2"}


def test_run_monitor_raises_when_configured_names_do_not_match(tmp_path):
    from common.ops import pipeline_job_ops

    client = _FakeClient(
        [
            {"name": "p_iPC_2025_Dev7_15347_1", "pipeline_id": "1"},
            {"name": "p_iPC_2025_Dev7_15347_2", "pipeline_id": "2"},
        ]
    )
    original = pipeline_job_ops.DatabricksRestClient
    pipeline_job_ops.DatabricksRestClient = lambda *a, **k: client  # type: ignore[misc]
    registry = tmp_path / "pipeline_names.json"
    registry.write_text(
        '{"pipelines": ["p_iPC_2025_Dev7_15447_1", "p_iPC_2025_Dev7_15447_2"]}',
        encoding="utf-8",
    )
    try:
        import pytest

        with pytest.raises(RuntimeError, match="No API pipelines matched pipeline_names.json"):
            pipeline_job_ops.run_monitor("p_", 900, str(registry))
    finally:
        pipeline_job_ops.DatabricksRestClient = original


def test_list_generated_pipelines_ignores_dev_prefix_and_case():
    client = _FakeClient(
        [
            {"name": "[dev user] p_iPC_2025_Dev7_15447_1", "pipeline_id": "1"},
            {"name": "other_pipeline", "pipeline_id": "2"},
        ]
    )
    found = _list_generated_pipelines(client, "P_")
    assert len(found) == 1
    assert found[0]["pipeline_id"] == "1"


def test_filter_pipelines_matches_logical_name():
    pipelines = [
        {"name": "[dev user] p_iPC_2025_Dev7_15447_1", "pipeline_id": "1"},
        {"name": "[dev user] p_iPC_2025_Dev7_15447_2", "pipeline_id": "2"},
        {"name": "[dev user] p_other_1", "pipeline_id": "3"},
    ]
    configured = {"p_iPC_2025_Dev7_15447_1", "p_iPC_2025_Dev7_15447_2"}
    filtered = _filter_pipelines(pipelines, configured)
    assert len(filtered) == 2
    assert {p["pipeline_id"] for p in filtered} == {"1", "2"}


class _DetailClient:
    def __init__(self, details: dict[str, dict]) -> None:
        self._details = details

    def get(self, path: str) -> dict:
        if path.startswith("/api/2.0/pipelines?"):
            return {
                "statuses": [
                    {"name": "p_test_1", "pipeline_id": "pid-1"},
                ]
            }
        if path.startswith("/api/2.0/pipelines/"):
            pid = path.rsplit("/", 1)[-1]
            return self._details.get(pid, {})
        raise AssertionError(path)


def test_describe_pipeline_status_includes_update_state():
    detail = {
        "spec": {"continuous": True},
        "state": {
            "latest_update": {
                "state": "RUNNING",
                "update_id": "abc",
                "start_time": 1_700_000_000_000,
            }
        },
    }
    text = describe_pipeline_status(detail)
    assert "continuous=True" in text
    assert "update_state=RUNNING" in text
    assert "update_id=abc" in text


def test_pipeline_health_detects_stale_heartbeat():
    import time

    old_ms = int((time.time() - 2000) * 1000)
    client = _DetailClient(
        {
            "pid-1": {
                "spec": {"continuous": True},
                "state": {"latest_update": {"state": "RUNNING", "start_time": old_ms}},
            }
        }
    )
    ok, reason = _pipeline_health(client, "pid-1", heartbeat_interval_sec=900)
    assert not ok
    assert "UNHEALTHY" in reason
    assert "stale heartbeat" in reason


def test_run_monitor_loop_single_iteration(tmp_path):
    import time

    now_ms = int(time.time() * 1000)
    client = _DetailClient(
        {
            "pid-1": {
                "spec": {"continuous": True},
                "state": {"latest_update": {"state": "RUNNING", "start_time": now_ms}},
            }
        }
    )
    # Patch DatabricksRestClient in run_monitor path
    from common.ops import pipeline_job_ops

    original = pipeline_job_ops.DatabricksRestClient
    pipeline_job_ops.DatabricksRestClient = lambda *a, **k: client  # type: ignore[misc]
    registry = tmp_path / "pipeline_names.json"
    registry.write_text('{"pipelines": ["p_test_1"]}', encoding="utf-8")
    try:
        rc = run_monitor_loop("p_", 900, str(registry), max_iterations=1)
        assert rc == 0
    finally:
        pipeline_job_ops.DatabricksRestClient = original
