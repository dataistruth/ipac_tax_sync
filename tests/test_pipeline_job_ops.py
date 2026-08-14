"""Tests for pipeline heartbeat monitor matching logic."""

from common.ops.pipeline_job_ops import (
    _filter_pipelines,
    _list_generated_pipelines,
    _strip_bundle_dev_prefix,
)


class _FakeClient:
    def __init__(self, statuses: list[dict]) -> None:
        self._statuses = statuses

    def get(self, path: str) -> dict:
        if path == "/api/2.0/pipelines":
            return {"statuses": self._statuses}
        raise AssertionError(path)


def test_strip_bundle_dev_prefix():
    assert _strip_bundle_dev_prefix("[dev ipacs_dev_oauth_spn] p_client_1") == "p_client_1"
    assert _strip_bundle_dev_prefix("p_client_1") == "p_client_1"


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
