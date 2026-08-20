"""Tests for continuous ingest job YAML generation."""

from util.pipeline_job_generator import (
    generate_continuous_ingest_job_yaml,
    pipeline_start_task_key,
)


def test_pipeline_start_task_key_sanitizes_name():
    assert pipeline_start_task_key("p_iPC_2025_Dev7_15347_1") == "start_p_iPC_2025_Dev7_15347_1"


def test_generate_continuous_ingest_job_yaml_includes_all_pipelines():
    keys = ["p_client_2", "p_client_1"]
    yaml_text = generate_continuous_ingest_job_yaml(keys)

    assert "continuous_ingest_all:" in yaml_text
    assert "name: j_ipac_delta_sync_continuous_ingest" in yaml_text
    assert "depends_on:" in yaml_text
    assert "        - resources.pipelines.p_client_1" in yaml_text
    assert "        - resources.pipelines.p_client_2" in yaml_text
    assert "task_key: start_p_client_1" in yaml_text
    assert "task_key: start_p_client_2" in yaml_text
    assert "pipeline_id: ${resources.pipelines.p_client_1.id}" in yaml_text
    assert "pipeline_id: ${resources.pipelines.p_client_2.id}" in yaml_text
