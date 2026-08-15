"""Tests for pipeline key normalization."""

from common.ops.pipeline_names import load_pipeline_names, normalize_pipeline_key


def test_normalize_pipeline_key_from_name():
    assert normalize_pipeline_key("p_iPC_2025_Dev7_15347_1") == "p_iPC_2025_Dev7_15347_1"


def test_normalize_pipeline_key_from_windows_path():
    raw = (
        "C:\\Users\\mukessingh\\PycharmProjects\\ipac_tax_sync\\src\\"
        "iPC_2025_Dev7_15347\\pipelines\\p_iPC_2025_Dev7_15347_1"
    )
    assert normalize_pipeline_key(raw) == "p_iPC_2025_Dev7_15347_1"


def test_normalize_pipeline_key_from_yaml_path():
    assert normalize_pipeline_key("src/client/pipelines/p_test_2.yml") == "p_test_2"


def test_load_pipeline_names_normalizes(tmp_path):
    path = tmp_path / "pipeline_names.json"
    path.write_text(
        '{"pipelines": ["C:/a/p_client_1", "p_client_2"]}',
        encoding="utf-8",
    )
    names = load_pipeline_names(str(path))
    assert names == ["p_client_1", "p_client_2"]
