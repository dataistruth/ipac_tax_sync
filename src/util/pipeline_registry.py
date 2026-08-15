"""Generate registry/config artifacts for generated pipeline names."""

from __future__ import annotations

import json
from pathlib import Path

from common.ops.pipeline_names import normalize_pipeline_key


def write_pipeline_name_registry(
    output_dir: Path | str,
    pipeline_names: list[str],
) -> str:
    """Write generated/config/pipeline_names.json used by ops jobs."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "pipeline_names.json"

    payload = {
        "pipelines": sorted({normalize_pipeline_key(n) for n in pipeline_names if normalize_pipeline_key(n)}),
        "generated_by": "ipac_delta_sync",
    }
    out_file.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return str(out_file)
