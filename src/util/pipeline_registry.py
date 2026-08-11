"""Generate registry/config artifacts for generated pipeline names."""

from __future__ import annotations

import json
from pathlib import Path


def write_pipeline_name_registry(
    output_dir: Path | str,
    pipeline_names: list[str],
) -> str:
    """Write generated/config/pipeline_names.json used by ops jobs."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "pipeline_names.json"

    payload = {
        "pipelines": sorted(set(pipeline_names)),
        "generated_by": "ipac_delta_sync",
    }
    out_file.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return str(out_file)
