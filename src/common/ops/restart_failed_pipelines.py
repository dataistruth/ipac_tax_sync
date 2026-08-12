"""Restart failed generated continuous Lakeflow pipelines.

Local/dev wrapper. Databricks jobs use pipeline_job_ops.py directly.
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

if __name__ == "__main__":
    script = Path(__file__).resolve().with_name("pipeline_job_ops.py")
    sys.argv = [str(script), "restart", *sys.argv[1:]]
    runpy.run_path(str(script), run_name="__main__")
