"""Load and normalize pipeline keys for monitor, recon, and restart jobs."""

from __future__ import annotations

import json
import re
from pathlib import Path

_PIPELINE_KEY_RE = re.compile(r"(p_.+_\d+)$", re.IGNORECASE)


def normalize_pipeline_key(raw: str) -> str:
    """
    Return logical bundle pipeline key p_<client>_<n>.

    Accepts pipeline_names.json entries, YAML paths, or Windows/POSIX file paths.
    """
    text = str(raw).strip()
    if not text:
        return ""
    # Take final path segment; handle Windows backslashes
    segment = text.replace("\\", "/").rsplit("/", 1)[-1]
    if segment.endswith(".yml") or segment.endswith(".yaml"):
        segment = Path(segment).stem
    match = _PIPELINE_KEY_RE.search(segment)
    if match:
        return match.group(1)
    return segment


def load_pipeline_names(path: str | None) -> list[str]:
    """Load pipeline keys from generated/config/pipeline_names.json."""
    if not path:
        return []
    file = Path(path)
    if not file.exists():
        raise FileNotFoundError(f"pipeline names file not found: {path}")
    payload = json.loads(file.read_text(encoding="utf-8"))
    names = payload.get("pipelines", []) if isinstance(payload, dict) else []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in names:
        key = normalize_pipeline_key(str(raw))
        if not key:
            continue
        folded = key.casefold()
        if folded in seen:
            continue
        seen.add(folded)
        normalized.append(key)
    return normalized
