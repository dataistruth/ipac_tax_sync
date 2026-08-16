"""Detect stale generated bundle/schema artifacts after suffix or layout changes."""

from __future__ import annotations

from pathlib import Path

from util.paths import generated_bundle_dir, generated_config_schema_dir


def find_stale_generated_suffix_markers(
    dest_schema_suffix: str,
    stale_suffixes: tuple[str, ...] = ("poc_1",),
) -> list[str]:
    """
    Return human-readable paths that still contain old suffix fragments.

    When dest_schema_suffix is `_poc1`, any `poc_1` in generated YAML is stale.
    """
    expected = dest_schema_suffix.strip()
    markers = [s for s in stale_suffixes if s and s != expected]
    if not markers:
        return []

    hits: list[str] = []
    for root in (generated_bundle_dir(), generated_config_schema_dir()):
        if not root.exists():
            continue
        for path in root.glob("*.yml"):
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                continue
            for marker in markers:
                if marker in text:
                    hits.append(f"{path} (contains '{marker}')")
                    break
    return hits


def find_embedded_schemas_in_bundle() -> list[str]:
    """Bundle pipelines should not embed resources.schemas (schemas live in generated/config/schema)."""
    bundle_dir = generated_bundle_dir()
    if not bundle_dir.exists():
        return []
    stale: list[str] = []
    for path in bundle_dir.glob("*.yml"):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        if "resources:" in text and "  schemas:" in text:
            stale.append(f"{path} (embeds resources.schemas — re-run generate)")
    return stale
