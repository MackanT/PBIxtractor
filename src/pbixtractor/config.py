"""Configuration from data/data.yaml: visual types, projection roles and DAX functions."""

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import yaml

from .data import YAML_FILE
from .visual_helpers import VisualTypeMapper

# How a visual type is extracted (see extract_types in data.yaml)
EXTRACT_TYPES = ("standard", "button", "skip")


@dataclass
class Config:
    visual_mapper: VisualTypeMapper
    # Visual types with a display name in data.yaml (others are extracted but logged)
    supported_visual_types: set[str]
    # Projection role -> friendly label, e.g. "Y" -> "Y-Values"
    data_types: dict[str, str]
    # Visual type -> extract type (standard | button | skip); unlisted types are "standard"
    extract_types: dict[str, str] = field(default_factory=dict)
    # DAX functions highlighted in the documentation
    function_names: list[str] = field(default_factory=list)

    def extract_type(self, visual_type: str) -> str:
        return self.extract_types.get(visual_type, "standard")


def parse_config(data: dict) -> Config:
    """
    Build a Config from the parsed data.yaml content.

    Args:
        data: YAML content

    Returns:
        Config

    Raises:
        ValueError: If an extract type is unknown
    """
    metadata = data.get("visual_type_metadata", {})
    extract_types = data.get("extract_types", {}) or {}
    unknown = {k: v for k, v in extract_types.items() if v not in EXTRACT_TYPES}
    if unknown:
        raise ValueError(f"Unknown extract_types in data.yaml: {unknown}; use {EXTRACT_TYPES}")

    special = metadata.get("special_visuals", {}) or {}
    supported = set(metadata.get("standard_visuals", []) or []) | set(special)
    # Buttons and groups are handled separately, not as query visuals
    supported -= {"actionButton", "Group"}

    return Config(
        visual_mapper=VisualTypeMapper(metadata),
        supported_visual_types=supported,
        data_types={dt["name"]: dt["friendly_name"] for dt in data.get("data_types", [])},
        extract_types=dict(extract_types),
        function_names=list(data.get("function_names", [])),
    )


@lru_cache(maxsize=None)
def load_config(path: Path = YAML_FILE) -> Config:
    """Load (and cache) the configuration file."""
    return parse_config(yaml.safe_load(Path(path).read_text(encoding="utf-8")))
