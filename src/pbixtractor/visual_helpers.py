"""Helper functions for visual type handling based on YAML configuration."""

import re
from typing import Optional, Tuple

# Custom (imported) visuals: "<name>" + a GUID, e.g. "PowerApps_PBI_CV_C29F1DCC_81F5_4973_94AD_
# 0517D44CC06A" or "castellumCharts9A467DB81DD645A3AF0FB12DA8C0231E"; older marketplace visuals
# use a 13-digit timestamp instead ("ChicletSlicer1448559807354")
_CUSTOM_VISUAL = re.compile(
    r"^(?P<name>[A-Za-z][A-Za-z0-9]*?)(?:(?:_PBI_CV)?_?"
    r"[0-9A-Fa-f]{8}_?[0-9A-Fa-f]{4}_?[0-9A-Fa-f]{4}_?[0-9A-Fa-f]{4}_?[0-9A-Fa-f]{12}|\d{13})$"
)


def custom_visual_name(visual_type: str) -> Optional[str]:
    """Readable name of a custom visual ("Power Apps"), or None for built-in visual types."""
    match = _CUSTOM_VISUAL.match(visual_type or "")
    if not match:
        return None
    words = re.findall(r"[A-Z]+(?![a-z])|[A-Z]?[a-z0-9]+", match.group("name"))
    return " ".join(word[:1].upper() + word[1:] for word in words)


class VisualTypeMapper:
    """Maps Power BI visual types to display names using YAML configuration."""

    def __init__(self, visual_metadata: dict):
        """
        Initialize mapper with visual type metadata from YAML.

        Args:
            visual_metadata: The visual_type_metadata section from data.yaml
        """
        self.standard_visuals = set(visual_metadata.get("standard_visuals", []))
        self.special_visuals = visual_metadata.get("special_visuals", {})
        self.button_types = set(visual_metadata.get("button_types", []))

    def get_visual_info(self, visual_type: str) -> Tuple[str, str]:
        """
        Get item type and display name for a visual type.

        Args:
            visual_type: Power BI internal visual type name

        Returns:
            Tuple of (item_type, display_name)
            - item_type: "Visual", "Slicer", "Button", or "Group"
            - display_name: Friendly name for display in reports
        """
        # Check special visuals first (custom mappings)
        if visual_type in self.special_visuals:
            config = self.special_visuals[visual_type]
            return config.get("item_type", "Visual"), config.get("display_name", visual_type)

        # Check button types
        if visual_type in self.button_types:
            return "Button", visual_type

        # Standard visuals - auto-generate display name from camelCase
        if visual_type in self.standard_visuals:
            display_name = self._camel_case_to_display_name(visual_type)
            return "Visual", display_name

        custom = custom_visual_name(visual_type)
        if custom:
            return "Visual", f"{custom} (custom visual)"

        # Unknown type - return as-is
        return "Visual", visual_type

    def _camel_case_to_display_name(self, camel_case: str) -> str:
        """
        Convert camelCase visual type to Display Name.

        Examples:
            clusteredColumnChart → Clustered Column Chart
            lineChart → Line Chart
            hundredPercentStackedBarChart → Hundred Percent Stacked Bar Chart

        Args:
            camel_case: camelCase string

        Returns:
            Display Name with proper capitalization
        """
        # Split camelCase into words
        words = re.findall(r"[a-zA-Z][^A-Z]*", camel_case)

        # Capitalize each word and join with spaces
        display_name = " ".join(word.capitalize() for word in words)

        return display_name.strip()

    def is_special_visual(self, visual_type: str) -> bool:
        """Check if visual type has custom configuration."""
        return visual_type in self.special_visuals

    def is_button_type(self, visual_type: str) -> bool:
        """Check if visual type is a button."""
        return visual_type in self.button_types


def create_visual_mapper(config_data: dict) -> VisualTypeMapper:
    """
    Factory function to create VisualTypeMapper from YAML config.

    Args:
        config_data: Full YAML configuration dict

    Returns:
        VisualTypeMapper instance
    """
    visual_metadata = config_data.get("visual_type_metadata", {})
    return VisualTypeMapper(visual_metadata)
