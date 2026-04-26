"""Pydantic models for Power BI report structures."""

from typing import Any, Optional
from pydantic import BaseModel, Field


class ExtractedItem(BaseModel):
    """Represents a single extracted visual item."""
    
    page: str
    visual_type: str
    item_name: str
    table_name: str
    val_name: str
    disp_name: Optional[str] = None
    data_type: str

    def to_list(self) -> list:
        """Convert to list format for legacy compatibility."""
        return [
            self.page,
            self.visual_type,
            self.item_name,
            self.table_name,
            self.val_name,
            self.disp_name,
            self.data_type,
        ]


class ExtractedFilter(BaseModel):
    """Represents a filter configuration."""
    
    page: str
    item_name: str
    filter_type: str
    table_name: str
    val_name: str
    operator: str  # Previously aliased as "ver"
    value: str

    def to_list(self) -> list:
        """Convert to list format for legacy compatibility."""
        return [
            self.page,
            self.item_name,
            self.filter_type,
            self.table_name,
            self.val_name,
            self.operator,
            self.value,
        ]


class VisualExtractionRule(BaseModel):
    """Defines how to extract data for a specific visual type."""
    
    visual_type: str
    projections_path: str = "$..projections"
    select_path: str = "$..Select"
    names_path: str = "$..Name"
    display_names_path: str = "$..NativeReferenceName"
    enabled: bool = True
    skip_types: list[str] = Field(default_factory=lambda: ["shape", "image", "textbox"])
    
    class Config:
        frozen = True  # Make immutable for safety


class FilterExtractionRule(BaseModel):
    """Defines how to extract filter data."""
    
    filter_type: str
    entity_path: str = "$..Entity"
    property_path: str = "$..Property"
    values_path: str = "$..Values"
    enabled: bool = True
    
    class Config:
        frozen = True


class ExtractionConfig(BaseModel):
    """Complete configuration for extraction process."""
    
    visual_rules: dict[str, VisualExtractionRule] = Field(default_factory=dict)
    filter_rules: dict[str, FilterExtractionRule] = Field(default_factory=dict)
    skip_template_page: bool = True
    log_unknown_types: bool = True
    
    def get_visual_rule(self, visual_type: str) -> Optional[VisualExtractionRule]:
        """Get extraction rule for a specific visual type."""
        return self.visual_rules.get(visual_type)
    
    def is_visual_enabled(self, visual_type: str) -> bool:
        """Check if visual type should be extracted."""
        rule = self.get_visual_rule(visual_type)
        return rule is not None and rule.enabled
