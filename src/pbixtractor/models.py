"""Pydantic models for Power BI report structures."""

from typing import Optional

from pydantic import BaseModel


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
