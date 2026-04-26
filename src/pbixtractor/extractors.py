"""Modular extractors for Power BI report components."""

import logging
from abc import ABC, abstractmethod
from typing import Any, Optional

from jsonpath_ng import parse as jsonpath_parse
from jsonpath_ng.ext import parse as jsonpath_ext_parse

from .logger import get_logger
from .models import ExtractedFilter, ExtractedItem


class BaseExtractor(ABC):
    """Base class for all extractors."""

    def __init__(self, config: dict, logger: logging.Logger = None):
        """
        Initialize extractor.

        Args:
            config: Extraction configuration from YAML
            logger: Logger instance for logging messages
        """
        self.config = config
        self.logger = logger or get_logger("pbixtractor")

    def query_json(self, data: dict, path: str, default: Any = None) -> Any:
        """
        Query JSON data using JSONPath.

        Args:
            data: JSON data to query
            path: JSONPath expression
            default: Value to return if no matches found

        Returns:
            First matching value or default
        """
        try:
            matches = jsonpath_ext_parse(path).find(data)
            if matches:
                return matches[0].value
            return default
        except Exception as e:
            self.logger.warning(f"JSONPath query failed: {path}. Error: {str(e)}")
            return default

    def query_all_json(self, data: dict, path: str) -> list:
        """
        Query JSON data and return all matches.

        Args:
            data: JSON data to query
            path: JSONPath expression

        Returns:
            List of all matching values
        """
        try:
            matches = jsonpath_ext_parse(path).find(data)
            return [match.value for match in matches]
        except Exception as e:
            self.log(f"JSONPath query failed: {path}", str(e), 1)
            return []

    def clean_value(self, value: str) -> str:
        """Clean extracted values (remove quotes, convert types)."""
        if not isinstance(value, str):
            return str(value)

        value = value.strip()

        # Handle boolean strings
        if value.lower() == "true":
            return "True"
        elif value.lower() == "false":
            return "False"

        # Handle datetime strings
        if "datetime" in value.lower():
            import re

            match = re.search(r"'(.*?)'", value)
            if match:
                return match.group(1)

        # Handle long integers (ending with L)
        if value.endswith("L"):
            try:
                return str(int(value[:-1]))
            except ValueError:
                pass

        # Try to convert to integer
        try:
            return str(int(value))
        except ValueError:
            pass

        # Remove excess quotes
        return value.replace("'", "")

    @abstractmethod
    def extract(self, data: dict) -> list:
        """Extract data from JSON structure."""
        pass


class VisualExtractor(BaseExtractor):
    """Extracts visual elements from Power BI pages."""

    def __init__(
        self, config: dict, visual_types: list, data_types: list, logger: logging.Logger = None
    ):
        """
        Initialize visual extractor.

        Args:
            config: Extraction rules config
            visual_types: List of supported visual types
            data_types: List of [name, friendly_name] data type mappings
            logger: Logger instance
        """
        super().__init__(config, logger)
        self.visual_types = set(visual_types)
        self.data_types = data_types
        self.skip_types = config.get("default", {}).get("skip_types", [])

    def extract(self, visual_container: dict, page_name: str) -> list[ExtractedItem]:
        """
        Extract items from a visual container.

        Args:
            visual_container: Visual container JSON
            page_name: Name of the page this visual is on

        Returns:
            List of ExtractedItem objects
        """
        items = []

        config = visual_container.get("config", {})
        if not config:
            return items

        # Extract basic visual info
        item_name = self.query_json(config, "$.name", "")
        visual_type = self.query_json(config, "$..visualType")

        # Skip certain visual types
        if visual_type in self.skip_types:
            return items

        # Handle groups (visuals without visualType)
        if visual_type is None:
            display_name = self.query_json(config, "$.displayName", "")
            items.append(
                ExtractedItem(
                    page=page_name,
                    visual_type="Group",
                    item_name="",
                    table_name="",
                    val_name="",
                    disp_name=display_name,
                    data_type="Group",
                )
            )
            return items

        # Handle action buttons
        if visual_type == "actionButton":
            return self._extract_button(config, page_name)

        # Handle standard visuals
        if visual_type in self.visual_types:
            return self._extract_standard_visual(config, page_name, item_name, visual_type)

        # Unknown visual type
        self.logger.warning(
            f"Unknown visual type: {visual_type}. Data: {str(visual_container)[:200]}"
        )
        return items

    def _extract_standard_visual(
        self, config: dict, page_name: str, item_name: str, visual_type: str
    ) -> list[ExtractedItem]:
        """Extract data from standard visual types."""
        items = []

        # Get projections (data field assignments)
        projections = self.query_json(config, "$..projections", {})

        # Build lookup of query references by data type
        data_list = []
        for data_type_name, data_type_friendly in self.data_types:
            refs = []
            for item in projections.get(data_type_name, []):
                if isinstance(item, dict) and "queryRef" in item:
                    refs.append(item["queryRef"])
            data_list.append(refs)

        # Get field selections
        select_items = self.query_json(config, "$..Select", [])

        # Extract display name mappings
        all_names = self.query_all_json(config, "$..Name")
        all_display_names = self.query_all_json(config, "$..NativeReferenceName")
        name_mapping = dict(zip(all_names, all_display_names))

        # Process each selected field
        for row in select_items:
            if not isinstance(row, dict):
                continue

            # Extract table and field name from the row
            table_name, val_name = self._extract_table_and_field(row)

            if not table_name or not val_name:
                continue

            # Determine data type and display name
            field_ref = row.get("Name", "")
            data_type = self._determine_data_type(field_ref, data_list)
            disp_name = name_mapping.get(field_ref)

            # Handle hierarchies
            if row.get("HierarchyLevel"):
                data_type = "Hierarchy"
                level = self.query_json(row, "HierarchyLevel.Level", "")
                hierarchy_name = field_ref.split(".")[1] if "." in field_ref else ""
                disp_name = f"{hierarchy_name}: {level}"

            items.append(
                ExtractedItem(
                    page=page_name,
                    visual_type=visual_type,
                    item_name=item_name,
                    table_name=table_name,
                    val_name=val_name,
                    disp_name=disp_name if disp_name != val_name else None,
                    data_type=data_type,
                )
            )

        return items

    def _extract_table_and_field(self, row: dict) -> tuple[str, str]:
        """Extract table and field name from a selection row."""
        # Try hierarchy format
        if row.get("HierarchyLevel"):
            name = self.query_json(row, "$.Name", "")
            parts = name.split(".")
            if len(parts) >= 3:
                return parts[0], parts[2]

        # Try measure or column format
        if row.get("Measure") or row.get("Column"):
            name = row.get("Name", "")
            # Handle Sum(Table.Field) format
            if name.startswith("Sum("):
                name = name[4:]
            parts = name.split(".", 1)
            if len(parts) == 2:
                return parts[0], parts[1]

        # Try aggregation format
        if row.get("Aggregation"):
            name = row.get("Name", "")
            # Extract from Sum(Table.Field) format
            start = name.find("(") + 1
            end = name.rfind(")")
            if start > 0 and end > start:
                inner = name[start:end]
                parts = inner.split(".")
                if len(parts) == 2:
                    return parts[0], parts[1]

        self.logger.warning(f"Could not extract table/field. Data: {str(row)[:200]}")
        return "", ""

    def _determine_data_type(self, field_ref: str, data_list: list) -> str:
        """Determine the data type category for a field."""
        for i, refs in enumerate(data_list):
            if field_ref in refs:
                return self.data_types[i][1]  # Return friendly name

        self.log(f"Unknown data type for field: {field_ref}", None, 1)
        return "UNKNOWN Data Type"

    def _extract_button(self, config: dict, page_name: str) -> list[ExtractedItem]:
        """Extract action button information."""
        items = []

        # Extract button properties
        values = self.query_all_json(config, "$..Value")

        disp_name = ""
        item_name = ""
        button_type = ""

        # Parse button configuration
        for val in values:
            # This is simplified - actual implementation would need path-based extraction
            pass

        # For now, return basic button info
        # Full implementation would handle Bookmark, PageNavigation, etc.

        return items


class FilterExtractor:
    """Extracts filter configurations from visuals and pages."""

    def __init__(self, config: dict, logger: logging.Logger = None):
        """
        Initialize filter extractor.

        Args:
            config: Extraction configuration
            logger: Logger instance
        """
        self.config = config
        self.logger = logger or get_logger("pbixtractor")

    def query_json(self, data: dict, path: str, default: Any = None) -> Any:
        """Query JSON data using JSONPath."""
        try:
            matches = jsonpath_ext_parse(path).find(data)
            if matches:
                return matches[0].value
            return default
        except Exception as e:
            self.logger.warning(f"JSONPath query failed: {path}. Error: {str(e)}")
            return default

    def query_all_json(self, data: dict, path: str) -> list:
        """Query JSON data and return all matches."""
        try:
            matches = jsonpath_ext_parse(path).find(data)
            return [match.value for match in matches]
        except Exception as e:
            self.logger.warning(f"JSONPath query failed: {path}. Error: {str(e)}")
            return []

    def clean_value(self, value: str) -> str:
        """Clean extracted values."""
        if not isinstance(value, str):
            return str(value)
        value = value.strip().replace("'", "")
        return value

    def extract_visual_filters(
        self, filters: list, item_name: str, page_name: str
    ) -> list[ExtractedFilter]:
        """Extract filters from a visual container."""
        extracted = []

        for filter_obj in filters:
            if not isinstance(filter_obj, dict):
                continue

            if not filter_obj.get("filter"):
                continue

            # Extract filter components
            table_name = self.query_json(filter_obj, "$..Entity")
            val_name = self.query_json(filter_obj, "$..Property")

            if not table_name or not val_name:
                # Try hierarchy format
                hierarchy = self.query_json(filter_obj, "$..HierarchyLevel")
                if hierarchy:
                    val_name = hierarchy.get("Level", "UNKNOWN")

            if not val_name:
                continue

            # Determine filter type and value
            filter_type_str = filter_obj.get("type", "")
            operator, value = self._parse_filter_expression(filter_obj, filter_type_str)

            extracted.append(
                ExtractedFilter(
                    page=page_name,
                    item_name=item_name,
                    filter_type="Visual",
                    table_name=table_name or "",
                    val_name=val_name,
                    operator=operator,
                    value=value,
                )
            )

        return extracted

    def extract_page_filters(self, filters: list, page_name: str) -> list[ExtractedFilter]:
        """Extract page-level filters."""
        extracted = []

        for filter_obj in filters:
            if not isinstance(filter_obj, dict):
                continue

            table_name = self.query_json(filter_obj, "$.Entity", "")
            val_name = self.query_json(filter_obj, "$.Property", "")
            item_name = filter_obj.get("displayName", val_name)

            filter_variant = self.query_json(filter_obj, "$.type", "")

            if filter_variant == "Categorical":
                operator, value = self._parse_categorical_filter(filter_obj)
            elif filter_variant == "Advanced":
                operator, value = self._parse_advanced_filter(filter_obj)
            elif filter_variant == "RelativeDate":
                operator, value = self._parse_relative_date_filter(filter_obj)
            else:
                self.logger.warning(
                    f"Unknown filter variant: {filter_variant}. Data: {str(filter_obj)[:200]}"
                )
                continue

            if operator and value:
                extracted.append(
                    ExtractedFilter(
                        page=page_name,
                        item_name=item_name,
                        filter_type="This Page",
                        table_name=table_name,
                        val_name=val_name,
                        operator=operator,
                        value=value,
                    )
                )

        return extracted

    def _parse_filter_expression(self, filter_obj: dict, filter_type: str) -> tuple[str, str]:
        """Parse filter expression into operator and value."""
        # Simplified - full implementation would handle all filter types
        # For now, extract basic comparisons

        values = self.query_all_json(filter_obj, "$..Value")
        comparison_kind = self.query_json(filter_obj, "$..ComparisonKind")

        if values:
            val_str = ", ".join([self.clean_value(str(v)) for v in values[:5]])
            operator = "in" if len(values) > 1 else "="
            return operator, val_str

        return "=", ""

    def _parse_categorical_filter(self, filter_obj: dict) -> tuple[str, str]:
        """Parse categorical filter."""
        values = self.query_json(filter_obj, "$.Values", [])
        is_inverted = self.query_json(
            filter_obj, "$..isInvertedSelectionMode.expr.Literal.Value", False
        )

        if not values:
            return "", ""

        # Extract literal values
        value_list = []
        for val in values:
            literal_val = self.query_json(val, "$..Value")
            if literal_val:
                value_list.append(self.clean_value(str(literal_val)))

        if not value_list:
            return "", ""

        # Determine operator
        if len(value_list) == 1:
            operator = "<>" if is_inverted else "="
        else:
            operator = "not in" if is_inverted else "in"

        return operator, ", ".join(value_list)

    def _parse_advanced_filter(self, filter_obj: dict) -> tuple[str, str]:
        """Parse advanced filter."""
        where_clauses = self.query_all_json(filter_obj, "$.Where[*]")

        values = []
        for clause in where_clauses:
            is_not = self.query_json(clause, "$.Not") is not None
            value = self.query_json(clause, "$.Right.Literal.Value", "")

            operator = "is not" if is_not else "is"
            values.append(f"{operator} {self.clean_value(str(value))}")

        return "advanced", " and ".join(values) if values else ""

    def _parse_relative_date_filter(self, filter_obj: dict) -> tuple[str, str]:
        """Parse relative date filter."""
        lower_bound = self.query_json(filter_obj, "$.LowerBound")

        if not lower_bound:
            return "", ""

        amount = self.query_json(lower_bound, "$..Amount", 0)
        time_unit = self.query_json(lower_bound, "$..TimeUnit", 0)

        unit_map = {0: "days", 1: "weeks", 2: "months", 3: "years"}
        unit_str = unit_map.get(time_unit, "unknown")

        if amount < 0:
            value = f"in the last {abs(amount)} {unit_str}"
        else:
            value = f"in the next {amount} {unit_str}"

        return "in the last", value


class PageExtractor(BaseExtractor):
    """Orchestrates extraction of all elements from a page."""

    def __init__(
        self, config: dict, visual_types: list, data_types: list, logger: logging.Logger = None
    ):
        """Initialize page extractor."""
        super().__init__(config, logger)
        self.visual_extractor = VisualExtractor(config, visual_types, data_types, logger)
        self.filter_extractor = FilterExtractor(config, logger)

    def extract(self, page: dict) -> tuple[list[ExtractedItem], list[ExtractedFilter]]:
        """
        Extract all items and filters from a page.

        Args:
            page: Page JSON data

        Returns:
            Tuple of (items list, filters list)
        """
        items = []
        filters = []

        page_name = page.get("displayName", "Unknown")

        # Skip template pages
        if self.config.get("default", {}).get("skip_template", True):
            if page_name == "Template":
                return items, filters

        # Extract from visual containers
        for visual_container in page.get("visualContainers", []):
            # Extract visual items
            visual_items = self.visual_extractor.extract(visual_container, page_name)
            items.extend(visual_items)

            # Extract visual filters
            if visual_container.get("filters"):
                item_name = self.query_json(visual_container, "$.config.name", "")
                visual_filters = self.filter_extractor.extract_visual_filters(
                    visual_container["filters"], item_name, page_name
                )
                filters.extend(visual_filters)

        # Extract page-level filters
        import json

        page_filters_str = page.get("filters", "[]")
        try:
            page_filters = (
                json.loads(page_filters_str)
                if isinstance(page_filters_str, str)
                else page_filters_str
            )
            page_level_filters = self.filter_extractor.extract_page_filters(page_filters, page_name)
            filters.extend(page_level_filters)
        except json.JSONDecodeError as e:
            self.logger.error(
                f"Failed to parse page filters. Error: {str(e)}. Data: {str(page_filters_str)[:200]}"
            )

        return items, filters
