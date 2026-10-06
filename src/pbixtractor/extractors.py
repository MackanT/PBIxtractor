"""Modular extractors for Power BI report components."""

import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

from .config import Config
from .logger import get_logger
from .models import ExtractedFilter, ExtractedItem
from .readers import FieldBinding, PageDefinition, ReportDefinition, VisualDefinition
from .visual_helpers import custom_visual_name

# Power BI QueryComparisonKind
COMPARISON_OPERATORS = {0: "=", 1: ">", 2: ">=", 3: "<=", 4: "<"}

# Power BI QueryTimeUnit (used by relative date/time filters)
TIME_UNITS = {
    0: "days",
    1: "weeks",
    2: "months",
    3: "years",
    4: "decades",
    5: "seconds",
    6: "minutes",
    7: "hours",
}

# Operators that flip when wrapped in a Not condition
NEGATED_OPERATORS = {
    "=": "<>",
    "<>": "=",
    "in": "not in",
    "not in": "in",
    ">": "<=",
    ">=": "<",
    "<": ">=",
    "<=": ">",
    "contains": "does not contain",
    "starts with": "does not start with",
    "ends with": "does not end with",
}


# ============================================================================
# Shared helpers
# ============================================================================


def clean_literal(value: Any) -> str:
    """
    Convert a Power BI query literal to readable text.

    Examples:
        "'abc'" -> "abc", "2019L" -> "2019", "0D" -> "0", "true" -> "True",
        "datetime'2024-01-01T00:00:00'" -> "2024-01-01T00:00:00"

    Args:
        value: Raw literal value from the layout JSON

    Returns:
        Cleaned string
    """
    if not isinstance(value, str):
        return str(value)

    value = value.strip()

    if value.lower() in ("true", "false"):
        return value.capitalize()

    match = re.fullmatch(r"datetime'(.*?)'", value)
    if match:
        return match.group(1)

    # Typed numbers: L = long, D = double, M = decimal
    match = re.fullmatch(r"(-?\d+(?:\.\d+)?)[LDM]", value)
    if match:
        return match.group(1)

    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1].replace("''", "'")

    return value


def get_aliases(query: dict) -> dict[str, str]:
    """
    Map the source aliases of a semantic query to table names.

    Args:
        query: Semantic query with a "From" list (e.g. prototypeQuery or a filter)

    Returns:
        Dict of alias -> table name, e.g. {"d": "Dates"}
    """
    return {
        source["Name"]: source.get("Entity", "")
        for source in (query or {}).get("From", [])
        if isinstance(source, dict) and source.get("Name")
    }


def _resolve_table(expression: dict, aliases: dict[str, str]) -> str:
    """Resolve the table of a SourceRef (direct entity or query alias)."""
    source_ref = expression.get("SourceRef")
    if source_ref is None and "PropertyVariationSource" in expression:
        source_ref = expression["PropertyVariationSource"].get("Expression", {}).get("SourceRef")
    source_ref = source_ref or {}
    return source_ref.get("Entity") or aliases.get(source_ref.get("Source"), "")


def resolve_field(expr: dict, aliases: dict[str, str]) -> Optional[tuple[str, str]]:
    """
    Resolve a query expression to the (table, field) it references.

    Uses the query aliases instead of the "Name"/queryRef string, which can be stale
    after measures are renamed or moved to another table.

    Args:
        expr: Expression node (Column, Measure, Aggregation or HierarchyLevel)
        aliases: Alias -> table mapping from get_aliases()

    Returns:
        (table, field) tuple, or None if the expression is not a field reference
    """
    if not isinstance(expr, dict):
        return None

    for key in ("Column", "Measure"):
        if isinstance(expr.get(key), dict):
            node = expr[key]
            return _resolve_table(node.get("Expression", {}), aliases), node.get("Property", "")

    if isinstance(expr.get("Aggregation"), dict):
        return resolve_field(expr["Aggregation"].get("Expression", {}), aliases)

    if isinstance(expr.get("HierarchyLevel"), dict):
        level = expr["HierarchyLevel"]
        hierarchy = level.get("Expression", {}).get("Hierarchy", {})
        return _resolve_table(hierarchy.get("Expression", {}), aliases), level.get("Level", "")

    return None


def iter_field_refs(obj: Any, aliases: dict[str, str]) -> Iterator[tuple[str, str]]:
    """
    Yield (table, field) for every Column/Measure reference nested anywhere in obj.

    Used for formatting objects (conditional formatting, titles, button actions bound to
    measures), which are not part of the visual's query.

    Args:
        obj: Any JSON structure
        aliases: Alias -> table mapping

    Yields:
        (table, field) tuples
    """
    if isinstance(obj, dict):
        for key in ("Column", "Measure"):
            node = obj.get(key)
            if isinstance(node, dict) and "Property" in node:
                yield _resolve_table(node.get("Expression", {}), aliases), node["Property"]
        for value in obj.values():
            yield from iter_field_refs(value, aliases)
    elif isinstance(obj, list):
        for value in obj:
            yield from iter_field_refs(value, aliases)


def merge_object_properties(entries: Any) -> dict:
    """
    Merge the "properties" of all entries in a formatting object list.

    Args:
        entries: List like [{"properties": {...}, "selector": {...}}, ...]

    Returns:
        Merged properties dict (later entries win)
    """
    merged = {}
    for entry in entries or []:
        if isinstance(entry, dict):
            merged.update(entry.get("properties", {}))
    return merged


def property_text(prop: Any) -> str:
    """Read a formatting property: a literal value, or the field it is bound to (fx)."""
    if not isinstance(prop, dict):
        return ""
    expr = prop.get("expr", {})
    if "Literal" in expr:
        return clean_literal(expr["Literal"].get("Value", ""))
    resolved = resolve_field(expr, {})
    if resolved:
        return f"fx: {resolved[0]}[{resolved[1]}]"
    return ""


def visual_title(visual: VisualDefinition) -> str:
    """A visual's title text ("" when it has none; "fx: T[F]" when bound to a field)."""
    return property_text(merge_object_properties(visual.container_objects.get("title")).get("text")).strip()


@dataclass
class ReportContext:
    """Report-wide lookups needed while extracting a single page."""

    page_names: dict[str, str] = field(default_factory=dict)  # page name -> display name
    bookmark_names: dict[str, str] = field(default_factory=dict)  # bookmark id -> display name

    @classmethod
    def from_report(cls, report: ReportDefinition) -> "ReportContext":
        """
        Build lookups from a normalised report.

        Args:
            report: Report read by readers.read_report()

        Returns:
            ReportContext instance
        """
        return cls(
            page_names={page.name: page.display_name for page in report.pages},
            bookmark_names=dict(report.bookmarks),
        )


# ============================================================================
# Extractors
# ============================================================================


class BaseExtractor(ABC):
    """Base class for all extractors."""

    def __init__(self, config: Optional[Config], logger: logging.Logger = None):
        """
        Initialize extractor.

        Args:
            config: Extraction configuration from YAML
            logger: Logger instance for logging messages
        """
        self.config = config
        self.logger = logger or get_logger("pbixtractor")

    @abstractmethod
    def extract(self, data: dict) -> list:
        """Extract data from JSON structure."""
        pass


class VisualExtractor(BaseExtractor):
    """Extracts visual elements from Power BI pages."""

    def __init__(self, config: Config, logger: logging.Logger = None):
        """
        Initialize visual extractor.

        Args:
            config: Configuration from data.yaml (supported types, roles, extract types)
            logger: Logger instance
        """
        super().__init__(config, logger)
        self.visual_types = config.supported_visual_types
        self.data_types = config.data_types
        self._warned: set[tuple] = set()  # unknown types/roles already reported
        # Extract type (data.yaml extract_types) -> handler(visual, page, visual_type, context).
        # "skip" has no handler: those visuals are dropped unless they have an action.
        self.handlers = {
            "standard": self._extract_standard_visual,
            "button": self._extract_button,
        }

    def extract(
        self, visual: VisualDefinition, page_name: str, context: ReportContext = None
    ) -> list[ExtractedItem]:
        """
        Extract items from a visual.

        Args:
            visual: Normalised visual (legacy or PBIR, see readers.py)
            page_name: Name of the page this visual is on
            context: Report-wide lookups (page and bookmark names)

        Returns:
            List of ExtractedItem objects
        """
        context = context or ReportContext()

        if visual.is_group:
            return [
                ExtractedItem(
                    page=page_name,
                    visual_type="Group",
                    item_name=visual.name,
                    table_name="",
                    val_name="",
                    disp_name=visual.group_name,
                    data_type="Group",
                )
            ]

        visual_type = visual.visual_type
        if visual_type is None:
            self.logger.warning(f"Visual {visual.name} on {page_name} has no visualType")
            return []

        extract_type = self.config.extract_type(visual_type)
        if extract_type == "skip":
            # Shapes/images are decoration, unless they have an action (clickable shape)
            link = merge_object_properties(visual.container_objects.get("visualLink"))
            if "type" not in link:
                return []
            visual_type, extract_type = "actionButton", "button"
        elif (
            extract_type == "standard"
            and visual_type not in self.visual_types
            and not custom_visual_name(visual_type)  # custom visuals: fields read generically
        ):
            self._warn_once(
                ("type", visual_type),
                f"Unknown visual type: {visual_type} (first on {page_name}). "
                "Extracting fields generically - add it to data.yaml.",
            )

        items = self.handlers[extract_type](visual, page_name, visual_type, context)
        items.extend(self._extract_formatting_refs(visual, page_name, visual_type, items))
        return items

    def _extract_standard_visual(
        self,
        visual: VisualDefinition,
        page_name: str,
        visual_type: str,
        context: ReportContext = None,
    ) -> list[ExtractedItem]:
        """Extract the fields of a standard (query-based) visual."""
        items = []

        for binding in visual.fields:
            resolved = resolve_field(binding.expr, visual.aliases)
            if not resolved or not resolved[0] or not resolved[1]:
                # Wrapped fields ("% of grand total", sparklines, ...): the Column/Measure
                # references nested inside still count as used
                nested = list(dict.fromkeys(iter_field_refs(binding.expr, visual.aliases)))
                nested = [(table, name) for table, name in nested if table and name]
                if nested:
                    data_type = self._determine_data_type(binding, visual_type)
                    for table_name, val_name in nested:
                        items.append(
                            ExtractedItem(
                                page=page_name,
                                visual_type=visual_type,
                                item_name=visual.name,
                                table_name=table_name,
                                val_name=val_name,
                                disp_name=binding.display_name
                                if binding.display_name != val_name
                                else None,
                                data_type=data_type,
                            )
                        )
                elif "NativeVisualCalculation" not in binding.expr:  # visual calcs: no model field
                    self.logger.warning(
                        f"Could not resolve table/field on {page_name}. "
                        f"Data: {str(binding.expr)[:200]}"
                    )
                continue
            table_name, val_name = resolved

            data_type = self._determine_data_type(binding, visual_type)
            disp_name = binding.display_name

            if "HierarchyLevel" in binding.expr:
                data_type = "Hierarchy"
                hierarchy_expr = binding.expr["HierarchyLevel"].get("Expression", {}).get("Hierarchy", {})
                hierarchy_name = hierarchy_expr.get("Hierarchy", "")
                disp_name = f"{hierarchy_name}: {val_name}"
                variation = hierarchy_expr.get("Expression", {}).get("PropertyVariationSource")
                if isinstance(variation, dict) and variation.get("Property"):
                    # Auto date/time: the levels live on a hidden LocalDateTable; the model
                    # column the visual really uses is the date column (e.g. OrderDate)
                    val_name = variation["Property"]
                else:
                    # The level name can differ from its column; the queryRef
                    # ("Table.Hierarchy.Column") holds the column used for unused-detection
                    prefix = f"{table_name}.{hierarchy_name}."
                    if binding.query_ref.startswith(prefix) and len(binding.query_ref) > len(prefix):
                        val_name = binding.query_ref[len(prefix) :]

            items.append(
                ExtractedItem(
                    page=page_name,
                    visual_type=visual_type,
                    item_name=visual.name,
                    table_name=table_name,
                    val_name=val_name,
                    disp_name=disp_name if disp_name != val_name else None,
                    data_type=data_type,
                )
            )

        return items

    def _determine_data_type(self, binding: FieldBinding, visual_type: str = "") -> str:
        """Map a field's projection role to its friendly name from data.yaml."""
        if binding.role is None:
            self.logger.warning(f"Field not bound to any visual role: {binding.query_ref}")
            return "UNKNOWN Data Type"

        if binding.role in self.data_types:
            return self.data_types[binding.role]
        if custom_visual_name(visual_type):
            # Roles of custom visuals are defined by their author: show them as they are
            return binding.role.replace("_", " ").strip().capitalize()
        self._warn_once(
            ("role", binding.role),
            f"Unknown visual role '{binding.role}' (first in {visual_type}) - add it to data.yaml "
            "data_types",
        )
        return binding.role

    def _warn_once(self, key: tuple, message: str) -> None:
        """Log a warning once per extraction run (not once per visual or field)."""
        if key not in self._warned:
            self._warned.add(key)
            self.logger.warning(message)

    def _extract_button(
        self,
        visual: VisualDefinition,
        page_name: str,
        visual_type: str,
        context: ReportContext = None,
    ) -> list[ExtractedItem]:
        """
        Extract an action button: its action type, target and label.

        Row layout: val_name = action target (bookmark/page display name),
        disp_name = button label, data_type = action type.
        """
        container_objects = visual.container_objects
        item_name = visual.name

        link = merge_object_properties(container_objects.get("visualLink"))
        action = self._property_text(link.get("type")) if link else ""

        target = ""
        if action == "Bookmark":
            target = self._lookup_target(
                link.get("bookmark"), context.bookmark_names, "bookmark", page_name, item_name
            )
        elif action in ("PageNavigation", "Drillthrough"):
            key = "navigationSection" if action == "PageNavigation" else "drillthroughSection"
            target = self._lookup_target(
                link.get(key), context.page_names, "page", page_name, item_name
            )
        elif action == "WebUrl":
            target = self._property_text(link.get("webUrl"))
        elif not action:
            action = "No Action"

        label = self._property_text(
            merge_object_properties(container_objects.get("title")).get("text")
        )
        if not label:
            label = self._property_text(
                merge_object_properties(visual.objects.get("text")).get("text")
            )

        return [
            ExtractedItem(
                page=page_name,
                visual_type=visual_type,
                item_name=item_name,
                table_name="",
                val_name=target,
                disp_name=label or None,
                data_type=action,
            )
        ]

    def _lookup_target(
        self, prop: Any, names: dict[str, str], kind: str, page_name: str, item_name: str
    ) -> str:
        """Resolve a button target id to its display name, flagging deleted targets."""
        target_id = self._property_text(prop)
        if not target_id or target_id.startswith("fx: "):
            return target_id  # not set, or chosen by a measure at runtime
        if target_id not in names:
            self.logger.warning(
                f"Button {item_name} on {page_name} points to a {kind} that no longer "
                f"exists: {target_id}"
            )
            return f"(missing {kind}: {target_id})"
        return names[target_id]

    def _extract_formatting_refs(
        self,
        visual: VisualDefinition,
        page_name: str,
        visual_type: str,
        existing: list[ExtractedItem],
    ) -> list[ExtractedItem]:
        """Extract fields referenced by formatting objects (e.g. conditional formatting)."""
        seen = {(item.table_name, item.val_name) for item in existing}

        items = []
        formatting = [visual.objects, visual.container_objects]
        for table_name, val_name in iter_field_refs(formatting, visual.aliases):
            if not table_name or (table_name, val_name) in seen:
                continue
            seen.add((table_name, val_name))
            items.append(
                ExtractedItem(
                    page=page_name,
                    visual_type=visual_type,
                    item_name=visual.name,
                    table_name=table_name,
                    val_name=val_name,
                    disp_name=None,
                    data_type="Formatting",
                )
            )
        return items

    def _property_text(self, prop: Any) -> str:
        """Read a formatting property: a literal value, or the field it is bound to (fx)."""
        return property_text(prop)


class FilterExtractor(BaseExtractor):
    """Extracts filter configurations from the report, pages and visuals."""

    def extract(self, data: dict) -> list:
        """Not used - call extract_filters() instead."""
        raise NotImplementedError("Use extract_filters()")

    def extract_filters(
        self,
        filters: list,
        page_name: str,
        filter_type: str,
        item_name: Optional[str] = None,
    ) -> list[ExtractedFilter]:
        """
        Extract filters from a filter pane list (report, page or visual level).

        Args:
            filters: Parsed filter list
            page_name: Page the filters belong to ("" for report level)
            filter_type: "Visual", "This Page" or "All Pages"
            item_name: Visual ID for visual filters; None uses the filter's display name

        Returns:
            List of ExtractedFilter objects
        """
        extracted = []

        for filter_obj in filters:
            if not isinstance(filter_obj, dict):
                continue

            # Filters without a "filter" key are in the pane but have no condition set
            query = filter_obj.get("filter")
            if not query:
                continue

            aliases = get_aliases(query)
            # Legacy layout uses "expression", PBIR uses "field"
            target = resolve_field(
                filter_obj.get("expression") or filter_obj.get("field") or {}, aliases
            )
            if not target:
                where = query.get("Where", [])
                target = (
                    next(iter(iter_field_refs(where[0].get("Condition", {}), aliases)), None)
                    if where
                    else None
                )
            if not target:
                self.logger.warning(
                    f"Could not resolve filter field on {page_name}. Data: {str(filter_obj)[:200]}"
                )
                continue
            table_name, val_name = target

            operator, value = self.describe_where(query.get("Where", []), aliases)

            extracted.append(
                ExtractedFilter(
                    page=page_name,
                    item_name=(
                        item_name
                        if item_name is not None
                        else filter_obj.get("displayName", val_name)
                    ),
                    filter_type=filter_type,
                    table_name=table_name,
                    val_name=val_name,
                    operator=operator,
                    value=value,
                )
            )

        return extracted

    def describe_where(self, where: list, aliases: dict[str, str]) -> tuple[str, str]:
        """
        Describe all Where clauses of a filter as (operator, value).

        Compound conditions return an empty operator and the full text as value,
        e.g. ("", "> 2019 and < 2030").
        """
        parts = [
            self.describe_condition(clause.get("Condition", {}), aliases)
            for clause in where
            if isinstance(clause, dict)
        ]
        if len(parts) == 1:
            return parts[0]
        return "", " and ".join(_join(op, val) for op, val in parts)

    def describe_condition(self, cond: dict, aliases: dict[str, str]) -> tuple[str, str]:
        """
        Describe a single filter condition as (operator, value).

        Args:
            cond: Condition node (In, Not, Comparison, And, Or, Between, Contains, ...)
            aliases: Alias -> table mapping of the filter query

        Returns:
            (operator, value) tuple
        """
        if "Not" in cond:
            operator, value = self.describe_condition(cond["Not"].get("Expression", {}), aliases)
            if operator in NEGATED_OPERATORS:
                return NEGATED_OPERATORS[operator], value
            return "not", f"({_join(operator, value)})"

        if "In" in cond:
            rows = cond["In"].get("Values")
            if rows is None:
                return "in", "(subquery, e.g. Top N)"
            values = [self._row_text(row, aliases) for row in rows]
            return ("=" if len(values) == 1 else "in"), ", ".join(values)

        if "Comparison" in cond:
            comparison = cond["Comparison"]
            operator = COMPARISON_OPERATORS.get(comparison.get("ComparisonKind"), "?")
            return operator, self._value_text(comparison.get("Right", {}), aliases)

        if "Between" in cond:
            between = cond["Between"]
            lower = self._value_text(between.get("LowerBound", {}), aliases)
            upper = self._value_text(between.get("UpperBound", {}), aliases)
            return "between", f"{lower} and {upper}"

        for key in ("And", "Or"):
            if key in cond:
                left = _join(*self.describe_condition(cond[key].get("Left", {}), aliases))
                right = _join(*self.describe_condition(cond[key].get("Right", {}), aliases))
                return "", f"{left} {key.lower()} {right}"

        for key, operator in (
            ("Contains", "contains"),
            ("StartsWith", "starts with"),
            ("EndsWith", "ends with"),
        ):
            if key in cond:
                return operator, self._value_text(cond[key].get("Right", {}), aliases)

        self.logger.warning(f"Unsupported filter condition: {str(cond)[:200]}")
        return "", "(unsupported condition)"

    def _row_text(self, row: list, aliases: dict[str, str]) -> str:
        """Describe one row of an In-condition (a tuple for multi-column filters)."""
        values = [self._value_text(value, aliases) for value in row]
        return values[0] if len(values) == 1 else f"({', '.join(values)})"

    def _value_text(self, expr: dict, aliases: dict[str, str]) -> str:
        """Describe a value expression: literal, relative date or field reference."""
        if not isinstance(expr, dict):
            return str(expr)
        if "Literal" in expr:
            return clean_literal(expr["Literal"].get("Value", ""))
        if "Now" in expr:
            return "now"
        if "DateAdd" in expr:
            date_add = expr["DateAdd"]
            base = self._value_text(date_add.get("Expression", {}), aliases)
            amount = date_add.get("Amount", 0)
            unit = TIME_UNITS.get(date_add.get("TimeUnit"), "units")
            return f"{base} {'+' if amount >= 0 else '-'} {abs(amount)} {unit}"
        if "DateSpan" in expr:
            date_span = expr["DateSpan"]
            base = self._value_text(date_span.get("Expression", {}), aliases)
            unit = TIME_UNITS.get(date_span.get("TimeUnit"), "units")
            return f"start of {unit.rstrip('s')} ({base})"
        resolved = resolve_field(expr, aliases)
        if resolved:
            return f"{resolved[0]}[{resolved[1]}]"
        return str(expr)[:100]


def _join(operator: str, value: str) -> str:
    """Join operator and value, skipping an empty operator."""
    return f"{operator} {value}".strip()


class PageExtractor(BaseExtractor):
    """Orchestrates extraction of all elements from a page."""

    def __init__(self, config: Config, logger: logging.Logger = None, skip_template: bool = False):
        """
        Initialize page extractor.

        Args:
            config: Configuration from data.yaml
            logger: Logger instance
            skip_template: Ignore pages named "Template" (opt-in: a real page with that name
                would otherwise lose its fields and make them look unused)
        """
        super().__init__(config, logger)
        self.skip_template = skip_template
        self.visual_extractor = VisualExtractor(config, logger)
        self.filter_extractor = FilterExtractor(config, logger)

    def extract(
        self, page: PageDefinition, context: ReportContext = None
    ) -> tuple[list[ExtractedItem], list[ExtractedFilter]]:
        """
        Extract all items and filters from a page.

        Args:
            page: Normalised page (see readers.py)
            context: Report-wide lookups (page and bookmark names)

        Returns:
            Tuple of (items list, filters list)
        """
        items = []
        filters = []

        page_name = page.display_name or "Unknown"

        if self.skip_template and page_name == "Template":
            return items, filters

        for visual in page.visuals:
            items.extend(self.visual_extractor.extract(visual, page_name, context))
            if visual.filters:
                filters.extend(
                    self.filter_extractor.extract_filters(
                        visual.filters, page_name, "Visual", visual.name
                    )
                )

        filters.extend(self.filter_extractor.extract_filters(page.filters, page_name, "This Page"))

        return items, filters
