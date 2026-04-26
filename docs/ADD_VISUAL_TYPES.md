# Adding New Visual Types to PBI-Ixtractor

## Overview
Visual type configuration is now fully modular and managed through the `data.yaml` file. No Python code changes are needed to add new visual types.

## YAML Configuration Structure

The visual type metadata is defined in `src/pbixtractor/data/data.yaml` under the `visual_type_metadata` section:

```yaml
visual_type_metadata:
  # Special visuals with custom display names
  special_visuals:
    tableEx: {display_name: "Table", item_type: "Visual"}
    pivotTable: {display_name: "Matrix", item_type: "Visual"}
    card: {display_name: "Card", item_type: "Visual"}
    cardVisual: {display_name: "Card (new)", item_type: "Visual"}
    gauge: {display_name: "Gauge", item_type: "Visual"}
    slicer: {display_name: "Slicer", item_type: "Slicer"}
    advancedSlicerVisual: {display_name: "Slicer (new)", item_type: "Slicer"}
    Group: {display_name: "Panel", item_type: "Group"}
    actionButton: {display_name: "Button", item_type: "Button"}
  
  # Standard visuals (auto-generate display names from camelCase)
  standard_visuals:
    - clusteredColumnChart
    - lineChart
    - pieChart
    # ... add more here ...
  
  # Button types (legacy)
  button_types:
    - Bookmark
    - PageNavigation
    - Button
```

## How to Add New Visual Types

### For Standard Visuals (Auto-Generated Display Names)

If the visual type follows camelCase naming (e.g., `barChart`, `scatterChart`), simply add it to the `standard_visuals` list:

```yaml
standard_visuals:
  - clusteredColumnChart
  - barChart  # NEW: Displays as "Bar Chart"
  - scatterChart  # NEW: Displays as "Scatter Chart"
```

The system automatically converts camelCase to Display Name:
- `clusteredColumnChart` → "Clustered Column Chart"
- `barChart` → "Bar Chart"
- `scatterChart` → "Scatter Chart"

### For Special Visuals (Custom Display Names)

If the visual needs a custom display name or special item type, add it to `special_visuals`:

```yaml
special_visuals:
  tableEx: {display_name: "Table", item_type: "Visual"}
  customVisual: {display_name: "My Custom Visual", item_type: "Visual"}  # NEW
  specialSlicer: {display_name: "Special Slicer", item_type: "Slicer"}  # NEW
```

**Parameters:**
- `display_name`: The friendly name shown in output files
- `item_type`: One of `"Visual"`, `"Slicer"`, `"Button"`, or `"Group"`

## Examples

### Example 1: Adding a New Standard Visual

**Power BI Visual Type:** `waterfallChart`

**Configuration:**
```yaml
standard_visuals:
  - waterfallChart  # Displays as "Waterfall Chart"
```

### Example 2: Adding a Custom Visual with Special Name

**Power BI Visual Type:** `image`  
**Desired Display Name:** "Image"

**Configuration:**
```yaml
special_visuals:
  image: {display_name: "Image", item_type: "Visual"}
```

### Example 3: Adding a New Slicer Type

**Power BI Visual Type:** `timelineSlicer`  
**Desired Display Name:** "Timeline Slicer"

**Configuration:**
```yaml
special_visuals:
  timelineSlicer: {display_name: "Timeline Slicer", item_type: "Slicer"}
```

## Testing

After modifying `data.yaml`, run the extraction:

```powershell
uv run pbixtractor --test
```

If you see a warning like:
```
New Visual type not yet supported! visualTypeName
```

This means the visual type needs to be added to either `standard_visuals` or `special_visuals` in the YAML configuration.

## Architecture Notes

The visual type mapping is handled by the `VisualTypeMapper` class in `src/pbixtractor/visual_helpers.py`. The mapper:

1. Loads configuration from `data.yaml`
2. Checks `special_visuals` for custom mappings
3. Falls back to auto-generated names for `standard_visuals`
4. Returns `(item_type, display_name)` tuple for each visual type

This modular approach means **no Python code changes** are needed when Power BI introduces new visual types - just update the YAML configuration.
