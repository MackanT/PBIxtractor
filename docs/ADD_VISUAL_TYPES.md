# Adding New Visual Types to PBIxtractor

## Overview
Visual type configuration lives in `data.yaml`. No Python code changes are needed to add new visual types.

Custom visuals from AppSource (types like `PowerApps_PBI_CV_<guid>` or `<name><13 digits>`) need no
configuration: they are recognised automatically, shown as "<Name> (custom visual)" and their
fields are read generically with the roles their author defined.

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
  
  # Button types (item type "Button", display name as is)
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

### How a Visual Is Extracted (`extract_types`)

Every visual type is extracted as `standard` unless listed under `extract_types`:

```yaml
extract_types:
  actionButton: button   # action type, target (bookmark/page/URL) and label
  shape: skip            # decoration, not documented
  image: skip
  textbox: skip
```

- `standard`: the fields in the visual's query and formatting (charts, tables, slicers, ...)
- `button`: action type, target and label, like the built-in buttons
- `skip`: not documented. Shapes/images **with an action** (clickable shapes) are still
  documented as buttons.

## Examples

### Example 1: Adding a New Standard Visual

**Power BI Visual Type:** `waterfallChart`

**Configuration:**
```yaml
standard_visuals:
  - waterfallChart  # Displays as "Waterfall Chart"
```

### Example 2: Documenting Images Too

**Power BI Visual Type:** `image` (skipped by default)
**Desired Display Name:** "Image"

**Configuration:** give it a display name *and* remove it from the skip list:
```yaml
special_visuals:
  image: {display_name: "Image", item_type: "Visual"}

extract_types:
  image: standard   # or delete the line
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
uv run --frozen pbixtractor extract "C:\Reports\MyReport.pbix"
```

If you see a warning like:
```
Unknown visual type: visualTypeName (first on <page>). Extracting fields generically - add it to data.yaml.
```
(logged once per visual type and run)

the visual's fields were still documented, but it has no display name yet: add it to
`standard_visuals` or `special_visuals` (and to `extract_types` if it is not a normal
query visual).

## Architecture Notes

`src/pbixtractor/config.py` loads `data.yaml` once into a `Config`: supported visual types
(everything in `standard_visuals` and `special_visuals`), projection role labels, extract types
and DAX function names. `VisualExtractor` in `extractors.py` picks a handler per visual from
`extract_types` (`standard` / `button`; `skip` has none).

The display names are handled by the `VisualTypeMapper` class in `src/pbixtractor/visual_helpers.py`. The mapper:

1. Loads configuration from `data.yaml`
2. Checks `special_visuals` for custom mappings
3. Falls back to auto-generated names for `standard_visuals`
4. Recognises custom visuals (`custom_visual_name()`)
5. Returns `(item_type, display_name)` tuple for each visual type

This modular approach means **no Python code changes** are needed when Power BI introduces new visual types - just update the YAML configuration.
