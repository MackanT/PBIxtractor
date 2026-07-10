# run_cmd() Simplification Plan

## Current State
The `run_cmd()` function is **~1,313 lines** and does too many things:
1. Sets up output directories
2. Checks if Excel file is open  
3. Generates TSV file
4. Extracts report data
5. Processes TSV data into DataFrame  
6. Builds relationships
7. Generates relationship graph
8. Creates two Excel workbooks with multiple sheets each
9. Handles DAX syntax highlighting
10. Saves logs

## Refactoring Strategy

### Phase 1: Extract helper functions (DONE ✓)
- Created module-level DAX analysis functions:
  - `find_functions()` - Find DAX functions in code
  - `find_measures()` - Extract measure references
  - `find_columns()` - Extract column references  
  - `parse_tsv_object_name()` - Parse TSV object names
- These replace the duplicate definitions inside `run_cmd()`

### Phase 2: Simplify run_cmd() structure (IN PROGRESS)  
Keep the existing logic but make it more readable:

```python
def run_cmd():
    # 1. Setup and validation
    output_path =setup_output_directory()
    if excel_file_open():
        return error()
    
    # 2. Generate TSV if needed
    ensure_tsv_exists()
    
    # 3. Extract report data
    report_info, report_filters = extract_report_data()
    
    # 4. Process TSV data  
    df, unused_columns, all_tables, all_hierarchies = process_tsv_data()
    
    # 5. Build relationships
    df_relations = build_relationships_dataframe(all_relationships)
    generate_relationship_graph()
    
    # 6. Update unused columns
    update_unused_columns(unused_columns, report_info, report_filters)
    
    # 7. Create Excel workbooks
    create_documentation_workbook()  # Main workbook
    create_data_workbook()  # Data workbook
    
    # 8. Save logs
    return save_logs_if_needed()
```

### Phase 3: Extract large sections to functions (FUTURE)
- `process_tsv_data()` - ~100 lines → separate function
- `build_relationships_dataframe()` - ~50 lines → separate function  
- `generate_relationship_graph()` - ~50 lines → separate function
- `create_documentation_workbook()` - ~400 lines → separate function
- `create_data_workbook()` - ~500 lines → separate function

### Phase 4: Further modularization (FUTURE)
- Extract DAX formatting to `dax_formatter.py`  
- Extract Excel generation to `excel_writer.py`
- Extract graph generation to `graph_generator.py`

## Benefits
- **Readability**: Main flow visible at a glance
- **Testability**: Each function can be tested independently  
- **Maintainability**: Changes isolated to specific functions
- **Reusability**: Functions can be used elsewhere
- **Debugging**: Easier to identify where issues occur

## Current Progress
✓ Helper functions extracted to module level
✓ Imports updated to use utils functions
✓ `run_cmd()` simplified to show main flow
⏳ Need to create the called helper functions
