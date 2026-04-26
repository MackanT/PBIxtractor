"""Main extractor module for PBI-Ixtractor."""

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from zipfile import ZipFile

import matplotlib
import networkx as nx
import pandas as pd
import xlsxwriter
import yaml
from matplotlib import pyplot as plt

matplotlib.use("agg")

# Local imports
from .constants import DEFAULT_COLORS, DESCRIPT_TAG, REPORT_COLUMNS, UI_COLORS
from .logger import get_logger, setup_logger
from .utils import (
    ensure_directory,
    find_nth_occurrence,
    find_vars,
    is_excel_open_with_file,
    rgba_tuple_to_hex,
    write_to_excel,
)

# Initialize logger
logger, log_capture = setup_logger("pbixtractor", level=logging.INFO, capture=True)

# Global state variables
LOG_DATA = True
SAVE_NAME = ""
_PBIX_ = [None, None]
_BIM_ = [None, None]


# ============================================================================
# DAX Analysis Helper Functions
# ============================================================================


def find_functions(dax_code: str, known_functions: list) -> list[str]:
    """
    Find all known DAX functions used in code.

    Args:
        dax_code: DAX code string
        known_functions: List of known function names

    Returns:
        List of functions found in the code
    """
    used_functions = [] 
    for func in known_functions:
        if func in dax_code:
            used_functions.append(func)
    return used_functions


def find_measures(dax_code: str) -> list[str]:
    """
    Extract measure references from DAX code.

    Args:
        dax_code: DAX code string

    Returns:
        List of unique measure references (e.g., "[Measure Name]")
    """
    pattern = r"\[.*?\]"
    all_measures = re.findall(pattern, dax_code)
    return list(set(all_measures))


def find_columns(dax_code: str) -> list[tuple[str, str]]:
    """
    Extract column references from DAX code.

    Args:
        dax_code: DAX code string

    Returns:
        List of (table, column) tuples
    """
    pattern = re.compile(r"(\w+)\[(.*?)\]")
    all_columns = re.findall(pattern, dax_code)
    return list(set(all_columns))


def parse_tsv_object_name(object_name: str) -> tuple[str, str, str]:
    """
    Parse TSV object name to extract type, table, and column.

    Args:
        object_name: Object name from TSV (e.g., "Model.Table.C.[Column]")

    Returns:
        Tuple of (type, table, column) where type is Table/Column/Hierarchy/Measure
    """
    data_type = "Table"
    start_pos = find_nth_occurrence(".", object_name, 2) + 1
    end_pos = find_nth_occurrence(".", object_name, 3)

    if end_pos == -1:
        table = object_name[start_pos:]
    else:
        table = object_name[start_pos:end_pos]

    column = ""
    if any(substring in object_name for substring in [".C.", ".H.", ".M."]):
        start_pos = find_nth_occurrence(".", object_name, 4) + 1
        end_pos = find_nth_occurrence(".", object_name, 5)

        if end_pos == -1 or end_pos < len(object_name):
            column = object_name[start_pos:]
        else:
            column = object_name[start_pos:end_pos]

        column = column.strip("[]")

        if ".C." in object_name:
            data_type = "Column"
        elif ".H." in object_name:
            data_type = "Hierarchy"
        elif ".M." in object_name:
            data_type = "Measure"

    return (data_type, table, column)


# Load configuration from YAML file
try:
    from .data import YAML_FILE
except ImportError:
    from pathlib import Path

    YAML_FILE = Path(__file__).parent / "data" / "data.yaml"

try:
    with open(YAML_FILE, "r") as yaml_file:
        config_data = yaml.safe_load(yaml_file)

    visual_type_list = config_data.get("visual_types", [])
    data_types_raw = config_data.get("data_types", [])
    data_type_list = [[dt["name"], dt["friendly_name"]] for dt in data_types_raw]
    known_functions = config_data.get("function_names", [])
    extraction_rules = config_data.get("extraction_rules", {})
    filter_rules = config_data.get("filter_rules", {})

    # Create visual type mapper for display names
    from .visual_helpers import create_visual_mapper

    visual_mapper = create_visual_mapper(config_data)

except Exception as e:
    print(f"Error loading YAML configuration file: {YAML_FILE}")
    print(f"Exception: {e}")
    sys.exit(1)


class ReportExtractor:
    """Extracts visual and filter data from Power BI .pbix files."""

    def __init__(self, path: str, name: str):
        """
        Initialize report extractor.

        Args:
            path: Directory path containing the .pbix file
            name: Name of the .pbix file
        """
        self.path = path
        self.name = name
        self.result = []
        self.filters = []
        self.logger = get_logger("pbixtractor")

        # Import modular extractors
        from .extractors import PageExtractor

        # Initialize page extractor with config
        self.page_extractor = PageExtractor(
            config=extraction_rules,
            visual_types=visual_type_list,
            data_types=data_type_list,
            logger=self.logger,
        )

    def add_item(
        self,
        page: str,
        visual_type: str,
        item_name: str,
        table_name: str,
        val_name: str,
        disp_name: str,
        data_type: str,
    ) -> None:
        """
        Store extracted item data.

        Args:
            page: Page name
            visual_type: Type of visual element
            item_name: Visual item identifier
            table_name: Table name
            val_name: Value/field name
            disp_name: Display name
            data_type: Data type
        """
        self.result.append(
            [
                page,
                visual_type,
                item_name,
                table_name,
                val_name,
                disp_name,
                data_type,
            ]
        )

    def add_filter(
        self,
        page: str,
        item_name: str,
        filter_type: str,
        table_name: str,
        val_name: str,
        operator: str,
        value: str,
    ) -> None:
        """
        Store extracted filter data.

        Args:
            page: Page name
            item_name: Visual item identifier
            filter_type: Type of filter
            table_name: Table name
            val_name: Field name
            operator: Filter operator
            value: Filter value
        """
        self.filters.append(
            [
                page,
                item_name,
                filter_type,
                table_name,
                val_name,
                operator,
                value,
            ]
        )

    def extract(self) -> None:
        """
        Extract all data from the Power BI report.

        This method:
        1. Extracts the .pbix file (ZIP archive)
        2. Loads the report layout JSON
        3. Parses visual containers and filters
        4. Extracts items and filters using PageExtractor
        5. Cleans up temporary files
        """
        # Prepare extraction folder
        temp_folder = f"{self.path}/temp_{self.name[:-5]}"
        try:
            shutil.rmtree(temp_folder)
        except FileNotFoundError:
            self.logger.debug(f"Temporary folder {temp_folder} not present")

        # Extract .pbix file (it's a ZIP archive)
        with ZipFile(f"{self.path}/{self.name}", "r") as zip_file:
            zip_file.extractall(temp_folder)

        # Load report layout JSON
        layout_path = f"{temp_folder}/Report/Layout"
        with open(layout_path, "r", encoding="utf-16 le") as layout_file:
            report_layout = json.loads(layout_file.read())

        # Parse nested JSON strings in the layout
        report_layout["config"] = json.loads(report_layout["config"])
        for section in report_layout["sections"]:
            for visual_container in section["visualContainers"]:
                for key in ["config", "filters", "query", "dataTransforms"]:
                    if key in visual_container:
                        visual_container[key] = json.loads(visual_container[key])

        # Extract data from each page using PageExtractor
        for page in report_layout["sections"]:
            items, filters = self.page_extractor.extract(page)

            # Convert Pydantic models to legacy list format
            for item in items:
                self.result.append(item.to_list())

            for filter_obj in filters:
                self.filters.append(filter_obj.to_list())

        # Clean up temporary folder
        shutil.rmtree(temp_folder)


def run_ui():
    """Launch the DearPyGUI-based user interface."""
    from tkinter import filedialog

    import dearpygui.dearpygui as dpg

    dpg.create_context()

    def show_and_hide(tag: str, msg: str, type: str = None):
        dpg.configure_item(tag, color=UI_COLORS[type], show=True)
        dpg.set_value(tag, msg)
        threading.Thread(target=lambda: wait_and_show(tag)).start()

    def wait_and_show(tag: str):
        time.sleep(8)
        dpg.hide_item(tag)

    def progress_bar(tag: str):
        threading.Thread(target=lambda: increment_loader(tag)).start()

    def increment_loader(tag: str):
        dpg.configure_item(tag, color=UI_COLORS["W"])
        i = 0
        while not stop_event.is_set():
            time.sleep(0.2)
            dpg.show_item(tag)
            dpg.set_value(tag, "." * i)
            i = (i + 1) % 16

    def run_extractor():
        global stop_event
        if _PBIX_ != [None, None] and _BIM_ != [None, None]:
            clear_textbox(container)
            stop_event = threading.Event()
            progress_bar("runTextExtra")
            run_code = run_cmd()
            stop_event.set()
            time.sleep(0.5)
            if run_code == "Log":
                show_and_hide(
                    "runTextExtra",
                    f"Documention generated with warnings. See output/{SAVE_NAME}/logs or Logs tab below for more information",
                    "Y",
                )
                update_log()
            elif run_code != "Success":
                show_and_hide("runTextExtra", run_code, "R")
            else:
                show_and_hide(
                    "runTextExtra",
                    f"Documentation generated without any issues. See: output/{SAVE_NAME}/{SAVE_NAME}.xlsx",
                    "G",
                )

    def update_log():
        """Update log display with captured log messages."""
        error_msg = log_capture.get_logs().split("\n")
        for msg in error_msg:
            if msg == "":
                continue

            # Determine color based on log level
            if "DEBUG:" in msg:
                c = UI_COLORS["W"]
            elif "INFO:" in msg:
                c = UI_COLORS["W"]
            elif "WARNING:" in msg:
                c = UI_COLORS["Y"]
            elif "ERROR:" in msg:
                c = UI_COLORS["O"]
            else:
                c = UI_COLORS["R"]

            # Remove log level prefix for display
            if ":" in msg:
                display_msg = msg.split(":", 1)[1].strip()
            else:
                display_msg = msg

            add_colored_text_at_top(container, display_msg, c)

    def disable_buttons():
        for tag in ["runPBIX", "genTSV"]:
            dpg.configure_item(tag, enabled=False)

    def enable_buttons():
        for tag in ["runPBIX", "genTSV"]:
            dpg.configure_item(tag, enabled=True)

    def generate_tsv():
        global stop_event
        if _PBIX_ != [None, None] and _BIM_ != [None, None]:
            stop_event = threading.Event()
            progress_bar("tsvTextExtra")
            tsv_result = gen_tsv(force=True)
            stop_event.set()
            time.sleep(0.5)
            if tsv_result == "NoTabEd":
                show_and_hide(
                    "tsvTextExtra",
                    "Could Not Find Tabular Editor 2 on PC. Please add location in Input/TabularEditorLocations.txt",
                    "R",
                )
            else:
                show_and_hide("tsvTextExtra", "TSV File generated successfully!", "G")

    ### UI Functions ###
    def load_file(input):
        global pbix_file_path, bim_file_path, unique_data_tables, _PBIX_, _BIM_, SAVE_NAME

        if input == "pbix":
            pbix_file_path = filedialog.askopenfilename(filetypes=[("pbix files", "*.pbix")])
            if pbix_file_path:
                dpg.set_value("pbix_file_path_label", f"Selected File: {pbix_file_path}")

                _PBIX_ = [
                    pbix_file_path[pbix_file_path.rfind("/") + 1 : -5],
                    pbix_file_path[: pbix_file_path.rfind("/")],
                ]

                SAVE_NAME = _PBIX_[0]
                dpg.set_value("outputFileName", SAVE_NAME)

                bim_file_path = pbix_file_path[:-4] + "bim"
                dpg.configure_item("bim_file_path_label", show=True)
                if not os.path.exists(bim_file_path):
                    dpg.configure_item("BimSelector", show=True, enabled=True)
                    dpg.set_value("bim_file_path_label", "Selected File: None")
                    _BIM_ = [None, None]
                    disable_buttons()
                else:
                    dpg.set_value("bim_file_path_label", f"Selected File: {bim_file_path}")
                    _BIM_ = [
                        bim_file_path[bim_file_path.rfind("/") + 1 : -4],
                        bim_file_path[: bim_file_path.rfind("/")],
                    ]
                    enable_buttons()

                rep_ex = ReportExtractor(_PBIX_[1], _PBIX_[0] + ".pbix")
                rep_ex.extract()
                report_info = pd.DataFrame(
                    rep_ex.result,
                    columns=[
                        "Page",
                        "Visual Type",
                        "Visual ID",
                        "Table",
                        "Name",
                        "Display Name",
                        "Type",
                    ],
                )
                unique_data_tables = list(report_info["Table"].unique())
                del report_info, rep_ex

                # Add all found tables to measure table dropdown
                for table_name in unique_data_tables:
                    items = dpg.get_item_configuration("defMeasTable")["items"]
                    items.append(table_name)
                    dpg.configure_item("defMeasTable", items=items)

        elif input == "bim":
            bim_file_path = filedialog.askopenfilename(filetypes=[("bim files", "*.bim")])
            if bim_file_path:
                dpg.set_value("bim_file_path_label", f"Selected File: {bim_file_path}")
                _BIM_ = [
                    bim_file_path[bim_file_path.rfind("/") + 1 : -4],
                    bim_file_path[: bim_file_path.rfind("/")],
                ]

            if bim_file_path and pbix_file_path:
                enable_buttons()

    def set_measure_table_name(sender, app_data):
        global default_measure_table
        default_measure_table = dpg.get_value("defMeasTable")

    def set_output_file_name(sender, app_data):
        global SAVE_NAME
        SAVE_NAME = dpg.get_value("outputFileName")

    def set_description_tag(sender, app_data):
        global DESCRIPT_TAG
        DESCRIPT_TAG = dpg.get_value("descriptionTag")

    def find_color(name):
        old_color = None
        name = name.split(" ")[0]
        for i, color in enumerate(DEFAULT_COLORS):
            if color[0] == name:
                old_color = color[1]
                break

        return [i, old_color]

    def set_colors(sender, app_data):
        button_type = dpg.get_value("radioColors")
        dpg.set_value("colorWheel", find_color(button_type)[1])

    def update_colors(sender, app_data):
        button_type = dpg.get_value("radioColors")
        color_index = find_color(button_type)[0]

        new_color = []
        for col in dpg.get_value("colorWheel"):
            new_color.append(int(col))
        # Note: This modifies the imported constant - consider using a mutable copy
        DEFAULT_COLORS[color_index][1] = new_color

    def toggle_log_toggle(sender, app_data, user_data):
        global LOG_DATA
        LOG_DATA = dpg.get_value(sender)

    def add_colored_text_at_top(container, text, color):
        new_text = dpg.add_text(text, parent=container, color=color)
        children = dpg.get_item_children(container)[1]
        if len(children) > 1:
            dpg.move_item(new_text, parent=container, before=children[0])

    def clear_textbox(container):
        children = dpg.get_item_children(container, 1)
        for child in children:
            if dpg.get_item_type(child) == "mvAppItemType::mvText":
                dpg.delete_item(child)

    def add_input(version):
        cwd = os.getcwd() + "\\Input\\"

        # Create Input directory if it doesn't exist
        if not os.path.exists(cwd):
            os.makedirs(cwd)

        if version == "dataType":
            info_tag = "dataTypeInputInfo"
            file_name = "DataTypes.csv"
            val1 = dpg.get_value("dataTypeInputO")
            val2 = dpg.get_value("dataTypeInputP")

            val1_state = False
            val2_state = False
            if val1 == "":
                val1_state = True
            if val2 == "":
                val2_state = True

            if val1_state and val2_state:
                msg = "Both Inputs are non-valid!"
            elif val2_state:
                msg = "Non-valid input in Data Type PBI!"
            elif val1_state:
                msg = "Non-valid input in Data Type Output!"

            if val1_state or val2_state:
                show_and_hide(info_tag, msg, "Y")
                return

            val1 = f"{val2},{val1}"

        elif version == "functionName":
            info_tag = "functionNameInputInfo"
            file_name = "FunctionNames.csv"
            val1 = dpg.get_value("functionNameInput")
            if val1 == "":
                show_and_hide(info_tag, "Cannot enter blank function name!", "Y")
                return

        elif version == "visualType":
            info_tag = "visualTypeInputInfo"
            file_name = "VisualTypes.csv"
            val1 = dpg.get_value("visualTypeInput")
            if val1 == "":
                show_and_hide(info_tag, "Cannot enter blank visual type!", "Y")
                return

        elif version == "TELocation":
            info_tag = "TELocationInputInfo"
            file_name = "TabularEditorLocations.txt"
            val1 = dpg.get_value("TELocation")
            if val1 == "":
                show_and_hide(
                    info_tag,
                    "Cannot enter blank save location of Tabular Editor 2!",
                    "Y",
                )
                return

        else:
            return

        if file_name[-3:] == "txt":
            with open(cwd + file_name, "a") as txt_file:
                txt_file.write(val1 + "\n")
        else:
            with open(cwd + file_name, "a", newline="") as csv_file:
                csv_file.write(val1 + "\n")

        show_and_hide(info_tag, "Data saved successfully!", "G")

    with dpg.theme() as disabled_theme:
        with dpg.theme_component(dpg.mvButton, enabled_state=False):
            dpg.add_theme_color(dpg.mvThemeCol_Text, [120, 120, 120])
            dpg.add_theme_color(dpg.mvThemeCol_Button, [75, 75, 77])
            dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, [75, 75, 77])
            dpg.add_theme_color(dpg.mvThemeCol_ButtonActive, [75, 75, 77])
    dpg.bind_theme(disabled_theme)

    with dpg.texture_registry(show=False):
        width, height, channels, data = dpg.load_image("logo_large.png")
        dpg.add_static_texture(width=width, height=height, default_value=data, tag="logo_texture")

    with dpg.window(label="PB-Ixtractor", width=1000, height=800):
        with dpg.collapsing_header(label="About"):
            dpg.add_image("logo_texture")
        with dpg.collapsing_header(label="File Settings", default_open=True, tag="File Settings"):
            dpg.add_button(label="Select .pbix File", callback=lambda: load_file("pbix"))
            dpg.add_text("Selected File: No .pbix file selected", tag="pbix_file_path_label")

            dpg.add_spacer(height=3)

            dpg.add_button(
                label="Select .bim File",
                enabled=True,
                tag="BimSelector",
                callback=lambda: load_file("bim"),
            )
            dpg.add_text("Selected File: No .bim file selected", tag="bim_file_path_label")

            dpg.add_spacer(height=3)

            dpg.add_spacer(height=10)
            dpg.add_combo(
                label="Used Measures Table. Use None if Non-Existant",
                items=["None"],
                default_value="None",
                callback=set_measure_table_name,
                tag="defMeasTable",
                show=False,
                enabled=False,
            )

            dpg.add_spacer(height=2)
            dpg.add_input_text(
                label="Output File Name",
                tag="outputFileName",
                callback=set_output_file_name,
            )
            dpg.set_value("outputFileName", SAVE_NAME)

            dpg.add_spacer(height=2)
            dpg.add_input_text(
                label="Description Tag",
                tag="descriptionTag",
                callback=set_description_tag,
            )
            dpg.set_value("descriptionTag", DESCRIPT_TAG)

            dpg.add_spacer(height=10)
            dpg.add_button(
                label="Regenerate tsv file",
                tag="genTSV",
                enabled=False,
                callback=generate_tsv,
            )
            dpg.add_text(
                "To properly extract data types for measures, ensure the .pbix file is open in PBI desktop!",
                tag="tsvText",
            )
            dpg.add_text(
                "",
                show=False,
                tag="tsvTextExtra",
            )
            dpg.add_spacer(height=10)
            dpg.add_button(
                label="Run PB-Ixtractor",
                tag="runPBIX",
                enabled=False,
                callback=run_extractor,
            )
            dpg.add_text(
                "Generates the documentation files, will generate the .tsv file if it does not exist.",
                tag="runText",
            )
            dpg.add_text(
                "",
                show=False,
                tag="runTextExtra",
            )
            dpg.add_spacer(height=20)

        with dpg.collapsing_header(
            label="Additional Settings", default_open=False, tag="Additional Settings"
        ):
            dpg.add_checkbox(
                label="Enable Error Logging",
                callback=toggle_log_toggle,
                tag="log_toggle",
                default_value=True,
            )
            dpg.add_spacer(height=5)

            with dpg.group(horizontal=True):
                dpg.add_color_picker(
                    default_value=(49, 101, 187, 255),
                    label="Selected Color",
                    tag="colorWheel",
                    width=200,
                    height=200,
                    callback=update_colors,
                )
                dpg.add_spacer(width=20)
                dpg.add_radio_button(
                    label="Color Types",
                    items=[
                        "Functions - Misc PBI Functions. See Input/FunctionNames.csv.",
                        "Measures - User Created Measures and Default Columns.",
                        "Return - The return statement.",
                        "Variables - User-Defined Variables Within a Measure.",
                        "Comments - Comments in Measures.",
                        "Quotes - Quoted Text in Measures.",
                        "VarNames - The Word VAR in Measures.",
                    ],
                    callback=set_colors,
                    tag="radioColors",
                )

        with dpg.collapsing_header(label="Logs", default_open=False, tag="Logs"):
            with dpg.group(horizontal=True, tag="log_data"):
                with dpg.child_window(width=880, height=300):
                    container = dpg.add_child_window(width=960, height=280)

                with dpg.child_window(width=80, height=300):
                    texts = ["Debug", "Info", "Warning", "Error", "Critical"]
                    for i, color in enumerate(UI_COLORS.values()):
                        with dpg.drawlist(width=20.0, height=20.0, tag=f"drawlist{i}"):
                            dpg.draw_rectangle(
                                pmin=[0.0, 0.0],
                                pmax=[20.0, 20.0],
                                color=color,
                                fill=color,
                            )
                        dpg.add_text(texts[i])

        with dpg.collapsing_header(label="User Input", default_open=False, tag="User Input"):
            dpg.add_input_text(
                label="Data Type PBI",
                tag="dataTypeInputP",
            )
            dpg.add_input_text(
                label="Data Type Output",
                tag="dataTypeInputO",
            )
            dpg.add_button(
                label="Data Type",
                tag="dataTypeInputButton",
                callback=lambda: add_input("dataType"),
            )
            dpg.add_text(
                "",
                show=False,
                tag="dataTypeInputInfo",
            )
            dpg.add_spacer(height=5)

            dpg.add_input_text(
                label="FunctionName",
                tag="functionNameInput",
            )
            dpg.add_button(
                label="Function Name",
                tag="functionNameInputButton",
                callback=lambda: add_input("functionName"),
            )
            dpg.add_text(
                "",
                show=False,
                tag="functionNameInputInfo",
            )
            dpg.add_spacer(height=5)

            dpg.add_input_text(
                label="Visual Type",
                tag="visualTypeInput",
            )
            dpg.add_button(
                label="Visual Type",
                tag="visualTypeInputButton",
                callback=lambda: add_input("visualType"),
            )
            dpg.add_text(
                "",
                show=False,
                tag="visualTypeInputInfo",
            )
            dpg.add_spacer(height=5)

            dpg.add_input_text(
                label="Tabular Editor Location",
                tag="TELocation",
            )
            dpg.add_button(
                label="TE Location",
                tag="TELocationButton",
                callback=lambda: add_input("TELocation"),
            )
            dpg.add_text(
                "",
                show=False,
                tag="TELocationInputInfo",
            )

    # Window
    dpg.create_viewport(title="PB-Ixtractor", width=1000, height=800, large_icon="logo.ico")
    dpg.setup_dearpygui()
    dpg.show_viewport()
    dpg.start_dearpygui()
    dpg.destroy_context()


def gen_tsv(force: bool = False):
    # Use output directory instead of current directory
    output_dir = os.path.join(os.getcwd(), "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    cwd = os.path.join(output_dir, SAVE_NAME)

    if not os.path.exists(cwd):
        os.makedirs(cwd)

    def find_tabular_editor_path() -> str:
        target_exe = Path("TabularEditor.exe")

        input_dir = os.getcwd() + "\\Input\\TabularEditorLocations.txt"

        # Ensure Input directory exists
        input_folder = os.path.dirname(input_dir)
        if not os.path.exists(input_folder):
            os.makedirs(input_folder)

        # Default directories to search
        if not os.path.exists(input_dir):
            common_directories = [
                Path("C:\\Program Files"),
                Path("C:\\Program Files (x86)"),
            ]
            with open(input_dir, "w") as file:
                for directory in common_directories:
                    file.write(str(directory) + "\n")
        else:
            with open(input_dir, "r") as file:
                common_directories = file.readlines()

            for i, row in enumerate(common_directories):
                common_directories[i] = Path(row.strip())

        for directory in common_directories:
            target_path = directory / "Tabular Editor" / target_exe
            if target_path.exists():
                result = str(target_path)
                result = '"' + result + '"'
                return result

        # Return None if the executable file is not found
        return None

    tab_edit_path = find_tabular_editor_path()
    if tab_edit_path is None:
        return "NoTabEd"

    if force and os.path.exists(f"{cwd}\\TabularScript.cs"):
        os.remove(f"{cwd}\\TabularScript.cs")

    ## If file not present, create it!
    if not os.path.isfile(f"{cwd}\\TabularScript.cs"):
        cwd_parsed = cwd.replace("\\", "//")

        c_code = f"""
    // Auto Formatting
    Model.AllMeasures.FormatDax();

    // Construct a list of objects:
    var objects = new List<TabularNamedObject>();
    objects.AddRange(Model.Tables);
    objects.AddRange(Model.AllColumns);
    objects.AddRange(Model.AllHierarchies);
    objects.AddRange(Model.AllLevels);
    objects.AddRange(Model.AllMeasures);
    objects.AddRange(Model.Relationships);
    objects.AddRange(Model.AllPartitions);


    // Get their properties in TSV format (tabulator-separated):
    var tsv = ExportProperties(objects,"Name,Description,SourceColumn,Expression,FormatString,DataType,DisplayFolder"); // Updated to include FormatString + DisplayFolder
    //var tsv = ExportProperties(objects);

    // Save the TSV to a file:
    SaveFile("{cwd_parsed}//documentation.tsv", tsv);
    """
        with open(f"{cwd}\\TabularScript.cs", "w", encoding="utf-8") as file:
            file.write(c_code)

    tsv_path = Path(f"{cwd}\\documentation.tsv")

    if os.path.exists(tsv_path):
        os.remove(tsv_path)

    command = f'& {tab_edit_path} "{_BIM_[1]}/{_BIM_[0]}.bim" -S "{cwd}\\TabularScript.cs"'
    process = subprocess.Popen(["powershell", "-Command", command])
    process.wait()

    ## Wait for file gen -
    def wait_for_file(file_path: str, timeout: int = None):
        """
        Waits for maximum timeout seconds or until file_path has been created
        """
        start_time = time.time()
        while not os.path.exists(file_path):
            if timeout is not None and time.time() - start_time > timeout:
                raise TimeoutError(f"File {file_path} not found within the timeout period")
            time.sleep(0.1)

    wait_for_file(file_path=f"{cwd}\\documentation.tsv", timeout=5)


def run_test_extraction():
    """
    Run extraction with hardcoded test file paths.
    Useful for development and testing.

    To use this function, update the paths below to point to your test files.
    """
    global SAVE_NAME, _BIM_, _PBIX_, LOG_DATA, REPORT_LOG

    test_pbix_path = r"C:\Users\MarcusToftås\OneDrive - Random Forest AB\Dokument\_Arbete\Rowico\Rowico Home Data Cloud\Reports\Reports V1\Invoices.pbix"
    test_bim_path = r"C:\Users\MarcusToftås\OneDrive - Random Forest AB\Dokument\_Arbete\Rowico\Rowico Home Data Cloud\Reports\Reports V1\Invoices.bim"
    # ============================================================================

    # Validate paths exist
    if not os.path.exists(test_pbix_path):
        return f"Test PBIX file not found: {test_pbix_path}\nPlease update the path in extractor.py -> run_test_extraction()"

    if not os.path.exists(test_bim_path):
        return f"Test BIM file not found: {test_bim_path}\nPlease update the path in extractor.py -> run_test_extraction()"

    # Extract file name and directory from paths
    pbix_path_obj = Path(test_pbix_path)
    bim_path_obj = Path(test_bim_path)

    # Set global variables
    _PBIX_ = [pbix_path_obj.stem, str(pbix_path_obj.parent)]
    _BIM_ = [bim_path_obj.stem, str(bim_path_obj.parent)]
    SAVE_NAME = pbix_path_obj.stem + "_NEW"
    LOG_DATA = True

    # Clear previous logs
    log_capture.clear()

    print(f"PBIX: {test_pbix_path}")
    print(f"BIM:  {test_bim_path}")
    print(f"Output: output/{SAVE_NAME}/\n")

    # Run the extraction
    result = run_cmd()

    # Display log if there were issues
    captured_logs = log_capture.get_logs()
    if captured_logs:
        print("\n" + "=" * 80)
        print("EXTRACTION LOG:")
        print("=" * 80)
        print(captured_logs)

    return result if result else "Success"


def run_cmd():
    """
    Main command to extract and document Power BI report.

    This function orchestrates the entire documentation generation process:
    1. Setup output directories and validate files
    2. Extract data from PBIX using ReportExtractor
    3. Process TSV file from Tabular Editor
    4. Build relationships and generate graph
    5. Create Excel documentation workbooks

    Returns:
        Status string: "Success", "Log", or error message
    """
    global SAVE_NAME, _BIM_, _PBIX_, LOG_DATA

    # ========================================================================
    # SECTION 1: SETUP AND INITIALIZATION
    # ========================================================================

    # Setup output directories
    output_dir = os.path.join(os.getcwd(), "output")
    ensure_directory(output_dir)

    cwd_save = os.path.join(output_dir, SAVE_NAME)
    ensure_directory(cwd_save)

    # Check if Excel file is already open
    file_path = os.path.join(cwd_save, f"{SAVE_NAME}.xlsx")
    if is_excel_open_with_file(file_path):
        return f"Please Close File: {SAVE_NAME}.xlsx before proceeding!"

    # Generate TSV file if it doesn't exist
    tsv_path = Path(os.path.join(cwd_save, "documentation.tsv"))
    if not tsv_path.is_file():
        if gen_tsv() == "NoTabEd":
            return "NoTabEd"

    excel_file = cwd_save + "\\" + SAVE_NAME + ".xlsx"

    # ========================================================================
    # SECTION 2: EXTRACT REPORT DATA FROM PBIX
    # ========================================================================

    rep_ex = ReportExtractor(_PBIX_[1], f"{_PBIX_[0]}.pbix")
    rep_ex.extract()

    report_info = pd.DataFrame(rep_ex.result, columns=REPORT_COLUMNS)

    # Process filters - remove duplicates and format
    report_filters = []
    [report_filters.append(sublist) for sublist in rep_ex.filters if sublist not in report_filters]

    report_filters_string = [
        [sublist[0], sublist[1], sublist[2], f"{sublist[3]}[{sublist[4]}]", " ".join(sublist[5:])]
        for sublist in report_filters
    ]

    # ========================================================================
    # SECTION 3: INITIALIZE DATA STRUCTURES
    # ========================================================================

    # Create the DataFrame
    data = {
        "Type": [],
        "Name": [],
        "DataType": [],
        "Description": [],
        "Definition": [],
        "Table": [],
        "Dependants": [],
        "Format": [],
        "Folder": [],
        "Comment": [],
        "Report File": [],
    }

    df = pd.DataFrame(data)

    # Define indexes of special columns
    definition_index = list(data.keys()).index("Definition")
    parent_index = list(data.keys()).index("Dependants")

    # Define lists for future calculations
    unused_columns = []
    all_tables = []
    all_relationships = []
    all_hierarchies = []
    all_visuals = []

    # Determine which items are used in which visual and on which page
    for visual_id, cols in report_info.groupby("Visual ID"):
        temp_visuals = [cols.iloc()[0]["Page"], 0]
        for _, row in cols.iterrows():
            temp_visuals.append((row["Table"], row["Name"]))
        all_visuals.append(temp_visuals)

    unique_pages = list(set(item[0] for item in all_visuals))
    unique_pages_index = [0 for i in range(len(unique_pages))]

    for ind, visual in enumerate(all_visuals):
        page_index = unique_pages.index(visual[0])
        all_visuals[ind][1] = unique_pages_index[page_index]
        unique_pages_index[page_index] += 1

    # ========================================================================
    # SECTION 4: PROCESS TSV FILE FROM TABULAR EDITOR
    # ========================================================================

    dataset = pd.read_csv(
        f"{cwd_save}\\documentation.tsv",
        sep="\t",
        header=0,
    )
    excel_file = cwd_save + "\\" + SAVE_NAME + ".xlsx"

    # Extract all Table names
    tab_rel_pattern = (
        r"^Relationship\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )
    for i in range(len(dataset)):
        data_type, table_name, column_name = parse_tsv_object_name(dataset.iloc[i]["Object"])
        if data_type == "Table":
            rel_pattern = re.match(tab_rel_pattern, table_name)
            if rel_pattern is not None and rel_pattern not in all_relationships:
                all_relationships.append(dataset.iloc[i]["Name"])

            elif table_name not in all_tables and "Relationship." not in table_name:
                all_tables.append(table_name)

    # Remove excess " ' " surrounding table names
    escape_pattern = r"'(?:\s*)(" + "|".join(map(re.escape, all_tables)) + r")(?:\s*)'"
    for i, row in enumerate(dataset.iloc()):
        exp = row["Expression"]
        if pd.isna(exp):
            continue

        exp = exp.replace("\\t", "    ")

        match = re.search(escape_pattern, exp)
        if match:
            dataset.at[i, "Expression"] = exp.replace(match.group(0), match.group(1))

    # Read .tsv file and convert to usable dataframe
    for i in range(len(dataset)):
        line_data = dataset.iloc[i]

        df_type, df_table, df_column = parse_tsv_object_name(line_data["Object"])

        # Currently don't need to do anything with all tables or hierarchies
        if df_type == "Table":
            continue

        elif df_type == "Hierarchy":
            all_hierarchies.append((df_table, df_column))
            continue

        elif df_type == "Column" or df_type == "Measure":
            unused_columns.append((df_table, df_column))

        if not isinstance(line_data["Expression"], float):
            definition = line_data["Expression"]
            definition = definition.replace("    ", "\t")
            definition = definition.replace("\\n", "\n")
        else:
            definition = ""

        # Extract description if embedded in definition
        if definition.find(DESCRIPT_TAG) != -1:
            comment_start = find_nth_occurrence(DESCRIPT_TAG, definition, 1) + 5
            comment_end = find_nth_occurrence(DESCRIPT_TAG, definition, 2) - 1
            definition_start = comment_end + 6
        else:
            comment_start = 0
            comment_end = comment_start
            definition_start = comment_start

        df_name = line_data["Name"]
        df_data_type = line_data["DataType"]
        if pd.isna(line_data["Description"]):
            df_description = definition[comment_start:comment_end].strip()
            df_description = df_description.replace("\\n", "\\r\\n")
        else:
            df_description = line_data["Description"]
        df_definition = definition[definition_start:].strip()
        df_definition = df_definition.replace("\r\n", "\n")
        df_definition = df_definition.replace("\r", "\n")
        df_format = (
            "" if pd.isna(line_data.get("FormatString", "")) else line_data.get("FormatString", "")
        )
        df_display = (
            ""
            if pd.isna(line_data.get("DisplayFolder", ""))
            else line_data.get("DisplayFolder", "")
        )

        # Find which page the measures/calculations are on
        df_report_pages = []
        for i, row in enumerate(report_info["Name"]):
            if row == df_name:
                df_report_pages.append(report_info["Page"][i])

        new_data = {
            "Type": df_type,
            "Name": df_name,
            "DataType": df_data_type,
            "Description": df_description,
            "Definition": df_definition,
            "Table": df_table,
            "Dependants": "",
            "Format": df_format,
            "Folder": df_display,
            "Comment": "",
            "Report File": _PBIX_[0],
        }

        df.loc[-1] = new_data
        df.index = df.index + 1
    df = df.sort_index()

    # ========================================================================
    # SECTION 5: BUILD RELATIONSHIPS AND GENERATE GRAPH
    # ========================================================================

    data = {
        "Type": [],
        "Child": [],
        "Direction": [],
        "Parent": [],
    }

    df_relations = pd.DataFrame(data)
    for row in sorted(all_relationships):
        i1 = row.find("]") + 1
        i2 = row.find(">") + 2
        t1 = row[:i1].replace("'", "")
        t2 = row[i2:].replace("'", "")
        rel = row[i1 + 1 : i2 - 1]

        relation = "Unknown Type"
        if rel == "-->":
            relation = "One Way"
        if rel == "<-->":
            relation = "Two Way"

        new_data_rel = {
            "Type": "Relationship",
            "Child": t1.split("[")[0],
            "Direction": relation,
            "Parent": t2.split("[")[0],
        }
        df_relations.loc[-1] = new_data_rel
        df_relations.index = df_relations.index + 1
    df_relations = df_relations.sort_index()

    def generate_graph(df_relations: pd.DataFrame, w: int, h: int):
        G = nx.DiGraph()

        for _, row in df_relations.iterrows():
            task_id = row["Child"]
            parent_task = row["Parent"]

            G.add_node(task_id)
            if not pd.isnull(parent_task):
                G.add_edge(str(parent_task), task_id)

        def split_label(label):
            return re.sub(r"([a-z])([A-Z])", r"\1\n\2", label)

        child_nodes = set(df_relations["Parent"].dropna().unique())
        parent_nodes = set(G.nodes) - child_nodes

        colors = plt.cm.tab20.colors
        color_map = {}
        for i, node in enumerate(child_nodes):
            color_map[node] = colors[i % len(colors)]

        node_colors = [color_map[node] if node in child_nodes else "lightgreen" for node in G.nodes]

        labels = {node: split_label(node) for node in parent_nodes}

        plt.figure(figsize=(w, h))

        pos = nx.spring_layout(G, k=2.5, iterations=500, scale=10)
        nx.draw(
            G,
            pos,
            with_labels=True,
            labels=labels,
            node_color=node_colors,
            font_weight="bold",
            node_size=300,
            arrowsize=10,
        )

        legend_handles = [
            plt.Line2D(
                [0],
                [0],
                marker="o",
                color="w",
                markerfacecolor=color_map[node],
                markersize=10,
                label=node,
            )
            for node in child_nodes
        ]
        plt.legend(
            handles=legend_handles,
            title="Dimensions",
            bbox_to_anchor=(1.05, 1),
            loc="upper left",
        )

        # Save to output directory
        output_path = os.path.join(cwd_save, f"{SAVE_NAME}_Relationships.png")
        plt.savefig(output_path, bbox_inches="tight")
        plt.close()

    generate_graph(df_relations, 12, (len(df_relations) + 1) * 14.4 / 72)

    # ========================================================================
    # SECTION 6: IDENTIFY UNUSED COLUMNS
    # ========================================================================

    # Remove Cols/Measures from 'unused_columns' that are used in visuals
    for row in report_info.iloc():
        used_columns = (row["Table"], row["Name"])
        if used_columns in unused_columns:
            unused_columns.remove(used_columns)

    for filter in report_filters:
        temp_col = (filter[3], filter[4])
        if temp_col in unused_columns:
            unused_columns.remove(temp_col)

    # ========================================================================
    # SECTION 7: CREATE MAIN EXCEL WORKBOOK
    # ========================================================================

    # Delete old data
    if os.path.exists(excel_file):
        os.remove(excel_file)

    workbook = xlsxwriter.Workbook(excel_file)
    worksheet = workbook.add_worksheet(f"{_PBIX_[0]} Common")

    # Add column formatting.
    def_format = workbook.add_format({"align": "top", "text_wrap": True})
    wrap_format = workbook.add_format({"text_wrap": True})
    worksheet.set_column(0, len(new_data), 30, wrap_format)
    worksheet.set_column(definition_index, definition_index, 100, def_format)
    worksheet.set_column(definition_index + 1, definition_index + 1, 30, wrap_format)
    worksheet.set_column(parent_index, parent_index, 50, wrap_format)

    def get_workbook_format(index: int):
        return workbook.add_format({"color": rgba_tuple_to_hex(DEFAULT_COLORS[index][1])})

    paranthesis_color = ["#0433fa", "#319331", "#7b3831"]
    formats = {
        "function": get_workbook_format(0),
        "measure": get_workbook_format(1),
        "return": get_workbook_format(2),
        "varname": get_workbook_format(3),
        "comment": get_workbook_format(4),
        "quote": get_workbook_format(5),
        "var": get_workbook_format(6),
        "bold": workbook.add_format({"bold": True}),
        "italic": workbook.add_format({"italic": True}),
        "bi": workbook.add_format({"bold": True, "italic": True}),
        "para": [workbook.add_format({"color": color}) for color in paranthesis_color * 5],
    }

    ## Print Relation Section
    num_relations = len(df_relations)
    row_num = 1
    if num_relations > 0:
        col = 0
        for name, value in new_data_rel.items():
            worksheet.write(0, col, name, formats["bi"])
            col += 1

        print_graph = True
        for _, row in df_relations.iterrows():
            if print_graph:
                worksheet.insert_image(
                    "E1",
                    os.path.join(cwd_save, f"{SAVE_NAME}_Relationships.png"),
                    {"x_scale": 1, "y_scale": 1},
                )
                print_graph = False

            for col, value in enumerate(row):
                worksheet.write(row_num, col, value)
            row_num += 1

    row_num += 2
    col = 0
    for name, value in new_data.items():
        worksheet.write(row_num, col, name, formats["bi"])
        col += 1

    def ls_app(*args):
        format_array.extend(args)

    row_num += 1
    for _, row in df.iterrows():
        v_definition = row["Definition"]

        # Skip traditional columns for now
        if row["Type"] == "Column":
            continue

        # Find Vars and measures
        var_names = find_vars(v_definition)
        function_names = find_functions(v_definition, known_functions)
        columns = find_columns(v_definition)
        tables = [i for i, _ in columns]
        columns_clean = ["[" + i + "]" for _, i in columns]
        measures = find_measures(v_definition)

        for column in columns:
            if column in unused_columns:
                unused_columns.remove(column)

        for measure in measures:
            for col_unused in unused_columns:
                if measure[1:-1] == col_unused[1]:
                    unused_columns.remove(col_unused)

        if row["Type"] == "Measure":
            for col_unused in unused_columns:
                name = (row["Table"], row["Name"])
                if name == col_unused:
                    unused_columns.remove(name)

        formated_text = v_definition.replace("\t", " XXX ")
        formated_text = formated_text.replace("\r\n", " YYY ")
        formated_text = formated_text.replace("\n", " YYY ")
        formated_text = formated_text.replace("&&", " ZZZ ")
        formated_text = formated_text.replace("||", " AAA ")

        # Split the text into rows
        pattern = re.compile(r"(\(|\)|\[.*?\]|,|//|\d+\.\d+|\w+|(?<!\d)\.(?!\d)|\W)")
        tokens = [token for token in re.findall(pattern, formated_text) if token.strip()]

        format_array = []
        parents_array = []
        parenthesis_count = -1
        is_whole_line_comment = False
        quote_counter = 0

        # Store away all parents used in func. Columns get table name as prefix, standalone measures as [Name]
        standalone_measures = [m for m in measures if m not in columns_clean]
        all_parents = [i + "[" + j + "]" for i, j in columns] + standalone_measures
        for token in all_parents:
            parents_array.append(token)
            parents_array.append("\n")
        if parents_array:
            parents_array.pop(-1)

        # Iternate through the segments and add a format before the corresponding tokens.
        for token in tokens:
            if token == "//":
                is_whole_line_comment = True
            elif token == "YYY":
                is_whole_line_comment = False

            if token == '"' and not is_whole_line_comment:
                quote_counter += 1

            if is_whole_line_comment:
                ls_app(formats["comment"], token + " ")
            elif quote_counter > 0:
                ls_app(formats["quote"])
                if quote_counter == 2:
                    ls_app(token + " ")
                    quote_counter = 0
                else:
                    ls_app(token)
            elif token == "XXX":
                ls_app("\t")
            elif token == "YYY":
                ls_app("\n")
            elif token == "ZZZ":
                ls_app("&& ")
            elif token == "AAA":
                ls_app("|| ")
            elif token == "(":
                parenthesis_count += 1
                safe_count = max(0, min(parenthesis_count, 14))
                ls_app(formats["para"][safe_count], token + " ")
            elif token == ")":
                safe_count = max(0, min(parenthesis_count, 14))
                ls_app(formats["para"][safe_count], token + " ")
                parenthesis_count -= 1
            elif token == "VAR":
                ls_app(formats["var"], token + " ")
            elif token in var_names:
                ls_app(formats["varname"], token + " ")
            elif token in measures:
                ls_app(
                    formats["para"][parenthesis_count + 1],
                    token[0],
                    formats["measure"],
                    token[1:-1],
                    formats["para"][parenthesis_count + 1],
                    token[-1] + " ",
                )
            elif token in tables or token in columns_clean:
                ls_app(formats["measure"], token)
            elif token in function_names:
                ls_app(formats["function"], token + " ")
            elif token == "RETURN":
                ls_app(formats["return"], token + " ")
            else:
                ls_app(token, " ")

        for col, value in enumerate(row):
            if col == definition_index and len(format_array) != 0:
                write_to_excel(worksheet, row_num, col, format_array)
                if len(format_array) == 1:
                    1
            elif col == parent_index and len(parents_array) != 0:
                write_to_excel(worksheet, row_num, col, parents_array)
                if len(parents_array) == 1:
                    1
            elif value != "":
                worksheet.write(row_num, col, value)
        row_num += 1

    row_num += 6
    for col_pair in unused_columns:
        worksheet.write(row_num, 0, col_pair[0] + "[" + col_pair[1] + "]")
        row_num += 1

    # ========================================================================
    # SECTION 8: CREATE PAGE-SPECIFIC TABS (One Tab Per Report Page)
    # ========================================================================

    # Create a tab per report page with visual info.
    for report_name in report_info["Page"].unique().tolist():
        save_report_name = report_name.replace("/", "_")
        worksheet_x = workbook.add_worksheet(save_report_name)

        worksheet_x.set_column(0, 6, 30, def_format)
        worksheet_x.set_column(2, 2, 50, def_format)
        worksheet_x.set_column(3, 3, 60, def_format)
        worksheet_x.set_column(4, 4, 60, def_format)

        local_df = report_info[report_info["Page"] == report_name]
        visual_ids = local_df[["Visual ID"]]["Visual ID"].unique().tolist()

        local_df = local_df.sort_values(by=["Visual Type", "Type"])

        data_x = {
            "Item Type": [],
            "Visual Type": [],
            "Type": [],
            "Field": [],
            "DisplayName": [],
            "Visual Filters": [],
            "Interactivity": [],
            "Comment": [],
            "ID": [],
        }

        df_x = pd.DataFrame(data_x)

        for visual in visual_ids:
            visual_type = local_df[local_df["Visual ID"] == visual].iloc[0]["Visual Type"]

            # Get visual type info from YAML configuration
            v_type, s_type = visual_mapper.get_visual_info(visual_type)

            # Log warning if visual type not found in config
            if (
                not visual_mapper.is_special_visual(visual_type)
                and visual_type not in visual_type_list
            ):
                if visual_type not in ["Group"] and not visual_mapper.is_button_type(visual_type):
                    logger.warning(f"New Visual type not yet supported: {visual_type}")

            new_data = {
                "Item Type": v_type,
                "Visual Type": s_type,
                "Type": "",
                "Field": "",
                "DisplayName": "",
                "Visual Filters": "",
                "Interactivity": "",
                "Comment": "",
                "ID": visual,
            }

            df_x.loc[-1] = new_data
            df_x.index = df_x.index + 1

        for i_filter, filter in enumerate(report_filters_string):
            if filter[2] == "This Page" and filter[0] == report_name:
                new_data = {
                    "Item Type": "Filter",
                    "Visual Type": "This Page",
                    "Type": "",
                    "Field": "",
                    "DisplayName": "",
                    "Visual Filters": "",
                    "Interactivity": "",
                    "Comment": "",
                    "ID": i_filter,
                }

                df_x.loc[-1] = new_data
                df_x.index = df_x.index + 1

        for col_idx, name in enumerate(
            [
                "Item Type",
                "Visual Type",
                "ID",
                "Description",
                "Visual Filters",
                "Interactivity",
                "Comment",
            ]
        ):
            worksheet_x.write(0, col_idx, name, formats["bi"])

        sort_order = ["Visual", "Slicer", "Filter", "Button", "Group"]
        df_x["Item Type"] = pd.Categorical(df_x["Item Type"], categories=sort_order, ordered=True)
        df_sorted = df_x.sort_values(by=["Item Type", "Visual Type"])

        row_num = 1
        for _, row in df_sorted.iterrows():
            filter_array = []
            for filter in report_filters_string:
                if filter[2] == "Visual" and filter[0] == report_name and filter[1] == row["ID"]:
                    filter_array.extend([formats["bold"], filter[3], " " + filter[4] + "\n"])

            if filter_array and filter_array[-1][-1] == "\n":
                filter_array[-1] = filter_array[-1][:-1]

            if row["Item Type"] in ["Visual", "Slicer"]:
                # One row per visual - build Description grouped by Type
                r_data = local_df[local_df["Visual ID"] == row["ID"]]
                description_parts = []
                for field_type, group in r_data.groupby("Type", sort=False):
                    description_parts.append(f"{field_type}:")
                    for _, rrow in group.iterrows():
                        display_name = (
                            str(rrow["Display Name"])
                            if not pd.isna(rrow["Display Name"]) and rrow["Display Name"]
                            else rrow["Name"]
                        )
                        if display_name != rrow["Name"]:
                            description_parts.append(
                                f"  {rrow['Table']}[{rrow['Name']}] ({display_name})"
                            )
                        else:
                            description_parts.append(f"  {rrow['Table']}[{rrow['Name']}]")
                worksheet_x.write(row_num, 0, row["Item Type"])
                worksheet_x.write(row_num, 1, row["Visual Type"])
                worksheet_x.write(row_num, 2, row["ID"])
                worksheet_x.write(row_num, 3, "\n".join(description_parts))
                if len(filter_array) != 0:
                    write_to_excel(worksheet_x, row_num, 4, filter_array)
                row_num += 1

            elif row["Item Type"] in ["Button", "Group"]:
                rrow = report_info[report_info["Visual ID"] == row["ID"]].iloc[0]
                display_name = (
                    str(rrow["Display Name"])
                    if not pd.isna(rrow["Display Name"]) and rrow["Display Name"]
                    else rrow["Name"]
                )
                worksheet_x.write(row_num, 0, row["Item Type"])
                worksheet_x.write(row_num, 1, row["Visual Type"])
                worksheet_x.write(row_num, 2, row["ID"])
                worksheet_x.write(row_num, 3, display_name)
                if len(filter_array) != 0:
                    write_to_excel(worksheet_x, row_num, 4, filter_array)
                row_num += 1

            elif row["Item Type"] == "Filter":
                filter_field = report_filters_string[row["ID"]][3]
                filter_details = report_filters_string[row["ID"]][4]
                worksheet_x.write(row_num, 0, row["Item Type"])
                worksheet_x.write(row_num, 1, row["Visual Type"])
                worksheet_x.write(row_num, 2, "")
                worksheet_x.write(row_num, 3, f"{filter_field} {filter_details}")
                row_num += 1

            else:
                if isinstance(row["Item Type"], float):
                    logger.error(f"NaN Item Type Encountered: {row}")
                    continue

    # Create consolidated "Pages" tab with all pages combined
    worksheet_pages = workbook.add_worksheet("Pages")
    worksheet_pages.set_column(0, 9, 30, def_format)
    worksheet_pages.set_column(3, 3, 50, def_format)
    worksheet_pages.set_column(4, 4, 20, def_format)
    worksheet_pages.set_column(5, 5, 50, def_format)
    worksheet_pages.set_column(6, 6, 30, def_format)
    worksheet_pages.set_column(7, 7, 60, def_format)

    # Write header with "Page" column added at the beginning
    col = 0
    worksheet_pages.write(0, col, "Page", formats["bi"])
    col += 1
    for name in [
        "Item Type",
        "Visual Type",
        "ID",
        "Type",
        "Field",
        "DisplayName",
        "Visual Filters",
        "Interactivity",
        "Comment",
    ]:
        worksheet_pages.write(0, col, name, formats["bi"])
        col += 1

    row_num = 1
    # Loop through all pages and consolidate data
    for report_name in report_info["Page"].unique().tolist():
        local_df = report_info[report_info["Page"] == report_name]
        visual_ids = local_df[["Visual ID"]]["Visual ID"].unique().tolist()
        local_df = local_df.sort_values(by=["Visual Type", "Type"])

        data_x = {
            "Item Type": [],
            "Visual Type": [],
            "Type": [],
            "Field": [],
            "DisplayName": [],
            "Visual Filters": [],
            "Interactivity": [],
            "Comment": [],
            "ID": [],
        }

        df_x = pd.DataFrame(data_x)

        for visual in visual_ids:
            visual_type = local_df[local_df["Visual ID"] == visual].iloc[0]["Visual Type"]

            # Get visual type info from YAML configuration
            v_type, s_type = visual_mapper.get_visual_info(visual_type)

            # Log warning if visual type not found in config
            if (
                not visual_mapper.is_special_visual(visual_type)
                and visual_type not in visual_type_list
            ):
                if visual_type not in ["Group"] and not visual_mapper.is_button_type(visual_type):
                    logger.warning(f"New Visual type not yet supported: {visual_type}")

            new_data = {
                "Item Type": v_type,
                "Visual Type": s_type,
                "Type": "",
                "Field": "",
                "DisplayName": "",
                "Visual Filters": "",
                "Interactivity": "",
                "Comment": "",
                "ID": visual,
            }

            df_x.loc[-1] = new_data
            df_x.index = df_x.index + 1

        for i_filter, filter in enumerate(report_filters_string):
            if filter[2] == "This Page" and filter[0] == report_name:
                new_data = {
                    "Item Type": "Filter",
                    "Visual Type": "This Page",
                    "Type": "",
                    "Field": "",
                    "DisplayName": "",
                    "Visual Filters": "",
                    "Interactivity": "",
                    "Comment": "",
                    "ID": i_filter,
                }

                df_x.loc[-1] = new_data
                df_x.index = df_x.index + 1

        sort_order = ["Visual", "Slicer", "Filter", "Button", "Group"]
        df_x["Item Type"] = pd.Categorical(df_x["Item Type"], categories=sort_order, ordered=True)
        df_sorted = df_x.sort_values(by=["Item Type", "Visual Type"])

        for _, row in df_sorted.iterrows():
            filter_array = []
            for filter in report_filters_string:
                if filter[2] == "Visual" and filter[0] == report_name and filter[1] == row["ID"]:
                    filter_array.extend([formats["bold"], filter[3], " " + filter[4] + "\n"])

            if filter_array and filter_array[-1][-1] == "\n":
                filter_array[-1] = filter_array[-1][:-1]

            if row["Item Type"] in ["Visual", "Slicer"]:
                # Create one row per field for visuals/slicers
                r_data = local_df[local_df["Visual ID"] == row["ID"]]
                for field_idx, rrow in enumerate(r_data.iloc()):
                    worksheet_pages.write(row_num, 0, report_name)
                    worksheet_pages.write(row_num, 1, row["Item Type"])
                    worksheet_pages.write(row_num, 2, row["Visual Type"])
                    worksheet_pages.write(row_num, 3, row["ID"])
                    worksheet_pages.write(row_num, 4, rrow["Type"])
                    worksheet_pages.write(row_num, 5, f"{rrow['Table']}[{rrow['Name']}]")
                    display_name = (
                        str(rrow["Display Name"])
                        if not pd.isna(rrow["Display Name"]) and rrow["Display Name"]
                        else rrow["Name"]
                    )
                    worksheet_pages.write(row_num, 6, display_name)
                    if len(filter_array) != 0:
                        write_to_excel(worksheet_pages, row_num, 7, filter_array)
                    row_num += 1

            elif row["Item Type"] in ["Button", "Group"]:
                rrow = report_info[report_info["Visual ID"] == row["ID"]].iloc[0]
                display_name = (
                    str(rrow["Display Name"])
                    if not pd.isna(rrow["Display Name"]) and rrow["Display Name"]
                    else rrow["Name"]
                )
                worksheet_pages.write(row_num, 0, report_name)
                worksheet_pages.write(row_num, 1, row["Item Type"])
                worksheet_pages.write(row_num, 2, row["Visual Type"])
                worksheet_pages.write(row_num, 3, row["ID"])
                worksheet_pages.write(row_num, 4, rrow["Type"])
                worksheet_pages.write(row_num, 5, display_name)
                if len(filter_array) != 0:
                    write_to_excel(worksheet_pages, row_num, 7, filter_array)
                row_num += 1

            else:
                if isinstance(row["Item Type"], float):
                    logger.error(f"NaN Item Type Encountered: {row}")
                    continue

                # Filters
                worksheet_pages.write(row_num, 0, report_name)
                worksheet_pages.write(row_num, 1, row["Item Type"])
                worksheet_pages.write(row_num, 2, row["Visual Type"])
                worksheet_pages.write(row_num, 3, "")
                filter_field = report_filters_string[row["ID"]][3]
                filter_details = report_filters_string[row["ID"]][4]
                worksheet_pages.write(row_num, 5, filter_field)
                worksheet_pages.write(row_num, 6, filter_details)
                row_num += 1

    workbook.close()

    # ========================================================================
    # SECTION 10: CREATE SECOND EXCEL WORKBOOK (_data.xlsx)
    # ========================================================================

    # Create second Excel file with reorganized structure
    excel_file_data = cwd_save + "\\" + SAVE_NAME + "_data.xlsx"
    if os.path.exists(excel_file_data):
        os.remove(excel_file_data)

    workbook_data = xlsxwriter.Workbook(excel_file_data)

    # Reuse the same formats from the first workbook
    def_format_data = workbook_data.add_format({"align": "top", "text_wrap": True})
    wrap_format_data = workbook_data.add_format({"text_wrap": True})

    def get_workbook_data_format(index: int):
        return workbook_data.add_format({"color": rgba_tuple_to_hex(DEFAULT_COLORS[index][1])})

    formats_data = {
        "function": get_workbook_data_format(0),
        "measure": get_workbook_data_format(1),
        "return": get_workbook_data_format(2),
        "varname": get_workbook_data_format(3),
        "comment": get_workbook_data_format(4),
        "quote": get_workbook_data_format(5),
        "var": get_workbook_data_format(6),
        "bold": workbook_data.add_format({"bold": True}),
        "italic": workbook_data.add_format({"italic": True}),
        "bi": workbook_data.add_format({"bold": True, "italic": True}),
        "para": [workbook_data.add_format({"color": color}) for color in paranthesis_color * 5],
    }

    # Tab 1: "pages" - Copy of the consolidated Pages tab
    worksheet_pages_data = workbook_data.add_worksheet("pages")
    worksheet_pages_data.set_column(0, 9, 30, def_format_data)
    worksheet_pages_data.set_column(3, 3, 50, def_format_data)
    worksheet_pages_data.set_column(4, 4, 20, def_format_data)
    worksheet_pages_data.set_column(5, 5, 50, def_format_data)
    worksheet_pages_data.set_column(6, 6, 30, def_format_data)
    worksheet_pages_data.set_column(7, 7, 60, def_format_data)
    worksheet_pages_data.set_column(8, 8, 60, def_format_data)

    # Write header
    col = 0
    worksheet_pages_data.write(0, col, "Page", formats_data["bi"])
    col += 1
    for name in [
        "Item Type",
        "Visual Type",
        "ID",
        "Type",
        "Field",
        "DisplayName",
        "Visual Filters",
        "Interactivity",
        "Comment",
        "Description",
    ]:
        worksheet_pages_data.write(0, col, name, formats_data["bi"])
        col += 1

    row_num = 1
    # Loop through all pages and consolidate data (same logic as before)
    for report_name in report_info["Page"].unique().tolist():
        local_df = report_info[report_info["Page"] == report_name]
        visual_ids = local_df[["Visual ID"]]["Visual ID"].unique().tolist()
        local_df = local_df.sort_values(by=["Visual Type", "Type"])

        data_x = {
            "Item Type": [],
            "Visual Type": [],
            "Type": [],
            "Field": [],
            "DisplayName": [],
            "Visual Filters": [],
            "Interactivity": [],
            "Comment": [],
            "ID": [],
            "Description": [],
        }

        df_x = pd.DataFrame(data_x)

        for visual in visual_ids:
            visual_type = local_df[local_df["Visual ID"] == visual].iloc[0]["Visual Type"]

            # Get visual type info from YAML configuration
            v_type, s_type = visual_mapper.get_visual_info(visual_type)

            # Log warning if visual type not found in config
            if (
                not visual_mapper.is_special_visual(visual_type)
                and visual_type not in visual_type_list
            ):
                if visual_type not in ["Group"] and not visual_mapper.is_button_type(visual_type):
                    logger.warning(f"New Visual type not yet supported: {visual_type}")

            new_data_visual = {
                "Item Type": v_type,
                "Visual Type": s_type,
                "Type": "",
                "Field": "",
                "DisplayName": "",
                "Visual Filters": "",
                "Interactivity": "",
                "Comment": "",
                "ID": visual,
            }

            df_x.loc[-1] = new_data_visual
            df_x.index = df_x.index + 1

        for i_filter, filter in enumerate(report_filters_string):
            if filter[2] == "This Page" and filter[0] == report_name:
                new_data_filter = {
                    "Item Type": "Filter",
                    "Visual Type": "This Page",
                    "Type": "",
                    "Field": "",
                    "DisplayName": "",
                    "Visual Filters": "",
                    "Interactivity": "",
                    "Comment": "",
                    "ID": i_filter,
                    "Description": "",
                }

                df_x.loc[-1] = new_data_filter
                df_x.index = df_x.index + 1

        sort_order = ["Visual", "Slicer", "Filter", "Button", "Group"]
        df_x["Item Type"] = pd.Categorical(df_x["Item Type"], categories=sort_order, ordered=True)
        df_sorted = df_x.sort_values(by=["Item Type", "Visual Type"])

        for _, row in df_sorted.iterrows():
            filter_array = []
            for filter in report_filters_string:
                if filter[2] == "Visual" and filter[0] == report_name and filter[1] == row["ID"]:
                    filter_array.extend([formats_data["bold"], filter[3], " " + filter[4] + "\n"])

            if filter_array and filter_array[-1][-1] == "\n":
                filter_array[-1] = filter_array[-1][:-1]

            if row["Item Type"] in ["Visual", "Slicer"]:
                r_data = local_df[local_df["Visual ID"] == row["ID"]]
                description_parts = []
                for field_type, group in r_data.groupby("Type", sort=False):
                    description_parts.append(f"{field_type}:")
                    for _, rrow_desc in group.iterrows():
                        display_name_desc = (
                            str(rrow_desc["Display Name"])
                            if not pd.isna(rrow_desc["Display Name"]) and rrow_desc["Display Name"]
                            else rrow_desc["Name"]
                        )
                        if display_name_desc != rrow_desc["Name"]:
                            description_parts.append(
                                f"  {rrow_desc['Table']}[{rrow_desc['Name']}] ({display_name_desc})"
                            )
                        else:
                            description_parts.append(f"  {rrow_desc['Table']}[{rrow_desc['Name']}]")
                full_description = "\n".join(description_parts)

                # Write one row per field
                for field_idx, rrow in enumerate(r_data.iloc()):
                    worksheet_pages_data.write(row_num, 0, report_name)
                    worksheet_pages_data.write(row_num, 1, row["Item Type"])
                    worksheet_pages_data.write(row_num, 2, row["Visual Type"])
                    worksheet_pages_data.write(row_num, 3, row["ID"])
                    worksheet_pages_data.write(row_num, 4, rrow["Type"])
                    worksheet_pages_data.write(row_num, 5, f"{rrow['Table']}[{rrow['Name']}]")
                    display_name = (
                        str(rrow["Display Name"])
                        if not pd.isna(rrow["Display Name"]) and rrow["Display Name"]
                        else rrow["Name"]
                    )
                    worksheet_pages_data.write(row_num, 6, display_name)
                    if len(filter_array) != 0:
                        write_to_excel(worksheet_pages_data, row_num, 7, filter_array)
                    worksheet_pages_data.write(row_num, 10, full_description)
                    row_num += 1

            elif row["Item Type"] in ["Button", "Group"]:
                rrow = report_info[report_info["Visual ID"] == row["ID"]].iloc[0]
                display_name = (
                    str(rrow["Display Name"])
                    if not pd.isna(rrow["Display Name"]) and rrow["Display Name"]
                    else rrow["Name"]
                )
                worksheet_pages_data.write(row_num, 0, report_name)
                worksheet_pages_data.write(row_num, 1, row["Item Type"])
                worksheet_pages_data.write(row_num, 2, row["Visual Type"])
                worksheet_pages_data.write(row_num, 3, row["ID"])
                worksheet_pages_data.write(row_num, 4, rrow["Type"])
                worksheet_pages_data.write(row_num, 5, display_name)
                if len(filter_array) != 0:
                    write_to_excel(worksheet_pages_data, row_num, 7, filter_array)
                row_num += 1

            else:
                if isinstance(row["Item Type"], float):
                    logger.error(f"NaN Item Type Encountered: {row}")
                    continue

                worksheet_pages_data.write(row_num, 0, report_name)
                worksheet_pages_data.write(row_num, 1, row["Item Type"])
                worksheet_pages_data.write(row_num, 2, row["Visual Type"])
                worksheet_pages_data.write(row_num, 3, "")
                filter_field = report_filters_string[row["ID"]][3]
                filter_details = report_filters_string[row["ID"]][4]
                worksheet_pages_data.write(row_num, 5, filter_field)
                worksheet_pages_data.write(row_num, 6, filter_details)
                row_num += 1

    # Tab 2: "common" - Main data without relationships and unused measures
    worksheet_common = workbook_data.add_worksheet("common")
    worksheet_common.set_column(0, len(new_data), 30, wrap_format_data)
    worksheet_common.set_column(definition_index, definition_index, 100, def_format_data)
    worksheet_common.set_column(definition_index + 1, definition_index + 1, 30, wrap_format_data)
    worksheet_common.set_column(parent_index, parent_index, 50, wrap_format_data)

    # Write header
    col = 0
    for name in df.columns:
        worksheet_common.write(0, col, name, formats_data["bi"])
        col += 1

    row_num = 1
    for _, row in df.iterrows():
        v_definition = row["Definition"]

        if row["Type"] == "Column":
            continue

        var_names = find_vars(v_definition)
        function_names = find_functions(v_definition, known_functions)
        columns = find_columns(v_definition)
        tables = [i for i, _ in columns]
        columns_clean = ["[" + i + "]" for _, i in columns]
        measures = find_measures(v_definition)

        formated_text = v_definition.replace("\t", " XXX ")
        formated_text = formated_text.replace("\r\n", " YYY ")
        formated_text = formated_text.replace("\n", " YYY ")
        formated_text = formated_text.replace("&&", " ZZZ ")
        formated_text = formated_text.replace("||", " AAA ")

        pattern = re.compile(r"(\(|\)|\[.*?\]|,|//|\d+\.\d+|\w+|(?<!\d)\.(?!\d)|\W)")
        tokens = [token for token in re.findall(pattern, formated_text) if token.strip()]

        format_array = []
        parents_array = []
        parenthesis_count = -1
        is_whole_line_comment = False
        quote_counter = 0

        standalone_measures = [m for m in measures if m not in columns_clean]
        all_parents = [i + "[" + j + "]" for i, j in columns] + standalone_measures
        for token in all_parents:
            parents_array.append(token)
            parents_array.append("\n")
        if parents_array:
            parents_array.pop(-1)

        for token in tokens:
            if token == "//":
                is_whole_line_comment = True
            elif token == "YYY":
                is_whole_line_comment = False

            if token == '"' and not is_whole_line_comment:
                quote_counter += 1

            if is_whole_line_comment:
                ls_app(formats_data["comment"], token + " ")
            elif quote_counter > 0:
                ls_app(formats_data["quote"])
                if quote_counter == 2:
                    ls_app(token + " ")
                    quote_counter = 0
                else:
                    ls_app(token)
            elif token == "XXX":
                ls_app("\t")
            elif token == "YYY":
                ls_app("\n")
            elif token == "ZZZ":
                ls_app("&& ")
            elif token == "AAA":
                ls_app("|| ")
            elif token == "(":
                parenthesis_count += 1
                safe_count = max(0, min(parenthesis_count, 14))
                ls_app(formats_data["para"][safe_count], token + " ")
            elif token == ")":
                safe_count = max(0, min(parenthesis_count, 14))
                ls_app(formats_data["para"][safe_count], token + " ")
                parenthesis_count -= 1
            elif token == "VAR":
                ls_app(formats_data["var"], token + " ")
            elif token in var_names:
                ls_app(formats_data["varname"], token + " ")
            elif token in measures:
                ls_app(
                    formats_data["para"][parenthesis_count + 1],
                    token[0],
                    formats_data["measure"],
                    token[1:-1],
                    formats_data["para"][parenthesis_count + 1],
                    token[-1] + " ",
                )
            elif token in tables or token in columns_clean:
                ls_app(formats_data["measure"], token)
            elif token in function_names:
                ls_app(formats_data["function"], token + " ")
            elif token == "RETURN":
                ls_app(formats_data["return"], token + " ")
            else:
                ls_app(token, " ")

        for col, value in enumerate(row):
            if col == definition_index and len(format_array) != 0:
                write_to_excel(worksheet_common, row_num, col, format_array)
            elif col == parent_index and len(parents_array) != 0:
                write_to_excel(worksheet_common, row_num, col, parents_array)
            elif value != "":
                worksheet_common.write(row_num, col, value)
        row_num += 1

    # Tab 3: "relationships"
    worksheet_relationships = workbook_data.add_worksheet("relationships")
    worksheet_relationships.set_column(0, 3, 30, wrap_format_data)

    if num_relations > 0:
        col = 0
        for name, value in new_data_rel.items():
            worksheet_relationships.write(0, col, name, formats_data["bi"])
            col += 1

        row_num = 1
        print_graph = True
        for _, row in df_relations.iterrows():
            if print_graph:
                worksheet_relationships.insert_image(
                    "E1",
                    os.path.join(cwd_save, f"{SAVE_NAME}_Relationships.png"),
                    {"x_scale": 1, "y_scale": 1},
                )
                print_graph = False

            for col, value in enumerate(row):
                worksheet_relationships.write(row_num, col, value)
            row_num += 1

    # Tab 4: "unused measures"
    worksheet_unused = workbook_data.add_worksheet("unused measures")
    worksheet_unused.set_column(0, 0, 50, wrap_format_data)

    worksheet_unused.write(0, 0, "Unused Columns and Measures", formats_data["bi"])
    row_num = 1
    for col_pair in unused_columns:
        worksheet_unused.write(row_num, 0, col_pair[0] + "[" + col_pair[1] + "]")
        row_num += 1

    # Tab 5: "dependencies" - one row per measure+dependent pair
    worksheet_deps = workbook_data.add_worksheet("dependencies")
    worksheet_deps.set_column(0, 0, 50, wrap_format_data)
    worksheet_deps.set_column(1, 1, 50, wrap_format_data)

    worksheet_deps.write(0, 0, "MeasureName", formats_data["bi"])
    worksheet_deps.write(0, 1, "Dependent", formats_data["bi"])

    row_num = 1
    for _, row in df.iterrows():
        if row["Type"] == "Column":
            continue

        v_definition = row["Definition"]
        measure_name = f"{row['Table']}[{row['Name']}]"

        dep_columns = find_columns(v_definition)
        dep_measures = find_measures(v_definition)
        columns_clean_local = ["[" + j + "]" for _, j in dep_columns]
        standalone = [m for m in dep_measures if m not in columns_clean_local]

        all_deps = [i + "[" + j + "]" for i, j in dep_columns] + standalone

        for dep in all_deps:
            worksheet_deps.write(row_num, 0, measure_name)
            worksheet_deps.write(row_num, 1, dep)
            row_num += 1

    workbook_data.close()

    # ========================================================================
    # SECTION 11: SAVE LOGS AND RETURN STATUS
    # ========================================================================

    ## Print Logging Info
    captured_logs = log_capture.get_logs()
    if captured_logs and LOG_DATA:
        t = time.localtime()
        current_time = time.strftime("%H_%M_%S", t)
        location_folder = os.path.join(cwd_save, "logs")
        location = os.path.join(location_folder, f"log_data_{current_time}.txt")

        if not os.path.exists(location_folder):
            os.makedirs(location_folder)

        with open(location, "w") as text_file:
            text_file.write(captured_logs)

        return "Log"

    return "Success"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="PBIXtractor automatically generates Documentation material for a given PBIX-file."
    )

    # Define the command-line arguments
    parser.add_argument("-i", dest="file", type=str, help="Name of PBIX-File")
    parser.add_argument("-o", dest="output", type=str, help="Name of output-File")
    parser.add_argument(
        "--ui",
        default=True,
        action="store_true",
        help="Runs in UI mode with additional options",
    )
    parser.add_argument(
        "--yes_man", dest="yes_man", action="store_true", help="Remove Input Protection"
    )

    # Parse the command-line arguments
    args = parser.parse_args()

    if args.ui:
        run_ui()
    else:
        if args.file:
            _file_ = args.file
            if args.output:
                SAVE_NAME = args.output
            else:
                SAVE_NAME = _file_
            yes_man = args.yes_man
        else:
            _file_ = "SemanticModell"
            yes_man = False
            SAVE_NAME = _file_

        _PBIX_ = [
            _file_,
            "C:\\Users\\Reports",
        ]
        _BIM_ = [
            _file_,
            "C:\\Users\\Reports",
        ]

        result = run_cmd()
        # result = run_ui()
        print(result)

# Maybe includes additional info to extract? https://www.linkedin.com/pulse/streamlining-model-documentation-tabular-editor-power-jarom-gleed


## Possibilities:
#
# extract conditional formatting of text
# number of decimals
# selection naming - title
#
# hierarchies
#
## Less valuable
# Font size, show blanks as, padding, label position
#
# BUGS:
# File Name måste vara under 31 filer
# Script att ladda ner paket automatiskt
# ÅÄÖ i filnamen
