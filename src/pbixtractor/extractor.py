"""Main extractor module for PBI-Ixtractor."""

import argparse
import logging
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pandas as pd
import yaml

# Local imports
from .constants import DEFAULT_COLORS, DESCRIPT_TAG, UI_COLORS
from .data import DATA_DIR
from .dax import find_columns, find_functions, find_measures  # noqa: F401 (re-exported)
from .documentation import (  # noqa: F401 (re-exported)
    build_documentation,
    button_target_and_label,
    parse_tsv_object_name,
)
from .excel_report import write_data_workbook, write_main_workbook
from .live_model import collect_live_statistics
from .logger import get_logger, setup_logger
from .relationship_graph import save_relationship_graph
from .semantic_model import model_to_dataset, read_model
from .tabular_editor import (
    drop_redundant_table_refs,
    export_dependencies,
    find_tabular_editor,
    run_best_practice_analyzer,
)
from .utils import ensure_directory, is_excel_open_with_file

# Initialize logger
logger, log_capture = setup_logger("pbixtractor", level=logging.INFO, capture=True)

# Global state variables
LOG_DATA = True
# False: read measures/columns straight from the .bim (no Tabular Editor needed).
# True: use Tabular Editor's TSV export, which also formats DAX via daxformatter.com
# (sends the DAX to an external web service).
USE_TABULAR_EDITOR = False
# Tabular Editor analysis (local, offline, if installed): Best Practice Analyzer and exact DAX
# dependencies. Falls back to text matching of DAX for dependencies when unavailable.
RUN_TE_ANALYSIS = True
SAVE_NAME = ""
_PBIX_ = [None, None]
_BIM_ = [None, None]


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
        1. Reads the report (legacy Layout or PBIR; .pbix, .pbip or .Report folder)
           into a normalised ReportDefinition (see readers.py)
        2. Extracts report-level filters and each page using PageExtractor
        """
        from .extractors import ReportContext
        from .readers import read_report

        report = read_report(os.path.join(self.path, self.name), self.logger)
        self.logger.debug(f"Read {self.name} ({report.format} format)")
        context = ReportContext.from_report(report)

        # Report-level filters (apply to all pages)
        for filter_obj in self.page_extractor.filter_extractor.extract_filters(
            report.filters, "", "All Pages"
        ):
            self.filters.append(filter_obj.to_list())

        # Extract data from each page using PageExtractor
        for page in report.pages:
            items, filters = self.page_extractor.extract(page, context)

            # Convert Pydantic models to legacy list format
            for item in items:
                self.result.append(item.to_list())

            for filter_obj in filters:
                self.filters.append(filter_obj.to_list())


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
            elif tsv_result == "TSVTimeout":
                show_and_hide(
                    "tsvTextExtra",
                    "Tabular Editor did not generate the TSV file in time, please retry.",
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

    def toggle_tabular_editor(sender, app_data, user_data):
        global USE_TABULAR_EDITOR
        USE_TABULAR_EDITOR = dpg.get_value(sender)

    def toggle_te_analysis(sender, app_data, user_data):
        global RUN_TE_ANALYSIS
        RUN_TE_ANALYSIS = dpg.get_value(sender)

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
        width, height, channels, data = dpg.load_image(str(DATA_DIR / "logo_large.png"))
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
                "Generates the documentation files. Reads the .bim directly (Tabular Editor only if enabled under Additional Settings).",
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
            dpg.add_checkbox(
                label="Use Tabular Editor (formats DAX via daxformatter.com - sends DAX online)",
                callback=toggle_tabular_editor,
                tag="tabular_editor_toggle",
                default_value=USE_TABULAR_EDITOR,
            )
            dpg.add_checkbox(
                label="Tabular Editor analysis (local): Best Practice Analyzer, exact DAX dependencies,"
                " and row counts/sizes/measure types when the report is open in Power BI Desktop",
                callback=toggle_te_analysis,
                tag="te_analysis_toggle",
                default_value=RUN_TE_ANALYSIS,
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
    dpg.create_viewport(
        title="PB-Ixtractor", width=1000, height=800, large_icon=str(DATA_DIR / "logo.ico")
    )
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

    tab_edit_exe = find_tabular_editor()
    if tab_edit_exe is None:
        return "NoTabEd"
    tab_edit_path = f'"{tab_edit_exe}"'

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

    try:
        wait_for_file(file_path=f"{cwd}\\documentation.tsv", timeout=5)
    except TimeoutError:
        logger.error("Tabular Editor did not produce documentation.tsv within 5 seconds")
        return "TSVTimeout"


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


def _tabular_editor_analysis(model, bim_path: str, report_path: str):
    """
    Run the optional Tabular Editor analysis (locally): Best Practice Analyzer and exact DAX
    dependencies on the .bim, plus live statistics if the report is open in Power BI Desktop.

    Returns:
        (bpa_violations, exact_dependencies, live_statistics); each None when unavailable
        (dependencies then fall back to DAX text matching)
    """
    bpa_violations = exact_dependencies = live_statistics = None
    if not RUN_TE_ANALYSIS:
        return bpa_violations, exact_dependencies, live_statistics

    tabular_editor = find_tabular_editor()
    if tabular_editor is None:
        logger.warning(
            "Tabular Editor analysis skipped: Tabular Editor 2 not found. Add its folder to "
            "Input/TabularEditorLocations.txt or disable it under Additional Settings."
        )
        return bpa_violations, exact_dependencies, live_statistics

    try:
        bpa_violations = run_best_practice_analyzer(tabular_editor, bim_path)
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as e:
        logger.warning(f"Best Practice Analyzer failed: {e}")
    try:
        exact_dependencies = drop_redundant_table_refs(
            export_dependencies(tabular_editor, bim_path)
        )
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as e:
        logger.warning(f"Exact dependency export failed, using DAX text matching: {e}")
    try:
        live_statistics, live_note = collect_live_statistics(
            tabular_editor, report_path, {table.name for table in model.tables}
        )
        # Debug only: not having the report open in Desktop is the normal case
        logger.debug(f"Live statistics: {live_note}")
        if live_statistics and live_statistics.errors:
            logger.warning(
                "Some live statistics are missing (Power BI Desktop too old?): "
                + "; ".join(f"{k}: {v[:150]}" for k, v in live_statistics.errors.items())
            )
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as e:
        logger.warning(f"Reading live statistics from Power BI Desktop failed: {e}")

    return bpa_violations, exact_dependencies, live_statistics


def run_cmd():
    """
    Main command to extract and document Power BI report.

    Steps:
    1. Read the semantic model (.bim) and run the optional Tabular Editor analysis
    2. Extract the report (visuals, buttons, filters) from the .pbix
    3. Analyse: objects, relationships, unused columns/measures (documentation.py)
    4. Write the relationship graph and both Excel workbooks (excel_report.py)
    5. Save captured logs

    Returns:
        Status string: "Success", "Log", or error message
    """
    # Only report logs from this run
    log_capture.clear()

    output_dir = os.path.join(os.getcwd(), "output")
    cwd_save = os.path.join(output_dir, SAVE_NAME)
    ensure_directory(cwd_save)

    excel_file = os.path.join(cwd_save, f"{SAVE_NAME}.xlsx")
    if is_excel_open_with_file(excel_file):
        return f"Please Close File: {SAVE_NAME}.xlsx before proceeding!"

    report_path = os.path.join(_PBIX_[1], f"{_PBIX_[0]}.pbix")
    bim_path = os.path.join(_BIM_[1], f"{_BIM_[0]}.bim")

    # 1. Model: always read from the .bim (relationships, sort-by and hierarchy columns come
    #    from here, even when Tabular Editor provides the TSV)
    try:
        model = read_model(bim_path)
    except (OSError, ValueError, NotImplementedError) as e:
        return f"Could not read the model file {bim_path}: {e}"

    bpa_violations, exact_dependencies, live_statistics = _tabular_editor_analysis(
        model, bim_path, report_path
    )

    # Measure data types are only known by a live model (the .bim usually lacks them)
    if live_statistics:
        for measure in model.all_measures:
            measure.data_type = live_statistics.measure_types.get(
                (measure.table, measure.name), measure.data_type
            )

    # Optionally use Tabular Editor's TSV export instead (formats DAX via daxformatter.com)
    if USE_TABULAR_EDITOR:
        tsv_path = Path(cwd_save) / "documentation.tsv"
        if not tsv_path.is_file():
            tsv_result = gen_tsv()
            if tsv_result == "NoTabEd":
                return "NoTabEd"
            if tsv_result == "TSVTimeout":
                return "Tabular Editor did not generate documentation.tsv in time, please retry."
        dataset = pd.read_csv(tsv_path, sep="\t", header=0)
    else:
        dataset = model_to_dataset(model)

    # 2. Report
    rep_ex = ReportExtractor(_PBIX_[1], f"{_PBIX_[0]}.pbix")
    rep_ex.extract()

    # 3. Analysis
    documentation = build_documentation(
        report_items=rep_ex.result,
        report_filters=rep_ex.filters,
        model=model,
        dataset=dataset,
        report_name=_PBIX_[0],
        description_tag=DESCRIPT_TAG,
        visual_mapper=visual_mapper,
        visual_types=visual_type_list,
        logger=logger,
        exact_dependencies=exact_dependencies,
        bpa_violations=bpa_violations,
        live_statistics=live_statistics,
    )

    # 4. Output
    graph_path = os.path.join(cwd_save, f"{SAVE_NAME}_Relationships.png")
    save_relationship_graph(documentation.relations, graph_path)
    write_main_workbook(excel_file, documentation, graph_path, known_functions)
    write_data_workbook(
        os.path.join(cwd_save, f"{SAVE_NAME}_data.xlsx"),
        documentation,
        graph_path,
        known_functions,
    )

    # 5. Logs
    captured_logs = log_capture.get_logs()
    if captured_logs and LOG_DATA:
        location_folder = os.path.join(cwd_save, "logs")
        ensure_directory(location_folder)
        current_time = time.strftime("%H_%M_%S", time.localtime())
        with open(os.path.join(location_folder, f"log_data_{current_time}.txt"), "w") as file:
            file.write(captured_logs)
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
