"""Legacy DearPyGUI UI and the global-state wrappers around the pipeline.

The documentation logic lives in pipeline.py (run_extraction) and report_extractor.py.
run_cmd() and run_test_extraction() keep the old global-variable interface for the
DearPyGUI UI and `pbixtractor --test`.
"""

import logging
import os
import subprocess
import threading
import time
from pathlib import Path

import pandas as pd

# Local imports
from .constants import DEFAULT_COLORS, DESCRIPT_TAG, UI_COLORS
from .data import DATA_DIR
from .dax import find_columns, find_functions, find_measures  # noqa: F401 (re-exported)
from .documentation import (  # noqa: F401 (re-exported)
    build_documentation,
    button_target_and_label,
    parse_tsv_object_name,
)
from .logger import setup_logger
from .pipeline import ExtractionOptions, run_extraction
from .report_extractor import (  # noqa: F401 (re-exported)
    CONFIG,
    ReportExtractor,
    known_functions,
    visual_mapper,
    visual_type_list,
)
from .tabular_editor import export_documentation_tsv, find_tabular_editor

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
            elif tsv_result == "TSVFailed":
                show_and_hide(
                    "tsvTextExtra",
                    "Tabular Editor could not generate the TSV file, see the Logs section.",
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
    """
    Export output/<SAVE_NAME>/documentation.tsv with Tabular Editor (UI "Regenerate tsv").

    Formats DAX via daxformatter.com first, as the original TabularScript.cs did.

    Returns:
        "NoTabEd" if Tabular Editor 2 is not installed, "TSVFailed" on errors, else None
    """
    tabular_editor = find_tabular_editor()
    if tabular_editor is None:
        return "NoTabEd"
    try:
        export_documentation_tsv(
            tabular_editor,
            os.path.join(_BIM_[1], f"{_BIM_[0]}.bim"),
            Path(os.getcwd()) / "output" / SAVE_NAME / "documentation.tsv",
        )
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as e:
        logger.error(f"Tabular Editor TSV export failed: {e}")
        return "TSVFailed"
    return None


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
    Document the report selected in the legacy UI (global SAVE_NAME, _PBIX_, _BIM_, ...).

    Returns:
        "Success", "Log" (finished with warnings) or an error message
    """
    # The legacy UI shows log_capture's content; only this run's messages
    log_capture.clear()
    options = ExtractionOptions(
        report_path=Path(_PBIX_[1]) / f"{_PBIX_[0]}.pbix",
        model_path=Path(_BIM_[1]) / f"{_BIM_[0]}.bim",
        output_dir=Path(os.getcwd()) / "output" / SAVE_NAME,
        name=SAVE_NAME,
        description_tag=DESCRIPT_TAG,
        tabular_editor_analysis=RUN_TE_ANALYSIS,
        tabular_editor_tsv=USE_TABULAR_EDITOR,
        write_log_file=LOG_DATA,
    )
    result = run_extraction(options)
    if result.status == "error":
        return result.message
    return "Log" if result.status == "warnings" else "Success"
