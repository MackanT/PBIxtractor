import argparse
import pandas as pd
import os
import sys
import re
import xlsxwriter
import time
import psutil

import json
from zipfile import ZipFile
import shutil

import subprocess
from pathlib import Path
import networkx as nx
from matplotlib import pyplot as plt
import matplotlib
import threading
import inspect

matplotlib.use("agg")


LOG_DATA = True
REPORT_LOG = ""
SAVE_NAME = ""
_PBIX_ = [None, None]
_BIM_ = [None, None]
DESCRIPT_TAG = "////"

default_colors = [
    ["Functions", (49, 101, 187, 255)],
    ["Measures", (0, 16, 128, 255)],
    ["Return", (24, 0, 255, 255)],
    ["Variables", (9, 134, 88, 255)],
    ["Comments", (8, 128, 15, 255)],
    ["Quotes", (163, 21, 21, 255)],
    ["VarNames", (0, 15, 255, 255)],
]

cwd = os.getcwd()

# Reads user defined Visual Types from external file
try:
    file_path = f"{cwd}\\Input\\VisualTypes.csv"
    visual_type_list = pd.read_csv(file_path)
    visual_type_list = visual_type_list["PBI Visual Name"].values.tolist()
except OSError:
    print(f"Could not open/read file: {file_path}")
    sys.exit()

# Reads user defined Data Types from external file
try:
    file_path = f"{cwd}\\Input\\DataTypes.csv"
    data_type_list = pd.read_csv(file_path)
    data_type_list = data_type_list[["PBI Name", "Output Name"]].values.tolist()
except OSError:
    print(f"Could not open/read file: {file_path}")
    sys.exit()

# Reads user defined PBI Functions from external file
try:
    file_path = f"{cwd}\\Input\\FunctionNames.csv"
    known_functions = pd.read_csv(file_path)
    known_functions = known_functions["PBI Function Name"].values.tolist()
except OSError:
    print(f"Could not open/read file: {file_path}")
    sys.exit()
    
def log_data(message: str, error: str, severity: int = 0):
        e = str(severity)
        
        if severity == -1:
            e += " Debug: "
        elif severity == 0:
            e += " Info: "
        elif severity == 1:
            e += " Warning: "
        elif severity == 2:
            e += " Error: "
        else:
            e += " Critical: "
            
        line_len = 130
        error = str(error)
        error_clean = ''
        error_lines = error.split('\n')
        for line in error_lines:
            
            msg_len = len(line)
            stansas = int(msg_len / line_len) + 1
            for i in range(stansas):
                error_clean += line[(line_len) * i: (line_len) * (i + 1)] + '\n'
        
        error_clean = error_clean[:-1]

        e += message + f". Error on line {inspect.currentframe().f_back.f_lineno}.\n" + str(error_clean)
        return e + "\n\n"


class ReportExtractor:
    def __init__(self, path, name):
        self.path = path
        self.name = name
        self.result = []
        self.filters = []
        self.log = ""

    def _log_data(self, message: str, error: str, severity: int = 0):
        self.log += log_data(message, error, severity)
        

    def find_value_by_key(self, data: dict, target_key: str) -> dict | None:
        """
        Looks through input dict and finds first occurence that matches specific key

        data: Input dictionary
        target_key: Searched for string key

        return dict with data or None
        """
        if isinstance(data, dict):
            for key, value in data.items():
                if key == target_key:
                    return value
                elif isinstance(value, (dict, list)):
                    result = self.find_value_by_key(value, target_key)
                    if result is not None:
                        return result
        elif isinstance(data, list):
            for item in data:
                result = self.find_value_by_key(item, target_key)
                if result is not None:
                    return result

        return None

    def find_all_values(self, data: dict, key_word: str = "Value") -> list:
        """
        Returns list of all paths and values from dict with specified key word

        data: input dict to search
        key_word: str to search for

        returns [[Path, Value], [Path, Value], ...]
        """

        occurrences = []

        def search_in_data(data, path=""):
            if isinstance(data, dict):
                for key, value in data.items():
                    new_path = f"{path}.{key}" if path else key
                    if isinstance(value, dict):
                        if key_word in value:
                            occurrences.append((new_path, value[key_word]))
                        search_in_data(value, new_path)
                    elif isinstance(value, (dict, list)):
                        search_in_data(value, new_path)
            elif isinstance(data, list):
                for idx, item in enumerate(data):
                    new_path = f"{path}[{idx}]"
                    if isinstance(item, dict):
                        if key_word in item:
                            occurrences.append((new_path, item[key_word]))
                        search_in_data(item, new_path)
                    elif isinstance(item, (dict, list)):
                        search_in_data(item, new_path)

        search_in_data(data)
        return occurrences

    def find_comparison_kind_occurrences(self, data: dict) -> list:
        """
        Searches data for 'ComparissonKind' information, returns list of comparisson integers

        data: input dict to search

        returns [x, y]
        """

        def _search_comparison_kind(data):
            occurrences = []
            if isinstance(data, dict):
                if "ComparisonKind" in data:
                    occurrences.append(data["ComparisonKind"])
                for key, value in data.items():
                    occurrences.extend(_search_comparison_kind(value))
            elif isinstance(data, list):
                for item in data:
                    occurrences.extend(_search_comparison_kind(item))
            return occurrences

        return _search_comparison_kind(data)

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
        Stores input data into the self.result field
        """
        field_values = [
            page,
            visual_type,
            item_name,
            table_name,
            val_name,
            disp_name,
            data_type,
        ]

        self.result.append(field_values)

    def add_filter(
        self,
        page: str,
        item_name: str,
        filter_type: str,
        table_name: str,
        val_name: str,
        ver: str,
        value: str,
    ):
        """
        Stores input data into the self.filters field
        """
        filter_set = [
            page,
            item_name,
            filter_type,
            table_name,
            val_name,
            ver,
            value,
        ]

        self.filters.append(filter_set)

    def clean_input(self, input_string: str) -> str:
        """Attempts to clean input to integer. If not applicable returns input value
        Also removes exxcess "'" from input

        Args:
            input_string (str): input string to attempt to convert

        Returns:
            str: either cleaned numeric input as a string or original string
        """

        try:
            if input_string == "true":
                return "True"
            elif input_string == "false":
                return "False"

            if "datetime" in input_string:
                return re.search(r"'(.*?)'", input_string).group(1)

            if input_string[-1] == "L":
                return str(int(input_string[:-1]))
            else:
                return str(int(input_string))
        except ValueError:
            return input_string.replace("'", "")

    def gen_val_string(self, all_values: list) -> tuple[str, bool]:
        """
        Converts list of incoming data into valid strings that can be stored in the self.result/self.filters fields

        all_values: list of incoming data

        returns valid string, boolean if condition is inverted
        """
        val_list = ""
        is_inverted = False

        for val in all_values:
            if "Where" in val[0]:
                val_list += self.clean_input(val[1]) + ", "
            elif "isInverted" in val[0]:
                is_inverted = self.clean_input(val[1])
            else:
                self._log_data('No Found message value', all_values, 0, 1)

        if val_list[-2:] == ", ":
            val_list = val_list[:-2]

        return val_list, is_inverted

    def extract(self):
        pathFolder = f"{self.path}/temp_{self.name[:-5]}"
        try:
            shutil.rmtree(pathFolder)
        except:
            print(f"folder {pathFolder} not present")
        f = ZipFile(f"{self.path}/{self.name}", "r")
        f.extractall(pathFolder)
        report_layout = json.loads(
            open(f"{pathFolder}/Report/Layout", "r", encoding="utf-16 le").read()
        )

        f.close()

        report_layout["config"] = json.loads(report_layout["config"])
        for section in report_layout["sections"]:
            for visual_container in section["visualContainers"]:
                for key in ["config", "filters", "query", "dataTransforms"]:
                    if key in visual_container.keys():
                        visual_container[key] = json.loads(visual_container[key])

        for s in report_layout["sections"]:
            page_name = s["displayName"]

            if page_name == "Template":
                continue

            for ex_data in s["visualContainers"]:
                if ex_data.get("config", "") != "":
                    t = ex_data["config"]

                    item_name = t["name"]
                    visual_type = self.find_value_by_key(t, "visualType")

                    if visual_type in ("shape", "image", "textbox"):
                        continue

                    elif visual_type in visual_type_list:
                        data_types = self.find_value_by_key(t, "projections")

                        data_list = []
                        for d_list in data_type_list:
                            temp_list = []
                            for row in data_types.get(d_list[0], []):
                                temp_list.append(row["queryRef"])

                            data_list.append(temp_list)

                        # Add Correct Display Names if applicable
                        vis_names = self.find_all_values(t, "Name")
                        vis_disp_names = self.find_all_values(t, "NativeReferenceName")
                        vis_name_disp_name = []
                        for vn in vis_names:
                            for vdn in vis_disp_names:
                                if vn[0] == vdn[0]:
                                    vis_name_disp_name.append([vn[1], vdn[1]])
                                    break

                        data = self.find_value_by_key(t, "Select")

                        for rowi, row in enumerate(data):
                            if row.get("HierarchyLevel", "") != "":
                                temp = self.find_value_by_key(row, "Name")
                                temp2 = temp.split(".")

                                # Find issues
                                if len(temp2) <= 2 or isinstance(temp2, str):
                                    self._log_data("Hierarchy is to short", row,  1)
                                    continue

                                table_name = temp2[0]
                                val_name = temp2[2]
                            elif (
                                row.get("Measure", "") != ""
                                or row.get("Column", "") != ""
                            ):
                                temp = row["Name"]
                                temp2 = temp.split(".", 1)
                                if temp2[0][0:4] == "Sum(":
                                    temp2[0] = temp2[0][4:]
                                table_name = temp2[0]
                                val_name = temp2[1]
                                val_name2 = self.find_value_by_key(row, "Property")
                                if val_name2 is not None and val_name != val_name2:
                                    val_name = val_name2
                            elif row.get("Aggregation", "") != "":
                                temp = row["Name"]
                                s1 = temp.find("(") + 1
                                s2 = temp.rfind(")")
                                temp2 = temp[s1:s2].split(".")

                                table_name = temp2[0]
                                val_name = temp2[1]
                            else:
                                self._log_data("Unspecified row type", row, 0)

                            # Determine Data Type + Display Name
                            data_type = None
                            disp_name = None
                            for di, dlist in enumerate(data_list):
                                if temp in dlist:
                                    data_type = data_type_list[di][1]

                                    for vname in vis_name_disp_name:
                                        if temp == vname[0]:
                                            disp_name = vname[1]
                                            break

                                    break
                            if not data_type:
                                data_type = "UNKNOWN Data Type"
                                self._log_data("Unknown data type", data_type, 1)

                            if not disp_name or disp_name == val_name:
                                disp_name = None

                            if data[rowi].get("HierarchyLevel", "") != "":
                                data_type = "Hierarchy"
                                temp = self.find_value_by_key(data[rowi], "Name").split(
                                    "."
                                )
                                disp_name = temp[1] + ": " + temp[2]

                            self.add_item(
                                page=page_name,
                                visual_type=visual_type,
                                item_name=item_name,
                                table_name=table_name,
                                val_name=val_name,
                                disp_name=disp_name,
                                data_type=data_type,
                            )

                    elif visual_type is None:
                        self.add_item(
                            page=page_name,
                            visual_type="Group",
                            item_name="",
                            table_name="",
                            val_name=self.find_value_by_key(t, "displayName"),
                            disp_name="",
                            data_type="Group",
                        )

                    elif visual_type == "actionButton":
                        ## TODO make this nicer!
                        temp = self.find_value_by_key(t, "type")
                        button_type = self.find_value_by_key(temp, "Value")
                        if button_type is None:
                            button_type = self.find_value_by_key(t, "Value")
                        button_type = button_type.replace("'", "")

                        visual_type = None
                        item_name = None
                        val_name = "MISSING"
                        table_name = None
                        disp_name = None
                        data_type = None

                        if button_type == "Bookmark":
                            temp2 = self.find_value_by_key(t, "bookmark")
                            item_name = self.find_value_by_key(ex_data, "Value")
                            item_name = item_name.replace("'", "")
                            data_type = "Bookmark"

                        elif button_type == "PageNavigation":
                            temp2 = self.find_value_by_key(t, "navigationSection")

                            ## Find issues
                            if not temp2:
                                self._log_data("Page Navigation error", ex_data, 1)
                                continue
                            item_name = self.find_value_by_key(temp2, "Value")
                            item_name = item_name.replace("'", "")

                            data_type = "Page"
                            disp_name = "Page Navigation"
                            for x in report_layout["sections"]:
                                if item_name == x.get("name", ""):
                                    val_name = x["displayName"]
                        elif button_type == "custom":
                            item_name = "Filter"
                            data_type = "Icon"
                            disp_name = "Filter Icon"  ## TODO currently not used as visual, is more of a "Button"
                            continue
                        else:
                            self._log_data(
                                f"Unknown visual type {button_type} on {page_name}",
                                ex_data,
                                1,
                            )
                            continue

                        visual_type = button_type

                        self.add_item(
                            page=page_name,
                            visual_type=visual_type,
                            item_name=item_name,
                            table_name=table_name,
                            val_name=val_name,
                            disp_name=disp_name,
                            data_type=data_type,
                        )

                    else:
                        self._log_data(
                            f"New Visual type not yet supported! {visual_type}",
                            ex_data,
                            1,
                        )

                # Add filters
                if ex_data.get("filters", []) != []:
                    t = ex_data["filters"]

                    local_config = ex_data["config"]
                    item_name = self.find_value_by_key(local_config, "name")

                    filter_type = "Visual"

                    for row in t:
                        if row.get("filter", "{}") == "{}":
                            continue

                        all_values = self.find_all_values(row)
                        comp_values = self.find_comparison_kind_occurrences(row)

                        table_name = self.find_value_by_key(row, "Entity")
                        val_name = self.find_value_by_key(row, "Property")
                        if val_name is None and self.find_value_by_key(
                            row, "HierarchyLevel"
                        ):
                            val_name = self.find_value_by_key(
                                row, "HierarchyLevel"
                            ).get("Level", "UNKNOWN!")
                            self._log_data("Unknown hierachy level!", row, 1)

                        val_list = ""
                        if row.get("type", "") == "RelativeDate":
                            unit = self.find_all_values(row, "TimeUnit")

                            # Is in this
                            if len(unit) == 1:
                                val_list = "is"
                                time_span = unit[0][1]
                                if time_span == 0:
                                    val_list += " today"
                                elif time_span == 1:
                                    val_list += " in this week"
                                elif time_span == 2:
                                    val_list += " in this month"
                                elif time_span == 3:
                                    val_list += " in this year"

                                filter_value = ""

                            else:
                                if len(unit) == 4:
                                    include_today = True
                                elif len(unit) == 6:
                                    include_today = False
                                else:
                                    self._log_data(
                                        'Unknown "Include Today" setting. Setting value to included',
                                        row,
                                        2,
                                    )
                                    include_today = True

                                f_val = ""
                                if unit[2][1] != 0:
                                    f_val += "calendar "
                                if unit[1][1] == 0:
                                    f_val += "days"
                                elif unit[1][1] == 1:
                                    f_val += "week"
                                elif unit[1][1] == 2:
                                    f_val += "month"
                                elif unit[1][1] == 3:
                                    f_val += "year"

                                lb = self.find_all_values(row, "Amount")

                                if lb[0][1] > 0:
                                    val_list = "is in the next "
                                else:
                                    val_list = "is in the last "

                                filter_value = ""
                                val_list += str(abs(lb[0][1])) + " " + f_val
                                if include_today:
                                    val_list += " including today"

                        elif row.get("type", "") == "TopN":
                            temp_t_name = []
                            for ttemp in self.find_all_values(row, "Entity"):
                                if "From[0]" in ttemp[0]:
                                    temp_t_name.append(ttemp)

                            count = self.find_value_by_key(row, "Top")
                            temp = self.find_value_by_key(row, "OrderBy")

                            val_list = (
                                temp_t_name[-1][1]
                                + "["
                                + self.find_value_by_key(temp, "Property")
                                + "]"
                            )

                            if temp[0].get("Direction", 0) == 2:
                                order = "Top"
                            else:
                                order = "Bottom"

                            filter_value = "by " + order + " " + str(count)

                        elif comp_values:
                            val_list = ""
                            filter_value = ""
                            if "And" in all_values[0][0]:
                                f_add = "and"
                            elif "Or" in all_values[0][0]:
                                f_add = "or"
                            else:
                                f_add = ""

                            for ival, c_val in enumerate(comp_values):
                                val_local, _ = self.gen_val_string([all_values[ival]])

                                if c_val == 0:
                                    if "Not" in all_values[ival][0]:
                                        if all_values[ival][1] == "null":
                                            f_value = "is not blank"
                                            val_local = ""
                                        else:
                                            f_value = "is not"
                                    else:
                                        if all_values[ival][1] == "null":
                                            f_value = "is blank"
                                            val_local = ""
                                        else:
                                            f_value = "is"

                                elif c_val == 1:
                                    f_value = "is greater than"
                                elif c_val == 2:
                                    f_value = "is greater than or equal to"
                                elif c_val == 3:
                                    f_value = "is less than"
                                elif c_val == 4:
                                    f_value = "is less than or equal to"
                                else:
                                    f_value = f"Not implemented... :') {c_val}"

                                val_list += f_value + " " + val_local + " "
                                if ival == 0:
                                    val_list += f_add + " "

                            val_list = " ".join(val_list.split())

                        else:
                            val_list, is_inverted = self.gen_val_string(all_values)

                            if is_inverted:
                                if val_list.find(",") != -1:
                                    filter_value = "not in"
                                else:
                                    filter_value = "<>"
                            else:
                                if val_list.find(",") != -1:
                                    filter_value = "in"
                                else:
                                    filter_value = "="

                        self.add_filter(
                            page=page_name,
                            item_name=item_name,
                            filter_type=filter_type,
                            table_name=table_name,
                            val_name=val_name,
                            ver=filter_value,
                            value=val_list,
                        )

            # Add page filters
            filter_data = json.loads(s["filters"])
            for ex_data in filter_data:
                filter_type = "This Page"
                table_name = self.find_value_by_key(ex_data, "Entity")
                val_name = self.find_value_by_key(ex_data, "Property")
                if ex_data.get("displayName", "") != "":
                    item_name = ex_data["displayName"]
                else:
                    item_name = val_name

                filter_variant = self.find_value_by_key(ex_data, "type")

                if filter_variant == "Categorical":
                    data_list = self.find_value_by_key(ex_data, "Values")

                    if data_list:
                        is_inverted = False
                        temp = self.find_value_by_key(
                            ex_data, "isInvertedSelectionMode"
                        )
                        if temp:
                            is_inverted = bool(temp["expr"]["Literal"]["Value"])

                        if len(data_list) == 1:
                            if is_inverted:
                                filter_ver = "<>"
                            else:
                                filter_ver = "="
                        else:
                            if is_inverted:
                                filter_ver = "not in"
                            else:
                                filter_ver = "in"

                        filter_value = ""
                        for il1, l1 in enumerate(data_list):
                            filter_value += self.find_value_by_key(l1, "Value")
                            if il1 < len(data_list) - 1:
                                filter_value += ", "

                        filter_value = filter_value.replace("'", "")

                        self.add_filter(
                            page=page_name,
                            item_name=item_name,
                            filter_type=filter_type,
                            table_name=table_name,
                            val_name=val_name,
                            ver=filter_ver,
                            value=filter_value,
                        )
                    else:
                        self._log_data(f"Unused filter on page {page_name}", ex_data, 0)
                elif filter_variant == "Advanced":
                    local_row = self.find_value_by_key(ex_data, "Where")

                    for r in local_row:
                        filter_ver = "is"
                        temp = self.find_value_by_key(r, "Not")
                        if temp:
                            filter_ver = "is not"

                        filter_value = self.find_value_by_key(r, "Right")["Literal"][
                            "Value"
                        ]
                        filter_value = filter_value.replace("'", "")

                        self.add_filter(
                            page=page_name,
                            item_name=item_name,
                            filter_type=filter_type,
                            table_name=table_name,
                            val_name=val_name,
                            ver=filter_ver,
                            value=filter_value,
                        )
                elif filter_variant == "RelativeDate":
                    LB = self.find_value_by_key(ex_data, "LowerBound")
                    UB = self.find_value_by_key(ex_data, "UpperBound")
                    if LB:
                        temp = LB["DateSpan"]["Expression"]["DateAdd"]
                        time_am = temp["Amount"]
                        time_span = temp["TimeUnit"]

                        filter_ver = "in the last"
                        filter_value = str(abs(time_am))

                        if time_span == 3:
                            filter_value += " years"
                        else:
                            filter_value += " unknown unit"
                            self._log_data("Unknown filter type.", ex_data, 2)

                        if UB:
                            filter_value += " including today"

                        self.add_filter(
                            page=page_name,
                            item_name=item_name,
                            filter_type=filter_type,
                            table_name=table_name,
                            val_name=val_name,
                            ver=filter_ver,
                            value=filter_value,
                        )

                    else:
                        self._log_data(
                            "Filter is relative date. No lower bound is set, skipping row!",
                            ex_data,
                            2,
                        )
                else:
                    self._log_data("Unknown filter variant", ex_data, 1)

        shutil.rmtree(pathFolder)


def rgba_tuple_to_hex(color):
    """Convert RGBA tuple to a hexadecimal color code."""
    r, g, b, _ = color
    hex_color = "#{:02x}{:02x}{:02x}".format(r, g, b)
    return hex_color


def run_ui():
    import dearpygui.dearpygui as dpg
    from tkinter import filedialog

    dpg.create_context()
    
    colors = {
        'G': (102, 204, 102),
        'R': (255, 77, 77),
        'Y': (255, 255, 102),
        'W': (255, 255, 255),
        "O": (255, 1543, 51),
    }
    
    def show_and_hide(tag:str, msg:str, type:str = None):
        
        dpg.configure_item(tag, color = colors[type], show=True)
        dpg.set_value(tag, msg)
        threading.Thread(target=lambda: wait_and_show(tag)).start()

    def wait_and_show(tag:str):
        time.sleep(8)
        dpg.hide_item(tag)
        
    def progress_bar(tag:str):
        threading.Thread(target=lambda: increment_loader(tag)).start()
        
    def increment_loader(tag:str):
        
        dpg.configure_item(tag, color=colors['W'])
        i = 0
        while not stop_event.is_set():
            time.sleep(0.2)
            dpg.show_item(tag)
            dpg.set_value(tag, '.'*i)
            i = (i + 1) % 16
        
    def run_extractor():
        global stop_event
        if _PBIX_ != [None, None] and _BIM_ != [None, None]:
            stop_event = threading.Event()
            progress_bar('runText')
            run_code = run_cmd()
            stop_event.set()
            time.sleep(0.5)
            if run_code == "Log":
                show_and_hide('runText', f"Documention generated with warnings. See /{SAVE_NAME}/logs", 'Y')
                update_log()
            elif run_code != "Success":
                show_and_hide('runText', run_code, 'R')
            else:
                show_and_hide('runText', f"Documentation generated without any issues. See: /{SAVE_NAME}/{SAVE_NAME}.xlsx", 'G')

    def update_log():
        global REPORT_LOG
        
        error_msg = REPORT_LOG.split('\n\n')
        for msg in error_msg:
            if msg == '':
                continue
            
            error_level = msg[0]
            
            if error_level == '-':
                c = colors['W']
            if error_level == '0':
                c = colors['W']
            elif error_level == '1':
                c = colors["Y"]
            elif error_level == '2':
                c = colors["O"]
            else:
                c = colors['R']
            
            add_colored_text_at_top(container, msg[2:], c)
            
        

    def disable_buttons():
        for tag in ["runPBIX", "genTSV"]:
            dpg.configure_item(tag, enabled=False, show=False)
        for tag in ["tsvText"]:
            dpg.configure_item(tag, show=False)

    def enable_buttons():
        for tag in ["runPBIX", "genTSV"]:
            dpg.configure_item(tag, enabled=True, show=True)
        for tag in ["tsvText"]:
            dpg.configure_item(tag, show=True)

    def generate_tsv():
        global stop_event
        if _PBIX_ != [None, None] and _BIM_ != [None, None]:
            stop_event = threading.Event()
            progress_bar('tsvTextExtra')
            tsv_result = gen_tsv(force=True)
            stop_event.set()
            time.sleep(0.5)
            if tsv_result == 'NoTabEd':
                show_and_hide('tsvTextExtra', 'Could Not Find Tabular Editor 2 on PC. Please add location in Input/TabularEditorLocations.txt', 'R')
            else:
                show_and_hide('tsvTextExtra', 'TSV File generated successfully!', 'G')


    ### UI Functions ###
    def load_file(input):
        global \
            pbix_file_path, \
            bim_file_path, \
            unique_data_tables, \
            _PBIX_, \
            _BIM_, \
            SAVE_NAME

        if input == "pbix":
            pbix_file_path = filedialog.askopenfilename(
                filetypes=[("pbix files", "*.pbix")]
            )
            if pbix_file_path:
                dpg.set_value(
                    "pbix_file_path_label", f"Selected File: {pbix_file_path}"
                )

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
                    dpg.set_value(
                        "bim_file_path_label", f"Selected File: {bim_file_path}"
                    )
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
            bim_file_path = filedialog.askopenfilename(
                filetypes=[("bim files", "*.bim")]
            )
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
        global default_colors
        old_color = None
        name = name.split(' ')[0]
        for i, color in enumerate(default_colors):
            if color[0] == name:
                old_color = color[1]
                break

        return [i, old_color]

    def set_colors(sender, app_data):
        button_type = dpg.get_value("radioColors")
        dpg.set_value("colorWheel", find_color(button_type)[1])

    def update_colors(sender, app_data):
        global default_colors
        button_type = dpg.get_value("radioColors")
        color_index = find_color(button_type)[0]

        new_color = []
        for col in dpg.get_value("colorWheel"):
            new_color.append(int(col))
        default_colors[color_index][1] = new_color

    def edit_settings(sender, app_data, user_data):
        dpg.configure_item("File Settings", default_open=False)
        dpg.configure_item("Additional Settings", default_open=True)
        dpg.configure_item("Logs", default_open=True)
        dpg.configure_item("User Input", default_open=False)

    def toggle_log_toggle(sender, app_data, user_data):
        global LOG_DATA
        LOG_DATA = dpg.get_value(sender)
    
    def add_colored_text_at_top(container, text, color):
        new_text = dpg.add_text(text, parent=container, color=color)
        children = dpg.get_item_children(container)[1]
        if len(children) > 1:
            dpg.move_item(new_text, parent=container, before=children[0])
            
    def add_input(version):
        
        cwd = os.getcwd() + '\\Input\\'
        
        if version == 'dataType':
            
            info_tag = 'dataTypeInputInfo'
            file_name = 'DataTypes.csv'
            val1 = dpg.get_value('dataTypeInputO')
            val2 = dpg.get_value('dataTypeInputP')
            
            val1_state = False
            val2_state = False
            if val1 == '':
                val1_state = True
            if val2 == '':
                val2_state = True
            
            if val1_state and val2_state:
                msg = 'Both Inputs are non-valid!'
            elif val2_state:
                msg = 'Non-valid input in Data Type PBI!'
            elif val1_state:
                msg = 'Non-valid input in Data Type Output!'
                
            if val1_state or val2_state:
                show_and_hide(info_tag, msg, 'Y')
                return
            
            val1 = f'{val2},{val1}'
            
        elif version == 'functionName':
            
            info_tag = 'functionNameInputInfo'
            file_name = 'FunctionNames.csv'
            val1 = dpg.get_value('functionNameInput')
            if val1 == '':
                show_and_hide(info_tag, 'Cannot enter blank function name!', 'Y')
                return
        
        elif version == 'visualType':
            
            info_tag = 'visualTypeInputInfo'
            file_name = 'VisualTypes.csv'
            val1 = dpg.get_value('visualTypeInput')
            if val1 == '':
                show_and_hide(info_tag, 'Cannot enter blank visual type!', 'Y')
                return
            
        elif version == 'TELocation':
            
            info_tag = 'TELocationInputInfo'
            file_name = 'TabularEditorLocations.txt'
            val1 = dpg.get_value('TELocation')
            if val1 == '':
                show_and_hide(info_tag, 'Cannot enter blank save location of Tabular Editor 2!', 'Y')
                return

        else:
            return
        
        if file_name[-3:] == 'txt':
            with open(cwd + file_name, 'a') as txt_file:
                txt_file.write(val1 + '\n')
        else:
            with open(cwd + file_name, 'a', newline='') as csv_file:
                csv_file.write(val1 + '\n')
        
        show_and_hide(info_tag, 'Data saved successfully!', 'G')
        
        
    with dpg.texture_registry(show=False):
        width, height, channels, data = dpg.load_image("logo_large.png")
        dpg.add_static_texture(
            width=width, height=height, default_value=data, tag="logo_texture"
        )

    with dpg.window(label="PB-Ixtractor", width=1000, height=800):
        with dpg.collapsing_header(label="PB-Ixtractor"):
            dpg.add_image("logo_texture")
        with dpg.collapsing_header(
            label="File Settings", default_open=True, tag="File Settings"
        ):
            dpg.add_button(
                label="Select .pbix File", callback=lambda: load_file("pbix")
            )
            dpg.add_text("Selected File: No file selected", tag="pbix_file_path_label")

            dpg.add_spacer(height=3)

            dpg.add_button(
                label="Select .bim File",
                show=False,
                enabled=False,
                tag="BimSelector",
                callback=lambda: load_file("bim"),
            )
            dpg.add_text(
                "Selected File: No file selected", tag="bim_file_path_label", show=False
            )

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

            dpg.add_spacer(height=15)
            dpg.add_button(label="Additional Settings", callback=edit_settings)
            dpg.add_spacer(height=1)

        with dpg.collapsing_header(
            label="Additional Settings", default_open=False, tag="Additional Settings"
        ):
            dpg.add_checkbox(label='Enable Error Logging', callback=toggle_log_toggle, tag='log_toggle', default_value=True)
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
                    tag="radioColors"
                )
                
        with dpg.collapsing_header(
            label="Logs", default_open=False, tag="Logs"
        ):
            
            with dpg.child_window(width=980, height=300):
                container = dpg.add_child_window(width=960, height=280)
        
        with dpg.collapsing_header(
            label="User Input", default_open=True, tag="User Input"
        ):
            
            
            dpg.add_input_text(
                label="Data Type PBI",
                tag="dataTypeInputP",
            )
            dpg.add_input_text(
                label="Data Type Output",
                tag="dataTypeInputO",
            )
            dpg.add_button(label="Data Type", tag='dataTypeInputButton', callback=lambda: add_input('dataType'))
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
            dpg.add_button(label="Function Name", tag='functionNameInputButton', callback=lambda: add_input('functionName'))
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
            dpg.add_button(label="Visual Type", tag='visualTypeInputButton', callback=lambda: add_input('visualType'))
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
            dpg.add_button(label="TE Location", tag='TELocationButton', callback=lambda: add_input('TELocation'))
            dpg.add_text(
                "",
                show=False,
                tag="TELocationInputInfo",
            )
            
        dpg.add_spacer(height=10)
        dpg.add_button(label="Regenerate tsv file", tag="genTSV", callback=generate_tsv)
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
        dpg.add_button(label="Run PB-Ixtractor", tag="runPBIX", callback=run_extractor)
        dpg.add_text(
            "",
            show=False,
            tag="runText",
        )
        disable_buttons()

    # Window
    dpg.create_viewport(
        title="PB-Ixtractor", width=1000, height=800, large_icon="logo.ico"
    )
    dpg.setup_dearpygui()
    dpg.show_viewport()
    dpg.start_dearpygui()
    dpg.destroy_context()


def gen_tsv(force: bool = False):
    cwd = os.getcwd() + f"\\{SAVE_NAME}"
    
    if not os.path.exists(cwd):
        os.makedirs(cwd)

    def find_tabular_editor_path() -> str:
        target_exe = Path("TabularEditor.exe")
        
        input_dir = os.getcwd() + '\\Input\\TabularEditorLocations.txt'

        # Default directories to search
        if not os.path.exists(input_dir):
            common_directories = [
                "C:\\Program Files",
                "C:\\Program Files (x86)",
            ]
            with open(input_dir, 'w') as file:
                for string in common_directories:
                    file.write(string + '\n')
        else:
            
            with open(input_dir, 'r') as file:
                common_directories = file.readlines()

            for i, row in enumerate(common_directories):
                common_directories[i] = Path(row.replace('\n', ''))

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
        return 'NoTabEd'

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
    //var tsv = ExportProperties(objects,"Name,ObjectType,Parent,Description,FormatString,DataType,Expression");
    var tsv = ExportProperties(objects);

    // Save the TSV to a file:
    SaveFile("{cwd_parsed}//documentation.tsv", tsv);
    """
        with open(f"{cwd}\\TabularScript.cs", "w", encoding="utf-8") as file:
            file.write(c_code)

    tsv_path = Path(f"{cwd}\\documentation.tsv")

    if os.path.exists(tsv_path):
        os.remove(tsv_path)

    command = (
        f'& {tab_edit_path} "{_BIM_[1]}/{_BIM_[0]}.bim" -S "{cwd}\\TabularScript.cs"'
    )
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
                raise TimeoutError(
                    f"File {file_path} not found within the timeout period"
                )
            time.sleep(0.1)

    wait_for_file(file_path=f"{cwd}\\documentation.tsv", timeout=5)

def write_to_excel(worksheet, row:int, col:int, text:list[str]):
    
    if len(text) <= 2:
        worksheet.write(row, col, *text)
    else:
        worksheet.write_rich_string(row, col, *text)
    

def is_excel_open_with_file(file_path: str) -> bool:
    """
    Check if Excel is open with a specific file.

    Parameters:
        file_path (str): The path of the Excel file to check.

    Returns:
        bool: True if Excel is open with the specified file, False otherwise.
    """
    for process in psutil.process_iter():
        try:
            if process.name().lower() == "excel.exe":
                for file in process.open_files():
                    if file.path.lower() == file_path.lower():
                        return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


def run_cmd():
    global SAVE_NAME, _BIM_, _PBIX_, LOG_DATA, REPORT_LOG

    cwd = os.getcwd()
    cwd_save = cwd + f"\\{SAVE_NAME}"

    file_path = f"{cwd_save}\\{SAVE_NAME}.xlsx"
    if is_excel_open_with_file(file_path):
        return f"Please Close File: {SAVE_NAME}.xlsx before proceeding!"

    button_type_list = ["Bookmark", "PageNavigation"]

    tsv_path = Path(f"{cwd_save}\\documentation.tsv")
    if not os.path.isfile(tsv_path):
        
        if gen_tsv() == 'NoTabEd':
            return 'NoTabEd'

    rep_ex = ReportExtractor(
        _PBIX_[1],
        f"{_PBIX_[0]}.pbix",
    )

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

    report_filters = []
    [
        report_filters.append(sublist)
        for sublist in rep_ex.filters
        if sublist not in report_filters
    ]
    report_filters_string = [
        [
            sublist[0],
            sublist[1],
            sublist[2],
            f"{sublist[3]}[{sublist[4]}]",
            " ".join(sublist[5:]),
        ]
        for sublist in report_filters
    ]

    REPORT_LOG = rep_ex.log

    def find_nth_occurence(substring: str, string: str, n: int) -> int:
        """
        returns starting index of n:th substring in string
        """
        count = 0
        index = -1

        while count < n:
            index = string.find(substring, index + 1)

            if index == -1:
                break

            count += 1

        return index

    def find_vars(string: str) -> tuple[str]:
        """Returns all formatted variable names"""
        var_names = []
        tokens = string.split()

        if tokens.count("VAR") != 0:
            indexes = [index for index, value in enumerate(tokens) if value == "VAR"]

            for index in indexes:
                var_names.append(tokens[index + 1])

        return var_names

    def find_functions(string: str) -> tuple[str]:
        """
        Checks through input string and returns list of all known functions
        """
        used_functions = []

        for func in known_functions:
            if string.find(func) != -1:
                used_functions.append(func)

        return used_functions

    def find_measures(string: str) -> tuple[str]:
        pattern = r"\[.*?\]"
        all_measures = re.findall(pattern, string)
        unique_measures = list(set(all_measures))
        return unique_measures

    def find_columns(string: str) -> tuple[str]:
        pattern = re.compile(r"(\w+)\[(.*?)\]")
        all_columns = re.findall(pattern, string)
        unique_columns = list(set(all_columns))
        return unique_columns

    def get_data_type(string: str) -> tuple[str, str, str]:
        data = "Table"
        start_pos = find_nth_occurence(".", string, 2) + 1
        end_pos = find_nth_occurence(".", string, 3)
        if end_pos == -1:
            table = string[start_pos:]
        else:
            table = string[start_pos:end_pos]

        column = ""
        if any(substring in string for substring in [".C.", ".H.", ".M."]):
            start_pos = find_nth_occurence(".", string, 4) + 1
            end_pos = find_nth_occurence(".", string, 5)
            if end_pos == -1 or end_pos < len(string):
                column = string[start_pos:]
            else:
                column = string[start_pos:end_pos]

            if column[0] == "[":
                column = column[1:]
            if column[-1] == "]":
                column = column[:-1]

            if ".C." in string:
                data = "Column"
            elif ".H." in string:
                data = "Hierarchy"
            elif ".M." in string:
                data = "Measure"

        return (data, table, column)

    # Create the DataFrame
    data = {
        "Type": [],
        "Name": [],
        "DataType": [],
        "Description": [],
        "Definition": [],
        "Table": [],
        "Dependants": [],
        "Comment": [],
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
        data_type = get_data_type(dataset.iloc[i]["Object"])
        if data_type[0] == "Table":
            rel_pattern = re.match(tab_rel_pattern, data_type[1])
            if rel_pattern is not None and rel_pattern not in all_relationships:
                all_relationships.append(dataset.iloc[i]["Name"])

            elif data_type[1] not in all_tables and "Relationship." not in data_type[1]:
                all_tables.append(data_type[1])

    # Remove excess " ' " surrounding table names
    escape_pattern = r"'(?:\s*)(" + "|".join(map(re.escape, all_tables)) + r")(?:\s*)'"
    for i, row in enumerate(dataset.iloc()):
        exp = row["Expression"]
        if pd.isna(exp):
            continue

        exp = exp.replace("\\t", "    ")

        match = re.search(escape_pattern, exp)
        if match:
            dataset.iloc[i]["Expression"] = exp.replace(match.group(0), match.group(1))

    # Read .tsv file and convert to usable dataframe
    for i in range(len(dataset)):
        line_data = dataset.iloc[i]

        data_type = get_data_type(line_data["Object"])

        # Currently don't need to do anything with all tables or hierarchies
        if data_type[0] == "Table":
            continue

        elif data_type[0] == "Hierarchy":
            all_hierarchies.append((data_type[1], data_type[2]))
            continue

        elif data_type[0] == "Column" or data_type[0] == "Measure":
            unused_columns.append((data_type[1], data_type[2]))

        if not isinstance(line_data["Expression"], float):
            definition = line_data["Expression"]
            definition = definition.replace("    ", "\t")
            definition = definition.replace("\\n", "\n")
        else:
            definition = ""

        # Extract description if embedded in definition
        if definition.find(DESCRIPT_TAG) != -1:
            comment_start = find_nth_occurence(DESCRIPT_TAG, definition, 1) + 5
            comment_end = find_nth_occurence(DESCRIPT_TAG, definition, 2) - 1
            definition_start = comment_end + 6
        else:
            comment_start = 0
            comment_end = comment_start
            definition_start = comment_start

        df_type = data_type[0]
        df_name = line_data["Name"]
        df_data_type = line_data["DataType"]
        if pd.isna(line_data["Description"]):
            df_description = definition[comment_start:comment_end].strip()
            df_description = df_description.replace("\\n", "\\r\\n")
        else:
            df_description = line_data["Description"]
        df_definition = definition[definition_start:].strip()
        df_definition = df_definition.replace("\n", "\r\n")
        df_table = data_type[1]

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
            "Comment": "",
        }

        df.loc[-1] = new_data
        df.index = df.index + 1
    df = df.sort_index()

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

        node_colors = [
            color_map[node] if node in child_nodes else "lightgreen" for node in G.nodes
        ]

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

        plt.savefig(f"{SAVE_NAME}\\{SAVE_NAME}_Relationships.png", bbox_inches="tight")
        plt.close()

    generate_graph(df_relations, 12, (len(df_relations) + 1) * 14.4 / 72)

    # Remove Cols/Measures from 'unused_columns' that are used in visuals
    for row in report_info.iloc():
        used_columns = (row["Table"], row["Name"])
        if used_columns in unused_columns:
            unused_columns.remove(used_columns)

    for filter in report_filters:
        temp_col = (filter[3], filter[4])
        if temp_col in unused_columns:
            unused_columns.remove(temp_col)

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
        return workbook.add_format(
            {"color": rgba_tuple_to_hex(default_colors[index][1])}
        )

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
        "para": [
            workbook.add_format({"color": color}) for color in paranthesis_color * 3
        ],
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
                    f"{SAVE_NAME}\\{SAVE_NAME}_Relationships.png",
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
        vDefinition = row["Definition"]

        # Skip traditional columns for now
        if row["Type"] == "Column":
            continue

        # Find Vars and measures
        var_names = find_vars(vDefinition)
        function_names = find_functions(vDefinition)
        columns = find_columns(vDefinition)
        tables = [i for i, _ in columns]
        columns_clean = ['[' + i + ']' for _, i in columns]
        measures = find_measures(vDefinition)
        
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

        formated_text = vDefinition.replace("\t", " XXX ")
        formated_text = formated_text.replace("\r\n", " YYY ")
        formated_text = formated_text.replace("&&", " ZZZ ")
        formated_text = formated_text.replace("||", " AAA ")

        # Split the text into rows
        pattern = re.compile(r"(\(|\)|\[.*?\]|,|//|\d+\.\d+|\w+|(?<!\d)\.(?!\d)|\W)")
        tokens = [
            token for token in re.findall(pattern, formated_text) if token.strip()
        ]

        format_array = []
        parents_array = []
        parenthesis_count = -1
        is_whole_line_comment = False
        quote_counter = 0

        # Store away all parents used in func. Columns get table name as prefix, measures get default measure table
        if len(columns) > 0:
            for token in [i + "[" + j + "]" for i, j in columns]:
                parents_array.append(token)
                parents_array.append("\r\n")
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
                ls_app("\r\n")
            elif token == "ZZZ":
                ls_app("&& ")
            elif token == "AAA":
                ls_app("|| ")
            elif token == "(":
                parenthesis_count += 1
                ls_app(formats["para"][parenthesis_count], token + " ")
            elif token == ")":
                ls_app(formats["para"][parenthesis_count], token + " ")
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
            elif col == parent_index and len(parents_array) != 0:
                write_to_excel(worksheet, row_num, col, parents_array)
            else:
                worksheet.write(row_num, col, value)
        row_num += 1

    row_num += 6
    for col_pair in unused_columns:
        worksheet.write(row_num, 0, col_pair[0] + "[" + col_pair[1] + "]")
        row_num += 1

    # Create a tab per report page with visual info.
    for report_name in report_info["Page"].unique().tolist():
        save_report_name = report_name.replace("/", "_")
        worksheetX = workbook.add_worksheet(save_report_name)

        worksheetX.set_column(0, 5, 30, def_format)
        worksheetX.set_column(2, 2, 100, def_format)
        worksheetX.set_column(3, 3, 60, def_format)

        local_df = report_info[report_info["Page"] == report_name]
        visual_ids = local_df[["Visual ID"]]["Visual ID"].unique().tolist()

        local_df = local_df.sort_values(by=["Visual Type", "Type"])

        dataX = {
            "Item Type": [],
            "Visual Type": [],
            "Description": [],
            "Visual Filters": [],
            "Interactivity": [],
            "Comment": [],
            "ID": [],
        }

        dfX = pd.DataFrame(dataX)

        for visual in visual_ids:
            visual_type = local_df[local_df["Visual ID"] == visual].iloc[0][
                "Visual Type"
            ]

            v_type = "Visual"
            if visual_type == "tableEx":
                s_type = "Table"
            elif visual_type == "pivotTable":
                s_type = "Matrix"
            elif visual_type == "card":
                s_type = "Card"
            elif visual_type == "cardVisual":
                s_type = "Card (new)"
            elif visual_type == "gauge":
                s_type = "Gauge"
            elif visual_type == "slicer":
                v_type = "Slicer"
                s_type = local_df[local_df["Visual ID"] == visual].iloc[0]["Table"]
            elif visual_type == "advancedSlicerVisual":
                v_type = "Slicer (new)"
                s_type = local_df[local_df["Visual ID"] == visual].iloc[0]["Table"]
            elif visual_type in visual_type_list:
                words = re.findall("[a-zA-Z][^A-Z]*", visual_type)
                s_type = ""
                for word in words:
                    s_type += word.capitalize() + " "
            elif visual_type in button_type_list:
                v_type = "Button"
                s_type = visual_type
            elif visual_type in ["actionButton"]:
                v_type = "Button"
                s_type = "Button"
            elif visual_type == "Group":
                v_type = "Group"
                s_type = "Panel"
            else:
                REPORT_LOG += log_data('New Visual type not yet supported!', visual_type, 1)

            new_data = {
                "Item Type": v_type,
                "Visual Type": s_type,
                "Description": "",
                "Visual Filters": "",
                "Interactivity": "",
                "Comment": "",
                "ID": visual,
            }

            dfX.loc[-1] = new_data
            dfX.index = dfX.index + 1

        for i_filter, filter in enumerate(report_filters_string):
            if filter[2] == "This Page" and filter[0] == report_name:
                new_data = {
                    "Item Type": "Filter",
                    "Visual Type": "This Page",
                    "Description": "",
                    "Visual Filters": "",
                    "Interactivity": "",
                    "Comment": "",
                    "ID": i_filter,
                }

                dfX.loc[-1] = new_data
                dfX.index = dfX.index + 1

        col = 0
        for name, value in new_data.items():
            if name == "ID":
                continue
            worksheetX.write(0, col, name, formats["bi"])
            col += 1

        sort_order = ["Visual", "Slicer", "Filter", "Button", "Group"]
        dfX["Item Type"] = pd.Categorical(
            dfX["Item Type"], categories=sort_order, ordered=True
        )
        df_sorted = dfX.sort_values(by=["Item Type", "Visual Type"])

        row_num = 0
        slicer_switch = True
        filter_switch = True
        button_switch = True
        for _, row in df_sorted.iterrows():
            filter_array = []
            for filter in report_filters_string:
                if (
                    filter[2] == "Visual"
                    and filter[0] == report_name
                    and filter[1] == row["ID"]
                ):
                    filter_array.extend(
                        [formats["bold"], filter[3], " " + filter[4] + "\n"]
                    )

            if filter_array and filter_array[-1][-1] == "\n":
                filter_array[-1] = filter_array[-1][:-1]

            if slicer_switch and row["Item Type"] == "Slicer":
                slicer_switch = False

            if filter_switch and row["Item Type"] == "Filter":
                filter_switch = False

            if button_switch and row["Item Type"] == "Button":
                button_switch = False

            if filter_switch:
                # Regular Measures
                format_array = []
                r_data = local_df[local_df["Visual ID"] == row["ID"]]
                current_type = None
                for im, rrow in enumerate(r_data.iloc()):
                    if rrow["Type"] != current_type:
                        current_type = rrow["Type"]
                        if im != 0:
                            ls_app("\n")
                        ls_app(formats["bold"], current_type + ": ")
                    ls_app("\n", formats["italic"], f'{rrow["Table"]}[{rrow["Name"]}]')

                    if rrow["Display Name"]:
                        ls_app(
                            "\n\t Display Name: ",
                            formats["italic"],
                            rrow["Display Name"],
                        )

            elif row["Item Type"] in ["Button", "Group"]:
                rrow = report_info[report_info["Visual ID"] == row["ID"]].iloc[0]
                format_array = [
                    formats["bold"],
                    rrow["Type"] + ": ",
                    formats["italic"],
                    rrow["Name"],
                ]

            else:
                if isinstance(row["Item Type"], float):
                    REPORT_LOG += log_data('NaN Item Type Encountered!', row, 2)
                    continue  # Temp fix for NaN Item Type

                # Filters
                format_array = [formats["bold"]]
                ls_app(
                    report_filters_string[row["ID"]][3],
                    " " + report_filters_string[row["ID"]][4],
                )

            for col, value in enumerate(row):
                if col == 2 and len(format_array) != 0:
                    write_to_excel(worksheetX, row_num, col, format_array)
                elif col == 3 and len(filter_array) != 0:
                    write_to_excel(worksheetX, row_num, col, filter_array)
                elif col == 6:
                    continue
                else:
                    worksheetX.write(row_num + 1, col, value)

            row_num += 1

    workbook.close()
    
    ## Print Logging Info -- Needs more love
    if REPORT_LOG and LOG_DATA:
        t = time.localtime()
        current_time = time.strftime("%H_%M_%S", t)
        location_folder = cwd + f"\\{SAVE_NAME}\\logs"
        location = location_folder + f"\\log_data_{current_time}.txt"
        
        if not os.path.exists(location_folder):
            os.makedirs(location_folder)
        
        with open(location, "w") as text_file:
            text_file.write(REPORT_LOG)

        return "Log"

    return "Success"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="PBIXtractor automatically generates Documentation material for a given PBIX-file."
    )

    # Define the command-line arguments
    parser.add_argument("-file", dest="file", type=str, help="Name of PBIX-File")
    parser.add_argument("-o", dest="output", type=str, help="Name of output-File")
    parser.add_argument(
        "-ui", action="store_true", help="Runs in UI mode with additional options"
    )
    parser.add_argument(
        "-yes_man", dest="yes_man", action="store_true", help="Remove Input Protection"
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
            _file_ = "Order Entry"
            yes_man = False
            SAVE_NAME = _file_

        _PBIX_ = [
            _file_,
            "C:\\Users\\Reports",
        ]
        _BIM_ = [
            _file_,
            "C:\\Users\\PB-Ixtractor",
        ]

        result = run_cmd()
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
