import pandas as pd
import os
import re
import xlsxwriter
import time

import json
from zipfile import ZipFile
import shutil

import subprocess
from pathlib import Path

TEST = False
REPLACE_TSV = False

if TEST:
    _file_ = "DataTest"
else:
    _file_ = "Inventory"#"Order Entry"


button_type_list = ["Bookmark", "PageNavigation"]

visual_type_list = [
    "lineClusteredColumnComboChart",
    "clusteredColumnChart",
    "clusteredBarChart",
    "donutChart",
    "azureMap",
    "barChart",
    "scatterChart",
    "columnChart",
    "hundredPercentStackedBarChart",
    "hundredPercentStackedColumnChart",
    "lineChart",
    "areaChart",
    "stackedAreaChart",
    "lineStackedColumnComboChart",
    "ribbonChart",
    "waterfallChart",
    "funnel",
    "pieChart",
    "treemap",
    "map",
    "filledMap",
    "multiRowCard",
    "kpi",
    "pivotTable",
    "keyDriversVisual",
    "decompositionTreeVisual",
    "card",
    "cardVisual",
    "gauge",
    "tableEx",
    "slicer",
    "advancedSlicerVisual",
]

data_type_list = [
    ["Tooltips", "Tooltips"],
    ["Category", "Categoricals"],
    ["X", "X-Values"],
    ["Y", "Y-Values"],
    ["Y2", "Y2-Values"],
    ["Size", "Size Indicator"],
    ["Series", "Series"],
    ["Rows", "Rows"],
    ["Columns", "Cols"],
    ["Play", "Play"],
    ["Breakdown", "Breakdown"],
    ["Group", "Group"],
    ["Values", "Values"],
    ["Details", "Details"],
    ["Indicator", "Indicators"],
    ["TrendLine", "Trendlines"],
    ["Goal", "Goals"],
    ["Target", "Targets"],
    ["ExplainBy", "Explained By"],
    ["Anazlze", "Analyzed"],
    ["Data", "Data"],
    ["MaxValue", "Max Values"],
    ["MinValue", "Min Values"],
    ["TargetValue", "Target Values"],
]


class ReportExtractor:
    def __init__(self, path, name):
        self.path = path
        self.name = name
        self.result = []
        self.filters = []

    def find_value_by_key(self, data: dict, target_key: str) -> dict | None:
        '''
        Looks through input dict and finds first occurence that matches specific key
        
        data: Input dictionary
        target_key: Searched for string key
        
        return dict with data or None
        '''
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
        '''
        Returns list of all paths and values from dict with specified key word
        
        data: input dict to search
        key_word: str to search for
        
        returns [[Path, Value], [Path, Value], ...]
        '''
        
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
        '''
        Searches data for 'ComparissonKind' information, returns list of comparisson integers
        
        data: input dict to search
        
        returns [x, y]
        '''
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
        self, page: str, visual_type: str, item_name: str, table_name: str, val_name: str, disp_name: str, data_type: str
    ) -> None:
        '''
        Stores input data into the self.result field
        '''
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
        self, page: str, item_name: str, filter_type: str, table_name: str, val_name: str, ver: str, value: str
    ):
        '''
        Stores input data into the self.filters field
        '''
        filter_set = [page, item_name, filter_type, table_name, val_name, ver, value]

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
        '''
        Converts list of incoming data into valid strings that can be stored in the self.result/self.filters fields
        
        all_values: list of incoming data
        
        returns valid string, boolean if condition is inverted
        '''
        val_list = ""
        is_inverted = False

        for val in all_values:
            if "Where" in val[0]:
                val_list += self.clean_input(val[1]) + ", "
            elif "isInverted" in val[0]:
                is_inverted = self.clean_input(val[1])
            else:
                print("Lost in gen_val_string!")

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

        for s in report_layout["sections"]:
            page_name = s["displayName"]

            for ex_data in s["visualContainers"]:
                # Add visuals
                if ex_data.get("config", "") != "":
                    t = json.loads(ex_data["config"])

                    item_name = t["name"]
                    visual_type = self.find_value_by_key(t, "visualType")

                    if visual_type in ("shape", "image", "textbox"):
                        # None = for ex. Filter Popup
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

                        if page_name == "Customer Overview" and visual_type == "card":
                            1

                        for row in data:
                            if row.get("HierarchyLevel", "") != "":
                                temp = self.find_value_by_key(row, "Name")
                                temp2 = temp.split(".")

                                table_name = temp2[0]
                                val_name = temp2[2]
                            elif (
                                row.get("Measure", "") != ""
                                or row.get("Column", "") != ""
                            ):
                                temp = row["Name"]
                                temp2 = temp.split(".", 1)
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
                                print("Something here!")

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

                            if not disp_name or disp_name == val_name:
                                disp_name = None

                            if (
                                data[0].get("HierarchyLevel", "") != ""
                                and data_type is None
                            ):
                                data_type = "Hierarchy"

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
                            item_name = self.find_value_by_key(temp2, "Value")
                            item_name = item_name.replace("'", "")
                            data_type = "Bookmark"

                        elif button_type == "PageNavigation":
                            temp2 = self.find_value_by_key(t, "navigationSection")
                            item_name = self.find_value_by_key(temp2, "Value")
                            item_name = item_name.replace("'", "")

                            data_type = "Page"
                            disp_name = "Page Navigation"
                            for x in report_layout["sections"]:
                                if item_name == x.get("name", ""):
                                    val_name = x["displayName"]
                        elif button_type == 'custom':
                            
                            item_name = 'Filter'
                            data_type = 'Icon'
                            disp_name = 'Filter Icon' ## TODO currently not used as visual, is more of a "Button"
                            continue
                        else:
                            print(f'Found unknown Visual Type: {button_type}, on page {page_name}. Skipping addition!')
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
                        print("New Visual Type not yet supported!")

                # Add filters
                if ex_data.get("filters", "[]") != "[]":
                    t = json.loads(ex_data["filters"])

                    local_config = json.loads(ex_data["config"])
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
                                    print("Unknown Include Today, setting to true!")
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
                        print(f"Unused filter on page/visual.. {page_name}")
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
                        1

                else:
                    1

        shutil.rmtree(pathFolder)


def find_tabular_editor_path() -> str:
    target_exe = Path("TabularEditor.exe")

    # Default directories to search
    common_directories = [
        Path("C:\\Program Files"),
        Path("C:\\Program Files (x86)"),
    ]

    for directory in common_directories:
        target_path = directory / "Tabular Editor" / target_exe
        if target_path.exists():
            result = str(target_path)
            result = '"' + result + '"'
            return result

    # Return None if the executable file is not found
    return None


cwd = os.getcwd()
tab_edit_path = find_tabular_editor_path()
if tab_edit_path is None:
    print(
        "Cannot find Tabular Editor on computer! Please edit common directories to include folder with file"
    )
    exit(0)
bim_path = f'"C:\\Users\\{_file_}.bim"'
script_path = f'"{cwd}\\TabularScript.cs"'

## If file not present, create it!
if not os.path.isfile(script_path):
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
    with open("TabularScript.cs", "w", encoding="utf-8") as file:
        file.write(c_code)

tsv_path = Path(f"{cwd}\\documentation.tsv")

if REPLACE_TSV:
    #Delete old .tsv file
    if os.path.exists(tsv_path):
        os.remove(tsv_path)

    command = f'& "C:\\Program Files (x86)\\Tabular Editor\\TabularEditor.exe" "C:/Users/PowerBIConverter/{_file_}.bim" -S "C:\\Users\\TabularScript.cs"'
    process = subprocess.Popen(["powershell", "-Command", command])
    process.wait()


## Wait for file gen -
def wait_for_file(file_path: str, timeout: int=None):
    '''
    Waits for maximum timeout seconds or until file_path has been created 
    '''
    start_time = time.time()
    while not os.path.exists(file_path):
        if timeout is not None and time.time() - start_time > timeout:
            raise TimeoutError(f"File {file_path} not found within the timeout period")
        time.sleep(0.1)

wait_for_file(file_path="documentation.tsv", timeout=5)

if os.path.isfile(tsv_path):
    dataset = pd.read_csv("documentation.tsv", sep="\t", header=0)

# Specify the file path where you want to save the Excel file
file_name = f"{_file_}_Documentation"
default_measure_table = "_Measures"

if TEST:
    rep_ex = ReportExtractor(
        "C:\\Users\\PowerBIConverter",
        f"{_file_}.pbix",
    )
else:
    rep_ex = ReportExtractor(
        "C:\\Users\\Reports",
        f"{_file_}.pbix",
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

def find_nth_occurence(substring: str, string: str, n: int) -> int:
    '''
    returns starting index of n:th substring in string
    '''
    count = 0
    index = -1

    while count < n:
        index = string.find(substring, index + 1)

        if index == -1:
            break

        count += 1

    return index


def find_vars(string: str) -> tuple[str]:
    """Find all formatted variable names and return them for future usage"""
    var_names = []
    tokens = string.split()

    if tokens.count("VAR") != 0:
        indexes = [index for index, value in enumerate(tokens) if value == "VAR"]

        for index in indexes:
            var_names.append(tokens[index + 1])

    return var_names


def find_functions(string: str) -> tuple[str]:
    '''
    Checks through input string and returns list of all known functions
    '''
    known_functions = [
        "IF",
        "DIVIDE",
        "MOD",
        "FORMAT",
        "CALCULATE",
        "CALCULATETABLE",
        "VALUES",
        "ISBLANK",
        "YEAR",
        "MONTH",
        "DAY",
        "WEEKDAY",
        "WEEKNUM",
        "DATEDIFF",
        "DATEADD",
        "DATESBETWEEN",
        "DATESINPERIOD",
        "SWITCH",
        "DATE",
        "LASTDATE",
        "FIRSTDATE",
        "TODAY",
        "EOMONTH",
        "ISFILTERED",
        "FALSE",
        "TRUE",
        "SUM",
        "SUMX",
        "AVERAGE",
        "AVERAGEX",
        "DISTINCTCOUNT",
        "COUNT",
        "COUNTX",
        "CONCATENATE",
        "CONCATENATEX",
        "SAMEPERIODLASTYEAR",
        "ALL",
        "ALLSELECTED",
        "COUNTROWS",
        "MIN",
        "MINX",
        "MAX",
        "MAXX",
        "IN",
        "LEFT",
        "RIGHT",
        "DISTINCT",
        "SELECTEDVALUE",
        "BLANK",
        "ISINSCOPE",
        "FILTER",
        "USERELATIONSHIP",
        "REMOVEFILTERS",
        "ABS",
        "HASONEVALUE",
        "RELATED",
    ]
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
    "C:\\Users\\documentation.tsv",
    sep="\t",
    header=0,
)
excel_file = file_name + ".xlsx"


# Extract all Table names
tab_rel_pattern = r'^Relationship\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
for i in range(len(dataset)):
    
    data_type = get_data_type(dataset.iloc[i]["Object"])
    if data_type[0] == "Table":
        
        rel_pattern = re.match(tab_rel_pattern, data_type[1])
        if rel_pattern is not None and rel_pattern not in all_relationships:
            all_relationships.append(data_type[1])
        
        elif data_type[1] not in all_tables:
            all_tables.append(data_type[1])

# Remove excess " ' " surrounding table names
escape_pattern = r"'(?:\s*)(" + '|'.join(map(re.escape, all_tables)) + r")(?:\s*)'"
for i, row in enumerate(dataset.iloc()):
    
    exp = row['Expression']
    if pd.isna(exp):
        continue
    
    exp = exp.replace('\\t', '    ')
    
    match = re.search(escape_pattern, exp)
    if match:
        dataset.iloc[i]['Expression'] = exp.replace(match.group(0), match.group(1))
    

# Read .tsv file and convert to usable dataframe
for i in range(len(dataset)):
    line_data = dataset.iloc[i]

    data_type = get_data_type(line_data["Object"])

    # Currently don't need to do anything with all tables or hierarchies
    if data_type[0] == "Table":
        continue

    if data_type[0] == "Hierarchy":
        all_hierarchies.append((data_type[1], data_type[2]))
        continue

    if data_type[0] == "Column" or data_type[0] == "Measure":
        unused_columns.append((data_type[1], data_type[2]))

    if not isinstance(line_data["Expression"], float):
        definition = line_data["Expression"]
        definition = definition.replace("    ", "\t")
        definition = definition.replace("\\n", "\n")
    else:
        definition = ""

    # Extract description if embedded in definition
    if definition.find("////") != -1:
        comment_start = find_nth_occurence("////", definition, 1) + 5
        comment_end = find_nth_occurence("////", definition, 2) - 1
        definition_start = comment_end + 6
    else:
        comment_start = 0
        comment_end = comment_start
        definition_start = comment_start

    df_type = data_type[0]
    df_name = line_data["Name"]
    df_data_type = line_data["DataType"]
    if pd.isna(line_data['Description']):
        df_description = definition[comment_start:comment_end].strip()
        df_description = df_description.replace("\\n", "\\r\\n")
    else:
        df_description = line_data['Description']
    df_definition = definition[definition_start:].strip()
    df_definition = df_definition.replace("\n", "\r\n")
    df_table = data_type[1]

    # Find which page the measures/calculations are on
    df_report_pages = []
    for i, row in enumerate(report_info["Name"]):
        if row == df_name:
            df_report_pages.append(report_info["Page"][i])
    unique_df_report_pages = list(set(df_report_pages))

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

# Remove Cols/Measures from 'unused_columns' that are used in visuals
for row in report_info.iloc():
    used_columns = (row["Table"], row["Name"])
    if used_columns in unused_columns:
        unused_columns.remove(used_columns)

for filter in report_filters:
    bracket_index = filter[2].find("[")
    temp_col = (filter[2][:bracket_index], filter[2][bracket_index + 1 : -1])
    if temp_col in unused_columns:
        unused_columns.remove(temp_col)


# Delete old data
if os.path.exists(excel_file):
    os.remove(excel_file)

workbook = xlsxwriter.Workbook(excel_file)
worksheet = workbook.add_worksheet(f"{_file_} Common")

# Add column formatting.
def_format = workbook.add_format()
def_format.set_align("top")
def_format.set_text_wrap()
wrap_format = workbook.add_format()
wrap_format.set_text_wrap()
worksheet.set_column(0, len(new_data), 30, wrap_format)
worksheet.set_column(definition_index, definition_index, 100, def_format)
worksheet.set_column(definition_index + 1, definition_index + 1, 30, wrap_format)
worksheet.set_column(parent_index, parent_index, 50, wrap_format)

fFUNCTION = workbook.add_format({"color": "#3165bb"})
fRETURN = workbook.add_format({"color": "1800ff"})
fVAR = workbook.add_format({"color": "#000fff"})
fVARNAME = workbook.add_format({"color": "#098658"})
fCOMMENT = workbook.add_format({"color": "#00800f"})
fBRACKETS = workbook.add_format({"color": "#6f349c"})
fQUOTE = workbook.add_format({"color": "#a31515"})
fMEASURE = workbook.add_format({"color": "#001080"})
fBOLDITALIC = workbook.add_format({"bold": True, "italic": True})
fITALIC = workbook.add_format({"italic": True})
fBOLD = workbook.add_format({"bold": True})

fColor = [
    workbook.add_format({"color": "#0433fa"}),
    workbook.add_format({"color": "#319331"}),
    workbook.add_format({"color": "#7b3831"}),
    workbook.add_format({"color": "#0433fa"}),
    workbook.add_format({"color": "#319331"}),
    workbook.add_format({"color": "#7b3831"}),
    workbook.add_format({"color": "#0433fa"}),
    workbook.add_format({"color": "#319331"}),
    workbook.add_format({"color": "#7b3831"}),
]

col = 0
for name, value in new_data.items():
    worksheet.write(0, col, name, fBOLDITALIC)
    col += 1

row_num = 0
for _, row in df.iterrows():
    vDefinition = row["Definition"]

    # Skip traditional columns for now
    if vDefinition == "":
        continue

    # Find Vars and measures
    var_names = find_vars(vDefinition)
    function_names = find_functions(vDefinition)
    columns = find_columns(vDefinition)
    tables = [i for i, _ in columns]
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
    pattern = re.compile(r"(\(|\)|\[.*?\]|,|[\w]+|//|\W)")
    tokens = [token for token in re.findall(pattern, formated_text) if token.strip()]

    # Iternate through the segments and add a format before the matches.
    format_array = []
    parents_array = []

    # Store away all parents used in func. Columns get table name as prefix, measures get default measure table
    if len(columns) > 0:
        for token in [i + "[" + j + "]" for i, j in columns]:
            parents_array.append(token)
            parents_array.append("\r\n")
        parents_array.pop(-1)

    parenthesis_count = -1
    is_whole_line_comment = False
    quote_counter = 0

    for token in tokens:
        if token == "//":
            is_whole_line_comment = True
        elif token == "YYY":
            is_whole_line_comment = False

        if token == '"' and not is_whole_line_comment:
            quote_counter += 1

        if is_whole_line_comment:
            format_array.append(fCOMMENT)
            format_array.append(token + " ")
        elif quote_counter > 0:
            format_array.append(fQUOTE)
            if quote_counter == 2:
                format_array.append(token + " ")
                quote_counter = 0
            else:
                format_array.append(token)
        elif token == "XXX":
            format_array.append("\t")
        elif token == "YYY":
            format_array.append("\r\n")
        elif token == 'ZZZ':
            format_array.append("&& ")
        elif token == 'AAA':
            format_array.append("|| ")
        elif token == "(":
            parenthesis_count += 1
            format_array.append(fColor[parenthesis_count])
            format_array.append(token + " ")
        elif token == ")":
            format_array.append(fColor[parenthesis_count])
            format_array.append(token + " ")
            parenthesis_count -= 1
        elif token == "VAR":
            format_array.append(fVAR)
            format_array.append(token + " ")
        elif token in var_names:
            format_array.append(fVARNAME)
            format_array.append(token + " ")
        elif token in measures:
            format_array.append(fColor[parenthesis_count + 1])
            format_array.append(token[0])
            format_array.append(fMEASURE)
            format_array.append(token[1:-1])
            format_array.append(fColor[parenthesis_count + 1])
            format_array.append(token[-1] + " ")
        elif token in tables:
            format_array.append(fMEASURE)
            format_array.append(token)
        elif token in function_names:
            format_array.append(fFUNCTION)
            format_array.append(token + " ")
        elif token == "RETURN":
            format_array.append(fRETURN)
            format_array.append(token + " ")
        else:
            format_array.append(token + " ")

    for col, value in enumerate(row):
        if col == definition_index and len(format_array) != 0:
            worksheet.write_rich_string(row_num + 1, definition_index, *format_array)
        elif col == parent_index and len(parents_array) != 0:
            worksheet.write_rich_string(row_num + 1, parent_index, *parents_array)
        else:
            worksheet.write(row_num + 1, col, value)
    row_num += 1

row_num += 5
for col_pair in unused_columns:
    worksheet.write(row_num + 1, 0, col_pair[0] + "[" + col_pair[1] + "]")
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
        table_type = local_df[local_df["Visual ID"] == visual].iloc[0]["Visual Type"]

        v_type = "Visual"
        if table_type == "tableEx":
            s_type = "Table"
        elif table_type == "pivotTable":
            s_type = "Matrix"
        elif table_type == "card":
            s_type = "Card"
        elif table_type == "cardVisual":
            s_type = "Card (new)"
        elif table_type == "gauge":
            s_type = "Gauge"
        elif table_type == "slicer":
            v_type = "Slicer"
            s_type = local_df[local_df["Visual ID"] == visual].iloc[0]["Table"]
        elif table_type == "advancedSlicerVisual":
            v_type = "Slicer (new)"
            s_type = local_df[local_df["Visual ID"] == visual].iloc[0]["Table"]
        elif table_type in visual_type_list:
            words = re.findall("[a-zA-Z][^A-Z]*", table_type)
            s_type = ""
            for word in words:
                s_type += word.capitalize() + " "
        elif table_type in button_type_list:
            v_type = "Button"
            s_type = table_type
        elif table_type in ["actionButton"]:
            v_type = "Button"
            s_type = "Button"
        elif table_type == "Group":
            v_type = "Group"
            s_type = "Panel"
        else:
            print(f"Missing catch on {table_type}!")
            1

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
        worksheetX.write(0, col, name, fBOLDITALIC)
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
    for ir, row in df_sorted.iterrows():
        filter_array = []
        for filter in report_filters_string:
            if (
                filter[2] == "Visual"
                and filter[0] == report_name
                and filter[1] == row["ID"]
            ):
                filter_array.append(fBOLD)
                filter_array.append(filter[3])
                filter_array.append(" " + filter[4] + "\n")

        if filter_array and filter_array[-1][-1] == "\n":
            filter_array[-1] = filter_array[-1][:-1]

        if slicer_switch and row["Item Type"] == "Slicer":
            # row_num += 1
            slicer_switch = False

        if filter_switch and row["Item Type"] == "Filter":
            # row_num += 1
            filter_switch = False

        if button_switch and row["Item Type"] == "Button":
            # row_num += 1
            button_switch = False

        if filter_switch:
            # Regular Measures
            format_array = []
            r_data = local_df[local_df["Visual ID"] == row["ID"]]
            current_type = None
            for im, rrow in enumerate(r_data.iloc()):
                if rrow["Type"] != current_type:
                    current_type = rrow["Type"]
                    type_count = len(r_data[r_data["Type"] == current_type])
                    if im != 0:
                        format_array.append("\n")
                    format_array.append(fBOLD)
                    format_array.append(current_type + ": ")
                format_array.append('\n')

                format_array.append(fITALIC)
                format_array.append(f'{rrow["Table"]}[{rrow["Name"]}]')

                if rrow["Display Name"]:
                    format_array.append("\n\t")
                    format_array.append(" Display Name: ")
                    format_array.append(fITALIC)
                    format_array.append(rrow["Display Name"])

        elif row["Item Type"] in ["Button", "Group"]:
            rrow = report_info[report_info["Visual ID"] == row["ID"]].iloc[0]

            format_array = [fBOLD]
            format_array.append(rrow["Type"] + ": ")
            format_array.append(fITALIC)
            format_array.append(rrow["Name"])

        else:
            if isinstance(row["Item Type"], float):
                continue  # Temp fix for NaN Item Type
            # Filters
            format_array = [fBOLD]
            format_array.append(report_filters_string[row["ID"]][3])
            format_array.append(" " + report_filters_string[row["ID"]][4])

        for col, value in enumerate(row):
            if col == 2 and len(format_array) != 0:
                if len(format_array) < 2:
                    worksheetX.write(row_num + 1, col, *format_array)
                else:
                    worksheetX.write_rich_string(row_num + 1, col, *format_array)
            if col == 3 and len(filter_array) != 0:
                if len(format_array) < 2:
                    worksheetX.write(row_num + 1, col, *filter_array)
                else:
                    worksheetX.write_rich_string(row_num + 1, col, *filter_array)
            elif col == 6:
                continue
            else:
                worksheetX.write(row_num + 1, col, value)

        row_num += 1

workbook.close()
print(f"DataFrame has been saved to {excel_file}")
