"""The sample model of test_semantic_model.BIM as TMDL, written by Tabular Editor 2.29.

Regenerate: save BIM (with compatibilityLevel 1604) as Sample.bim and run
    TabularEditor.exe Sample.bim -TMDL <folder>
"""

from pathlib import Path

SAMPLE_TMDL = {
    'database.tmdl': (
        'database SampleModel\n'
        '\tcompatibilityLevel: 1604\n'
        '\tcompatibilityMode: powerBI\n'
    ),
    'expressions.tmdl': (
        'expression DatabaseQuery =\n'
        '\t\tlet\n'
        '\t\t  x = 1\n'
        '\t\tin\n'
        '\t\t  x\n'
    ),
    'model.tmdl': (
        'model Model\n'
        '\tculture: en-US\n'
        '\n'
        'annotation __TEdtr = 1\n'
        '\n'
        'ref table Sales\n'
        'ref table Dates\n'
        '\n'
        'ref role Nordics\n'
    ),
    'relationships.tmdl': (
        'relationship rel-1\n'
        "\tfromColumn: Sales.'Date Key'\n"
        "\ttoColumn: Dates.'Date Key'\n"
        '\n'
        'relationship rel-2\n'
        '\tisActive: false\n'
        '\tcrossFilteringBehavior: bothDirections\n'
        '\ttoCardinality: many\n'
        '\tfromColumn: Sales.Amount\n'
        "\ttoColumn: Dates.'Year Number'\n"
    ),
    'roles/Nordics.tmdl': (
        'role Nordics\n'
        '\tmodelPermission: read\n'
        '\n'
        '\ttablePermission Sales = Sales[Amount] > 0\n'
    ),
    'tables/Dates.tmdl': (
        'table Dates\n'
        '\n'
        "\tcolumn 'Date Key'\n"
        '\t\tdataType: int64\n'
        '\t\tsourceColumn: DateKey\n'
        '\n'
        '\tcolumn Month\n'
        '\t\tdataType: string\n'
        '\t\tsourceColumn: Month\n'
        "\t\tsortByColumn: 'Month Number'\n"
        '\n'
        "\tcolumn 'Month Number'\n"
        '\t\tdataType: int64\n'
        '\t\tsourceColumn: MonthNumber\n'
        '\n'
        "\tcolumn 'Year Number'\n"
        '\t\tdataType: int64\n'
        '\t\tsourceColumn: Year\n'
        '\n'
        "\thierarchy 'Date Hierarchy'\n"
        '\n'
        '\t\tlevel Year\n'
        "\t\t\tcolumn: 'Year Number'\n"
        '\n'
        '\t\tlevel Month\n'
        '\t\t\tcolumn: Month\n'
        '\n'
        '\tpartition Dates = entity\n'
        '\t\tmode: directLake\n'
        '\t\tsource\n'
        '\t\t\tentityName: dim_date\n'
        '\t\t\tschemaName: gold\n'
        '\t\t\texpressionSource: DatabaseQuery\n'
    ),
    'tables/Sales.tmdl': (
        'table Sales\n'
        '\n'
        '\t/// Sum of\n'
        '\t/// amount\n'
        "\tmeasure 'Total Amount' = SUM ( Sales[Amount] )\n"
        '\t\tformatString: #,0\n'
        '\t\tdisplayFolder: _Totals\n'
        '\n'
        '\tmeasure Dynamic = [Total Amount]\n'
        '\n'
        '\t\tformatStringDefinition = "#,0"\n'
        '\n'
        "\tcolumn 'Date Key'\n"
        '\t\tdataType: int64\n'
        '\t\tisHidden\n'
        '\t\tsourceColumn: DateKey\n'
        '\n'
        '\tcolumn Amount\n'
        '\t\tdataType: decimal\n'
        '\t\tformatString: """TRUE"";""TRUE"";""FALSE"""\n'
        '\t\tsourceColumn: Amount\n'
        '\n'
        "\tcolumn 'Is Big' =\n"
        '\t\t\tIF (\n'
        '\t\t\t    Sales[Amount] > 100,\n'
        '\t\t\t    TRUE()\n'
        '\t\t\t)\n'
        '\t\tdataType: boolean\n'
        '\n'
        '\tpartition Sales-1234 = m\n'
        '\t\tmode: import\n'
        '\t\tsource =\n'
        '\t\t\t\tlet\n'
        '\t\t\t\t  Source = 1\n'
        '\t\t\t\tin\n'
        '\t\t\t\t  Source\n'
    ),
}


def write_sample_tmdl(folder: Path) -> Path:
    """Write the TMDL files into folder (the definition folder) and return it."""
    for name, text in SAMPLE_TMDL.items():
        path = folder / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(text.replace("\n", "\r\n").encode("utf-8"))  # CRLF, as TE writes
    return folder
