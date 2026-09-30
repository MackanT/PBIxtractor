"""Compare two .xlsx files: sheets, cell values, rich-text runs, resolved cell styles, columns.

    python tools/compare_xlsx.py OLD.xlsx NEW.xlsx

Styles are compared resolved (font/fill/border/number format/alignment), so a different internal
style numbering is not reported as a difference. Exit code 0 when identical.
"""

import re
import sys
import zipfile
from pathlib import Path


def _section(xml: str, tag: str) -> str:
    match = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", xml, re.S)
    return match.group(1) if match else ""


def _style_resolver(z: zipfile.ZipFile):
    xml = z.read("xl/styles.xml").decode()
    fonts = re.findall(r"<font>.*?</font>|<font/>", _section(xml, "fonts"), re.S)
    fills = re.findall(r"<fill>.*?</fill>", _section(xml, "fills"), re.S)
    borders = re.findall(r"<border>.*?</border>|<border/>", _section(xml, "borders"), re.S)
    numfmts = dict(re.findall(r'<numFmt numFmtId="(\d+)" formatCode="([^"]*)"', xml))
    xfs = re.findall(r"<xf [^>]*?(?:/>|>.*?</xf>)", _section(xml, "cellXfs"), re.S)

    def resolve(index: int) -> str:
        xf = xfs[index]
        attr = dict(re.findall(r'(\w+)="([^"]*)"', xf.split(">")[0]))
        align = re.search(r"<alignment[^>]*/>", xf)
        number_format = attr.get("numFmtId", "0")
        return "|".join(
            [
                fonts[int(attr.get("fontId", 0))],
                fills[int(attr.get("fillId", 0))],
                borders[int(attr.get("borderId", 0))],
                numfmts.get(number_format, number_format),
                align.group(0) if align else "",
            ]
        )

    return resolve


def _resolve_cols(cols: str, resolve) -> str:
    """Column definitions with style indexes replaced by the resolved style."""
    return re.sub(r' style="(\d+)"', lambda m: f' style="{resolve(int(m.group(1)))}"', cols)


def load(path: str | Path) -> dict:
    """Workbook content: {sheet: {"cells": {ref: (value, style)}, "cols": ..., "images": n}}."""
    z = zipfile.ZipFile(path)
    strings = []
    if "xl/sharedStrings.xml" in z.namelist():
        strings = re.findall(r"<si>(.*?)</si>", z.read("xl/sharedStrings.xml").decode(), re.S)
    resolve = _style_resolver(z)
    workbook = z.read("xl/workbook.xml").decode()
    rels = z.read("xl/_rels/workbook.xml.rels").decode()
    targets = dict(re.findall(r'Id="(rId\d+)"[^>]*Target="([^"]+)"', rels))
    targets.update({k: v for v, k in re.findall(r'Target="([^"]+)"[^>]*Id="(rId\d+)"', rels)})

    sheets = {}
    for name, rid in re.findall(r'<sheet name="([^"]+)" sheetId="\d+" r:id="(rId\d+)"', workbook):
        xml = z.read("xl/" + targets[rid]).decode()
        cells = {}
        for ref, attrs, body in re.findall(
            r'<c r="([A-Z]+\d+)"([^>]*?)(?:/>|>(.*?)</c>)', xml, re.S
        ):
            value = re.search(r"<v>(.*?)</v>", body or "")
            value = value.group(1) if value else ""
            if 't="s"' in attrs and value:
                value = strings[int(value)]  # raw <si>, including rich-text run formatting
            style = re.search(r' s="(\d+)"', attrs)
            cells[ref] = (value, resolve(int(style.group(1))) if style else "")
        cols = re.search(r"<cols>(.*?)</cols>", xml, re.S)
        sheets[name] = {
            "cells": cells,
            "cols": _resolve_cols(cols.group(1), resolve) if cols else "",
            "images": len(re.findall(r"<drawing ", xml)),
        }
    return sheets


def compare(old_path: str | Path, new_path: str | Path, limit: int = 25) -> list[str]:
    """
    Differences between two workbooks (empty list when identical).

    Args:
        old_path: Reference workbook
        new_path: Workbook to check
        limit: Stop after this many cell differences per sheet
    """
    old, new = load(old_path), load(new_path)
    problems = []
    if list(old) != list(new):
        problems.append(f"sheet names/order differ:\n  old {list(old)}\n  new {list(new)}")
    for sheet in old:
        if sheet not in new:
            continue
        a, b = old[sheet], new[sheet]
        if a["cols"] != b["cols"]:
            problems.append(f"[{sheet}] column widths/formats differ")
        if a["images"] != b["images"]:
            problems.append(f"[{sheet}] images differ: {a['images']} vs {b['images']}")
        refs = sorted(
            set(a["cells"]) | set(b["cells"]), key=lambda r: (int(re.sub(r"\D", "", r)), r)
        )
        found = 0
        for ref in refs:
            va, vb = a["cells"].get(ref), b["cells"].get(ref)
            if va != vb:
                what = "value" if (va or ("", ""))[0] != (vb or ("", ""))[0] else "style"
                problems.append(f"[{sheet}] {ref} {what}: {str(va)[:120]!r} -> {str(vb)[:120]!r}")
                found += 1
                if found >= limit:
                    problems.append(f"[{sheet}] ... (more differences not listed)")
                    break
    return problems


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__)
        return 2
    problems = compare(*argv)
    for problem in problems[:60]:
        print(problem)
    status = "IDENTICAL" if not problems else f"{len(problems)} differences"
    print(f"{status}: {Path(argv[1]).name}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
