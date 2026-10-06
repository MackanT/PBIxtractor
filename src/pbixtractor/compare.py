"""The same measure, column, table or visual in several models: is it really the same?

A difference is often a mistake - a measure copied and changed in one model, a column with
another data type, a card with the same title showing another field - so the catalog and the
lineage across models flag them.

    groups = compare_entries(catalog_entries)
    group = groups["measure"][0]
    group["name"], group["differs"]   # "Total Sales", ["DAX"]   ([] = the same everywhere)
    group["members"]                  # [{"entry", "id", "where", "version", "values": {...}}]

The same means: a measure by name, a column by table and column name, a table by name, a
visual by type and title (visuals without a title are not compared) - found in at least two
models (visuals: in at least two reports). A title that one report itself uses for visuals
showing different fields (e.g. "Slicer Date" on several slicers) does not identify a visual:
such titles are left out. What is compared:

    measure  DAX (whitespace and case ignored), format string
    column   data type, kind (data / calculated), expression
    table    sources, storage mode, the set of columns
    visual   the fields it shows
"""

from typing import Callable, Iterator

KINDS = ("measure", "column", "table", "visual")


def _dax(text: str) -> str:
    return "".join((text or "").split()).lower()


# How values are compared (they are shown as they are); exact when not listed
_NORMALIZE: dict[str, Callable[[str], str]] = {"DAX": _dax, "Expression": _dax, "Sources": str.lower}


def _members(entries: list[dict]) -> Iterator[tuple[str, str, str, dict]]:
    """(kind, comparison key, display name, member) for every comparable item."""
    for entry in entries:
        base = {"entry": entry["key"], "model": entry["name"]}
        for table in entry["tables"]:
            yield "table", table["name"].lower(), table["name"], {
                **base,
                "id": table["name"],
                "where": entry["name"],
                "values": {
                    "Sources": ", ".join(sorted(table.get("sources") or [])) or "-",
                    "Storage mode": table.get("storage_mode") or "-",
                    "Columns": ", ".join(sorted(c["name"] for c in table["columns"])),
                },
            }
            for column in table["columns"]:
                ref = f"{table['name']}[{column['name']}]"
                yield "column", ref.lower(), ref, {
                    **base,
                    "id": ref,
                    "where": entry["name"],
                    "values": {
                        "Data type": column.get("data_type") or "-",
                        "Kind": column.get("kind") or "-",
                        "Expression": column.get("expression") or "",
                    },
                }
            for measure in table["measures"]:
                yield "measure", measure["name"].lower(), measure["name"], {
                    **base,
                    "id": f"{table['name']}[{measure['name']}]",
                    "where": f"{entry['name']} · {table['name']}",
                    "values": {"DAX": measure.get("expression") or "", "Format": measure.get("format") or ""},
                }
        for report in entry["reports"]:
            report_name = report["name"] or entry["name"]
            for page in report["pages"]:
                for visual in page.get("items") or []:
                    title = (visual.get("title") or "").strip()
                    if not title:
                        continue
                    yield "visual", f"{visual['type']}|{title}".lower(), f"{visual['type']}: {title}", {
                        **base,
                        "report": report_name,
                        "id": f"{page['id']}/{visual['id']}",
                        "where": f"{report_name} › {page['name']}",
                        "values": {"Fields": ", ".join(sorted(visual["fields"])) or "-"},
                    }


def _signature(member: dict) -> tuple:
    return tuple(_NORMALIZE.get(name, str)(value) for name, value in member["values"].items())


def differences(member: dict, other: dict) -> list[str]:
    """The values in which two members of a group differ (as compared: DAX formatting ignored)."""
    return [
        name for name, value in member["values"].items()
        if _NORMALIZE.get(name, str)(value) != _NORMALIZE.get(name, str)(other["values"].get(name, ""))
    ]


def compare_entries(entries: list[dict]) -> dict[str, list[dict]]:
    """
    Items found in several models (visuals: several reports), per kind.

    Returns:
        {kind: [{"kind", "name", "members", "differs", "versions", "note"}]}: differing groups
        first (then the most widespread); each member has "version" (1 = the most common
        definition; members with the same version are the same). "differs" names the values
        that are not the same everywhere; "note" says which columns only some tables have.
    """
    found: dict[str, dict[str, dict]] = {kind: {} for kind in KINDS}
    for kind, key, name, member in _members(entries):
        found[kind].setdefault(key, {"kind": kind, "name": name, "members": []})["members"].append(member)

    result: dict[str, list[dict]] = {}
    for kind in KINDS:
        groups = []
        for group in found[kind].values():
            members = group["members"]
            spread = {(m["entry"], m.get("report")) for m in members} if kind == "visual" else {m["entry"] for m in members}
            if len(spread) < 2:
                continue
            # Versions: the same definition = the same version, the most common first
            signatures = [_signature(m) for m in members]
            if kind == "visual":
                per_report: dict[tuple, set] = {}
                for member, signature in zip(members, signatures):
                    per_report.setdefault((member["entry"], member["report"]), set()).add(signature)
                if any(len(found_in) > 1 for found_in in per_report.values()):
                    continue  # a generic title within one report: not one visual
            ranked = sorted(set(signatures), key=lambda s: (-signatures.count(s), signatures.index(s)))
            for member, signature in zip(members, signatures):
                member["version"] = ranked.index(signature) + 1
            names = list(members[0]["values"])
            group["differs"] = [
                name for i, name in enumerate(names) if len({s[i] for s in signatures}) > 1
            ]
            group["versions"] = len(ranked)
            if kind == "table" and "Columns" in group["differs"]:
                sets = [set(filter(None, m["values"]["Columns"].split(", "))) for m in members]
                only_some = sorted(set.union(*sets) - set.intersection(*sets))
                group["note"] = "Columns only some of them have: " + ", ".join(only_some)
            groups.append(group)
        groups.sort(key=lambda g: (not g["differs"], -len(g["members"]), g["name"].lower()))
        result[kind] = groups
    return result
