"""Compare two semantic models as PBIxtractor reads them, e.g. a .bim with its TMDL export.

    python tools/compare_models.py Model.bim Model.SemanticModel/definition

Every table, column, measure, hierarchy, partition, relationship, role and expression is compared
field by field (multi-line text line by line, ignoring trailing spaces). Useful to check the TMDL
reader: export a .bim to TMDL with Tabular Editor 2 (TabularEditor.exe Model.bim -TMDL <folder>)
and compare. Known difference: TMDL has no dataType for calculated columns with an inferred type.
Exit code 0 when identical.
"""

import dataclasses
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pbixtractor.semantic_model import model_to_dataset, read_model  # noqa: E402


def _norm(value):
    if isinstance(value, str):
        return "\n".join(line.rstrip() for line in value.strip().splitlines())
    return value


def diff(a, b, path: str, out: list[str]) -> None:
    """Append the differences between a and b (dataclasses, lists, dicts, values) to out."""
    if dataclasses.is_dataclass(a):
        for f in dataclasses.fields(a):
            diff(getattr(a, f.name), getattr(b, f.name), f"{path}.{f.name}", out)
    elif isinstance(a, list):
        if len(a) != len(b):
            names_a = {str(getattr(x, "name", x)) for x in a}
            names_b = {str(getattr(x, "name", x)) for x in b}
            out.append(
                f"{path}: {len(a)} vs {len(b)} items; only first: {sorted(names_a - names_b)}; "
                f"only second: {sorted(names_b - names_a)}"
            )
            return
        for i, (x, y) in enumerate(zip(a, b)):
            diff(x, y, f"{path}[{getattr(x, 'name', i)}]", out)
    elif isinstance(a, dict):
        if set(a) != set(b):
            out.append(f"{path}: keys differ {sorted(set(a) ^ set(b))}")
        for key in set(a) & set(b):
            diff(a[key], b[key], f"{path}[{key}]", out)
    elif _norm(a) != _norm(b):
        out.append(f"{path}: {str(a)[:80]!r} vs {str(b)[:80]!r}")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__)
        return 2
    first, second = read_model(argv[0]), read_model(argv[1])
    out: list[str] = []
    diff(first, second, "model", out)
    for line in out[:80]:
        print(line)
    rows = len(model_to_dataset(first)), len(model_to_dataset(second))
    print(f"{len(out)} differences; dataset rows {rows[0]} vs {rows[1]}")
    return 1 if out else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
