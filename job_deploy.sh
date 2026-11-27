#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$ROOT_DIR/src"
JOB_DIR="$ROOT_DIR/job"

if [[ ! -d "$SRC_DIR" ]]; then
    printf 'Source directory not found: %s\n' "$SRC_DIR" >&2
    exit 1
fi

PYTHON_BIN="${PYTHON:-}"
if [[ -z "$PYTHON_BIN" ]]; then
    if command -v python3 >/dev/null 2>&1; then
        PYTHON_BIN=python3
    elif command -v python >/dev/null 2>&1; then
        PYTHON_BIN=python
    else
        printf 'Python 3 is required to convert notebooks.\n' >&2
        exit 1
    fi
fi

"$PYTHON_BIN" - "$ROOT_DIR" <<'PY'
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
src_dir = root / "src"
job_dir = root / "job"
notebooks = sorted(src_dir.rglob("*.ipynb"))

if not notebooks:
    raise SystemExit(f"No notebooks found under {src_dir}")


def cell_text(source, notebook_path, cell_index):
    if isinstance(source, list):
        if not all(isinstance(part, str) for part in source):
            raise ValueError(f"{notebook_path}: invalid source in cell {cell_index}")
        return "".join(source)
    if isinstance(source, str):
        return source
    raise ValueError(f"{notebook_path}: invalid source in cell {cell_index}")


def markdown_lines(text):
    lines = ["# %% [markdown]\n"]
    for line in text.splitlines():
        lines.append(f"# {line}\n" if line else "#\n")
    lines.append("\n")
    return lines


def convert(notebook_path):
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    cells = notebook.get("cells")
    if not isinstance(cells, list):
        raise ValueError(f"{notebook_path}: notebook has no cells list")

    relative_path = notebook_path.relative_to(src_dir)
    target_path = job_dir / relative_path.with_suffix(".py")
    output = [
        "# Generated from the matching src notebook by job_deploy.sh.\n",
        "# Edit the notebook in src, then regenerate this production script.\n",
        f"# Source: src/{relative_path.as_posix()}\n",
        "\n",
    ]

    for index, cell in enumerate(cells):
        cell_type = cell.get("cell_type")
        text = cell_text(cell.get("source", ""), notebook_path, index)
        if cell_type == "markdown":
            output.extend(markdown_lines(text))
        elif cell_type == "code":
            for line_number, line in enumerate(text.splitlines(), start=1):
                if re.match(r"^\s*(?:%%|[%!])", line):
                    raise ValueError(
                        f"{notebook_path}: IPython magic at cell {index}, line {line_number} "
                        "cannot be exported as a plain Python script"
                    )
            output.append("# %%\n")
            output.append(text)
            if text and not text.endswith("\n"):
                output.append("\n")
            output.append("\n")
        elif cell_type == "raw":
            output.extend(["# %% [raw]\n"])
            output.extend(markdown_lines(text))
        else:
            raise ValueError(f"{notebook_path}: unsupported cell type {cell_type!r} in cell {index}")

    script = "".join(output)
    compile(script, str(target_path), "exec")
    return target_path, script


converted = [convert(path) for path in notebooks]

for target_path, script in converted:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = target_path.with_name(target_path.name + ".tmp")
    try:
        temporary_path.write_text(script, encoding="utf-8", newline="\n")
        temporary_path.replace(target_path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()
    print(f"{target_path.relative_to(root).as_posix()}")

print(f"Converted {len(converted)} notebook(s).")
PY
