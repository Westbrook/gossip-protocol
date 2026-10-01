"""Command-line interface; supplied as trusted integration code."""

import argparse
import json
from pathlib import Path
import sys

from .parser import parse_tasks
from .summary import summarize


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize a CSV task list")
    parser.add_argument("csv_path", help="UTF-8 CSV file to summarize")
    arguments = parser.parse_args(argv)
    try:
        tasks = parse_tasks(Path(arguments.csv_path).read_text(encoding="utf-8"))
        result = summarize(tasks)
    except (OSError, UnicodeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0
