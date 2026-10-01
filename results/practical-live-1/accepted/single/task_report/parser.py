"""Read the task-report CSV format specified in README.md."""

from __future__ import annotations

import csv
import io
import re

_HEADER = ["id", "title", "status", "estimate"]
_STATUS_VALUES = {"todo", "doing", "done"}
_ASCII_DIGITS = re.compile(r"^[0-9]+$")


def parse_tasks(csv_text: str) -> list[dict]:
    reader = csv.reader(io.StringIO(csv_text), strict=True)
    tasks: list[dict] = []
    seen_ids: set[str] = set()
    header_seen = False

    try:
        for row in reader:
            if row == []:
                continue

            if not header_seen:
                if row != _HEADER:
                    raise ValueError("invalid header")
                header_seen = True
                continue

            if len(row) != 4:
                raise ValueError("invalid task record")

            task_id = row[0].strip()
            title = row[1].strip()
            status = row[2].strip()
            estimate_text = row[3].strip()

            if not task_id or not title:
                raise ValueError("invalid task record")
            if task_id in seen_ids:
                raise ValueError("duplicate task id")
            if status not in _STATUS_VALUES:
                raise ValueError("invalid task record")
            if not _ASCII_DIGITS.fullmatch(estimate_text):
                raise ValueError("invalid task record")

            seen_ids.add(task_id)
            tasks.append(
                {
                    "id": task_id,
                    "title": title,
                    "status": status,
                    "estimate": int(estimate_text),
                }
            )
    except csv.Error as exc:
        raise ValueError("invalid csv") from exc

    if not header_seen:
        raise ValueError("missing header")

    return tasks
