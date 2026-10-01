"""Deterministic task-report pilot inputs and external acceptance checks.

Only INITIAL_FILES and TASKS belong in a live worker's input. CHECKS belongs in
the verifier's separate read-only mount. KNOWN_SOLUTIONS is exclusively for an
offline plumbing rehearsal; never include it in generated worker repositories
or worker prompts. This small project is an integration smoke test, not a
representative software-engineering benchmark.
"""

from textwrap import dedent


SPEC_VERSION = "task-report-v1"
EXPECTED_TEST_COUNTS = {"parser": 13, "summary": 6, "all": 23}


def _source(text: str) -> str:
    return dedent(text).lstrip("\n").rstrip() + "\n"


PARSER_SPEC = _source("""
    Implement parse_tasks(csv_text: str) -> list[dict] in task_report/parser.py.
    Use Python's standard comma-separated, double-quoted CSV dialect with strict
    parsing. Support quoted commas, escaped double quotes, and quoted newlines.
    Ignore completely empty CSV records (empty physical lines); a whitespace-only,
    quoted-empty, or delimiter-only record is not an empty record.

    The first nonempty record must be exactly the four header cells
    id,title,status,estimate in that order, with no header whitespace, extra
    columns, or alternate names. A header is required; a header with no data is
    valid and returns []. Every subsequent nonempty record must have four cells.
    Strip leading/trailing whitespace from each data cell, preserving internal
    whitespace. IDs and titles must be nonempty. IDs must be unique after
    stripping and are case-sensitive. Status must be exactly todo, doing, or done.
    An estimate must contain one or more ASCII decimal digits after stripping;
    signs, decimals, separators, exponents, and non-ASCII digits are invalid.
    Leading zeros are valid. Convert estimates to Python integers >= 0.

    Return dictionaries with exactly id, title, status, estimate, in input record
    order. Preserve the stripped ID, title, and status strings. Reject malformed
    CSV, invalid headers, or any invalid data record with ValueError; do not
    silently skip invalid records or return a partial result. Error-message text
    is not prescribed. The input is always a Python str; no external libraries
    or file/network access are required.
""")


SUMMARY_SPEC = _source("""
    Implement summarize(tasks: list[dict]) -> dict in task_report/summary.py.
    Inputs are valid records from the parser contract: exactly id/title/status/
    estimate fields, status todo|doing|done, and a nonnegative integer estimate.
    Validation of invalid task objects is outside this function's contract.

    Return exactly:
    {
      "counts": {"todo": <count>, "doing": <count>, "done": <count>},
      "total_estimate": <sum of all estimates>,
      "remaining_estimate": <sum of estimates for todo and doing tasks>
    }
    All count and estimate values must be Python ints, not floats or booleans.
    Always include all three status counts, using integer zeros when absent.
    Count zero-estimate tasks normally. For an empty input all values are zero.
    Do not mutate the input list or its dictionaries. Calls must be independent;
    return fresh output data, without accumulated or shared mutable state.
    Use only the Python standard library; no file/network access is required.
""")


INITIAL_FILES = {
    "README.md": (
        "# Task report pilot\n\n"
        f"Contract version: `{SPEC_VERSION}`. Python 3.12, standard library only.\n\n"
        "Complete the parser and summary modules described below. The CLI is\n"
        "already implemented. Run it with `python -m task_report tasks.csv`.\n"
        "It reads a UTF-8 CSV file and writes one JSON summary to standard output.\n"
        "Invalid input or an unreadable file prints an error to standard error\n"
        "and exits with status 2. A successful command exits with status 0.\n\n"
        "## CSV parser\n\n" + PARSER_SPEC + "\n## Summary\n\n" + SUMMARY_SPEC
    ),
    "task_report/__init__.py": '"""Summarize work recorded in a CSV task list."""\n',
    "task_report/parser.py": _source('''
        """Read the task-report CSV format specified in README.md."""


        def parse_tasks(csv_text: str) -> list[dict]:
            raise NotImplementedError("Implement the task-report CSV parser")
    '''),
    "task_report/summary.py": _source('''
        """Aggregate parser-valid tasks without modifying them."""


        def summarize(tasks: list[dict]) -> dict:
            raise NotImplementedError("Implement the task-report summary")
    '''),
    "task_report/cli.py": _source('''
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
    '''),
    "task_report/__main__.py": _source('''
        from .cli import main

        raise SystemExit(main())
    '''),
}


TASKS = [
    {
        "id": "parser", "subsystem": "parser",
        "allowed_paths": ("task_report/parser.py",), "dependencies": (),
        "instructions": PARSER_SPEC + "\nChange only task_report/parser.py. The supplied CLI and README must remain unchanged.\n",
    },
    {
        "id": "summary", "subsystem": "summary",
        "allowed_paths": ("task_report/summary.py",), "dependencies": (),
        "instructions": SUMMARY_SPEC + "\nChange only task_report/summary.py. The supplied CLI and README must remain unchanged.\n",
    },
]


# This checker is supplied separately to the verifier, never inside INITIAL_FILES.
CHECKS = {"run_checks.py": _source(r'''
    """External task-report acceptance checks. CLI: run_checks.py parser|summary|all."""

    import argparse
    import copy
    import importlib
    import json
    from pathlib import Path
    import subprocess
    import sys
    import tempfile
    import unittest

    SPEC_VERSION = "task-report-v1"
    EXPECTED_TEST_COUNTS = {"parser": 13, "summary": 6, "all": 23}
    WORKSPACE = Path("/workspace")
    HEADER = "id,title,status,estimate\n"


    class ParserChecks(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            cls.parse = staticmethod(importlib.import_module("task_report.parser").parse_tasks)

        def test_basic_normalization_and_order(self):
            self.assertEqual(self.parse(HEADER + " a , First task , todo , 3 \nb,Second task,doing,5\n"), [
                {"id": "a", "title": "First task", "status": "todo", "estimate": 3},
                {"id": "b", "title": "Second task", "status": "doing", "estimate": 5},
            ])

        def test_quoted_commas_quotes_and_newlines(self):
            value = HEADER + 'a,"Build, test ""twice""\nand ship",done,2\n'
            self.assertEqual(self.parse(value), [{
                "id": "a", "title": 'Build, test "twice"\nand ship',
                "status": "done", "estimate": 2,
            }])

        def test_empty_physical_lines(self):
            self.assertEqual(self.parse("\n" + HEADER + "\na,Task,todo,1\n\n"), [
                {"id": "a", "title": "Task", "status": "todo", "estimate": 1},
            ])

        def test_header_without_data(self):
            self.assertEqual(self.parse(HEADER), [])

        def test_exact_required_header(self):
            for value in ("", "\n\n", "title,id,status,estimate\n",
                          "id,title,state,estimate\n", "id,title,status,estimate,extra\n",
                          " id,title,status,estimate\n", "id,title,status\n"):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    self.parse(value)

        def test_wrong_record_width(self):
            for row in ("a,Task,todo\n", "a,Task,todo,1,extra\n", ",\n", "   \n", '""\n'):
                with self.subTest(row=row), self.assertRaises(ValueError):
                    self.parse(HEADER + row)

        def test_empty_identity_or_title(self):
            for row in (",Task,todo,1\n", "  ,Task,todo,1\n", "a,  ,todo,1\n", ",,,\n"):
                with self.subTest(row=row), self.assertRaises(ValueError):
                    self.parse(HEADER + row)

        def test_duplicate_trimmed_identity(self):
            with self.assertRaises(ValueError):
                self.parse(HEADER + " a,First,todo,1\na ,Second,done,2\n")

        def test_case_sensitive_identity_and_internal_whitespace(self):
            rows = self.parse(HEADER + "a,One  title,todo,1\nA,Other title,done,2\n")
            self.assertEqual([row["id"] for row in rows], ["a", "A"])
            self.assertEqual(rows[0]["title"], "One  title")

        def test_status_vocabulary(self):
            for status in ("", "Done", "TODO", "blocked"):
                with self.subTest(status=status), self.assertRaises(ValueError):
                    self.parse(HEADER + f"a,Task,{status},1\n")

        def test_decimal_estimate_vocabulary(self):
            for value in ("", " ", "-1", "+1", "1.5", "1e2", "1_000", "１２", "١"):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    self.parse(HEADER + f"a,Task,todo,{value}\n")

        def test_zero_leading_zeros_and_large_estimates(self):
            rows = self.parse(HEADER + "a,Zero,todo,0\nb,Padded,doing,0007\nc,Large,done,1099511627776\n")
            self.assertEqual([row["estimate"] for row in rows], [0, 7, 1099511627776])
            self.assertTrue(all(type(row["estimate"]) is int for row in rows))

        def test_malformed_csv_and_late_invalid_record(self):
            for value in (HEADER + 'a,"unclosed,todo,1\n',
                          HEADER + 'a,"title"unexpected,todo,1\n',
                          HEADER + "a,Valid,todo,1\nb,Invalid,unknown,2\n"):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    self.parse(value)


    class SummaryChecks(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            cls.summarize = staticmethod(importlib.import_module("task_report.summary").summarize)

        @staticmethod
        def task(identity, status, estimate):
            return {"id": identity, "title": f"Task {identity}", "status": status, "estimate": estimate}

        def assert_summary(self, actual, expected):
            self.assertEqual(actual, expected)
            for value in (*actual["counts"].values(), actual["total_estimate"],
                          actual["remaining_estimate"]):
                self.assertIs(type(value), int)

        def test_counts_and_remaining_estimate(self):
            tasks = [self.task("a", "todo", 3), self.task("b", "doing", 5),
                     self.task("c", "done", 8), self.task("d", "todo", 2)]
            self.assert_summary(self.summarize(tasks), {
                "counts": {"todo": 2, "doing": 1, "done": 1},
                "total_estimate": 18, "remaining_estimate": 10,
            })

        def test_empty_input(self):
            self.assert_summary(self.summarize([]), {
                "counts": {"todo": 0, "doing": 0, "done": 0},
                "total_estimate": 0, "remaining_estimate": 0,
            })

        def test_absent_statuses_are_zero(self):
            self.assert_summary(self.summarize([self.task("a", "done", 12)]), {
                "counts": {"todo": 0, "doing": 0, "done": 1},
                "total_estimate": 12, "remaining_estimate": 0,
            })

        def test_zero_estimate_tasks_still_count(self):
            result = self.summarize([self.task("a", "todo", 0), self.task("b", "doing", 0),
                                     self.task("c", "done", 0)])
            self.assert_summary(result, {
                "counts": {"todo": 1, "doing": 1, "done": 1},
                "total_estimate": 0, "remaining_estimate": 0,
            })

        def test_input_not_mutated(self):
            tasks = [self.task("b", "doing", 4), self.task("a", "todo", 3)]
            original = copy.deepcopy(tasks)
            self.summarize(tasks)
            self.assertEqual(tasks, original)

        def test_calls_do_not_share_output_or_accumulate(self):
            first = self.summarize([self.task("a", "todo", 2)])
            first_snapshot = copy.deepcopy(first)
            second = self.summarize([self.task("b", "doing", 3)])
            expected = {
                "counts": {"todo": 0, "doing": 1, "done": 0},
                "total_estimate": 3, "remaining_estimate": 3,
            }
            self.assertEqual(first, first_snapshot)
            self.assertIsNot(first, second)
            self.assertIsNot(first["counts"], second["counts"])
            self.assert_summary(second, expected)
            first["counts"]["todo"] = 999
            self.assert_summary(self.summarize([self.task("b", "doing", 3)]), expected)
            self.assertEqual(second, expected)


    class CliChecks(unittest.TestCase):
        def invoke(self, csv_text=None):
            with tempfile.TemporaryDirectory(prefix="task-report-check-") as directory:
                path = Path(directory) / "tasks.csv"
                if csv_text is not None:
                    path.write_text(csv_text, encoding="utf-8")
                bootstrap = (
                    "import runpy,sys;sys.path.insert(0,sys.argv.pop(1));"
                    "runpy.run_module('task_report',run_name='__main__')"
                )
                return subprocess.run(
                    [sys.executable, "-I", "-c", bootstrap, str(WORKSPACE), str(path)],
                    cwd=WORKSPACE, capture_output=True, text=True, timeout=10,
                )

        def test_cli_combines_parser_and_summary(self):
            result = self.invoke(HEADER + 'a,"Build, test\nand ship",todo,3\nb,Review,doing,2\nc,Done,done,7\n')
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stderr, "")
            self.assertEqual(json.loads(result.stdout), {
                "counts": {"todo": 1, "doing": 1, "done": 1},
                "total_estimate": 12, "remaining_estimate": 5,
            })

        def test_cli_header_only(self):
            result = self.invoke(HEADER)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout), {
                "counts": {"todo": 0, "doing": 0, "done": 0},
                "total_estimate": 0, "remaining_estimate": 0,
            })

        def test_cli_rejects_invalid_csv(self):
            result = self.invoke(HEADER + "a,Task,blocked,1\n")
            self.assertEqual(result.returncode, 2)
            self.assertEqual(result.stdout, "")
            self.assertTrue(result.stderr.startswith("error:"))

        def test_cli_rejects_missing_file(self):
            result = self.invoke()
            self.assertEqual(result.returncode, 2)
            self.assertEqual(result.stdout, "")
            self.assertTrue(result.stderr.startswith("error:"))


    def run_checks(target, workspace="/workspace"):
        global WORKSPACE
        if target not in EXPECTED_TEST_COUNTS:
            raise ValueError(f"Unknown check target: {target}")
        WORKSPACE = Path(workspace).resolve()
        sys.path.insert(0, str(WORKSPACE))
        classes = {"parser": (ParserChecks,), "summary": (SummaryChecks,),
                   "all": (ParserChecks, SummaryChecks, CliChecks)}[target]
        loader = unittest.TestLoader()
        suite = unittest.TestSuite(loader.loadTestsFromTestCase(cls) for cls in classes)
        if suite.countTestCases() != EXPECTED_TEST_COUNTS[target]:
            raise RuntimeError("Trusted check count differs from the fixture contract")
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        successful = result.wasSuccessful() and result.testsRun == EXPECTED_TEST_COUNTS[target]
        print(json.dumps({
            "spec_version": SPEC_VERSION, "target": target,
            "tests_run": result.testsRun, "failures": len(result.failures),
            "errors": len(result.errors), "successful": successful,
        }, sort_keys=True))
        return 0 if successful else 1


    if __name__ == "__main__":
        arguments = argparse.ArgumentParser(description=__doc__)
        arguments.add_argument("target", choices=tuple(EXPECTED_TEST_COUNTS))
        raise SystemExit(run_checks(arguments.parse_args().target))
''')}


# Rehearsal oracle only. Do not serialize this mapping into live agent input.
KNOWN_SOLUTIONS = {
    "task_report/parser.py": _source('''
        """Parse the task-report CSV contract using the standard CSV reader."""

        import csv
        import io


        def parse_tasks(csv_text: str) -> list[dict]:
            tasks = []
            identities = set()
            reader = csv.reader(io.StringIO(csv_text, newline=""), strict=True)
            try:
                header = next((row for row in reader if row), None)
                if header != ["id", "title", "status", "estimate"]:
                    raise ValueError("Expected id,title,status,estimate header")
                for row in reader:
                    if not row:
                        continue
                    if len(row) != 4:
                        raise ValueError("Each task must contain four fields")
                    identity, title, status, estimate = (cell.strip() for cell in row)
                    if not identity or not title:
                        raise ValueError("Task ID and title must be nonempty")
                    if identity in identities:
                        raise ValueError("Task IDs must be unique")
                    if status not in ("todo", "doing", "done"):
                        raise ValueError("Unknown task status")
                    if not estimate or not estimate.isascii() or not estimate.isdigit():
                        raise ValueError("Estimate must contain ASCII decimal digits")
                    identities.add(identity)
                    tasks.append({"id": identity, "title": title, "status": status,
                                  "estimate": int(estimate)})
            except csv.Error as error:
                raise ValueError(f"Malformed CSV: {error}") from error
            return tasks
    '''),
    "task_report/summary.py": _source('''
        """Aggregate parser-valid records without retaining mutable state."""


        def summarize(tasks: list[dict]) -> dict:
            counts = {"todo": 0, "doing": 0, "done": 0}
            total = 0
            remaining = 0
            for task in tasks:
                counts[task["status"]] += 1
                total += task["estimate"]
                if task["status"] != "done":
                    remaining += task["estimate"]
            return {"counts": counts, "total_estimate": total,
                    "remaining_estimate": remaining}
    '''),
}
