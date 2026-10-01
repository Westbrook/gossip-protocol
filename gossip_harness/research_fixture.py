"""A versioned feature migration of the accepted task-report project.

The initial implementation is the exact accepted v1 source, not fresh stubs.
Only INITIAL_FILES and TASKS are live worker inputs. CHECKS stays in the trusted
verifier mount; KNOWN_SOLUTIONS is an offline oracle and never a worker input.
This is a small internal existing-project task, not an industrial benchmark.
"""

from textwrap import dedent

from .pilot_fixture import CHECKS as _LEGACY_CHECKS


SPEC_VERSION = "task-report-v2"
EXPECTED_TEST_COUNTS = {"parser": 19, "summary": 11, "all": 36}
BASELINE_COMMIT = 'c007e925c58b4a051d9be853d74e71fe0eeb3bc0'
BASELINE_FILES_SHA256 = 'ac289f7bddbc32cb9531dd1aee39f52b6f39a7885d7155907d6f8a354d42446e'
BASELINE_FILES = {'README.md': '# Task report pilot\n'
              '\n'
              'Contract version: `task-report-v1`. Python 3.12, standard library only.\n'
              '\n'
              'Complete the parser and summary modules described below. The CLI is\n'
              'already implemented. Run it with `python -m task_report tasks.csv`.\n'
              'It reads a UTF-8 CSV file and writes one JSON summary to standard output.\n'
              'Invalid input or an unreadable file prints an error to standard error\n'
              'and exits with status 2. A successful command exits with status 0.\n'
              '\n'
              '## CSV parser\n'
              '\n'
              'Implement parse_tasks(csv_text: str) -> list[dict] in task_report/parser.py.\n'
              "Use Python's standard comma-separated, double-quoted CSV dialect with strict\n"
              'parsing. Support quoted commas, escaped double quotes, and quoted newlines.\n'
              'Ignore completely empty CSV records (empty physical lines); a whitespace-only,\n'
              'quoted-empty, or delimiter-only record is not an empty record.\n'
              '\n'
              'The first nonempty record must be exactly the four header cells\n'
              'id,title,status,estimate in that order, with no header whitespace, extra\n'
              'columns, or alternate names. A header is required; a header with no data is\n'
              'valid and returns []. Every subsequent nonempty record must have four cells.\n'
              'Strip leading/trailing whitespace from each data cell, preserving internal\n'
              'whitespace. IDs and titles must be nonempty. IDs must be unique after\n'
              'stripping and are case-sensitive. Status must be exactly todo, doing, or done.\n'
              'An estimate must contain one or more ASCII decimal digits after stripping;\n'
              'signs, decimals, separators, exponents, and non-ASCII digits are invalid.\n'
              'Leading zeros are valid. Convert estimates to Python integers >= 0.\n'
              '\n'
              'Return dictionaries with exactly id, title, status, estimate, in input record\n'
              'order. Preserve the stripped ID, title, and status strings. Reject malformed\n'
              'CSV, invalid headers, or any invalid data record with ValueError; do not\n'
              'silently skip invalid records or return a partial result. Error-message text\n'
              'is not prescribed. The input is always a Python str; no external libraries\n'
              'or file/network access are required.\n'
              '\n'
              '## Summary\n'
              '\n'
              'Implement summarize(tasks: list[dict]) -> dict in task_report/summary.py.\n'
              'Inputs are valid records from the parser contract: exactly id/title/status/\n'
              'estimate fields, status todo|doing|done, and a nonnegative integer estimate.\n'
              "Validation of invalid task objects is outside this function's contract.\n"
              '\n'
              'Return exactly:\n'
              '{\n'
              '  "counts": {"todo": <count>, "doing": <count>, "done": <count>},\n'
              '  "total_estimate": <sum of all estimates>,\n'
              '  "remaining_estimate": <sum of estimates for todo and doing tasks>\n'
              '}\n'
              'All count and estimate values must be Python ints, not floats or booleans.\n'
              'Always include all three status counts, using integer zeros when absent.\n'
              'Count zero-estimate tasks normally. For an empty input all values are zero.\n'
              'Do not mutate the input list or its dictionaries. Calls must be independent;\n'
              'return fresh output data, without accumulated or shared mutable state.\n'
              'Use only the Python standard library; no file/network access is required.\n',
 'task_report/__init__.py': '"""Summarize work recorded in a CSV task list."""\n',
 'task_report/__main__.py': 'from .cli import main\n\nraise SystemExit(main())\n',
 'task_report/cli.py': '"""Command-line interface; supplied as trusted integration code."""\n'
                       '\n'
                       'import argparse\n'
                       'import json\n'
                       'from pathlib import Path\n'
                       'import sys\n'
                       '\n'
                       'from .parser import parse_tasks\n'
                       'from .summary import summarize\n'
                       '\n'
                       '\n'
                       'def main(argv: list[str] | None = None) -> int:\n'
                       '    parser = argparse.ArgumentParser(description="Summarize a CSV task list")\n'
                       '    parser.add_argument("csv_path", help="UTF-8 CSV file to summarize")\n'
                       '    arguments = parser.parse_args(argv)\n'
                       '    try:\n'
                       '        tasks = '
                       'parse_tasks(Path(arguments.csv_path).read_text(encoding="utf-8"))\n'
                       '        result = summarize(tasks)\n'
                       '    except (OSError, UnicodeError, ValueError) as error:\n'
                       '        print(f"error: {error}", file=sys.stderr)\n'
                       '        return 2\n'
                       '    print(json.dumps(result, sort_keys=True))\n'
                       '    return 0\n',
 'task_report/parser.py': '"""Read the task-report CSV format specified in README.md."""\n'
                          '\n'
                          'from __future__ import annotations\n'
                          '\n'
                          'import csv\n'
                          'import io\n'
                          'import re\n'
                          '\n'
                          '_HEADER = ["id", "title", "status", "estimate"]\n'
                          '_STATUS_VALUES = {"todo", "doing", "done"}\n'
                          '_ASCII_DIGITS = re.compile(r"^[0-9]+$")\n'
                          '\n'
                          '\n'
                          'def parse_tasks(csv_text: str) -> list[dict]:\n'
                          '    reader = csv.reader(io.StringIO(csv_text), strict=True)\n'
                          '    tasks: list[dict] = []\n'
                          '    seen_ids: set[str] = set()\n'
                          '    header_seen = False\n'
                          '\n'
                          '    try:\n'
                          '        for row in reader:\n'
                          '            if row == []:\n'
                          '                continue\n'
                          '\n'
                          '            if not header_seen:\n'
                          '                if row != _HEADER:\n'
                          '                    raise ValueError("invalid header")\n'
                          '                header_seen = True\n'
                          '                continue\n'
                          '\n'
                          '            if len(row) != 4:\n'
                          '                raise ValueError("invalid task record")\n'
                          '\n'
                          '            task_id = row[0].strip()\n'
                          '            title = row[1].strip()\n'
                          '            status = row[2].strip()\n'
                          '            estimate_text = row[3].strip()\n'
                          '\n'
                          '            if not task_id or not title:\n'
                          '                raise ValueError("invalid task record")\n'
                          '            if task_id in seen_ids:\n'
                          '                raise ValueError("duplicate task id")\n'
                          '            if status not in _STATUS_VALUES:\n'
                          '                raise ValueError("invalid task record")\n'
                          '            if not _ASCII_DIGITS.fullmatch(estimate_text):\n'
                          '                raise ValueError("invalid task record")\n'
                          '\n'
                          '            seen_ids.add(task_id)\n'
                          '            tasks.append(\n'
                          '                {\n'
                          '                    "id": task_id,\n'
                          '                    "title": title,\n'
                          '                    "status": status,\n'
                          '                    "estimate": int(estimate_text),\n'
                          '                }\n'
                          '            )\n'
                          '    except csv.Error as exc:\n'
                          '        raise ValueError("invalid csv") from exc\n'
                          '\n'
                          '    if not header_seen:\n'
                          '        raise ValueError("missing header")\n'
                          '\n'
                          '    return tasks\n',
 'task_report/summary.py': '"""Aggregate parser-valid tasks without modifying them."""\n'
                           '\n'
                           'from __future__ import annotations\n'
                           '\n'
                           '\n'
                           'def summarize(tasks: list[dict]) -> dict:\n'
                           '    counts = {"todo": 0, "doing": 0, "done": 0}\n'
                           '    total_estimate = 0\n'
                           '    remaining_estimate = 0\n'
                           '\n'
                           '    for task in tasks:\n'
                           '        status = task["status"]\n'
                           '        estimate = task["estimate"]\n'
                           '        counts[status] += 1\n'
                           '        total_estimate += estimate\n'
                           '        if status in ("todo", "doing"):\n'
                           '            remaining_estimate += estimate\n'
                           '\n'
                           '    return {\n'
                           '        "counts": counts,\n'
                           '        "total_estimate": total_estimate,\n'
                           '        "remaining_estimate": remaining_estimate,\n'
                           '    }\n'}


def _source(text: str) -> str:
    return dedent(text).lstrip("\n").rstrip() + "\n"


PARSER_SPEC = _source("""
    Upgrade parse_tasks(csv_text: str) -> list[dict] in task_report/parser.py.
    Use Python's standard comma/double-quote CSV dialect with strict parsing;
    quoted commas, doubled quotes, and quoted newlines must work. Ignore truly
    empty CSV records (empty physical lines), but not whitespace-only,
    quoted-empty, or delimiter-only records. A header is required.

    Accept exactly one of these headers, with exact spelling/order/no whitespace:
      id,title,status,estimate
      id,title,status,estimate,depends_on
    Every nonempty data record must have the header's number of cells. A header
    without data is valid and returns []. Strip surrounding whitespace from
    each data cell; preserve internal whitespace. IDs and titles are nonempty;
    IDs are unique after stripping and case-sensitive. Status is todo|doing|done.

    VERSIONED CHANGE: estimates are integer minutes and accept exactly these
    ASCII forms after outer stripping: N, Nm, Nh, or NhMm, where N and M each
    mean one or more ASCII digits. Thus 90, 90m, 2h, and 1h30m mean 90,90,120,90.
    Leading zeros and zero-valued durations are valid. In the combined NhMm form
    only, M must be less than 60; 60m is valid but 0h60m is invalid. Units are
    lowercase; no signs, decimals, separators, internal spaces, exponents, or
    non-ASCII digits. Return estimate as a Python int, never float/bool. Legacy
    integer estimates retain their numeric values without conversion or scaling.

    VERSIONED ADDITION: with the five-column header, depends_on is blank for no
    dependencies or is a | separated sequence of IDs. Strip each reference;
    reject empty tokens and duplicate references after stripping. References are
    case-sensitive and retain their written order. Five-column IDs cannot contain
    |; the legacy four-column format still permits | in IDs. Return depends_on
    as a fresh list of strings on EVERY five-column record, including [].
    Parse references syntactically only: forward, unknown, self, and cyclic
    references are allowed here; summarize owns graph consistency validation.

    Four-column records retain exactly id,title,status,estimate fields.
    Five-column records have exactly those fields plus depends_on. Preserve
    input record order and stripped strings. Lists must not be shared between
    records or calls. Reject invalid CSV/header/record with ValueError and never
    silently skip invalid records or return partial results. Error text is not
    prescribed. Input is a Python str. Use only the standard library.
""")


SUMMARY_SPEC = _source("""
    Upgrade summarize(tasks: list[dict]) -> dict in task_report/summary.py.
    Inputs follow the parser's complete v2 contract above, except graph
    consistency is not yet validated. Other malformed record values are outside
    this function's contract. Do not import or call the parser to do this work.

    Preserve the legacy summary exactly for records without depends_on:
      counts: {todo: count, doing: count, done: count}
      total_estimate: sum of every estimate
      remaining_estimate: sum of estimates for todo and doing records
    Always include all three counts. Empty input [] retains that three-key legacy
    result with integer zeros, including when produced from a five-column header
    with no data (an empty list carries no header/schema information).

    For nonempty input, require either every record or no record to contain
    depends_on; mixed schemas raise ValueError. With dependencies, validate the
    entire directed graph BEFORE returning a result: every reference must name
    an existing task, self references are invalid, and ANY cycle raises
    ValueError. This includes disconnected cycles and cycles among done tasks.
    Forward references and shared prerequisites (including diamonds) are valid.

    For valid dependency-bearing records return the three legacy fields PLUS:
      ready_ids: IDs of non-done tasks whose EVERY DIRECT dependency is done
      blocked_ids: IDs of all other non-done tasks
      ready_estimate: sum of estimates for ready_ids
      blocked_estimate: sum of estimates for blocked_ids
    Both ID lists follow INPUT order, not lexical/topological order, and are
    disjoint. No-dependency tasks are ready unless done. Both todo and doing
    tasks use this same criterion; doing is not automatically ready. Done tasks
    appear in neither list. A done prerequisite is fulfilled even if its own
    prerequisites are unfinished; graph validity still applies globally.
    Zero-estimate tasks still count and appear in their proper list. The two
    scheduling estimates sum to remaining_estimate, in normalized minutes.

    Return exactly the specified keys for the selected schema. Every count and
    estimate is a Python int, not float/bool, and ID collections are lists of
    strings. Do not mutate the input list, records, or dependency lists. Every
    call returns fresh outer/count dictionaries and fresh scheduling lists;
    later calls and mutations of one result cannot change an earlier result.
    Use only the standard library. Error messages are not prescribed.
""")


COMBINED_SPEC = (
    f"Task-report feature migration: {SPEC_VERSION}. Python 3.12, standard library only.\n"
    "Evolve the existing accepted v1 implementation. The supplied CLI stays unchanged.\n"
    "It reads a UTF-8 CSV file, prints one JSON summary on success, and catches\n"
    "ValueError/OSError as an error on stderr with exit status 2.\n\n"
    + PARSER_SPEC + "\n" + SUMMARY_SPEC
)

INITIAL_FILES = {
    **BASELINE_FILES,
    "FEATURE_SPEC.md": (
        "# Task report v2: durations and dependency readiness\n\n"
        f"Starting implementation: accepted internal pilot commit `{BASELINE_COMMIT}`.\n"
        "The original README documents v1; this complete v2 contract supersedes\n"
        "only the explicitly described additions and changes. This is an internal\n"
        "existing-project exercise, not an industrial repository benchmark.\n\n"
        + COMBINED_SPEC
    ),
}

TASKS = [
    {
        "id": name, "subsystem": name, "dependencies": (),
        "allowed_paths": (f"task_report/{name}.py",),
        "instructions": COMBINED_SPEC + (
            f"\nYour responsibility is {name}. Change ONLY task_report/{name}.py.\n"
            "The full peer contract above is available for compatibility. Keep the\n"
            "CLI, original README, feature document, and other module unchanged.\n"
        ),
    }
    for name in ("parser", "summary")
]


CHECKS = {
    "legacy_checks.py": _LEGACY_CHECKS["run_checks.py"],
    "run_checks.py": _source(r'''
        """External v2 acceptance checks, including all 23 unchanged v1 regressions."""

        import argparse
        import copy
        import importlib.util
        import json
        from pathlib import Path
        import sys
        import unittest

        SPEC_VERSION = "task-report-v2"
        EXPECTED_TEST_COUNTS = {"parser": 19, "summary": 11, "all": 36}
        _spec = importlib.util.spec_from_file_location(
            "trusted_v1_checks", Path(__file__).with_name("legacy_checks.py"))
        legacy = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(legacy)
        HEADER = "id,title,status,estimate\n"
        EXTENDED = "id,title,status,estimate,depends_on\n"


        def task(identity, status, estimate, depends=()):
            return {"id": identity, "title": identity, "status": status,
                    "estimate": estimate, "depends_on": list(depends)}


        def expected(counts, total, remaining, ready, blocked, ready_minutes, blocked_minutes):
            return {"counts": dict(zip(("todo", "doing", "done"), counts)),
                    "total_estimate": total, "remaining_estimate": remaining,
                    "ready_ids": ready, "blocked_ids": blocked,
                    "ready_estimate": ready_minutes, "blocked_estimate": blocked_minutes}


        class ParserChecks(legacy.ParserChecks):
            def test_v2_duration_forms_in_both_schemas(self):
                for duration, minutes in (("90", 90), ("60m", 60), ("2h", 120),
                                          ("1h30m", 90), ("0002h03m", 123),
                                          ("0h0m", 0), (" 9m ", 9), ("0h59m", 59)):
                    for extended in (False, True):
                        with self.subTest(duration=duration, extended=extended):
                            row = self.parse((EXTENDED if extended else HEADER) +
                                             f"a,Task,todo,{duration}" + (",\n" if extended else "\n"))[0]
                            wanted = {"id": "a", "title": "Task", "status": "todo", "estimate": minutes}
                            if extended:
                                wanted["depends_on"] = []
                            self.assertEqual(row, wanted)
                            self.assertIs(type(row["estimate"]), int)

            def test_v2_invalid_duration_grammar(self):
                for duration in ("0h60m", "1h99m", "1.5h", "1H", "2M", "1h 2m", "h", "m",
                                 "1hm", "1m2h", "-2h", "+1m", "１h", "1h٢m", "1h2", "2m3m"):
                    for header, suffix in ((HEADER, "\n"), (EXTENDED, ",\n")):
                        with self.subTest(duration=duration, header=header), self.assertRaises(ValueError):
                            self.parse(header + f"a,Task,todo,{duration}" + suffix)

            def test_v2_dependency_normalization_and_syntax_only(self):
                rows = self.parse(EXTENDED + 'later,"Build, then ship",todo,1h, first | SECOND \n'
                                  'first,Plan,done,15m,\nSECOND,Review,done,0h,\n')
                self.assertEqual(rows[0], {"id": "later", "title": "Build, then ship", "status": "todo",
                                           "estimate": 60, "depends_on": ["first", "SECOND"]})
                self.assertEqual(rows[1]["depends_on"], [])
                for raw in ("a,Task,todo,1,missing\n", "a,Task,todo,1,a\n",
                            "a,Task,todo,1,b\nb,Other,done,2,a\n"):
                    with self.subTest(raw=raw):
                        self.assertGreater(len(self.parse(EXTENDED + raw)), 0)
                self.assertEqual(self.parse(EXTENDED), [])

            def test_v2_dependency_tokens_and_extended_ids(self):
                for refs in ("a||b", "|a", "a|", "a| a", "a|  |b"):
                    with self.subTest(refs=refs), self.assertRaises(ValueError):
                        self.parse(EXTENDED + f"x,Task,todo,1,{refs}\n")
                with self.assertRaises(ValueError):
                    self.parse(EXTENDED + "a|b,Task,todo,1,\n")
                self.assertEqual(self.parse(HEADER + "a|b,Task,todo,1\n")[0]["id"], "a|b")
                self.assertEqual(self.parse(EXTENDED + "x,Task,todo,1,a|A\n")[0]["depends_on"], ["a", "A"])

            def test_v2_exact_header_and_record_width(self):
                for text in ("id,title,status,depends_on,estimate\n",
                             "id,title,status,estimate, depends_on\n",
                             "id,title,status,estimate,depends_on,extra\n",
                             EXTENDED + "a,Task,todo,1\n", EXTENDED + "a,Task,todo,1,,extra\n",
                             HEADER + "a,Task,todo,1,\n"):
                    with self.subTest(text=text), self.assertRaises(ValueError):
                        self.parse(text)

            def test_v2_dependency_lists_are_fresh(self):
                text = EXTENDED + "a,Task,todo,1,root\nb,Other,doing,2,root\nempty-one,Empty,todo,0,\nempty-two,Empty,todo,0,\n"
                first = self.parse(text)
                second = self.parse(text)
                self.assertIs(type(first[0]["depends_on"]), list)
                self.assertIsNot(first[0]["depends_on"], first[1]["depends_on"])
                self.assertIsNot(first[0]["depends_on"], second[0]["depends_on"])
                first[0]["depends_on"].append("mutated")
                self.assertEqual(first[1]["depends_on"], ["root"])
                self.assertEqual(second[0]["depends_on"], ["root"])
                self.assertIsNot(first[2]["depends_on"], first[3]["depends_on"])
                self.assertIsNot(first[2]["depends_on"], second[2]["depends_on"])
                first[2]["depends_on"].append("empty-mutation")
                self.assertEqual(first[3]["depends_on"], [])
                self.assertEqual(second[2]["depends_on"], [])


        class SummaryChecks(legacy.SummaryChecks):
            def assert_extended(self, actual, wanted):
                self.assert_summary(actual, wanted)
                for name in ("ready_estimate", "blocked_estimate"):
                    self.assertIs(type(actual[name]), int)
                for name in ("ready_ids", "blocked_ids"):
                    self.assertIs(type(actual[name]), list)
                    self.assertTrue(all(type(identity) is str for identity in actual[name]))

            def test_v2_direct_readiness_and_input_order(self):
                tasks = [task("ship", "todo", 45, ("build", "review")),
                         task("build", "doing", 120, ("design",)),
                         task("review", "done", 20, ("backlog",)), task("design", "done", 90),
                         task("backlog", "todo", 15), task("isolated", "todo", 0),
                         task("publish", "todo", 5, ("review",)), task("aaa", "doing", 25, ("backlog",))]
                self.assert_extended(self.summarize(tasks), expected(
                    (4, 2, 2), 320, 210, ["build", "backlog", "isolated", "publish"], ["ship", "aaa"], 140, 70))

            def test_v2_graph_validation_and_valid_diamond(self):
                invalid = [
                    [task("a", "todo", 1, ("missing",))],
                    [task("a", "done", 1, ("missing",))],
                    [task("a", "done", 0), task("b", "todo", 1, ("A",))],
                    [task("a", "done", 1, ("a",))],
                    [task("a", "todo", 1, ("b",)), task("b", "done", 2, ("a",))],
                    [task("a", "todo", 1, ("b",)), task("b", "doing", 2, ("c",)), task("c", "done", 3, ("a",))],
                    [task("root", "todo", 0), task("x", "done", 1, ("y",)), task("y", "done", 2, ("x",))],
                ]
                for tasks in invalid:
                    with self.subTest(tasks=tasks), self.assertRaises(ValueError):
                        self.summarize(tasks)
                diamond = [task("root", "done", 0), task("left", "todo", 2, ("root",)),
                           task("right", "doing", 3, ("root",)), task("join", "todo", 4, ("left", "right"))]
                self.assert_extended(self.summarize(diamond), expected(
                    (2, 1, 1), 9, 9, ["left", "right"], ["join"], 5, 4))
                case_sensitive = [task("a", "done", 1), task("A", "todo", 2), task("use", "todo", 3, ("a",))]
                self.assert_extended(self.summarize(case_sensitive), expected(
                    (2, 0, 1), 6, 5, ["A", "use"], [], 5, 0))

            def test_v2_mixed_schema_rejected_in_both_orders(self):
                old = {"id": "old", "title": "Legacy", "status": "done", "estimate": 1}
                new = task("new", "todo", 2, ("old",))
                for tasks in ([old, new], [new, old]):
                    with self.subTest(tasks=tasks), self.assertRaises(ValueError):
                        self.summarize(tasks)

            def test_v2_done_zero_and_state_transition(self):
                self.assert_extended(self.summarize([task("b", "done", 7, ("a",)), task("a", "done", 3)]),
                                     expected((0, 0, 2), 10, 0, [], [], 0, 0))
                tasks = [task("wait", "doing", 0, ("plan",)), task("plan", "todo", 0)]
                self.assert_extended(self.summarize(tasks), expected((1, 1, 0), 0, 0, ["plan"], ["wait"], 0, 0))
                tasks[1]["status"] = "done"
                self.assert_extended(self.summarize(tasks), expected((0, 1, 1), 0, 0, ["wait"], [], 0, 0))

            def test_v2_nested_inputs_and_results_stay_independent(self):
                tasks = [task("later", "todo", 2, ("z-first", "a-first")),
                         task("z-first", "done", 1), task("a-first", "done", 3)]
                snapshot = copy.deepcopy(tasks)
                first = self.summarize(tasks)
                first_snapshot = copy.deepcopy(first)
                second = self.summarize(tasks)
                self.assertEqual(tasks, snapshot)
                self.assertEqual(first, first_snapshot)
                for key in ("counts", "ready_ids", "blocked_ids"):
                    self.assertIsNot(first[key], second[key])
                self.assertIsNot(first, second)
                self.assertIsNot(first["ready_ids"], first["blocked_ids"])
                first["ready_ids"].append("mutation")
                first["counts"]["todo"] = 99
                self.assert_extended(second, expected((1, 0, 2), 6, 2, ["later"], [], 2, 0))
                self.assertEqual(tasks, snapshot)


        class CliChecks(legacy.CliChecks):
            def test_v2_units_and_forward_dependency_cli(self):
                text = EXTENDED + 'build,"Build, test\nand ship",doing,2h,design\n'
                text += 'ship,Ship,todo,45m,build\ndesign,Design,done,1h30m,\nfree,Other,todo,15m,\n'
                result = self.invoke(text.replace("\n", "\r\n"))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stderr, "")
                self.assertEqual(json.loads(result.stdout), expected(
                    (2, 1, 1), 270, 180, ["build", "free"], ["ship"], 135, 45))

            def test_v2_invalid_dependency_cli(self):
                for text in (EXTENDED + "a,Task,todo,1h,missing\n",
                             EXTENDED + "a,Task,done,1h,b\nb,Other,done,1m,a\n",
                             EXTENDED + "a,Task,todo,1h,b|b\n"):
                    with self.subTest(text=text):
                        result = self.invoke(text)
                        self.assertEqual(result.returncode, 2)
                        self.assertEqual(result.stdout, "")
                        self.assertTrue(result.stderr.startswith("error:"))


        def run_checks(target, workspace="/workspace"):
            if target not in EXPECTED_TEST_COUNTS:
                raise ValueError(f"Unknown check target: {target}")
            legacy.WORKSPACE = Path(workspace).resolve()
            sys.path.insert(0, str(legacy.WORKSPACE))
            classes = {"parser": (ParserChecks,), "summary": (SummaryChecks,),
                       "all": (ParserChecks, SummaryChecks, CliChecks)}[target]
            loader = unittest.TestLoader()
            suite = unittest.TestSuite(loader.loadTestsFromTestCase(cls) for cls in classes)
            if suite.countTestCases() != EXPECTED_TEST_COUNTS[target]:
                raise RuntimeError("Trusted check count differs from the feature contract")
            # Keep test output and the completion JSON on one stream so Docker
            # cannot interleave a stderr test line into the stdout receipt.
            result = unittest.TextTestRunner(stream=sys.stdout, verbosity=2).run(suite)
            successful = result.wasSuccessful() and result.testsRun == EXPECTED_TEST_COUNTS[target]
            print(json.dumps({"spec_version": SPEC_VERSION, "target": target,
                              "tests_run": result.testsRun, "failures": len(result.failures),
                              "errors": len(result.errors), "successful": successful}, sort_keys=True))
            return 0 if successful else 1


        if __name__ == "__main__":
            arguments = argparse.ArgumentParser(description=__doc__)
            arguments.add_argument("target", choices=tuple(EXPECTED_TEST_COUNTS))
            raise SystemExit(run_checks(arguments.parse_args().target))
    '''),
}


# Offline reference implementation; never mount or serialize into worker inputs.
KNOWN_SOLUTIONS = {
    "task_report/parser.py": _source('''
        """Read legacy tasks and v2 minute durations/dependency syntax."""

        import csv
        import io
        import re


        def _minutes(value):
            if re.fullmatch(r"[0-9]+", value):
                return int(value)
            match = re.fullmatch(r"([0-9]+)m", value)
            if match:
                return int(match[1])
            match = re.fullmatch(r"([0-9]+)h(?:([0-9]+)m)?", value)
            if match:
                minutes = int(match[2]) if match[2] is not None else 0
                if match[2] is not None and minutes >= 60:
                    raise ValueError("Combined minute component must be below 60")
                return int(match[1]) * 60 + minutes
            raise ValueError("Invalid minute duration")


        def parse_tasks(csv_text: str) -> list[dict]:
            reader = csv.reader(io.StringIO(csv_text, newline=""), strict=True)
            tasks, identities = [], set()
            try:
                header = next((row for row in reader if row), None)
                legacy = ["id", "title", "status", "estimate"]
                if header not in (legacy, legacy + ["depends_on"]):
                    raise ValueError("Invalid header")
                extended = len(header) == 5
                for row in reader:
                    if not row:
                        continue
                    if len(row) != len(header):
                        raise ValueError("Wrong record width")
                    cells = [cell.strip() for cell in row]
                    identity, title, status, estimate = cells[:4]
                    if not identity or not title or identity in identities:
                        raise ValueError("Invalid or duplicate identity/title")
                    if status not in ("todo", "doing", "done"):
                        raise ValueError("Invalid status")
                    result = {"id": identity, "title": title, "status": status,
                              "estimate": _minutes(estimate)}
                    if extended:
                        if "|" in identity:
                            raise ValueError("Extended IDs cannot contain |")
                        refs = [part.strip() for part in cells[4].split("|")] if cells[4] else []
                        if any(not ref for ref in refs) or len(refs) != len(set(refs)):
                            raise ValueError("Empty or duplicate dependency")
                        result["depends_on"] = refs
                    identities.add(identity)
                    tasks.append(result)
            except csv.Error as error:
                raise ValueError("Invalid CSV") from error
            return tasks
    '''),
    "task_report/summary.py": _source('''
        """Summarize minutes and direct dependency readiness without shared state."""


        def summarize(tasks: list[dict]) -> dict:
            schemas = {"depends_on" in task for task in tasks}
            if len(schemas) > 1:
                raise ValueError("Mixed task schemas")
            extended = schemas == {True}
            by_id = {task["id"]: task for task in tasks}
            if extended:
                for task in tasks:
                    for dependency in task["depends_on"]:
                        if dependency not in by_id or dependency == task["id"]:
                            raise ValueError("Unknown or self dependency")
                colors = {}

                def visit(identity):
                    if colors.get(identity) == 1:
                        raise ValueError("Dependency cycle")
                    if colors.get(identity) == 2:
                        return
                    colors[identity] = 1
                    for dependency in by_id[identity]["depends_on"]:
                        visit(dependency)
                    colors[identity] = 2

                for identity in by_id:
                    visit(identity)
            result = {"counts": {"todo": 0, "doing": 0, "done": 0},
                      "total_estimate": 0, "remaining_estimate": 0}
            if extended:
                result.update(ready_ids=[], blocked_ids=[], ready_estimate=0, blocked_estimate=0)
            for task in tasks:
                result["counts"][task["status"]] += 1
                result["total_estimate"] += task["estimate"]
                if task["status"] != "done":
                    result["remaining_estimate"] += task["estimate"]
                    if extended:
                        ready = all(by_id[dependency]["status"] == "done"
                                    for dependency in task["depends_on"])
                        bucket = "ready" if ready else "blocked"
                        result[bucket + "_ids"].append(task["id"])
                        result[bucket + "_estimate"] += task["estimate"]
            return result
    '''),
}
