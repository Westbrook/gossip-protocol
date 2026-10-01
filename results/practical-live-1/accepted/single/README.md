# Task report pilot

Contract version: `task-report-v1`. Python 3.12, standard library only.

Complete the parser and summary modules described below. The CLI is
already implemented. Run it with `python -m task_report tasks.csv`.
It reads a UTF-8 CSV file and writes one JSON summary to standard output.
Invalid input or an unreadable file prints an error to standard error
and exits with status 2. A successful command exits with status 0.

## CSV parser

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

## Summary

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
