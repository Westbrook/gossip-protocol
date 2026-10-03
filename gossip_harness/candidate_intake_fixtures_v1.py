"""Independent finite directory, ZIP and JSON admission recipes.

Only the published V0/M1 requirements and inventory define expected values.
Archive bytes are fixture construction, never candidate/reference observations.
"""
from __future__ import annotations

import io
import json
import stat
import warnings
from typing import Any
import zipfile


def intake_cases() -> list[dict[str, Any]]:
    # Lazy import: the parent calls this after its recipe primitives are defined.
    from .candidate_intake_store_cases_v1 import _Case, encoded, file_fixture, zip_bytes

    rows: list[dict[str, Any]] = []

    def operation(kind: str, input_name: str, namespace: Any = "ns") -> dict[str, Any]:
        args: list[Any] = ["case", {"$path": input_name}]
        if kind != "json":
            args.append(namespace)
        return _Case.op("manager", "submit_" + kind, *args)

    def make(case_id: str, kind: str, fixtures: list[dict[str, Any]], requirements: list[str],
             facet: str, category: str = "boundary") -> Any:
        case = _Case("intake-" + case_id, "intake",
                     list(dict.fromkeys(["M1-I01", *requirements])), category, facet)
        case.row["recipe"]["fixtures"].extend(fixtures)
        case.row["omissions"].extend([
            "Symlink and explicit-input fixtures assert returned rejection/admitted content and persisted state; they do not independently trace attempted reads or writes.",
            "Nonstring namespace arguments are omitted because the frozen contract does not assign their exact error classification.",
        ])
        case.snapshot("before")
        return case

    def valid(case_id: str, kind: str, fixtures: list[dict[str, Any]],
              entries: list[dict[str, str]], requirements: list[str], facet: str,
              input_name: str = "batch", namespace: Any = "ns") -> None:
        case = make(case_id, kind, fixtures, requirements, facet, "positive")
        job = case.put_job("case", entries)
        case.call("after", operation(kind, input_name, namespace), job["public"])
        case.call("after", case.op("store", "job_manifest", "case"), job["manifest"])
        case.put_job("case", entries, "running")
        token = {"job_id": "case", "epoch": 1}
        case.call("after", case.op("manager", "prepare", "case"), token)
        job = case.put_job("case", entries, "completed")
        case.call("after", case.op("manager", "commit", token), job["receipt"])
        case.call("reopened", case.op("store", "get_job", "case"), job["public"])
        case.call("reopened", case.op("store", "job_manifest", "case"), job["manifest"])
        rows.append(case.finish())

    def reject(case_id: str, kind: str, fixtures: list[dict[str, Any]], error: str,
               requirements: list[str], facet: str, input_name: str = "batch",
               namespace: Any = "ns") -> None:
        case = make(case_id, kind, fixtures, ["M1-I26", *requirements], facet, "negative")
        case.call("after", operation(kind, input_name, namespace), error=error)
        case.call("after", case.op("store", "get_job", "case"), error="not_found")
        rows.append(case.finish())

    def deferred(case_id: str, entries: list[dict[str, str]], error: str,
                 requirements: list[str], facet: str, *, raw: bytes | None = None) -> None:
        data = encoded({"entries": entries}) if raw is None else raw
        case = make(case_id, "json", [file_fixture("batch", data)],
                    ["M1-I29", *requirements], facet, "history")
        job = case.put_job("case", entries)
        case.call("before", operation("json", "batch"), job["public"])
        case.call("before", case.op("secondary_store", "get_job", "case"), job["public"])
        case.call("before", case.op("store", "job_manifest", "case"), job["manifest"])
        case.snapshot("before")
        case.row["snapshot_semantics"] = {
            "before": "After JSON submission and an independent-connection queued-job observation, before prepare.",
            "after": "After prepare returns the specified semantic error and persists failed state.",
            "reopened": "After reopening the failed job; exact admitted manifest and preexisting catalog remain.",
        }
        case.put_job("case", entries, "failed", error=error)
        case.call("after", case.op("manager", "prepare", "case"), error=error)
        rows.append(case.finish())

    def directory(files: list[tuple[str, bytes]]) -> list[dict[str, Any]]:
        return [{"path": "batch", "kind": "directory"},
                *[file_fixture("batch/" + name, raw) for name, raw in files]]

    def archive(members: list[dict[str, Any]], **kwargs: Any) -> list[dict[str, Any]]:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Duplicate name:", category=UserWarning)
            return [file_fixture("batch", zip_bytes(members, **kwargs))]

    def json_bundle(entries: Any) -> list[dict[str, Any]]:
        return [file_fixture("batch", encoded({"entries": entries}))]

    def entry(name: str, text: str, namespace: str = "ns") -> dict[str, str]:
        return {"source": namespace + "/" + name, "text": text}

    for kind in ("directory", "zip", "json"):
        original = [{"source": "b.md", "text": "b"}, {"source": "a.txt", "text": "a"}]
        changed = [{"source": "b.md", "text": "changed"}, {"source": "a.txt", "text": "a"}]
        def replay_fixtures(entries: list[dict[str, str]]) -> list[dict[str, Any]]:
            if kind == "directory":
                return directory([(item["source"], item["text"].encode()) for item in entries])
            if kind == "zip":
                return archive([{"name": item["source"], "raw": item["text"].encode()} for item in entries])
            return json_bundle(entries)
        fixtures = replay_fixtures(original)
        fixtures.extend([{**item, "path": "changed" + item["path"][len("batch"):]} for item in replay_fixtures(changed)])
        entries = original if kind == "json" else [entry(item["source"], item["text"]) for item in original]
        case = make(kind + "-replay-conflict", kind, fixtures, ["M1-J04", "M1-T04"],
                    "Two explicit readonly inputs establish same-ID equal-manifest replay and changed-text conflict without changing the admitted queued job.", "history")
        job = case.put_job("case", entries)
        case.call("after", operation(kind, "batch"), job["public"])
        case.call("after", case.op("secondary_store", "get_job", "case"), job["public"])
        case.call("after", operation(kind, "batch"), job["public"])
        case.call("after", operation(kind, "changed"), error="job_conflict")
        case.call("after", case.op("store", "job_manifest", "case"), job["manifest"])
        case.call("after", case.op("secondary_store", "get_job", "case"), job["public"])
        case.call("reopened", case.op("store", "get_job", "case"), job["public"])
        case.call("reopened", case.op("store", "job_manifest", "case"), job["manifest"])
        rows.append(case.finish())

    # Exact text, lexical ordering and confinement to the explicit input are
    # inspected through stored manifests and actual complete commit receipts.
    texts = [("z.txt", b""), ("nested/a.md", "caf\u00e9\r\n".encode()),
             ("\u03b1.html", b"<img src='https://example.invalid/x'>\n")]
    valid("directory-recursive", "directory", directory(texts) + [
        {"path": "batch/empty", "kind": "directory"}, file_fixture("neighbor.txt", b"excluded")],
        [entry(name, raw.decode(), "outer/inner") for name, raw in texts],
        ["M1-I02", "M1-I03", "M1-I06"],
        "Nested namespace, recursive source ordering, literal HTML/UTF-8/CRLF and an excluded neighbor; no rendering claim.",
        namespace="outer/inner")
    valid("zip-regular-and-untyped", "zip", archive([
        {"name": "z.txt", "raw": b"", "mode": 0o600},
        {"name": "a.html", "raw": "\u00e9\r\n".encode()},
        {"name": "nested/", "raw": b"", "mode": stat.S_IFDIR | 0o700},
        {"name": "nested/b.md", "raw": b"b"}]),
        [entry("z.txt", ""), entry("a.html", "\u00e9\r\n"), entry("nested/b.md", "b")],
        ["M1-I03", "M1-I10", "M1-I21"],
        "Explicit regular and absent type bits accepted; validated directory omitted and no phantom document.")
    json_entries = [{"source": "z.txt", "text": ""}, {"source": "a.html", "text": "\u00e9\r\n"}]
    valid("json-literal", "json", json_bundle(json_entries), json_entries,
          ["M1-I03", "M1-I05", "M1-I24"], "JSON entries preserve literal text and canonical source order.")
    for kind, fixtures in (("directory", directory([])), ("zip", archive([])),
                           ("json", json_bundle([]))):
        valid(kind + "-empty", kind, fixtures, [], ["M1-I23"],
              "An empty batch reaches a durable completed receipt with zero documents.")
    valid("zip-directory-only", "zip", archive([
        {"name": "unused/", "mode": stat.S_IFDIR | 0o700},
        {"name": "unused/nested/", "mode": stat.S_IFDIR | 0o700}]), [],
        ["M1-I21", "M1-I23"], "Directory-only ZIP validates names and completes with no source members.")

    for kind in ("directory", "zip"):
        fixtures = directory([("a.txt", b"a")]) if kind == "directory" else archive([{"name": "a.txt", "raw": b"a"}])
        # One representative for each distinct key grammar class; both material
        # namespace parser entrypoints are exercised without normalizing input.
        for label, namespace in (("empty", ""), ("absolute", "/x"), ("backslash", "x\\y"),
                                 ("nul", "x\0y"), ("empty-segment", "x//y"),
                                 ("dot", "x/./y"), ("parent", "x/../y"),
                                 ("trailing", "x/"),
                                 ("long-segment", "n" * 129)):
            reject(kind + "-namespace-" + label, kind, fixtures, "invalid_source", ["M1-I06", "M1-I11"],
                   "Invalid namespace is rejected without admission or normalization.", namespace=namespace)
    for label, name in (("absolute", "/a.txt"), ("backslash", "x\\a.txt"),
                        ("empty-segment", "x//a.txt"), ("dot", "x/./a.txt"),
                        ("parent", "x/../a.txt"), ("long-segment", "a" * 125 + ".txt")):
        reject("zip-member-" + label, "zip", archive([
            {"name": "safe.txt", "raw": b"safe"}, {"name": name, "raw": b"bad"}]),
            "invalid_source", ["M1-I11"], "A single invalid last member rejects the entire archive.")
    reject("zip-directory-parent", "zip", archive([{"name": "../escape/", "mode": stat.S_IFDIR | 0o700}]),
           "invalid_source", ["M1-I11", "M1-I21"], "Ignored directory entries still validate traversal after removing their final slash.")
    reject("zip-directory-empty-segment", "zip", archive([{"name": "valid//", "mode": stat.S_IFDIR | 0o700}]),
           "invalid_source", ["M1-I11", "M1-I21"], "Removing exactly the final slash must not normalize a remaining empty segment.")
    for kind in ("directory", "zip"):
        name = "\u00e9" * 62 + ".txt"  # 128 bytes; namespace/leaf total is 256/257.
        fixtures = directory([(name, b"x")]) if kind == "directory" else archive([{"name": name, "raw": b"x"}])
        valid(kind + "-key-bytes-limit", kind, fixtures, [entry(name, "x", "n" * 127)],
              ["M1-I06", "M1-I11"], "Combined key exactly 256 UTF-8 bytes with a 128-byte multibyte leaf.", namespace="n" * 127)
        reject(kind + "-key-bytes-over", kind, fixtures, "invalid_source", ["M1-I06", "M1-I11"],
               "Combined key 257 bytes while each individual segment remains legal.", namespace="n" * 128)
    for kind in ("directory", "zip", "json"):
        for depth in (16, 17):
            relative = "/".join(["p"] * (depth - (2 if kind != "json" else 1)) + ["a.txt"])
            full = "ns/" + relative if kind != "json" else relative
            fixtures = (directory([(relative, b"a")]) if kind == "directory" else
                        archive([{"name": relative, "raw": b"a"}]) if kind == "zip" else
                        json_bundle([{"source": full, "text": "a"}]))
            if depth == 16:
                valid(kind + "-depth-limit", kind, fixtures, [{"source": full, "text": "a"}],
                      ["M1-I20"], "Exactly 16 combined source segments are permitted.")
            elif kind == "json":
                deferred("json-depth-over", [{"source": full, "text": "a"}], "invalid_source",
                         ["M1-I20"], "Valid-schema JSON admits a 17-segment source before semantic prepare rejection.")
            else:
                reject(kind + "-depth-over", kind, fixtures, "invalid_source", ["M1-I20"],
                       "The namespace contributes to the 17-segment rejection.")

    # All symlink targets are harmless fixtures; no actual sensitive path is used.
    reject("directory-leaf-symlink", "directory", directory([("safe.txt", b"safe")]) + [
        file_fixture("target.txt", b"sentinel"), {"path": "batch/link.txt", "kind": "symlink", "target": "../target.txt"}],
        "invalid_source", ["M1-I08"], "Directory leaf symlink to a neighboring sentinel is rejected; attempted-read tracing remains separate.")
    reject("directory-descendant-symlink", "directory", directory([("safe.txt", b"safe")]) + [
        {"path": "target", "kind": "directory"}, file_fixture("target/a.txt", b"sentinel"),
        {"path": "batch/nested", "kind": "symlink", "target": "../target"}],
        "invalid_source", ["M1-I02", "M1-I08"],
        "Directory discovery must reject a symlinked subdirectory instead of skipping or following it.")
    reject("directory-root-symlink", "directory", [
        {"path": "target", "kind": "directory"}, file_fixture("target/a.txt", b"a"),
        {"path": "batch", "kind": "symlink", "target": "target"}],
        "invalid_source", ["M1-I08"], "Explicit directory root symlink is rejected.")
    for kind in ("directory", "zip", "json"):
        target_raw = zip_bytes([{"name": "a.txt", "raw": b"a"}]) if kind == "zip" else encoded({"entries": []})
        fixtures = [{"path": "target", "kind": "directory"}, {"path": "link", "kind": "symlink", "target": "target"}]
        fixtures += ([{"path": "target/batch", "kind": "directory"}, file_fixture("target/batch/a.txt", b"a")]
                     if kind == "directory" else [file_fixture("target/batch", target_raw)])
        reject(kind + "-ancestor-symlink", kind, fixtures, "invalid_source", ["M1-I08"],
               "No symlink may be followed in the explicit input path's ancestors.", input_name="link/batch")
        if kind != "directory":
            reject(kind + "-bundle-symlink", kind, [file_fixture("target", target_raw),
                   {"path": "batch", "kind": "symlink", "target": "target"}], "invalid_source", ["M1-I08"],
                   "Explicit bundle-file symlink is rejected.")
    case = make("directory-fifo", "directory", directory([("safe.txt", b"safe")]) + [
        {"path": "batch/pipe.txt", "kind": "fifo"}], ["M1-I02", "M1-I26"],
        "Nonregular directory member must reject before admission; the normative contract does not assign this class an exact error code.", "negative")
    case.call("after", operation("directory", "batch"))
    case.row["expected"]["results"]["after"][-1] = {"error_present": True}
    case.call("after", case.op("store", "get_job", "case"), error="not_found")
    case.row["omissions"].append("The directory FIFO control asserts a nonempty domain error and conserved storage, not an inferred exact error code.")
    rows.append(case.finish())
    for label, mode, error in (("symlink", stat.S_IFLNK | 0o777, "invalid_source"),
                                ("fifo", stat.S_IFIFO | 0o600, "invalid_archive"),
                                ("socket", stat.S_IFSOCK | 0o600, "invalid_archive")):
        reject("zip-mode-" + label, "zip", archive([
            {"name": "safe.txt", "raw": b"safe"}, {"name": "bad.txt", "raw": b"x", "mode": mode}]),
            error, ["M1-I08" if label == "symlink" else "M1-I10"], "Unix member type is checked before admission.")
    reject("zip-encrypted", "zip", archive([
        {"name": "locked.txt", "raw": b"x"}, {"name": "safe.txt", "raw": b"safe"}], encrypted=True),
        "invalid_archive", ["M1-I09"], "Encryption metadata with a valid neighboring member rejects the archive.")
    for kind in ("directory", "zip"):
        files = [("safe.txt", b"safe"), ("bad.csv", b"csv")]
        fixtures = directory(files) if kind == "directory" else archive([{"name": n, "raw": b} for n, b in files])
        reject(kind + "-unsupported", kind, fixtures, "unsupported_type", ["M1-I04"],
               "Mixed supported/unsupported members fail the whole discovery operation.")
        files = [("safe.txt", b"safe"), ("bad.txt", b"\xff")]
        fixtures = directory(files) if kind == "directory" else archive([{"name": n, "raw": b} for n, b in files])
        reject(kind + "-invalid-utf8", kind, fixtures, "invalid_utf8", ["M1-I13"],
               "Invalid member bytes reject all discovered members before admission.")
    for different in (False, True):
        reject("zip-duplicate-" + ("different" if different else "same"), "zip", archive([
            {"name": "same.txt", "raw": b"a"}, {"name": "same.txt", "raw": b"b" if different else b"a"}]),
            "invalid_batch", ["M1-I12"], "Duplicate canonical ZIP source rejects both equal and unequal text.")
        entries = [{"source": "same.txt", "text": "a"}, {"source": "same.txt", "text": "b" if different else "a"}]
        deferred("json-duplicate-" + ("different" if different else "same"), entries, "invalid_batch",
                 ["M1-I12"], "Duplicate-source semantics in a valid JSON schema are deferred to prepare.")

    # Exact declared byte and count boundaries remain distinct from total limits.
    for kind in ("directory", "zip", "json"):
        for count in (64, 65):
            entries = [{"source": f"f{number:02d}.txt", "text": ""} for number in range(count)]
            fixtures = (directory([(e["source"], b"") for e in entries]) if kind == "directory" else
                        archive([{"name": e["source"], "raw": b""} for e in entries]) if kind == "zip" else json_bundle(entries))
            expected = [entry(e["source"], e["text"]) for e in entries] if kind != "json" else entries
            if count == 64:
                valid(kind + "-count-limit", kind, fixtures, expected, ["M1-I14"], "Exactly 64 empty files count as 64 admitted entries.")
            elif kind == "json":
                deferred("json-count-over", entries, "invalid_batch", ["M1-I14"], "Valid-schema 65-entry JSON is deferred to semantic validation.")
            else:
                reject(kind + "-count-over", kind, fixtures, "invalid_batch", ["M1-I14", "M1-I22"], "The 65th discovered file rejects all admission.")
        for size in (32768, 32769):
            text = "\u00e9" * 16384 + ("x" if size > 32768 else "")
            entries = [{"source": "a.txt", "text": text}]
            fixtures = (directory([("a.txt", text.encode())]) if kind == "directory" else
                        archive([{"name": "a.txt", "raw": text.encode()}]) if kind == "zip" else json_bundle(entries))
            expected = [entry("a.txt", text)] if kind != "json" else entries
            if size == 32768:
                valid(kind + "-member-bytes-limit", kind, fixtures, expected, ["M1-I13", "M1-I15", *(["M1-I24"] if kind == "json" else [])],
                      "32768 decoded UTF-8 bytes are valid; multibyte text separates bytes from characters.")
            elif kind == "json":
                deferred("json-member-bytes-over", entries, "too_large", ["M1-I15", "M1-I24"],
                         "Valid-schema JSON text has 32769 encoded bytes and fails at prepare.")
            else:
                reject(kind + "-member-bytes-over", kind, fixtures, "too_large", ["M1-I15", "M1-I22"],
                       "32769-byte member rejects discovery before admission.")
        for total in (524288, 524289):
            entries = [{"source": f"f{number:02d}.txt", "text": "x" * 32768} for number in range(16)]
            if total > 524288:
                entries.append({"source": "last.txt", "text": "x"})
            fixtures = (directory([(e["source"], e["text"].encode()) for e in entries]) if kind == "directory" else
                        archive([{"name": e["source"], "raw": e["text"].encode()} for e in entries]) if kind == "zip" else json_bundle(entries))
            expected = [entry(e["source"], e["text"]) for e in entries] if kind != "json" else entries
            if total == 524288:
                valid(kind + "-total-bytes-limit", kind, fixtures, expected, ["M1-I16"], "Exact total cap with legal individual sizes and count.")
            elif kind == "json":
                deferred("json-total-bytes-over", entries, "too_large", ["M1-I16"], "JSON total is one byte over while each member and count are legal.")
            else:
                reject(kind + "-total-bytes-over", kind, fixtures, "too_large", ["M1-I16", "M1-I22"],
                       "Total is one byte over while each member and count are legal.")

    control_entries = [{"source": f"control{n:02d}.txt", "text": "\u0001" * 32768} for n in range(16)]
    valid("json-control-total-byte-limit", "json", json_bundle(control_entries), control_entries,
          ["M1-I16", "M1-I24"],
          "524288 legal decoded control-character bytes stress persisted JSON escaping and observer bounds; archive-byte cap is the ZIP archive rule, not a new JSON text limit.")

    # Directory metadata is excluded from file count, but retained for byte/ratio accounting.
    valid("zip-64-files-plus-directory", "zip", archive([
        *[{"name": f"f{n:02d}.txt", "raw": b""} for n in range(64)],
        {"name": "empty/", "raw": b"", "mode": stat.S_IFDIR | 0o700}]),
        [entry(f"f{n:02d}.txt", "") for n in range(64)], ["M1-I14", "M1-I21"],
        "Ignored directory entry does not turn 64 files into an invalid 65-file batch.")
    for size in (1048576, 1048577):
        fixtures = archive([{"name": "a.txt", "raw": b"a"}], pad_to=size)
        if size == 1048576:
            valid("zip-archive-bytes-limit", "zip", fixtures, [entry("a.txt", "a")], ["M1-I17"],
                  "Valid ZIP with a legal leading prefix is exactly the archive byte cap; content/ratio bounds are independent.")
        else:
            reject("zip-archive-bytes-over", "zip", fixtures, "too_large", ["M1-I17", "M1-I22"],
                   "Legal prefixed ZIP is one byte above the input archive cap.")
    for size in (1199, 1200, 1201):
        raw = zip_bytes([{"name": "a.txt", "raw": b"x" * size, "compression": zipfile.ZIP_DEFLATED}])
        with zipfile.ZipFile(io.BytesIO(raw)) as reader:
            member = reader.infolist()[0]
            if member.file_size != size or member.compress_size != 12:
                raise ValueError("Fixture compressor changed the independently declared ratio boundary")
        if size <= 1200:
            valid("zip-ratio-" + ("limit" if size == 1200 else "below"), "zip", [file_fixture("batch", raw)],
                  [entry("a.txt", "x" * size)], ["M1-I18", "M1-I19"],
                  f"Declared sizes {size}/12 isolate the <=100 member and same-population aggregate ratio.")
        else:
            reject("zip-ratio-over", "zip", [file_fixture("batch", raw)], "too_large", ["M1-I18", "M1-I19", "M1-I22"],
                   "Declared sizes 1201/12 exceed 100 while file/member/archive/count bounds remain legal.")
    valid("zip-ratio-zero-compressed", "zip", archive([{"name": "empty.txt", "raw": b""}]),
          [entry("empty.txt", "")], ["M1-I18", "M1-I19"], "Empty stored member has 0/max(1,0), never division by zero.")
    reject("zip-ignored-directory-byte-bound", "zip", archive([
        {"name": "ignored/", "raw": b"x" * 32769, "mode": stat.S_IFDIR | 0o700}]), "too_large",
        ["M1-I15", "M1-I21", "M1-I22"], "Ignored directory payload cannot bypass the individual member byte bound.")
    reject("zip-ignored-directory-ratio", "zip", archive([
        {"name": "ignored/", "raw": b"x" * 1201, "mode": stat.S_IFDIR | 0o700, "compression": zipfile.ZIP_DEFLATED}]),
        "too_large", ["M1-I18", "M1-I19", "M1-I21", "M1-I22"], "Ignored directory still contributes its compressed and raw bytes to ratio validation.")
    reject("zip-ignored-directory-total", "zip", archive([
        *[{"name": f"f{n:02d}.txt", "raw": b"x" * 32768} for n in range(16)],
        {"name": "ignored/", "raw": b"x", "mode": stat.S_IFDIR | 0o700}]), "too_large",
        ["M1-I16", "M1-I21", "M1-I22"], "Ignored one-byte directory payload takes otherwise valid raw total one byte over.")

    schemas: list[tuple[str, Any]] = [
        ("top-list", []), ("top-null", None), ("top-missing", {}),
        ("top-extra", {"entries": [], "extra": 1}), ("entries-object", {"entries": {}}),
        ("entries-null", {"entries": None}), ("member-list", {"entries": [[]]}),
        ("member-missing-source", {"entries": [{"text": "x"}]}),
        ("member-missing-text", {"entries": [{"source": "a.txt"}]}),
        ("member-extra", {"entries": [{"source": "a.txt", "text": "x", "extra": 1}]}),
        ("source-nonstr", {"entries": [{"source": 1, "text": "x"}]}),
        ("text-nonstr", {"entries": [{"source": "a.txt", "text": False}]}),
    ]
    for label, data in schemas:
        reject("json-schema-" + label, "json", [file_fixture("batch", encoded(data))], "invalid_json",
               ["M1-I05"], "JSON schema rejects this structural class before any job exists.")
    reject("json-syntax", "json", [file_fixture("batch", b'{"entries":[')], "invalid_json", ["M1-I27"], "Truncated JSON syntax is distinguished from schema failure.")
    reject("json-invalid-utf8", "json", [file_fixture("batch", b'{"entries":[]}\xff')], "invalid_utf8", ["M1-I13"], "Raw JSON bytes are not valid UTF-8.")
    reject("zip-syntax", "zip", [file_fixture("batch", b"PK\x03\x04truncated")], "invalid_archive", ["M1-I27"], "Truncated ZIP syntax admits no job.")
    for kind in ("directory", "zip", "json"):
        reject(kind + "-missing", kind, [], "io_error", ["M1-I28"], "Missing explicit input reports an I/O error.")
        wrong = [file_fixture("batch", b"a")] if kind == "directory" else [{"path": "batch", "kind": "directory"}]
        reject(kind + "-wrong-kind", kind, wrong, "io_error", ["M1-I28"], "Existing input has the wrong filesystem kind and admits nothing.")
    for label, source, text, error, requirement in (
        ("source-parent", "../bad.txt", "bad", "invalid_source", "M1-I11"),
        ("unsupported", "bad.csv", "bad", "unsupported_type", "M1-I04"),
        ("unencodable", "bad.txt", "\ud800", "invalid_utf8", "M1-I13"),
    ):
        deferred("json-deferred-" + label, [{"source": "safe.txt", "text": "safe"}, {"source": source, "text": text}],
                 error, [requirement], "Valid JSON schema admits the exact manifest, then prepare fails semantics without catalog writes.")
    literal_entries = [{"source": "unicode.txt", "text": "\u00e9" * 16384}]
    literal_raw = json.dumps({"entries": literal_entries}, ensure_ascii=False, separators=(",", ":")).encode()
    valid("json-literal-byte-limit", "json", [file_fixture("batch", literal_raw)], literal_entries,
          ["M1-I24"], "Literal and escaped JSON Unicode representations have the same decoded 32768-byte member limit.")
    return rows
