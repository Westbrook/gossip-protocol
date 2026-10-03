"""Portable qualification of inert HTTP catalog declarations, never a candidate run."""
from __future__ import annotations

from collections import Counter
from dataclasses import FrozenInstanceError, replace
import hashlib
import io
import json
from typing import Any
import unittest
import zipfile

from gossip_harness import candidate_http_cases_core_v1 as core
from gossip_harness import candidate_http_cases_v1 as catalog
from gossip_harness import candidate_http_relations_v1 as relations
from gossip_harness import candidate_http_semantics_v1 as sem


_FAMILY_COUNTS = {
    "HTTP-EMPTY-HEALTH": 6, "HTTP-DOCUMENT-ROUTES": 26, "HTTP-ERROR-STATUS": 11,
    "HTTP-POST-SHAPES": 56, "HTTP-QUERY-VALUES": 8, "HTTP-BODY-WIRE": 9,
    "HTTP-INTAKE-ROUTES": 27, "HTTP-ACTION-STATE": 58, "HTTP-ROOT-PATH": 69,
    "HTTP-PERSIST-LISTENER": 6,
}
_DOCUMENT_IDS = (
    "doc-4e5111992f051db7335c4efe6f07ec35396cb58dc3b8ae76d6120adfe001ced9",
    "doc-1987335a5a35396fabb1dc200353e9ffef855c380d58fb331afabeaf141ddf15",
    "doc-6558ed3404d057ccf599001d527438de0eee7be3b4883c327d292d9b1d37c106",
    "doc-082e839e633313c193014498aa1d2a95dc08717ba3196c15ad0fd20a30b90e4a",
)
_SOURCES = ("Alpha.txt", "alpha.md", "nested/é.md", "z.txt")
_TEXTS = ("Straße [a.b]\n", "Straße [a.b]\n", "CAFÉ\r\n", "")


def _requests(case: core.LiteralCase) -> tuple[core.Step, ...]:
    return tuple(step for step in case.steps if step.kind == "request")


def _semantic(step: core.Step) -> sem.Expectation:
    assert step.expectation is not None and step.expectation.semantic is not None
    return step.expectation.semantic


def _request(step: core.Step) -> core.HttpRequest:
    assert step.request is not None
    return step.request


def _body(step: core.Step) -> Any:
    return json.loads(_request(step).body)


def _value(step: core.Step) -> Any:
    body = _semantic(step).expected_body
    assert body is not None
    return json.loads(body)


def _last_shape(case: core.LiteralCase, shape: str) -> Any:
    for step in reversed(_requests(case)):
        if step.expectation and step.expectation.semantic and step.expectation.semantic.shape == shape:
            return _value(step)
    raise AssertionError(f"No {shape} expectation in {case.row_id}")


def _job(case: core.LiteralCase, job_id: str = "subject") -> dict[str, Any]:
    return next(job for job in _last_shape(case, "jobs")["jobs"] if job["job_id"] == job_id)


def _subject(case: core.LiteralCase) -> core.Step:
    return next(step for step in _requests(case) if step.step_id.endswith("-subject"))


def _post(case: core.LiteralCase, target: str) -> tuple[core.Step, ...]:
    return tuple(step for step in _requests(case)
                 if _request(step).method == "POST" and _request(step).target == target)


def _tiny_case() -> core.LiteralCase:
    builder = core.Builder("HTTP-EMPTY-HEALTH/declaration-test", ("V0-HTTP-04",))
    builder.start()
    builder.request("GET", "/health", sem.success("health"), label="source", check=False)
    return builder.finish()


def _relation_raw(*, jobs: list[dict[str, Any]] | None = None,
                  source: str = "source", jobs_step: str = "jobs") -> bytes:
    if jobs is None:
        jobs = [{"job_id": "subject", "epoch": 1, "state": "failed", "total": 1,
                 "completed": 0, "error": {"from_error_step": source, "expected_status": 400}}]
    return core.encode({"protocol": "candidate-http-relations-v1", "kind": "jobs-with-related-errors",
                        "jobs_step_id": jobs_step, "jobs": jobs})


def _relation_case(raw: bytes | None = None, *, source: sem.Expectation | None = None) -> core.LiteralCase:
    tiny = _tiny_case()
    request = core.HttpRequest("POST", "/api/jobs/subject/prepare",
        (("Host", f"127.0.0.1:{core.PORT}"), ("Connection", "close"), ("Content-Type", "application/json"),
         ("Content-Length", "2")), b"{}")
    error = core.Step("source", "request", 1, "/inputs", request=request,
        expectation=core.HttpExpectation(semantic=source or sem.unclassified_error(400)))
    jobs = core.Step("jobs", "request", 1, "/inputs",
        request=core.HttpRequest("GET", "/api/jobs", (("Host", f"127.0.0.1:{core.PORT}"), ("Connection", "close")), b""),
        expectation=core.HttpExpectation(relation_json=_relation_raw() if raw is None else raw))
    return replace(tiny, steps=(tiny.steps[0], error, jobs, tiny.steps[-1]))


class CandidateHttpCatalogMaterializationTests(unittest.TestCase):
    """Check literal scientific inputs and histories without dispatching them."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.definitions = catalog.definitions()
        cls.cases = {case.row_id: case for case in cls.definitions}

    def case(self, row_id: str) -> core.LiteralCase:
        return self.cases[row_id]

    def test_complete_ordered_roster_and_family_counts(self) -> None:
        self.assertEqual(len(self.definitions), 276)
        self.assertEqual(len(self.cases), 276)
        self.assertEqual(tuple(case.row_id for case in self.definitions), catalog.ROW_IDS)
        self.assertEqual(dict(catalog.FAMILY_COUNTS), _FAMILY_COUNTS)
        self.assertEqual(Counter(case.family_id for case in self.definitions), _FAMILY_COUNTS)
        self.assertEqual({item for case in self.definitions for item in case.requirement_ids}, {
            "V0-HTTP-01", "V0-HTTP-02", "V0-HTTP-03", "V0-HTTP-04",
            "M1-HTTP-01", "M1-HTTP-02", "M1-I30", "M1-I31"})
        self.assertEqual({item for case in self.definitions for item in case.interaction_ids},
                         {"V0-CLI-03", "M1-A08"})

    def test_empty_rows_have_no_guard_or_seeded_data(self) -> None:
        for name in ("documents", "export", "jobs", "health", "missing-documents", "missing-jobs"):
            with self.subTest(row=name):
                case = self.case("HTTP-EMPTY-HEALTH/" + name)
                self.assertEqual(case.fixtures, ())
                self.assertEqual(_last_shape(case, "jobs"), {"jobs": []})
                self.assertEqual(_last_shape(case, "documents"), {"documents": [], "total": 0})
                self.assertEqual(_last_shape(case, "export"),
                                 {"format": "local-research-library-v0", "documents": []})
        self.assertEqual(_value(_subject(self.case("HTTP-EMPTY-HEALTH/health"))), {"status": "ok", "schema": 0})

    def test_imports_bind_literal_corpus_bytes_and_independent_document_ids(self) -> None:
        for index, (source, text, document_id) in enumerate(zip(_SOURCES, _TEXTS, _DOCUMENT_IDS)):
            with self.subTest(source=source):
                case = self.case(f"HTTP-DOCUMENT-ROUTES/import-{index}")
                self.assertEqual(_body(_subject(case)), {"source": source})
                self.assertEqual(next(item.data for item in case.fixtures if item.path == source), text.encode("utf-8"))
                documents = _last_shape(case, "documents")["documents"]
                self.assertEqual(len(documents), 1)
                self.assertEqual(documents[0]["document_id"], document_id)
                self.assertEqual(documents[0]["source"], source)
                self.assertEqual(documents[0]["text"], text)
                self.assertEqual(documents[0]["title"], source.rsplit("/", 1)[-1])
                self.assertEqual(set(documents[0]), {"source", "source_id", "document_id", "blob_id", "title", "text"})
                self.assertEqual(_last_shape(case, "jobs"), {"jobs": []})
        first = _last_shape(self.case("HTTP-DOCUMENT-ROUTES/import-0"), "documents")["documents"][0]
        second = _last_shape(self.case("HTTP-DOCUMENT-ROUTES/import-1"), "documents")["documents"][0]
        self.assertEqual(first["blob_id"], "blob-5ca4f749bfa7d76933ed142a1eb3b10afc52f0f4869e4c3c3a5993128e582eb2")
        self.assertEqual(first["blob_id"], second["blob_id"])
        self.assertNotEqual(first["source_id"], second["source_id"])

    def test_queries_are_once_encoded_and_totals_precede_pagination(self) -> None:
        rows = (
            ("casefold", "STRASSE", 0, 100, 2, _SOURCES[:2]),
            ("literal", "%5Ba.b%5D", 0, 100, 2, _SOURCES[:2]),
            ("unicode-casefold", "caf%C3%A9", 0, 100, 1, (_SOURCES[2],)),
            ("offset-one", "", 1, 100, 4, _SOURCES[1:]),
            ("limit-one", "", 0, 1, 4, _SOURCES[:1]),
            ("offset-beyond", "", 9, 100, 4, ()),
            ("offset-positive-limit", "", 1, 1, 4, (_SOURCES[1],)),
            ("query-256", "%C3%A9" * 256, 0, 100, 0, ()),
        )
        for name, encoded, offset, limit, total, sources in rows:
            with self.subTest(row=name):
                case = self.case("HTTP-DOCUMENT-ROUTES/" + name)
                step = _subject(case)
                self.assertEqual(_request(step).target, f"/api/documents?q={encoded}&offset={offset}&limit={limit}")
                value = _value(step)
                self.assertEqual(value["total"], total)
                self.assertEqual(tuple(doc["source"] for doc in value["documents"]), sources)
                self.assertEqual(_last_shape(case, "jobs"), {"jobs": []})
        invalid = _subject(self.case("HTTP-QUERY-VALUES/5-q"))
        self.assertEqual(_request(invalid).target, "/api/documents?q=" + "%C3%A9" * 257)
        self.assertEqual((_semantic(invalid).status, _semantic(invalid).code), (400, "invalid_request"))

    def test_export_selection_orders_source_and_keeps_unknown_codes_unknown(self) -> None:
        step = _subject(self.case("HTTP-DOCUMENT-ROUTES/export-reverse"))
        self.assertEqual(_body(step), {"ids": [_DOCUMENT_IDS[3], _DOCUMENT_IDS[0]]})
        self.assertEqual([doc["document_id"] for doc in _value(step)["documents"]],
                         [_DOCUMENT_IDS[0], _DOCUMENT_IDS[3]])
        for name in ("export-duplicate", "export-missing-after-valid"):
            expected = _semantic(_subject(self.case("HTTP-DOCUMENT-ROUTES/" + name)))
            self.assertEqual((expected.shape, expected.status, expected.code), ("error", None, None))
        route = _semantic(_subject(self.case("HTTP-ERROR-STATUS/unknown-route")))
        self.assertEqual((route.shape, route.status, route.code), ("error", 404, None))

    def test_source_changed_uses_immutable_file_and_prior_completed_job(self) -> None:
        case = self.case("HTTP-ERROR-STATUS/source-changed")
        self.assertEqual(next(item.data for item in case.fixtures if item.path == "same.txt"), b"second")
        submits = [_body(step) for step in _post(case, "/api/jobs")]
        self.assertIn({"job_id": "subject", "entries": [{"source": "same.txt", "text": "first"}]}, submits)
        self.assertEqual(_semantic(_subject(case)).code, "source_changed")
        self.assertEqual(_job(case), {"job_id": "subject", "epoch": 1, "state": "completed",
                                     "total": 1, "completed": 1, "error": None})
        self.assertEqual([(doc["source"], doc["text"]) for doc in _last_shape(case, "documents")["documents"]],
                         [("guard.txt", "guard\n"), ("same.txt", "first")])

    def test_shape_faults_reject_before_admission_but_empty_entries_are_valid(self) -> None:
        for name in ("object", "null", "string", "number", "entry-nonobject", "entry-missing-text"):
            with self.subTest(shape=name):
                case = self.case("HTTP-POST-SHAPES/entries-shape-" + name)
                subject = _post(case, "/api/jobs")[-1]
                self.assertEqual((_semantic(subject).status, _semantic(subject).code), (400, "invalid_request"))
                self.assertEqual([job["job_id"] for job in _last_shape(case, "jobs")["jobs"]], ["sentinel"])
        case = self.case("HTTP-POST-SHAPES/job-id-valid-64-ascii")
        self.assertEqual(_job(case, "a" * 64), {"job_id": "a" * 64, "epoch": 1, "state": "queued",
                                               "total": 0, "completed": 0, "error": None})

    def test_body_boundary_binds_raw_utf8_hash_and_content_length(self) -> None:
        hashes = {65536: "0eb37848fedad2a718f470eef4acdf95ab05e677208a5c7bbe826cd7dd219560",
                  65537: "ba1bb39465a8c8dd744116805238e3e4e3f1c9bfba1ed6101b62364110d78ca9"}
        for size in (65536, 65537):
            with self.subTest(size=size):
                case = self.case(f"HTTP-BODY-WIRE/raw-bytes-{size}")
                step = next(step for step in _requests(case) if step.step_id.endswith("-subject-request"))
                request = _request(step)
                self.assertEqual(len(request.body), size)
                self.assertEqual(hashlib.sha256(request.body).hexdigest(), hashes[size])
                self.assertIn(("Content-Length", str(size)), request.headers)
                self.assertTrue(request.wire_bytes().endswith(b"\r\n\r\n" + request.body))
                self.assertEqual(_body(step)["entries"], [{"source": "wire.txt", "text": "é" * 4096}])
                if size == 65536:
                    self.assertEqual((_job(case, "wire")["state"], _job(case, "wire")["completed"]), ("completed", 1))
                else:
                    self.assertEqual((_semantic(step).status, _semantic(step).code), (400, None))
                    self.assertEqual([job["job_id"] for job in _last_shape(case, "jobs")["jobs"]], ["sentinel"])

    def test_four_intake_forms_have_literal_closed_bodies_and_same_final_content(self) -> None:
        keys = {"entries": {"job_id", "entries"}, "directory": {"job_id", "directory", "namespace"},
                "zip": {"job_id", "zip", "namespace"}, "json": {"job_id", "json"}}
        for form in keys:
            for empty in (False, True):
                with self.subTest(form=form, empty=empty):
                    case = self.case(f"HTTP-INTAKE-ROUTES/{form}-{'empty' if empty else 'nonempty'}")
                    subject = _post(case, "/api/jobs")[-1]
                    self.assertEqual(set(_body(subject)), keys[form])
                    self.assertEqual(_job(case), {"job_id": "subject", "epoch": 1, "state": "completed",
                        "total": 0 if empty else 3, "completed": 0 if empty else 3, "error": None})
                    expected = [("guard.txt", "guard\n")] if empty else [
                        ("guard.txt", "guard\n"), ("ns/a.html", "<b>literal</b>\n"),
                        ("ns/nested/a.md", "café\r\n"), ("ns/z.txt", "")]
                    self.assertEqual([(doc["source"], doc["text"]) for doc in _last_shape(case, "documents")["documents"]], expected)

    def test_discovery_count_boundary_preserves_all_literal_members(self) -> None:
        for form in ("directory", "zip", "json"):
            for count in (64, 65):
                with self.subTest(form=form, count=count):
                    case = self.case(f"HTTP-INTAKE-ROUTES/{form}-file-count-{count}")
                    files = {item.path: item.data for item in case.fixtures if item.kind == "file"}
                    if form == "directory":
                        members = [path for path in files if path.startswith(f"count{count}/")]
                    elif form == "zip":
                        with zipfile.ZipFile(io.BytesIO(files[f"count{count}.zip"])) as archive:
                            members = archive.namelist()
                    else:
                        members = json.loads(files[f"count{count}.json"])["entries"]
                    self.assertEqual(len(members), count)
                    if count == 64:
                        self.assertEqual((_job(case)["state"], _job(case)["total"], _job(case)["completed"]),
                                         ("completed", 64, 64))
                    else:
                        self.assertEqual(_semantic(_post(case, "/api/jobs")[-1]).code, "invalid_batch")
                        self.assertEqual([job["job_id"] for job in _last_shape(case, "jobs")["jobs"]], ["sentinel"])

    def test_semantic_entry_faults_admit_then_fail_prepare(self) -> None:
        for form in ("entries", "json"):
            for fault, code in (("traversal", "invalid_source"), ("unsupported", "unsupported_type")):
                with self.subTest(form=form, fault=fault):
                    case = self.case(f"HTTP-INTAKE-ROUTES/{form}-semantic-deferred-{fault}")
                    admission = _post(case, "/api/jobs")[-1]
                    prepare = _post(case, "/api/jobs/subject/prepare")[-1]
                    self.assertLess(case.steps.index(admission), case.steps.index(prepare))
                    self.assertEqual((_semantic(admission).status, _semantic(admission).shape), (200, "unspecified"))
                    queued = [step for step in _requests(case) if admission.step_id < step.step_id < prepare.step_id
                              and step.expectation and step.expectation.semantic
                              and step.expectation.semantic.shape == "jobs"]
                    self.assertTrue(any(any(job == {"job_id": "subject", "epoch": 1, "state": "queued",
                        "total": 1, "completed": 0, "error": None} for job in _value(step)["jobs"]) for step in queued))
                    self.assertEqual(_job(case), {"job_id": "subject", "epoch": 1, "state": "failed",
                                                 "total": 1, "completed": 0, "error": code})
                    self.assertEqual([doc["source"] for doc in _last_shape(case, "documents")["documents"]], ["guard.txt"])

    def test_twenty_state_actions_match_independent_table(self) -> None:
        # Expected final state, epoch, completed count, persisted error, response code.
        table = {
            "queued": (("running", 1, 0, None, None), ("queued", 1, 0, None, "job_state"),
                       ("cancelled", 2, 0, None, None), ("queued", 1, 0, None, "job_state")),
            "running": (("running", 1, 0, None, None), ("completed", 1, 2, None, None),
                        ("cancelled", 2, 0, None, None), ("running", 1, 0, None, "job_state")),
            "completed": (("completed", 1, 2, None, "job_state"), ("completed", 1, 2, None, None),
                          ("completed", 1, 2, None, "job_state"), ("completed", 1, 2, None, "job_state")),
            "cancelled": (("cancelled", 2, 0, None, "job_state"), ("cancelled", 2, 0, None, "job_state"),
                          ("cancelled", 2, 0, None, None), ("queued", 3, 0, None, None)),
            "failed": (("failed", 1, 0, "invalid_source", "job_state"),
                       ("failed", 1, 0, "invalid_source", "job_state"),
                       ("failed", 1, 0, "invalid_source", "job_state"), ("queued", 2, 0, None, None)),
        }
        for initial, outcomes in table.items():
            for action, expected in zip(("prepare", "commit", "cancel", "retry"), outcomes):
                with self.subTest(initial=initial, action=action):
                    case = self.case(f"HTTP-ACTION-STATE/{initial}-{action}")
                    final, epoch, completed, error, code = expected
                    self.assertEqual(_job(case), {"job_id": "subject", "epoch": epoch, "state": final,
                                                 "total": 2, "completed": completed, "error": error})
                    step = _post(case, "/api/jobs/subject/" + action)[-1]
                    self.assertEqual((_semantic(step).status, _semantic(step).code), (200 if code is None else 409, code))

    def test_all_state_stale_epoch_requests_use_known_distinct_integers(self) -> None:
        for state, epoch in (("queued", 1), ("running", 1), ("completed", 1), ("cancelled", 2), ("failed", 1)):
            with self.subTest(state=state):
                case = self.case(f"HTTP-ACTION-STATE/{state}-stale-commit")
                step = _post(case, "/api/jobs/subject/commit")[-1]
                self.assertEqual(_body(step), {"epoch": epoch + 1})
                self.assertEqual((_semantic(step).status, _semantic(step).code), (409, "stale_epoch"))
                self.assertEqual((_job(case)["epoch"], _job(case)["state"]), (epoch, state))

    def test_retry_preserves_invalid_manifest_and_second_failure_epoch(self) -> None:
        case = self.case("HTTP-ACTION-STATE/failed-retry-prepare-fails-again")
        submits = [step for step in _post(case, "/api/jobs") if _body(step)["job_id"] == "subject"]
        self.assertEqual(len(submits), 1)
        self.assertEqual(_body(submits[0])["entries"],
                         [{"source": "../bad.txt", "text": "bad"}, {"source": "a.txt", "text": "a"}])
        self.assertEqual([_semantic(step).code for step in _post(case, "/api/jobs/subject/prepare")],
                         ["invalid_source", "invalid_source"])
        self.assertEqual(_job(case), {"job_id": "subject", "epoch": 2, "state": "failed",
                                     "total": 2, "completed": 0, "error": "invalid_source"})

    def test_old_token_is_stale_after_successor_conflict_failure(self) -> None:
        case = self.case("HTTP-ACTION-STATE/old-token-after-successor-failed")
        commits = _post(case, "/api/jobs/subject/commit")
        self.assertEqual([_body(step) for step in commits], [{"epoch": 3}, {"epoch": 1}])
        self.assertEqual([_semantic(step).code for step in commits], ["source_changed", "stale_epoch"])
        self.assertEqual(_job(case), {"job_id": "subject", "epoch": 3, "state": "failed",
                                     "total": 2, "completed": 0, "error": "source_changed"})
        self.assertEqual([(doc["source"], doc["text"]) for doc in _last_shape(case, "documents")["documents"]],
                         [("guard.txt", "guard\n"), ("item.txt", "file text")])

    def test_exactly_four_rows_use_declared_error_equality_after_prepare(self) -> None:
        expected_rows = {f"HTTP-ROOT-PATH/{form}-source-{boundary}"
                         for form in ("entries", "json") for boundary in ("bytes257", "segments17")}
        actual = set()
        for case in self.definitions:
            for step in _requests(case):
                if step.expectation and step.expectation.relation_json:
                    actual.add(case.row_id)
                    relation = json.loads(step.expectation.relation_json)
                    self.assertEqual(relation["jobs_step_id"], step.step_id)
                    self.assertEqual([job["job_id"] for job in relation["jobs"]], ["sentinel", "subject"])
                    subject = relation["jobs"][1]
                    self.assertEqual({key: value for key, value in subject.items() if key != "error"},
                                     {"job_id": "subject", "epoch": 1, "state": "failed", "total": 1, "completed": 0})
                    self.assertEqual(subject["error"]["expected_status"], 400)
                    source = next(candidate for candidate in case.steps if candidate.step_id == subject["error"]["from_error_step"])
                    self.assertLess(case.steps.index(source), case.steps.index(step))
                    self.assertEqual(_request(source).target, "/api/jobs/subject/prepare")
                    self.assertEqual((_semantic(source).status, _semantic(source).code), (400, None))
        self.assertEqual(actual, expected_rows)

    def test_root_switch_preserves_declared_db_and_distinct_fixture_bytes(self) -> None:
        for suffix, second in (("root-independence-source-ids", b"first"), ("root-change-source-changed", b"second")):
            with self.subTest(row=suffix):
                case = self.case("HTTP-PERSIST-LISTENER/" + suffix)
                starts = [step for step in case.steps if step.kind == "start"]
                self.assertEqual([(step.epoch, step.root) for step in starts], [(1, "/inputs/root-a"), (2, "/inputs/root-b")])
                self.assertTrue(all(step.argv[3:5] == ("--db", "/tmp/catalog.sqlite") for step in starts))
                files = {item.path: item.data for item in case.fixtures if item.kind == "file"}
                self.assertEqual((files["root-a/same.txt"], files["root-b/same.txt"]), (b"first", second))
                self.assertEqual(next(doc["text"] for doc in _last_shape(case, "documents")["documents"]
                                      if doc["source"] == "same.txt"), "first")
                self.assertIn("per-epoch-root-argv", case.required_mechanics)

    def test_confined_links_have_owned_targets_and_root_decoys(self) -> None:
        for variant, link_path, target in (("direct-link", "root/link", "../other/batch"),
                                          ("ancestor-link", "root/linked", "../other")):
            case = self.case("HTTP-ROOT-PATH/directory-" + variant)
            link = next(item for item in case.fixtures if item.path == link_path)
            self.assertEqual((link.kind, link.target), ("symlink", target))
            files = {item.path: item.data for item in case.fixtures if item.kind == "file"}
            self.assertEqual(files["root/nested/batch/a.html"], b"<b>literal</b>\n")
            self.assertNotEqual(files["root/nested/batch/a.html"], files["other/batch/a.html"])
            self.assertNotEqual(files["root/nested/batch/a.html"], files["nested/batch/a.html"])
            self.assertEqual(_semantic(_post(case, "/api/jobs")[-1]).code, "invalid_source")
            self.assertIn("confined-data-links-lstat-readlink", case.required_mechanics)

    def test_normal_restart_keeps_real_history_and_literal_successor_epoch(self) -> None:
        case = self.case("HTTP-PERSIST-LISTENER/http-normal-restart-cancel-retry")
        self.assertEqual([step.epoch for step in case.steps if step.kind == "start"], [1, 2])
        self.assertEqual([step.epoch for step in case.steps if step.kind == "stop"], [1, 2])
        self.assertFalse(any(step.kind == "cli" for step in case.steps))
        self.assertEqual(_job(case), {"job_id": "subject", "epoch": 3, "state": "completed",
                                     "total": 2, "completed": 2, "error": None})
        self.assertEqual(_body(_post(case, "/api/jobs/subject/commit")[-1]), {"epoch": 3})
        self.assertIn("same-db-source-epoch-restart", case.required_mechanics)

    def test_cli_handoff_binds_each_before_after_state_and_process_interval(self) -> None:
        case = self.case("HTTP-PERSIST-LISTENER/http-cli-http-same-db")
        cli = [step for step in case.steps if step.kind == "cli"]
        self.assertTrue(cli)
        stops = [index for index, step in enumerate(case.steps) if step.kind == "stop"]
        starts = [index for index, step in enumerate(case.steps) if step.kind == "start"]
        self.assertTrue(all(stops[0] < case.steps.index(step) < starts[1] for step in cli))
        expected = {
            "job-retry": (("cancelled", 2, 0), ("queued", 3, 0)),
            "job-prepare": (("queued", 3, 0), ("running", 3, 0)),
            "job-commit": (("running", 3, 0), ("completed", 3, 2)),
        }
        seen = set()
        for step in cli:
            snapshots = json.loads(step.cli_state_json)
            self.assertEqual(core.encode(snapshots), step.cli_state_json)
            self.assertEqual(step.record()["cli_expectation"]["public_state"], snapshots)
            self.assertEqual(step.argv[3:7], ("--db", "/tmp/catalog.sqlite", "--root", "/inputs"))
            command = step.argv[7]
            if command in expected:
                seen.add(command)
                actual = []
                for phase in ("before", "after"):
                    state = snapshots[phase]
                    subject = next(job for job in state["jobs"]["jobs"] if job["job_id"] == "subject")
                    actual.append((subject["state"], subject["epoch"], subject["completed"]))
                    self.assertIsNone(subject["error"])
                    self.assertEqual(state["documents"], state["export"]["documents"])
                self.assertEqual(tuple(actual), expected[command])
            else:
                self.assertEqual(snapshots["before"], snapshots["after"])
        self.assertEqual(seen, set(expected))
        self.assertEqual(_job(case)["state"], "completed")
        self.assertEqual(_job(case)["epoch"], 3)
        self.assertIn("authorized-qualified-same-db-CLI-handoff", case.required_mechanics)

    def test_root_page_is_raw_observation_without_json_or_browser_credit(self) -> None:
        case = self.case("HTTP-PERSIST-LISTENER/root-page-reachable")
        root = next(step for step in _requests(case) if _request(step).target == "/")
        self.assertTrue(root.expectation and root.expectation.raw_facts_only)
        self.assertIsNone(root.expectation.semantic)
        self.assertFalse(root.expectation.record()["independent_status_and_syntax"])
        self.assertIn("non-JSON-raw-fact-observation", case.required_mechanics)
        self.assertFalse(case.acceptance_authority)

    def test_large_histories_remain_nondispatchable_with_complete_resource_counts(self) -> None:
        large = [case for case in self.definitions if len(case.steps) > 64]
        self.assertTrue(large)
        for case in self.definitions:
            with self.subTest(row=case.row_id):
                self.assertFalse(case.dispatch_authority)
                self.assertFalse(case.acceptance_authority)
                self.assertFalse(case.fresh_execution)
                self.assertFalse(case.record()["v2_CasePlan_compatible"])
                counts = case.counts
                self.assertEqual(counts["server_epochs"], counts["server_stops"])
                self.assertEqual(counts["http_requests"], len(_requests(case)))
                self.assertEqual(counts["planned_total_processes"], counts["http_requests"] + counts["server_epochs"]
                                 + counts["finite_cli_processes"] + 1)
                if len(case.steps) > 64:
                    self.assertIn("versioned-step-bound-above-v2-64", case.required_mechanics)

    def test_serialization_returns_fresh_records_and_bound_immutable_values(self) -> None:
        case = self.case("HTTP-DOCUMENT-ROUTES/import-2")
        identity = case.definition_sha256
        record = case.record()
        record["row_id"] = "rewritten"
        record["fixtures"].clear()
        record["steps"].clear()
        self.assertEqual(case.definition_sha256, identity)
        self.assertEqual(case.record()["row_id"], "HTTP-DOCUMENT-ROUTES/import-2")
        with self.assertRaises(FrozenInstanceError):
            case.row_id = "rewrite"  # type: ignore[misc]


class CandidateHttpCatalogDeclarationBoundaryTests(unittest.TestCase):
    """Reject forged lifecycle, dependency, staging and relation declarations."""

    def test_wrong_request_port_is_rejected(self) -> None:
        request = core.HttpRequest("GET", "/health",
            (("Host", "127.0.0.1:18766"), ("Connection", "close")), b"", port=18766)
        with self.assertRaises(core.CatalogError):
            core.Step("wrong-port", "request", 1, "/inputs", request=request,
                      expectation=core.HttpExpectation(semantic=sem.success("health")))

    def test_unknown_and_relation_only_dependency_facets_are_rejected(self) -> None:
        for facet in ("error_code", "jobs_fixed_fields", "related_error_equality", "missing"):
            with self.subTest(facet=facet), self.assertRaises(core.CatalogError):
                core.Dependency("source", facet)

    def test_raw_facts_cannot_supply_a_semantic_prerequisite(self) -> None:
        case = _tiny_case()
        raw = replace(case.steps[1], expectation=core.HttpExpectation(raw_facts_only=True))
        dependent = replace(case.steps[1], step_id="dependent", expectation=core.HttpExpectation(
            semantic=sem.success("health"), content_requires=(core.Dependency(raw.step_id, "status"),)))
        with self.assertRaises(core.CatalogError):
            replace(case, steps=(case.steps[0], raw, dependent, case.steps[-1]))

    def test_missing_or_future_prerequisites_are_rejected(self) -> None:
        case = _tiny_case()
        for source in ("missing", case.steps[-1].step_id, "dependent"):
            with self.subTest(source=source), self.assertRaises(core.CatalogError):
                dependent = replace(case.steps[1], step_id="dependent", expectation=core.HttpExpectation(
                    semantic=sem.success("health"), content_requires=(core.Dependency(source),)))
                replace(case, steps=(case.steps[0], dependent, case.steps[-1]))

    def test_prerequisite_must_be_an_http_step(self) -> None:
        case = _tiny_case()
        dependent = replace(case.steps[1], expectation=core.HttpExpectation(
            semantic=sem.success("health"), content_requires=(core.Dependency(case.steps[0].step_id),)))
        with self.assertRaises(core.CatalogError):
            replace(case, steps=(case.steps[0], dependent, case.steps[-1]))

    def test_empty_relation_is_rejected_by_shared_relation_contract(self) -> None:
        with self.assertRaises((core.CatalogError, relations.RelationError)):
            core.HttpExpectation(relation_json=_relation_raw(jobs=[]))

    def test_all_literal_relation_without_reference_is_rejected(self) -> None:
        jobs = [{"job_id": "subject", "epoch": 1, "state": "failed", "total": 1,
                 "completed": 0, "error": "invented"}]
        with self.assertRaises((core.CatalogError, relations.RelationError)):
            core.HttpExpectation(relation_json=_relation_raw(jobs=jobs))

    def test_conflicting_status_references_are_rejected(self) -> None:
        jobs = [{"job_id": name, "epoch": 1, "state": "failed", "total": 1, "completed": 0,
                 "error": {"from_error_step": "source", "expected_status": status}}
                for name, status in (("a", 400), ("b", 404))]
        with self.assertRaises((core.CatalogError, relations.RelationError)):
            core.HttpExpectation(relation_json=_relation_raw(jobs=jobs))

    def test_relation_source_is_prior_error_with_matching_status(self) -> None:
        for source in (sem.success("health"), sem.unclassified_error(404)):
            with self.subTest(source=source), self.assertRaises(core.CatalogError):
                _relation_case(source=source)

    def test_relation_source_cannot_be_missing_future_or_self(self) -> None:
        for source in ("missing", "jobs", "later"):
            with self.subTest(source=source), self.assertRaises((core.CatalogError, relations.RelationError)):
                _relation_case(_relation_raw(source=source))

    def test_relation_census_identity_is_exact(self) -> None:
        with self.assertRaises(core.CatalogError):
            _relation_case(_relation_raw(jobs_step="different-census"))

    def test_noncanonical_or_extra_relation_fields_are_rejected(self) -> None:
        obj = json.loads(_relation_raw())
        with self.assertRaises(core.CatalogError):
            core.HttpExpectation(relation_json=json.dumps(obj, indent=2).encode())
        obj["approved"] = True
        with self.assertRaises(core.CatalogError):
            core.HttpExpectation(relation_json=core.encode(obj))

    def test_fixture_paths_reject_escape_and_nonimmutable_data(self) -> None:
        for path in ("/outside", "../outside", "a/../outside", "a//b", "a\\b", "a\0b"):
            with self.subTest(path=path), self.assertRaises(core.CatalogError):
                core.Fixture(path, "file", b"x")
        with self.assertRaises(core.CatalogError):
            core.Fixture("a", "file", bytearray(b"x"))  # type: ignore[arg-type]

    def test_fixture_link_cannot_escape_owned_input_mount(self) -> None:
        for path, target in (("link", "../outside"), ("dir/link", "../../outside"), ("link", "/outside")):
            with self.subTest(path=path, target=target), self.assertRaises(core.CatalogError):
                core.Fixture(path, "symlink", target=target)

    def test_fixture_link_target_cannot_traverse_another_link_then_parent(self) -> None:
        # Lexical normpath says "outside"; real link expansion reaches /outside.
        # A declaration must reject this without constructing a real symlink.
        case = _tiny_case()
        fixtures = (core.Fixture("p", "directory"), core.Fixture("other", "directory"),
                    core.Fixture("outside", "file", b"owned"),
                    core.Fixture("p/link", "symlink", target="../other"),
                    core.Fixture("x", "symlink", target="p/link/../../outside"))
        with self.assertRaises(core.CatalogError):
            replace(case, fixtures=fixtures)

    def test_fixture_parent_and_link_target_must_be_explicit(self) -> None:
        case = _tiny_case()
        for fixtures in ((core.Fixture("dir/file", "file", b"x"),),
                         (core.Fixture("link", "symlink", target="missing"),)):
            with self.subTest(fixtures=fixtures), self.assertRaises(core.CatalogError):
                replace(case, fixtures=fixtures)

    def test_builder_rejects_fixture_rewrite_and_non_directory_parent(self) -> None:
        builder = core.Builder("HTTP-ROOT-PATH/declaration-test", ("M1-I31",))
        builder.add_file("a", b"first")
        with self.assertRaises(core.CatalogError):
            builder.add_file("a", b"second")
        with self.assertRaises(core.CatalogError):
            builder.add_file("a/child", b"x")

    def test_request_epoch_and_root_are_bound_to_active_server(self) -> None:
        case = _tiny_case()
        for change in ({"epoch": 2}, {"root": "/inputs/other"}):
            with self.subTest(change=change), self.assertRaises(core.CatalogError):
                changed = replace(case.steps[1], **change)
                replace(case, steps=(case.steps[0], changed, case.steps[-1]))

    def test_cli_requires_stopped_server_and_same_database(self) -> None:
        builder = core.Builder("HTTP-PERSIST-LISTENER/declaration-test", ("M1-HTTP-01",))
        builder.start()
        with self.assertRaises(core.CatalogError):
            builder.cli(("jobs",))
        state = {"jobs": {"jobs": []}, "documents": [],
                 "export": {"format": "local-research-library-v0", "documents": []}}
        snapshots = core.encode({"before": state, "after": state})
        with self.assertRaisesRegex(core.CatalogError, "source/db/root-bound"):
            core.Step("cli", "cli", 1, "/inputs",
                      ("python", "-m", "library", "--db", "/tmp/other.sqlite", "--root", "/inputs", "jobs"),
                      cli_state_json=snapshots)

    def test_cli_state_snapshot_is_mandatory_closed_and_canonical(self) -> None:
        builder = core.Builder("HTTP-PERSIST-LISTENER/declaration-test", ("M1-HTTP-01",))
        builder.start()
        builder.request("GET", "/health", sem.success("health"), check=False)
        builder.stop()
        builder.cli(("jobs",))
        step = next(item for item in builder.finish().steps if item.kind == "cli")
        obj = json.loads(step.cli_state_json)
        for raw in (b"", json.dumps(obj, indent=2).encode(), core.encode({"before": obj["before"]})):
            with self.subTest(raw=raw), self.assertRaises(core.CatalogError):
                replace(step, cli_state_json=raw)
        with self.assertRaises(core.CatalogError):
            replace(_tiny_case().steps[1], cli_state_json=step.cli_state_json)

    def test_case_cannot_omit_final_stop_or_repeat_server_epoch(self) -> None:
        case = _tiny_case()
        with self.assertRaises(core.CatalogError):
            replace(case, steps=case.steps[:-1])
        with self.assertRaises(core.CatalogError):
            replace(case, steps=case.steps + (replace(case.steps[0], step_id="start-again"),
                                             replace(case.steps[-1], step_id="stop-again")))

    def test_authority_labels_cannot_be_supplied_to_constructor(self) -> None:
        case = _tiny_case()
        for field in ("dispatch_authority", "acceptance_authority", "fresh_execution"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                replace(case, **{field: True})

    def test_mutable_nested_collections_are_not_retained(self) -> None:
        case = _tiny_case()
        with self.assertRaises(core.CatalogError):
            replace(case, steps=list(case.steps))
        with self.assertRaises(core.CatalogError):
            core.HttpExpectation(semantic=sem.success("health"), content_requires=[])
        with self.assertRaises(core.CatalogError):
            core.HttpRequest("GET", "/health", [("Host", "127.0.0.1:18765")], b"")


if __name__ == "__main__":
    unittest.main()
