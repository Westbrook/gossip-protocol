"""B02 v2 source-reviewed applicability and separately named schedule controls."""
from __future__ import annotations

import ast
import base64
from copy import deepcopy
import hashlib
import inspect
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import tarfile
import sys
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_intake_store_cases_v2 as cases
from gossip_harness import candidate_intake_store_driver_v2 as driver
from gossip_harness import candidate_intake_store_profile_v1 as profile
from gossip_harness import candidate_intake_store_observer_v1 as observer
from gossip_harness.library_v2_json_reference_v2 import corrected_v2_binary_files, corrected_v2_files
from gossip_harness.sandbox import DockerValidator


def empty_recipe():
    return {"fixtures": [], "phases": {phase: [] for phase in driver.PHASES}}


def adapter_functions(**values):
    """Execute only the trusted evaluator's function definitions with fake APIs.

    No candidate import or generated product source runs in this offline check.
    """
    nodes = [node for node in ast.parse(driver.CHILD_ADAPTER).body
             if isinstance(node, (ast.FunctionDef, ast.ClassDef))]
    namespace = {"json": json, "inspect": inspect, "Path": Path, "base64": base64,
                 "refs": {}, "secondary": None, "unavailable": False,
                 "applicability": {"decision": "instrumentable-control"}, "path": Path("/not-a-real-database"),
                 "LibraryError": type("FakeLibraryError", (Exception,), {}), **values}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "trusted-b02-adapter-functions", "exec"), namespace)
    return namespace


class CandidateIntakeStoreDriverV2Tests(unittest.TestCase):
    def test_every_registered_recipe_is_data_only_and_within_physical_bounds(self):
        rows = cases.definitions()
        self.assertEqual(len(rows), len({row["case_id"] for row in rows}))
        self.assertTrue(rows)
        for row in rows:
            with self.subTest(case_id=row["case_id"]):
                self.assertEqual(driver.validate_recipe(row["recipe"]), row["recipe"])
                alternatives = row.get("expected_alternatives", {"only": row["expected"]})
                for expected in alternatives.values():
                    total = sum(len(driver.encoded({"phase": phase, "value": expected["results"][phase]})) + 1
                                for phase in driver.PHASES)
                    self.assertLessEqual(total, driver.MAX_STREAM_BYTES)
                    for phase in driver.PHASES:
                        self.assertLessEqual(len(driver.encoded(expected[phase])), observer.MAX_CAPTURE_BYTES)
                        for job in expected[phase]["jobs"]:
                            for field in ("manifest", "content_hashes", "receipt"):
                                self.assertLessEqual(len(driver.encoded(job[field])), observer.MAX_FIELD_BYTES)

    def test_recipe_rejects_executable_hooks_methods_and_unsafe_fixture_ancestors(self):
        invalid = []
        recipe = empty_recipe()
        recipe["phases"]["after"] = [{"target": "store", "method": "_write", "args": []}]
        invalid.append(recipe)
        recipe = empty_recipe()
        recipe["phases"]["after"] = [{"op": "exec", "source": "print(1)"}]
        invalid.append(recipe)
        recipe = empty_recipe()
        recipe["fixtures"] = [{"path": "link", "kind": "symlink", "target": "/outside"},
                              {"path": "link/file", "kind": "file", "bytes_base64": ""}]
        invalid.append(recipe)
        recipe = empty_recipe()
        recipe["fixtures"] = [{"path": "../outside", "kind": "directory"}]
        invalid.append(recipe)
        for recipe in invalid:
            with self.subTest(recipe=recipe), self.assertRaises(driver.DriverError):
                driver.validate_recipe(recipe)

    def test_markers_require_confined_paths_known_references_and_valid_bytes(self):
        for value in ({"$path": "../escape"}, {"$path": "/absolute"}, {"$ref": "future"},
                      {"$bytes_base64": "!notbase64"}, {"nested": [{"$ref": "missing"}]}):
            recipe = empty_recipe()
            recipe["phases"]["after"] = [{"target": "store", "method": "create_job", "args": ["case", value]}]
            with self.subTest(value=value), self.assertRaises(driver.DriverError):
                driver.validate_recipe(recipe)

    def test_references_persist_before_to_after_and_clear_on_reopen(self):
        recipe = empty_recipe()
        recipe["phases"]["before"] = [{"op": "bind", "as": "input", "value": []}]
        recipe["phases"]["after"] = [{"target": "store", "method": "create_job", "args": ["case", {"$ref": "input"}]}]
        self.assertEqual(driver.validate_recipe(recipe), recipe)
        recipe["phases"]["reopened"] = deepcopy(recipe["phases"]["after"])
        with self.assertRaises(driver.DriverError):
            driver.validate_recipe(recipe)

    def test_stage_fixture_readback_preserves_links_fifo_and_exact_bytes(self):
        recipe = empty_recipe()
        recipe["fixtures"] = [
            {"path": "tree/file.txt", "kind": "file", "bytes_base64": base64.b64encode(b"data\x00").decode()},
            {"path": "alias", "kind": "symlink", "target": "tree"},
            {"path": "pipe", "kind": "fifo"}, {"path": "empty", "kind": "directory"}]
        driver.validate_recipe(recipe)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            driver._stage_inputs(root, recipe["fixtures"])
            observed = driver._verify_inputs(root, recipe["fixtures"])
            self.assertEqual({item["kind"] for item in observed}, {"file", "directory", "symlink", "fifo"})
            (root / "extra").write_bytes(b"intrusion")
            with self.assertRaises(driver.DriverError):
                driver._verify_inputs(root, recipe["fixtures"])

    def test_new_source_protocol_binds_same_bytes_differently_and_rejects_mismatch(self):
        files = {"library/__init__.py": b""}
        self.assertNotEqual(driver.source_sha256(files), driver.transport.source_sha256(files))
        with tempfile.TemporaryDirectory() as temporary, patch.object(subprocess, "Popen") as process:
            with self.assertRaises(driver.DriverError):
                driver.run_intake_store_case(files, cases.CASE_IDS[0], Path(temporary) / "new",
                    expected_source_sha256="0" * 64, expected_definition_sha256=cases.definition_sha256(), expected_review_sha256="0" * 64)
            process.assert_not_called()

    def test_definition_mismatch_unknown_case_and_existing_output_reject_before_dispatch(self):
        files = {name: text.encode() for name, text in corrected_v2_files().items()} | corrected_v2_binary_files()
        review = driver.review_for_source(files)
        with tempfile.TemporaryDirectory() as temporary, patch.object(subprocess, "Popen") as process:
            root = Path(temporary)
            for case_id, definition, output in ((cases.CASE_IDS[0], "0" * 64, root / "new"),
                    ("unknown-case", cases.definition_sha256(), root / "new"),
                    (cases.CASE_IDS[0], cases.definition_sha256(), root)):
                with self.subTest(case_id=case_id, definition=definition), self.assertRaises(driver.DriverError):
                    driver.run_intake_store_case(files, case_id, output,
                        expected_source_sha256=driver.source_sha256(files), expected_definition_sha256=definition, expected_review_sha256=review["review_sha256"])
            process.assert_not_called()
            self.assertFalse((root / "new").exists())

    def test_container_inputs_are_independently_readonly_and_candidate_is_unprivileged(self):
        sandbox = DockerValidator(driver.RUNTIME_IMAGE, {"check.py": ""})
        arguments = driver._start_arguments(sandbox, "owned", Path("/source"), Path("/checks"), Path("/inputs"), "volume")
        self.assertIn("type=bind,source=/inputs,target=/inputs,readonly,bind-propagation=rprivate", arguments)
        self.assertIn("--network=none", arguments)
        self.assertIn("--read-only", arguments)
        self.assertIn("--cap-drop=ALL", arguments)
        compile(driver.CHILD_ADAPTER, "intake_store_adapter.py", "exec")

    def test_adapter_keeps_result_history_while_mutating_original_return_alias(self):
        returned = {"state": "queued"}
        class FakeStore:
            def get_job(self, job_id):
                return returned
        recipe = empty_recipe()
        recipe["phases"]["before"] = [{"target": "store", "method": "get_job", "args": ["case"], "as": "value"}]
        recipe["phases"]["after"] = [{"op": "mutate", "ref": "value", "path": ["state"], "value": "corrupted"},
            {"target": "store", "method": "get_job", "args": ["case"]}]
        namespace = adapter_functions(store=FakeStore(), recipe=recipe)
        before = namespace["phase"]("before")
        after = namespace["phase"]("after")
        self.assertEqual(before, [{"value": {"state": "queued"}}])
        self.assertEqual(after, [{"value": None}, {"value": {"state": "corrupted"}}])
        self.assertIs(namespace["refs"]["value"], returned)

    def test_adapter_bind_preserves_original_manifest_and_import_dispatches_secondary(self):
        imports = []
        class FakeStore:
            def __init__(self, path):
                pass
        primary = object()
        namespace = adapter_functions(store=primary, Store=FakeStore,
            import_file=lambda connection, root, source: imports.append((connection, root, source)))
        self.assertIsNone(namespace["operate"]({"op": "bind", "as": "input", "value": [{"text": "original"}]}))
        self.assertEqual(namespace["refs"]["input"], [{"text": "original"}])
        namespace["operate"]({"op": "import", "source": "keep.txt", "connection": "secondary"})
        self.assertIs(imports[0][0], namespace["secondary"])
        self.assertIsNot(imports[0][0], primary)
        self.assertEqual(imports[0][1:], ("/inputs", "keep.txt"))

    def test_interference_orders_second_call_at_exact_public_store_boundary(self):
        events = []
        class FakeStore:
            def __init__(self, path):
                pass
            def start_job(self, job_id, epoch):
                events.append("start")
                return {"job_id": job_id, "epoch": epoch}
            def cancel_job(self, job_id):
                events.append("cancel")
                return {"job_id": job_id, "epoch": 2}
            def close(self):
                events.append("close")
        class FakeManager:
            def __init__(self, store):
                self.store = store
            def prepare(self, job_id):
                return self.store.start_job(job_id, 1)
        primary = FakeStore(None)
        namespace = adapter_functions(store=primary, manager=FakeManager(primary), Store=FakeStore, JobManager=FakeManager)
        result = namespace["operate"]({"op": "interfere", "boundary": "start_job", "job_id": "case", "second_actions": ["cancel_job"]})
        self.assertEqual(events, ["cancel", "start", "close"])
        self.assertEqual(result["boundary_calls"], 1)
        self.assertEqual(result["second_results"], [{"value": {"job_id": "case", "epoch": 2}}])

    def test_unsupported_instance_interposition_is_unavailable_not_product_failure(self):
        class SlottedStore:
            __slots__ = ()
            def __init__(self, path):
                pass
            def start_job(self, job_id, epoch):
                return None
            def close(self):
                pass
        namespace = adapter_functions(store=SlottedStore(None), Store=SlottedStore)
        result = namespace["wrapped"](lambda: namespace["operate"]({"op": "interfere", "boundary": "start_job",
            "job_id": "case", "second_actions": ["cancel_job"]}))
        self.assertEqual(result, {"observation_unavailable": "public_boundary_interposition"})

    def test_versioned_capture_parser_accepts_larger_file_without_changing_frozen_parser(self):
        stream = io.BytesIO()
        raw = b"x" * (9 * 1024 * 1024)
        with tarfile.open(fileobj=stream, mode="w:") as archive:
            root = tarfile.TarInfo("tmp")
            root.type = tarfile.DIRTYPE
            archive.addfile(root)
            item = tarfile.TarInfo("tmp/catalog.sqlite")
            item.size = len(raw)
            archive.addfile(item, io.BytesIO(raw))
        capsule = stream.getvalue()
        self.assertEqual(driver.parse_capture(capsule), {"catalog.sqlite": raw})
        with self.assertRaises(driver.CaptureLayoutError):
            driver.transport.parse_capture(capsule)
        with self.assertRaises(driver.DriverError):
            driver.parse_capture(capsule[:len(raw) + 1024 + 512])

    def test_versioned_session_retains_three_large_frames_within_new_bound(self):
        script = """import json,sys
for phase in ('before','after','reopened'):
    assert sys.stdin.readline().strip()==phase
    print(json.dumps({'phase':phase,'value':'x'*(2*1024*1024)}),flush=True)
assert sys.stdin.readline().strip()=='finish'
"""
        with tempfile.TemporaryDirectory() as temporary:
            commands = driver.transport._Commands(Path(temporary), 5)
            session = driver._Session(commands, [sys.executable, "-I", "-c", script])
            try:
                for phase in driver.PHASES:
                    self.assertEqual(len(session.phase(phase)), 2 * 1024 * 1024)
                self.assertTrue(session.finish(True))
            finally:
                if session.process.poll() is None:
                    session.finish(False)
            record = json.loads((Path(temporary) / "session.json").read_bytes())
            self.assertGreater(record["stdout"]["bytes"], driver.transport.MAX_STREAM_BYTES)
            self.assertFalse(record["stdout"]["truncated"])

    def test_unavailable_interposition_stops_dependent_actions_and_retains_prefix(self):
        calls = []
        class SlottedStore:
            __slots__ = ()
            def __init__(self, path):
                pass
            def start_job(self, job_id, epoch):
                calls.append("must-not-run")
            def close(self):
                pass
        recipe = empty_recipe()
        recipe["phases"]["after"] = [
            {"op": "bind", "as": "known", "value": []},
            {"op": "interfere", "boundary": "start_job", "job_id": "case", "second_actions": ["cancel_job"]},
            {"target": "store", "method": "start_job", "args": ["case", 1]}]
        namespace = adapter_functions(store=SlottedStore(None), Store=SlottedStore, recipe=recipe)
        result = namespace["phase"]("after")
        self.assertEqual(result, [{"value": None}, {"observation_unavailable": "public_boundary_interposition"},
                                 {"not_run": "dependency_unavailable"}])
        self.assertEqual(calls, [])

    def test_late_auxiliary_unavailable_preserves_prior_product_failure(self):
        product = {"checks": {"before.persisted-state": False, "after.result.0": True},
                   "all_local_assertions_passed": False, "unavailable_phases": [], "full_requirement_verdict": None}
        for error in (observer.ObservationUnavailable("unreviewed auxiliary schema"), RuntimeError("evaluator failure")):
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                with patch.object(cases, "evaluate_case", return_value=deepcopy(product)), \
                     patch.object(cases, "select_expected_case", return_value={}), \
                     patch.object(profile, "evaluate_auxiliary", side_effect=error):
                    local = combine_observations(root, "case", [None, None, None], {})
                retained = json.loads((root / "product-local-observation.json").read_bytes())
                self.assertIs(retained["checks"]["before.persisted-state"], False)
                self.assertIs(local["checks"]["before.persisted-state"], False)
                self.assertEqual(local["qualification_auxiliary_checks"], {"profile-observation": None})
                self.assertEqual(local["auxiliary_observation_error"]["exception"], type(error).__name__)
                self.assertTrue((root / "auxiliary-observation.json").is_file())
                self.assertTrue((root / "normalized-local-observation.json").is_file())

    def test_closed_review_binds_source_components_purpose_and_definition(self):
        files = {name: text.encode() for name, text in corrected_v2_files().items()} | corrected_v2_binary_files()
        review = driver.review_for_source(files)
        self.assertEqual(review["purpose"], "harness_qualification")
        self.assertEqual(review["definition_sha256"], cases.definition_sha256())
        self.assertEqual(review["source_sha256"], driver.source_sha256(files))
        self.assertEqual(set(review["source_components"]), set(driver.COMPONENT_PATHS))
        for name, digest in review["source_components"].items():
            self.assertEqual(driver.sha256(files[name]), digest)
        review["forced_schedule"] = "instrumentable-control"
        self.assertEqual(driver.review_for_source(files)["forced_schedule"], "unavailable")
        self.assertFalse(driver.review_for_source(files)["production_review_authority"])

    def test_source_substitution_and_forged_review_reject_before_dispatch(self):
        files = {name: text.encode() for name, text in corrected_v2_files().items()} | corrected_v2_binary_files()
        review = driver.review_for_source(files)
        for name in ("library/ingestion/jobs.py", "library/catalog/store.py", "README.md"):
            altered = files | {name: files[name] + b"\n# substituted\n"}
            with self.subTest(path=name), self.assertRaises(driver.DriverError):
                driver.review_for_source(altered)
        with tempfile.TemporaryDirectory() as temporary, patch.object(subprocess, "Popen") as process:
            output = Path(temporary) / "uncreated"
            with self.assertRaisesRegex(driver.DriverError, "review identity"):
                driver.run_intake_store_case(files, cases.CASE_IDS[0], output,
                    expected_source_sha256=review["source_sha256"],
                    expected_definition_sha256=cases.definition_sha256(), expected_review_sha256="0" * 64)
            self.assertFalse(output.exists())
            process.assert_not_called()

    def test_limited_control_and_mutants_cannot_escape_their_reviewed_case_scope(self):
        files = {name: text.encode() for name, text in corrected_v2_files().items()} | corrected_v2_binary_files()
        controlled = instrumentable_control(files)
        review = driver.review_for_source(controlled)
        for case_id in driver.FORCED_CASE_IDS:
            decision = driver.reviewed_applicability(controlled, case_id, expected_review_sha256=review["review_sha256"])
            self.assertEqual(decision["decision"], "instrumentable-control")
        with self.assertRaisesRegex(driver.DriverError, "outside qualified review scope"):
            driver.reviewed_applicability(controlled, "state-cancel_job-failed", expected_review_sha256=review["review_sha256"])
        mutant = mutant_source(files, "stale-epoch")
        mutant_review = driver.review_for_source(mutant)
        self.assertEqual(mutant_review["allowed_case_ids"], ["store-start_job-running-lower"])
        with self.assertRaisesRegex(driver.DriverError, "outside qualified review scope"):
            driver.reviewed_applicability(mutant, "store-start_job-running-current",
                expected_review_sha256=mutant_review["review_sha256"])

    def test_reviewed_unavailable_schedule_never_creates_secondary_store_or_calls_prepare(self):
        def forbidden(*args, **kwargs):
            self.fail("ineligible schedule attempted a candidate operation")
        namespace = adapter_functions(Store=forbidden, store=object(), manager=object(),
            applicability={"decision": "unavailable"})
        result = namespace["wrapped"](lambda: namespace["operate"]({"op": "interfere",
            "boundary": "start_job", "job_id": "case", "second_actions": ["cancel_job"]}))
        self.assertEqual(result, {"observation_unavailable": "reviewed_enclosing_write_transaction"})

    def test_candidate_operational_error_does_not_become_reviewed_unavailable(self):
        class FakeStore:
            def __init__(self, path):
                pass
            def start_job(self, job_id, epoch):
                return {"job_id": job_id, "epoch": epoch}
            def cancel_job(self, job_id):
                raise RuntimeError("ordinary candidate failure")
            def close(self):
                pass
        primary = FakeStore(None)
        class FakeManager:
            def prepare(self, job_id):
                return primary.start_job(job_id, 1)
        namespace = adapter_functions(Store=FakeStore, store=primary, manager=FakeManager())
        result = namespace["wrapped"](lambda: namespace["operate"]({"op": "interfere",
            "boundary": "start_job", "job_id": "case", "second_actions": ["cancel_job"]}))
        self.assertEqual(result["value"]["second_results"], [{"unexpected_exception": "RuntimeError"}])
        self.assertNotIn("observation_unavailable", result)

    def test_known_ordinary_failure_survives_reviewed_unavailable_schedule(self):
        case_id = driver.FORCED_CASE_IDS[0]
        case = cases.case_definition(case_id)
        snapshots = []
        for phase in driver.PHASES:
            state = deepcopy(case["expected"]["before"])
            strings = {row["public"]["job_id"]: {"manifest": driver.encoded(row["manifest"]).decode(),
                "content_hashes": driver.encoded(row["content_hashes"]).decode(),
                "receipt": None if row["receipt"] is None else driver.encoded(row["receipt"]).decode()}
                for row in state["jobs"]}
            snapshots.append(observer.Observation(state, strings, (), "a" * 64, "b" * 64, {}, "synthetic"))
        responses = deepcopy(case["expected"]["results"])
        responses["before"][0] = {"error": "independent-broken-import"}
        responses["after"] = [{"observation_unavailable": "reviewed_enclosing_write_transaction"}]
        local = cases.evaluate_case(case_id, *snapshots, responses)
        self.assertIs(local["checks"]["before.result.0"], False)
        self.assertIsNone(local["checks"]["after.persisted-state"])
        self.assertFalse(local["all_local_assertions_passed"])
        self.assertIsNone(local["full_requirement_verdict"])

    def test_source_binding_includes_transport_definitions_profile_and_mapper(self):
        sources = driver.driver_sources()
        self.assertTrue({"candidate_storage_driver_v1.py", "candidate_storage_observer_v1.py", "sandbox.py",
            "candidate_intake_store_cases_v1.py", "candidate_intake_store_cases_v2.py", "candidate_intake_store_driver_v2.py", "candidate_intake_fixtures_v1.py",
            "candidate_intake_store_profile_v1.py"} <= set(sources))
        for name, digest in sources.items():
            self.assertEqual(digest, hashlib.sha256(Path(driver.__file__).with_name(name).read_bytes()).hexdigest())


def mutant_source(files, defect):
    result = dict(files)
    if defect == "stale-epoch":
        addition = '''
class Store(Store):
    def start_job(self, job_id, epoch):
        return super().start_job(job_id, self.get_job(job_id)['epoch'])
'''
        result["library/catalog/store.py"] += addition.encode()
    elif defect == "cancel-failed":
        addition = '''
class Store(Store):
    def cancel_job(self, job_id):
        if self.get_job(job_id)['state'] == 'failed':
            self.retry_job(job_id)
        return super().cancel_job(job_id)
'''
        result["library/catalog/store.py"] += addition.encode()
    elif defect == "interference-stale-success":
        addition = '''
class JobManager(JobManager):
    def prepare(self, job_id):
        try:
            return super().prepare(job_id)
        except LibraryError as error:
            if error.code == 'stale_epoch':
                return {'job_id':job_id,'epoch':1}
            raise
'''
        result["library/ingestion/jobs.py"] += addition.encode()
    elif defect == "archive-rejection-admits":
        addition = '''
class JobManager(JobManager):
    def submit_zip(self, job_id, archive_path, namespace):
        try:
            return super().submit_zip(job_id, archive_path, namespace)
        except LibraryError as error:
            if error.code == 'invalid_archive':
                return self.submit(job_id, [])
            raise
'''
        result["library/ingestion/jobs.py"] += addition.encode()
    elif defect == "symlink-rejection-admits":
        addition = '''
class JobManager(JobManager):
    def submit_directory(self, job_id, root, namespace):
        try:
            return super().submit_directory(job_id, root, namespace)
        except LibraryError as error:
            if error.code == 'invalid_source':
                return self.submit(job_id, [])
            raise
'''
        result["library/ingestion/jobs.py"] += addition.encode()
    else:
        raise ValueError("Unknown prospective defect")
    return result



INSTRUMENTABLE_PREPARE = """
# Evaluator-authored schedule control only. This is NOT the repaired v2 product.
# It deliberately uses a public-API sequence that exposes the tested schedule.
class JobManager(JobManager):
    def prepare(self, job_id):
        check_job_id(job_id)
        job = self.store.get_job(job_id)
        if job['state'] == 'running':
            return {'job_id':job_id,'epoch':job['epoch']}
        if job['state'] != 'queued':
            raise LibraryError('job_state')
        try:
            validate_entries(self.store.job_manifest(job_id),self.store.documents())
        except LibraryError as error:
            self.store.fail_job(job_id,job['epoch'],error.code)
            raise
        return self.store.start_job(job_id,job['epoch'])
"""


def instrumentable_control(files):
    result = dict(files)
    result["library/ingestion/jobs.py"] += INSTRUMENTABLE_PREPARE.encode()
    return result

class ExecutionInfrastructureError(RuntimeError):
    pass


def qualification_setup(cls):
    cls.files = {name: text.encode() for name, text in corrected_v2_files().items()} | corrected_v2_binary_files()
    cls.schema_fixture, cls.schema_profile = profile.authored_schema_profile()
    ready, detail = DockerValidator(driver.RUNTIME_IMAGE, {"unused.py": ""}).preflight()
    if not ready:
        raise RuntimeError(detail)


def combine_observations(root, case_id, observations, responses):
    local = cases.evaluate_case(case_id, *observations, responses)
    # Persist independently established product assertions before a later mapper
    # can fail. Unavailable auxiliary evidence cannot erase known failures.
    (root / "product-local-observation.json").write_bytes(driver.encoded(local))
    try:
        auxiliary = profile.evaluate_auxiliary(cases.select_expected_case(case_id, responses), *observations,
            unavailable_phases=tuple(local["unavailable_phases"]))
        auxiliary_record = {"checks": auxiliary, "error": None}
    except Exception as error:
        auxiliary = {"profile-observation": None}
        diagnostic = {"kind": "observation-unavailable" if isinstance(error, observer.ObservationUnavailable)
            else "evaluator-error", "exception": type(error).__name__, "detail": str(error)[:500]}
        auxiliary_record = {"checks": auxiliary, "error": diagnostic}
        local["auxiliary_observation_error"] = diagnostic
    (root / "auxiliary-observation.json").write_bytes(driver.encoded(auxiliary_record))
    local["qualification_auxiliary_checks"] = auxiliary
    (root / "normalized-local-observation.json").write_bytes(driver.encoded(local))
    return local


def qualify(test, root, case_id, *, defect=None, control=False):
    root.mkdir(mode=0o700)
    base = instrumentable_control(test.files) if control else test.files
    files = base if defect is None else mutant_source(base, defect)
    review = driver.review_for_source(files)
    source_sha = driver.source_sha256(files)
    (root / "prospective-schema.sqlite").write_bytes(test.schema_fixture)
    prospective = test.schema_profile | {"source_sha256": source_sha,
        "qualification_only": True, "production_review_authority": False}
    (root / "prospective-schema-profile.json").write_bytes(driver.encoded(prospective))
    registration = observer.Registration(source_sha, observer.V2_SQLITE_LAYOUT, observer.V2_STORAGE_PATHS,
        driver.sha256(driver.encoded(prospective)), prospective["schema_sha256"])
    (root / "prospective-registration.json").write_bytes(driver.encoded(vars(registration)))
    result = driver.run_intake_store_case(files, case_id, root / "execution", expected_source_sha256=source_sha,
        expected_definition_sha256=cases.definition_sha256(), expected_review_sha256=review["review_sha256"])
    if not result.completed:
        raise ExecutionInfrastructureError(str(result.infrastructure))
    if result.layout_errors:
        raise observer.ObservationUnavailable(str(result.layout_errors))
    observations = [observer.observe_capture(result.snapshots[phase], registration, source_sha) for phase in driver.PHASES]
    local = combine_observations(root, case_id, observations, result.responses)
    if case_id in driver.FORCED_CASE_IDS and not control:
        test.assertEqual(result.responses["after"], [{"observation_unavailable": "reviewed_enclosing_write_transaction"}])
        test.assertEqual(local["unavailable_phases"], ["after", "reopened"])
        test.assertEqual(result.snapshots["before"], result.snapshots["after"])
        test.assertEqual(observations[0].data, observations[1].data)
        test.assertEqual(observations[0].data, observations[2].data)
        test.assertEqual(observations[0].auxiliary_tables, observations[1].auxiliary_tables)
        test.assertEqual(observations[0].auxiliary_tables, observations[2].auxiliary_tables)
    terminal = json.loads((Path(result.output_root) / "terminal.json").read_bytes())
    test.assertEqual(terminal["runtime"]["image"]["Id"], driver.RUNTIME_IMAGE)
    for label in ("container-remove", "container-after", "volume-remove", "volume-after"):
        record = json.loads((Path(result.output_root) / (label + ".json")).read_bytes())
        test.assertTrue(driver.transport._clean(record), label)
        for stream in ("stdout", "stderr"):
            raw = (Path(result.output_root) / record[stream]["path"]).read_bytes()
            test.assertEqual(driver.sha256(raw), record[stream]["sha256"])
    test.assertEqual((Path(result.output_root) / "container-after-stdout.bin").read_bytes().strip(), b"")
    test.assertEqual((Path(result.output_root) / "volume-after-stdout.bin").read_bytes().strip(), b"")
    local["applicability"] = terminal["applicability"]
    test.assertEqual(terminal["applicability"]["review_sha256"], review["review_sha256"])
    (root / "qualification-outcome.json").write_bytes(driver.encoded(local))
    return local, result.responses


def assert_mutant_transport(test, responses):
    def visit(value):
        if type(value) is dict:
            test.assertFalse(set(value) & {"unexpected_exception", "observation_unavailable", "not_run"}, value)
            for item in value.values():
                visit(item)
        elif type(value) is list:
            for item in value:
                visit(item)
    visit(responses)


def run_roster(test, *, intake):
    roster = [row["case_id"] for row in cases.definitions() if row["case_id"].startswith("intake-") == intake]
    test.assertTrue(roster)
    with ArtifactDirectory("candidate-b02-v2-intake-matrix" if intake else "candidate-b02-v2-store-matrix", retain_success=True) as artifacts:
        (artifacts.root / "prospective-roster.json").write_bytes(driver.encoded({"case_ids": roster,
            "definition_sha256": cases.definition_sha256(), "source_sha256": driver.source_sha256(test.files),
            "initial_outcomes": [{"case_id": case_id, "status": "not-run"} for case_id in roster]}))
        outcomes = []
        try:
            for ordinal, case_id in enumerate(roster):
                with test.subTest(case_id=case_id):
                    outcome = {"case_id": case_id, "ordinal": ordinal, "status": "not-run"}
                    try:
                        local, _ = qualify(test, artifacts.root / case_id, case_id)
                        checks = dict(local["checks"]) | {"auxiliary." + key: value
                            for key, value in local["qualification_auxiliary_checks"].items()}
                        outcome["known_failed_assertions"] = [key for key, value in checks.items() if value is False]
                        outcome["unavailable_assertions"] = [key for key, value in checks.items() if value is None]
                        failed, unavailable = bool(outcome["known_failed_assertions"]), bool(outcome["unavailable_assertions"])
                        outcome["status"] = ("failed-and-unavailable" if failed and unavailable else
                            "product-failed" if failed else "observation-unavailable" if unavailable else "observed")
                        if "auxiliary_observation_error" in local:
                            outcome["auxiliary_observation_error"] = local["auxiliary_observation_error"]
                        outcome["all_local_assertions_passed"] = local["all_local_assertions_passed"]
                        outcome["all_auxiliary_checks_passed"] = all(local["qualification_auxiliary_checks"].values())
                        outcome["acceptance_credit"] = False
                        if case_id in driver.FORCED_CASE_IDS:
                            test.assertEqual(local["applicability"]["decision"], "unavailable")
                            test.assertEqual(outcome["status"], "observation-unavailable", outcome)
                            test.assertTrue(outcome["unavailable_assertions"])
                            test.assertFalse(outcome["known_failed_assertions"])
                            test.assertTrue(all(value is True for key, value in checks.items()
                                if key.startswith("before.") or key.startswith("auxiliary.before.")))
                            outcome["qualification"] = "declared-availability-observed"
                        else:
                            test.assertTrue(local["all_local_assertions_passed"], local)
                            test.assertTrue(outcome["all_auxiliary_checks_passed"], local["qualification_auxiliary_checks"])
                            outcome["qualification"] = "ordinary-observations-verified"
                        test.assertIsNone(local["full_requirement_verdict"])
                    except ExecutionInfrastructureError as error:
                        outcome.update({"status": "infrastructure-failure", "detail": str(error)[:500]})
                        raise
                    except observer.ObservationUnavailable as error:
                        outcome.update({"status": "observation-unavailable", "detail": str(error)[:500]})
                        raise
                    except BaseException as error:
                        if outcome["status"] == "not-run":
                            outcome["status"] = "qualification-failure"
                        outcome["detail"] = type(error).__name__ + ":" + str(error)[:500]
                        raise
                    finally:
                        outcomes.append(outcome)
                        (artifacts.root / (f"case-{ordinal:04d}.json")).write_bytes(driver.encoded(outcome))
        finally:
            (artifacts.root / "completion-census.json").write_bytes(driver.encoded({"planned": roster,
                "outcomes": outcomes, "not_run": [case_id for case_id in roster if case_id not in {row["case_id"] for row in outcomes}]}))
        test.assertEqual(len(outcomes), len(roster))


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateIntakeDriverV2DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        qualification_setup(cls)

    def test_complete_registered_intake_roster(self):
        run_roster(self, intake=True)

    def test_archive_acceptance_defect_is_distinguished(self):
        rows = [row for row in cases.definitions() if row["case_id"].startswith("intake-")
                and any(op.get("method") == "submit_zip" for op in row["recipe"]["phases"]["after"])
                and {"error": "invalid_archive"} in row["expected"]["results"]["after"]]
        self.assertTrue(rows)
        with ArtifactDirectory("candidate-b02-v2-archive-mutant", retain_success=True) as artifacts:
            local, responses = qualify(self, artifacts.root / rows[0]["case_id"], rows[0]["case_id"], defect="archive-rejection-admits")
            self.assertEqual(responses["after"][0]["value"]["state"], "queued")
            assert_mutant_transport(self, responses)
            self.assertFalse(local["all_local_assertions_passed"])

    def test_symlink_acceptance_defect_is_distinguished(self):
        rows = [row for row in cases.definitions() if row["case_id"].startswith("intake-")
                and any(op.get("method") == "submit_directory" for op in row["recipe"]["phases"]["after"])
                and any(item["kind"] == "symlink" for item in row["recipe"]["fixtures"])
                and {"error": "invalid_source"} in row["expected"]["results"]["after"]]
        self.assertTrue(rows)
        with ArtifactDirectory("candidate-b02-v2-symlink-mutant", retain_success=True) as artifacts:
            local, responses = qualify(self, artifacts.root / rows[0]["case_id"], rows[0]["case_id"], defect="symlink-rejection-admits")
            self.assertEqual(responses["after"][0]["value"]["state"], "queued")
            assert_mutant_transport(self, responses)
            self.assertFalse(local["all_local_assertions_passed"])


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateDirectStoreDriverV2DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        qualification_setup(cls)

    def test_complete_registered_store_roster(self):
        run_roster(self, intake=False)

    def test_stale_epoch_defect_is_distinguished(self):
        with ArtifactDirectory("candidate-b02-v2-stale-mutant", retain_success=True) as artifacts:
            local, responses = qualify(self, artifacts.root / "stale", "store-start_job-running-lower", defect="stale-epoch")
            self.assertEqual(responses["after"][0], {"value": {"job_id": "case", "epoch": 2}})
            assert_mutant_transport(self, responses)
            self.assertFalse(local["all_local_assertions_passed"])

    def test_illegal_cancel_defect_is_distinguished(self):
        with ArtifactDirectory("candidate-b02-v2-cancel-mutant", retain_success=True) as artifacts:
            local, responses = qualify(self, artifacts.root / "cancel", "state-cancel_job-failed", defect="cancel-failed")
            self.assertEqual(responses["after"][0]["value"]["state"], "cancelled")
            self.assertEqual(responses["after"][0]["value"]["epoch"], 4)
            assert_mutant_transport(self, responses)
            self.assertFalse(local["all_local_assertions_passed"])



@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateScheduleControlV2DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        qualification_setup(cls)

    def test_all_five_instrumentable_control_schedules(self):
        roster = list(driver.FORCED_CASE_IDS)
        with ArtifactDirectory("candidate-b02-v2-instrumentable-control", retain_success=True) as artifacts:
            (artifacts.root / "prospective-roster.json").write_bytes(driver.encoded({
                "case_ids": roster, "source_sha256": driver.source_sha256(instrumentable_control(self.files)),
                "purpose": "instrumentable-schedule-control-only", "acceptance_credit": False,
                "initial_outcomes": [{"case_id": case_id, "status": "not-run"} for case_id in roster]}))
            outcomes = []
            try:
                for ordinal, case_id in enumerate(roster):
                    with self.subTest(case_id=case_id):
                        outcome = {"case_id": case_id, "status": "not-run", "acceptance_credit": False}
                        try:
                            local, responses = qualify(self, artifacts.root / case_id, case_id, control=True)
                            assert_mutant_transport(self, responses)
                            self.assertEqual(local["applicability"]["decision"], "instrumentable-control")
                            self.assertTrue(local["all_local_assertions_passed"], local)
                            self.assertTrue(all(local["qualification_auxiliary_checks"].values()))
                            outcome["status"] = "control-observed"
                        except BaseException as error:
                            outcome.update({"status": "control-qualification-failure", "detail": str(error)[:500]})
                            raise
                        finally:
                            outcomes.append(outcome)
                            (artifacts.root / (f"case-{ordinal:04d}.json")).write_bytes(driver.encoded(outcome))
            finally:
                (artifacts.root / "completion-census.json").write_bytes(driver.encoded({"planned": roster,
                    "outcomes": outcomes, "not_run": [case_id for case_id in roster
                    if case_id not in {row["case_id"] for row in outcomes}]}))

    def test_instrumentable_false_success_mutant_is_distinguished(self):
        with ArtifactDirectory("candidate-b02-v2-interference-mutant", retain_success=True) as artifacts:
            local, responses = qualify(self, artifacts.root / "interference", "interfere-start_job-cancel-retry",
                defect="interference-stale-success", control=True)
            self.assertEqual(local["applicability"]["decision"], "instrumentable-control")
            self.assertEqual(responses["after"][0]["value"]["prepare"], {"value": {"job_id": "case", "epoch": 1}})
            assert_mutant_transport(self, responses)
            self.assertFalse(local["all_local_assertions_passed"])
