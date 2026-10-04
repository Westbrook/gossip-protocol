"""Prospective workflow qualification, never independent product acceptance.

The root verifier owns all execution.  Every product history below runs once in
the real Engine owner; observations are reconstructed from retained originals.
The separately anchored review deliveries are explicitly simulated control
scaffolding over fixed, independently source-reviewed test fixtures.  They test
the delivery/chronology mechanism and grant no real semantic review authority.
No candidate source is imported or evaluated in the host Python process.
"""
from __future__ import annotations

import ast
from contextlib import ExitStack
from dataclasses import asdict, replace
import hashlib
import io
import json
import os
from pathlib import Path
import tarfile
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import candidate_storage_review_authority_v1 as storage_review
from gossip_harness import candidate_workflow_execution_v1 as execution
from gossip_harness import candidate_workflow_observation_v1 as observation
from gossip_harness import candidate_workflow_profile_v1 as profiles
from gossip_harness import candidate_workflow_review_v1 as review
from gossip_harness import cumulative_workflow_exposure_v1 as exposure
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
from gossip_harness.library_v2_json_reference_v2 import (
    corrected_v2_binary_files,
    corrected_v2_files,
    corrected_v2_source_inputs,
)
from tests import candidate_storage_m2_physical_v1 as storage_fixture
from tests.test_candidate_clients_docker_v4 import make_store

PROTOCOL = "candidate-workflow-physical-qualification-v1"
PURPOSE = "public_release"
COHORT = ("workflow-qualification", *(f"workflow-reserved-{i}" for i in range(1, 6)))
WORKFLOW = "library/clients/workflow.py"
STORE = "library/catalog/store.py"
HISTORIES = (
    "inherited-text-persistence", "identity-and-replay", "errors-preserve-catalog",
    "m1-cancel-fences-old-token", "m1-atomic-conflict-after-prepare",
    "m1-per-source-provenance", "m1-invalid-member-rolls-back",
    "m1-injected-transaction-failure", "WF09-empty-modes",
    "WF10-defaults-and-export-selection", "WF11-v0-unsupported-m1-continuation",
    "WF12-mixed-job-state-order-and-reopen", "WF13-domain-errors-continue",
    "WF14-exact64-operations", "WF15-existing-job-token-domain",
    "WF16-absent-token-and-hook-distinction", "WF17-normalized-answer-growth",
    "WF18-same-process-call-isolation", "WF19-provisional-fault-boundary",
    "WF20-invalid-query-options-continue",
)
OPERATION_COUNTS = ((5,), (6,), (6,), (9,), (6,), (5,), (4,), (8,), (0, 0),
                    (9,), (9,), (23,), (7,), (64,), (10,), (16,), (2,), (4, 6, 0), (8,), (5,))
MECHANISMS = (
    "WQ-NORMALIZED-61824", "WQ-FRAME-EXACT", "WQ-FRAME-OVER",
    "WQ-STDOUT-EXACT", "WQ-STDOUT-OVER", "WQ-STDERR-EXACT", "WQ-STDERR-OVER",
)
FIXTURE_LIMIT = (
    "Simulated fixture reviewer, control scaffolding only. No real independent "
    "semantic authority, scope approval, independent acceptance, repeatability "
    "or scientific sample is established by this qualification."
)


def encoded(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False,
                      sort_keys=True, separators=(",", ":")).encode("ascii")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def write_raw(path, raw):
    """Exclusive retained artifacts; failures are never erased for a rerun."""
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def write_json(path, value):
    raw = encoded(value)
    write_raw(path, raw)
    return raw


def typed_equal(actual, expected):
    if type(actual) is not type(expected):
        return False
    if type(expected) is dict:
        return set(actual) == set(expected) and all(typed_equal(actual[k], expected[k]) for k in expected)
    if type(expected) is list:
        return len(actual) == len(expected) and all(typed_equal(a, b) for a, b in zip(actual, expected, strict=True))
    return actual == expected


def replace_one(files, path, before, after):
    original = files[path]
    old, new = before.encode(), after.encode()
    if original.count(old) != 1:
        raise ValueError("Exact authored mutation seam changed: " + path)
    files[path] = original.replace(old, new, 1)
    ast.parse(files[path], filename=path)
    return {"path": path, "operation": "replace_exactly_one_literal",
            "before_literal": before, "after_literal": after,
            "before_sha256": sha(original), "after_sha256": sha(files[path])}


def append_source(files, path, addition):
    original = files[path]
    files[path] += addition.encode()
    ast.parse(files[path], filename=path)
    return {"path": path, "operation": "append_exact_bytes", "addition": addition,
            "before_sha256": sha(original), "after_sha256": sha(files[path])}


def baseline_files():
    """Trusted generators emit inert bytes; only Engine imports these files."""
    inputs = corrected_v2_source_inputs()
    text, binary = corrected_v2_files(), corrected_v2_binary_files()
    if set(text) & set(binary):
        raise ValueError("Fixture source path collision")
    files = {name: value.encode() for name, value in text.items()} | binary
    original_manifest = admission.source_manifest(files)
    originals = {name: files[name].decode() for name in (WORKFLOW, "library/ingestion/jobs.py")}
    # Approved prospective test fixture only. The frozen reference generators
    # prevalidate a malformed hook before a missing-job lookup. The workflow
    # addendum explicitly preserves not_found for a valid epoch and absent job.
    # Keep actual token validation, lookup and errors in the real JobManager.
    changes = [replace_one(files, WORKFLOW,
        "                        if type(fail) is not bool:\n"
        "                            raise LibraryError('invalid_request')\n", "")]
    changes.append(replace_one(files, "library/ingestion/jobs.py",
        "        if (type(token) is not dict or set(token) != {'job_id', 'epoch'}\n"
        "                or type(fail_before_commit) is not bool):",
        "        if type(token) is not dict or set(token) != {'job_id', 'epoch'}:"))
    changes.append(replace_one(files, "library/ingestion/jobs.py",
        "        counter(token['epoch'],1)\n        return self.store.commit_job(token['job_id'], token['epoch'],",
        "        counter(token['epoch'],1)\n"
        "        if type(fail_before_commit) is not bool:\n"
        "            self.get(token['job_id'])\n"
        "            raise LibraryError('invalid_request')\n"
        "        return self.store.commit_job(token['job_id'], token['epoch'],"))
    files[exposure.RELEASE_PATH] = exposure.text().encode()
    files[exposure.cli.RELEASE_PATH] = exposure.cli.text().encode()
    adaptation = {"protocol": "workflow-fixture-v2-hook-precedence-v1",
        "scope": "prospective authored qualification fixture only; frozen generators and expected values unchanged",
        "contract": {"path": exposure.TEXT_PATH, "sha256": exposure.TEXT_SHA256,
                     "rule": "valid-domain absent token retains historical not_found despite malformed optional hook"},
        "original_generator_source_manifest": original_manifest,
        "original_changed_files": originals, "changes": changes,
        "added_contract_files": {name: sha(files[name]) for name in (exposure.RELEASE_PATH, exposure.cli.RELEASE_PATH)}}
    return files, inputs, adaptation


RUNTIME_DEFECTS = (
    ("reused-root", "WF18-same-process-call-isolation"),
    ("fresh-reopen", "inherited-text-persistence"),
    ("unbounded-epoch", "WF15-existing-job-token-domain"),
    ("lookup-before-token", "WF16-absent-token-and-hook-distinction"),
    ("old-epoch-comparator", "m1-cancel-fences-old-token"),
    ("empty-export-is-all", "WF10-defaults-and-export-selection"),
    ("v0-job-dispatch", "WF11-v0-unsupported-m1-continuation"),
    ("reverse-final-jobs", "WF12-mixed-job-state-order-and-reopen"),
    ("stop-after-error", "WF13-domain-errors-continue"),
    ("after-commit-fault", "WF19-provisional-fault-boundary"),
    ("rollback-orphan", "WF19-provisional-fault-boundary"),
)
SOURCE_DEFECTS = (
    ("duplicate-solver", "immutable_solve_entry_and_real_workflow_delegation"),
    ("early-hook", "actual_provisional_write_fault_hook_and_rollback_structure"),
    ("fake-injected-error", "actual_provisional_write_fault_hook_and_rollback_structure"),
)


def candidate_files(variant="reference"):
    files, inputs, adaptation = baseline_files()
    original = dict(files)
    changes = []
    if variant == "reference":
        pass
    elif variant == "reused-root":
        changes.append(replace_one(files, WORKFLOW, "import tempfile\n",
            "import tempfile\nfrom contextlib import nullcontext\n"
            "_shared_directory = tempfile.mkdtemp(prefix='workflow-reused-control-')\n"))
        changes.append(replace_one(files, WORKFLOW,
            "with tempfile.TemporaryDirectory(prefix='research-library-') as directory:",
            "with nullcontext(_shared_directory) as directory:"))
        changes.append(replace_one(files, WORKFLOW, "root.mkdir()", "root.mkdir(exist_ok=True)"))
    elif variant == "fresh-reopen":
        changes.append(replace_one(files, WORKFLOW,
            "elif op == 'reopen':\n                        store.close()\n                        store = Store(path)",
            "elif op == 'reopen':\n                        store.close()\n"
            "                        path.unlink()\n                        store = Store(path)"))
    elif variant == "unbounded-epoch":
        changes.append(replace_one(files, "library/counters.py",
            "if type(value) is not int or not minimum <= value <= COUNTER_MAX:",
            "if type(value) is not int or value < minimum:"))
    elif variant == "lookup-before-token":
        changes.append(replace_one(files, "library/ingestion/jobs.py",
            "check_job_id(token['job_id'])\n        counter(token['epoch'],1)",
            "check_job_id(token['job_id'])\n        self.store.get_job(token['job_id'])\n"
            "        counter(token['epoch'],1)"))
    elif variant == "old-epoch-comparator":
        changes.append(replace_one(files, "library/catalog/legacy_m1.py",
            "if row['epoch'] != epoch:", "if row['epoch'] < epoch:"))
    elif variant == "empty-export-is-all":
        changes.append(replace_one(files, WORKFLOW,
            "if 'sources' in operation else None", "if operation.get('sources') else None"))
    elif variant == "v0-job-dispatch":
        for suffix in ("op == 'submit'", "op in ('prepare', 'cancel', 'retry', 'job')", "op == 'commit'"):
            changes.append(replace_one(files, WORKFLOW,
                "payload.get('milestone') == 'm1' and " + suffix, suffix))
    elif variant == "reverse-final-jobs":
        changes.append(replace_one(files, WORKFLOW,
            "value['jobs'] = store.list_jobs()", "value['jobs'] = list(reversed(store.list_jobs()))"))
    elif variant == "stop-after-error":
        changes.append(replace_one(files, WORKFLOW,
            "result = {'error': error.code}\n                results.append(result)",
            "result = {'error': error.code}\n                    results.append(result)\n"
            "                    break\n                results.append(result)"))
    elif variant == "after-commit-fault":
        changes.append(replace_one(files, "library/catalog/m4_legacy.py",
            "if fail_before_commit:\n                raise LibraryError('injected_failure')",
            "if fail_before_commit:\n                self.db.commit()\n"
            "                raise LibraryError('injected_failure')"))
    elif variant == "rollback-orphan":
        changes.append(append_source(files, STORE, '''
class Store(Store):
    def commit_job(self, job_id, epoch, *, fail_before_commit=False):
        from library.common import LibraryError
        import hashlib
        try:
            return super().commit_job(job_id, epoch, fail_before_commit=fail_before_commit)
        except LibraryError as error:
            if error.code == 'injected_failure':
                raw = b'workflow-orphan-after-rollback'
                self.db.execute('INSERT INTO blobs VALUES (?,?)',
                    ('blob-' + hashlib.sha256(raw).hexdigest(), raw))
            raise
'''))
    elif variant == "duplicate-solver":
        # A table of authored fixture answers is a deliberate delegation defect.
        # It never enters a product profile or the ordinary candidate request.
        answers = {encoded(call['input']).decode(): call['expected']
            for definition in profiles.definitions() for call in definition['calls']}
        duplicate = ("import json\n_ANSWERS = json.loads(" + repr(encoded(answers).decode()) + ")\n"
            "def solve(payload):\n"
            "    key = json.dumps(payload, ensure_ascii=True, allow_nan=False, "
            "sort_keys=True, separators=(',', ':'))\n"
            "    return json.loads(json.dumps(_ANSWERS[key]))\n")
        before = files[WORKFLOW]
        files[WORKFLOW] = duplicate.encode()
        ast.parse(files[WORKFLOW], filename=WORKFLOW)
        changes.append({"path": WORKFLOW, "operation": "replace_with_declared_lookup_mutant",
                        "before_sha256": sha(before), "after_sha256": sha(files[WORKFLOW])})
    elif variant in ("early-hook", "fake-injected-error"):
        if variant == "early-hook":
            changes.append(replace_one(files, "library/catalog/m4_legacy.py",
                "            try:\n                entries = validate_entries(json.loads(row['manifest']),self.all_documents())",
                "            if fail_before_commit:\n"
                "                raise LibraryError('injected_failure')\n"
                "            try:\n                entries = validate_entries(json.loads(row['manifest']),self.all_documents())"))
        else:
            changes.append(replace_one(files, WORKFLOW,
                "result = jobs.commit({'job_id': operation['job_id'], 'epoch': operation['epoch']},",
                "if fail:\n                            raise LibraryError('injected_failure')\n"
                "                        result = jobs.commit({'job_id': operation['job_id'], 'epoch': operation['epoch']},"))
    else:
        raise ValueError("Closed authored workflow fixture variant required")
    return files, {"protocol": PROTOCOL, "variant": variant, "generator_inputs": inputs,
        "baseline_adaptation": adaptation,
        "original_source_manifest": admission.source_manifest(original),
        "source_manifest": admission.source_manifest(files), "mutations": changes,
        "authored_candidate_fixture": True, "review_delivery": "simulated_fixture_only",
        "semantic_scope_approval": False, "scientific_samples": 0, "whole_project_acceptance": False}


def fixture_delivery(stack, root, request, *, rejected=(), references):
    """Transport a fixed fixture judgment through real protected originals.

    A host-inspection caller must obtain ``request`` from begin_review FIRST.
    Source-plan mechanism requests are separately supplied by plan.request().
    This helper deliberately claims no independent human/agent semantic review.
    """
    root.mkdir()
    raw, delta = root / "raw", root / "delta"
    head = ExternalHead.create(root / "head", journal_roots=(raw, delta))
    stack.callback(head.close)
    journal = chain.CheckpointChain.create(raw, delta, context={
        "protocol": PROTOCOL, "purpose": "fixture-only protected review transport",
        "semantic_authority": False}, authority=head)
    stack.callback(journal.close)
    duties = request["duties"]
    if type(duties) is not list or not duties or not all(type(x) is str for x in duties):
        raise ValueError("Closed original review duty list required")
    if not set(rejected) <= set(duties):
        raise ValueError("A declared fixture defect must identify an actual source duty")
    request_raw = encoded(request)
    journal.retain("request.json", request_raw)
    reviewer_id = "fixture-workflow-source-review"
    report = {"protocol": request["protocol"], "purpose": request["purpose"],
        "reviewer_id": reviewer_id, "request_sha256": sha(request_raw),
        "decisions": [{"id": duty, "decision": "rejected" if duty in rejected else "approved",
            "rationale": FIXTURE_LIMIT + " Fixed declared fixture duty: " + duty,
            "source_references": list(references)} for duty in duties],
        "remaining_obligations": [FIXTURE_LIMIT,
            "All actual candidate source reviews and full cumulative product acceptance remain required."]}
    report_raw = encoded(report)
    journal.retain("report.json", report_raw)
    delivery_raw = encoded({"protocol": request["protocol"], "purpose": request["purpose"],
        "reviewer_id": reviewer_id, "request_sha256": sha(request_raw),
        "report_sha256": sha(report_raw), "origin": "independently_delivered_host_review"})
    # The fixed origin is protocol vocabulary, not a factual claim of independent
    # semantic review: the surrounding retained control declaration states scope.
    journal.retain("delivery.json", delivery_raw)
    write_json(root / "fixture-disclosure.json", {"protocol": PROTOCOL,
        "simulated_reviewer_decisions": True, "semantic_authority": False,
        "scope": FIXTURE_LIMIT, "request_sha256": sha(request_raw),
        "report_sha256": sha(report_raw), "delivery_sha256": sha(delivery_raw),
        "declared_rejected_duties": list(rejected), "prefix": asdict(journal.commitment)})
    enrollment = storage_review.ReviewEnrollment(reviewer_id,
        "request.json", sha(request_raw), "report.json", sha(report_raw),
        "delivery.json", sha(delivery_raw))
    return journal, enrollment


def source_references(files, variant):
    paths = [WORKFLOW, STORE]
    if variant in ("after-commit-fault", "early-hook"):
        paths.append("library/catalog/m4_legacy.py")
    return tuple(name + ":sha256:" + sha(files[name]) for name in paths)


def line_containing(raw, literal):
    lines = raw.decode().splitlines()
    matches = [index + 1 for index, line in enumerate(lines) if literal in line]
    if len(matches) != 1:
        raise ValueError("Source-qualified fixture line must be unique: " + literal)
    return matches[0]


class CandidateWorkflowPhysicalDefinitionTests(unittest.TestCase):
    """Cheap inert-source prerequisites; never counted as physical evidence."""

    def test_every_fixed_mutant_has_exact_single_file_provenance_and_valid_syntax(self):
        baseline, _ = candidate_files()
        variants = tuple(row[0] for row in RUNTIME_DEFECTS + SOURCE_DEFECTS)
        self.assertEqual(len(variants), len(set(variants)))
        for variant in variants:
            with self.subTest(variant=variant):
                files, provenance = candidate_files(variant)
                self.assertEqual(set(files), set(baseline))
                changed = {name for name in files if files[name] != baseline[name]}
                self.assertEqual(len(changed), 1)
                self.assertEqual(changed, {row["path"] for row in provenance["mutations"]})
                for name in changed:
                    ast.parse(files[name], filename=name)
                self.assertIs(provenance["semantic_scope_approval"], False)
                self.assertIs(provenance["whole_project_acceptance"], False)
                self.assertEqual(provenance["scientific_samples"], 0)
                self.assertEqual(provenance["review_delivery"], "simulated_fixture_only")

    def test_unknown_mutant_and_nonunique_source_seam_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "Closed authored"):
            candidate_files("caller-chosen-output")
        for source in (b"missing", b"needle\nneedle\n"):
            with self.assertRaisesRegex(ValueError, "Exact authored mutation seam"):
                replace_one({"x.py": source}, "x.py", "needle", "replacement")

    def test_seven_producers_match_independently_authored_bytes_and_purpose(self):
        self.assertEqual(tuple(execution.QUALIFICATION_IDS), MECHANISMS)
        for control_id in MECHANISMS:
            with self.subTest(control=control_id):
                stdout, stderr = independent_mechanism_bytes(control_id)
                self.assertEqual(execution.qualification_bytes(control_id), (stdout, stderr))
                definition = execution.WorkflowQualificationProfile(control_id).record()
                self.assertEqual(definition["stdout"], {"bytes": len(stdout), "sha256": sha(stdout)})
                self.assertEqual(definition["stderr"], {"bytes": len(stderr), "sha256": sha(stderr)})
                self.assertIs(definition["product_history"], False)
                self.assertIs(definition["product_acceptance_authority"], False)
                self.assertEqual(definition["original_definition_purpose"], "harness_qualification")


def registered_admission(root, registered, *, independent=False):
    """Durable fixture controller originals, explicitly outside study authority."""
    raw = write_json(root / "fixture-registration.json", asdict(registered))
    frozen = None
    if independent:
        subject = registered.gate.binding.subject
        frozen = registry.CohortFreeze(
            tuple(replace(subject, trajectory_id=key) for key in COHORT),
            sha(encoded({"fixture_stop": PROTOCOL})), sha(encoded({"fixture_stop_check": PROTOCOL})), True)
        freeze_raw = write_json(root / "fixture-freeze.json", asdict(frozen))
        def current_freeze():
            if (root / "fixture-freeze.json").read_bytes() != freeze_raw:
                raise ValueError("Fixture cohort original changed")
            return frozen
    else:
        def current_freeze():
            return None
    def current_registration():
        return registered if (root / "fixture-registration.json").read_bytes() == raw else None
    write_json(root / "fixture-controller-disclosure.json", {"scope": FIXTURE_LIMIT,
        "simulated_registration": True, "simulated_six_source_cohort": independent,
        "actual_study_stopping_rule": False, "semantic_authority": False})
    return admission.ObservationAdmission(registered, verify_registration=current_registration,
                                          verify_cohort=current_freeze), frozen


def inspection_plan(files, store, value):
    tree, actual = review.capture_git_source(store, store.head())
    if actual != files:
        raise ValueError("Actual immutable Git source differs from declared fixture")
    schema, _ = storage_fixture.schema_fixture(files)
    return review.WorkflowSourcePlan(admission.source_sha256(files), review.source_sha256(files),
        store.head(), tree, value.case_id, value.sha256, "reviewed-workflow-final-sqlite-v1",
        ("library.sqlite3", "library.sqlite3.maintenance/maintenance.lock",
         "library.sqlite3.maintenance/worker.lock"),
        storage_fixture.mapper.sqlite_schema_sha256(schema), value.purpose, (), "workflow_inspection")


def workflow_plan(files, store, value):
    tree, actual = execution.capture_git_source(store, store.head())
    if actual != files:
        raise ValueError("Actual immutable Git source differs from declared fixture")
    schema, _ = storage_fixture.schema_fixture(files)
    # The return event is in the inherited real close(), before TemporaryDirectory
    # cleanup. Its carried path origin is the actual post-construction workflow
    # frame. A call's last close is final cleanup; earlier closes precede reopen.
    points = (
        review.WorkflowBoundary("constructed-store", WORKFLOW, "solve",
            line_containing(files[WORKFLOW], "results = []"), "line", "base", "path", "solve_enter"),
        review.WorkflowBoundary("closed-store", "library/catalog/legacy_m1.py", "close",
            line_containing(files["library/catalog/legacy_m1.py"], "self.db.close()"),
            "return", None, None, "solve_exit",
            max(sum(op["op"] == "reopen" for op in call["input"]["operations"]) + 1 for call in value.calls)),
    )
    captures = ()
    if value.case_id == "WF19-provisional-fault-boundary":
        # This fixed history's first caught error is the injected commit fault.
        # The handler line runs after JobManager/Store unwound the transaction,
        # before appending the error and before the following list operation.
        # The real reopened Store must finish constructing before its success
        # result line can run. An unsuccessful reopen cannot supply that role.
        points += (
            review.WorkflowBoundary("caught-fault", WORKFLOW, "solve",
                line_containing(files[WORKFLOW], "result = {'error': error.code}"),
                "line", "base", "path", "post_fault"),
            review.WorkflowBoundary("reopened-store", WORKFLOW, "solve",
                line_containing(files[WORKFLOW], "result = {'reopened': True}"),
                "line", "base", "path", "reopened"),
        )
        captures = (
            review.WorkflowCapturePoint("initial", 0, "constructed-store", 0),
            review.WorkflowCapturePoint("post_fault", 0, "caught-fault", 0),
            review.WorkflowCapturePoint("reopened", 0, "reopened-store", 0),
            review.WorkflowCapturePoint("final", 0, "closed-store", 1),
        )
    return review.WorkflowSourcePlan(admission.source_sha256(files), profiles.source_sha256(files),
        store.head(), tree, value.case_id, value.sha256, "reviewed-workflow-final-sqlite-v1",
        ("library.sqlite3", "library.sqlite3.maintenance/maintenance.lock",
         "library.sqlite3.maintenance/worker.lock"),
        storage_fixture.mapper.sqlite_schema_sha256(schema), value.purpose, points,
        capture_points=captures)


class CandidateWorkflowSourceDefectQualificationTests(unittest.TestCase):
    """Real host delivery flow with fixed simulated source judgments; no Engine."""

    def inspect_fixture(self, variant, rejected):
        with ArtifactDirectory("workflow-source-control-" + variant, retain_success=True) as artifacts, ExitStack() as stack:
            root = artifacts.root
            files, provenance = candidate_files(variant)
            write_json(root / "source-provenance.json", provenance)
            store = make_store(root / "candidate.git", files)
            value = review.WorkflowInspectionProfile()
            plan = inspection_plan(files, store, value)
            mechanism_journal, mechanism_enrollment = fixture_delivery(stack, root / "mechanism-review",
                plan.request(), references=source_references(files, variant))
            authority = review.WorkflowReviewAuthority(mechanism_journal, mechanism_journal.commitment,
                                                      mechanism_enrollment)
            subject = registry.Subject(PROTOCOL, COHORT[0], "M4", sha(encoded({"fixture": variant})),
                                       exposure.BASE_SHA256, admission.source_sha256(files))
            registration = review.inspection_registration_for(files, subject, value=value, plan=plan,
                review_authority=authority, commit_oid=store.head(), tree_oid=plan.tree_oid,
                repetition_id="fixture-source-" + variant, gate_id="source-" + variant,
                cohort_trajectory_ids=COHORT)
            admitted, freeze = registered_admission(root,
                review.inspection_observation_registration(registration), independent=True)
            raw, delta = root / "execution-raw", root / "execution-delta"
            head = ExternalHead.create(root / "execution-head", journal_roots=(raw, delta))
            stack.callback(head.close)
            owner = review.WorkflowInspectionExecution(raw, store, registration, value=value, plan=plan,
                review_authority=authority, admission_authority=admitted, checkpoint_authority=head,
                delta_root=delta, mode="physical")
            stack.callback(owner.close)
            self.assertFalse(owner.has_retained("review-request.json"))
            request = owner.begin_review()
            issued_raw = owner.read_authenticated("review-request.json")
            self.assertEqual(encoded(request), issued_raw)
            # Delivery is created only after the actual fresh issued request.
            delivery_journal, delivery_enrollment = fixture_delivery(stack, root / "product-review",
                request, rejected=rejected, references=source_references(files, variant))
            self.assertEqual(delivery_journal.read("request.json"), issued_raw)
            delivery = review.ProductInspectionDelivery(delivery_journal, delivery_journal.commitment,
                                                       delivery_enrollment)
            terminal = owner.complete_review(delivery)
            self.assertIs(terminal["engine_used"], False)
            self.assertEqual(terminal["infrastructure"], [])
            prefix = observation.publish_inspection_verifier(owner)
            result = observation.WorkflowInspectionObservationSource(owner, prefix).observation(registration.gate, freeze)
            failed = [row.case_id for row in result.execution.outcomes if row.status == "failed"]
            self.assertEqual(failed, [review.INSPECTION_FAMILY + ":" + duty
                                      for duty in review.INSPECTION_DUTIES if duty in rejected])
            self.assertTrue(all(row.status in ("passed", "failed") for row in result.execution.outcomes))
            self.assertEqual(tuple(row.case_id for row in result.execution.outcomes), value.ordered_case_ids)
            self.assertLess(owner.authenticated_position("intent.json"), owner.authenticated_position("review-request.json"))
            self.assertLess(owner.authenticated_position("review-request.json"), owner.authenticated_position("review-enrollment.json"))
            self.assertLess(owner.authenticated_position("review-enrollment.json"), owner.authenticated_position("terminal.json"))
            write_json(root / "fixture-source-observation.json", {"scope": FIXTURE_LIMIT,
                "new_engine_executions": 0, "physical_runtime_defect_detection": False,
                "real_independent_semantic_authority": False, "simulated_judgments": list(rejected),
                "observation": asdict(result), "prefix": asdict(prefix)})
            with self.assertRaises(admission.AdmissionError):
                owner.begin_review()

    def test_reference_source_delivery_control(self):
        self.inspect_fixture("reference", ())

    def test_duplicate_solver_rejected_by_source_duties_not_output_or_unknown(self):
        self.inspect_fixture("duplicate-solver", review.INSPECTION_DUTIES)

    def test_early_hook_rejected_by_exact_control_flow_duty(self):
        self.inspect_fixture("early-hook", (SOURCE_DEFECTS[1][1],))

    def test_fake_injected_failure_rejected_by_exact_control_flow_duty(self):
        self.inspect_fixture("fake-injected-error", (SOURCE_DEFECTS[2][1],))


class _PhysicalWorkflowControl:
    """One sequential, fresh owner and retained journal per declared history."""

    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("workflow-physical-" + cls.__name__, retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.completed = []
        cls.addClassCleanup(cls.save_census)
        cls.endpoint = execution.process.EngineEndpoint.from_environment()
        cls.runtime = execution.process.runtime_identity(cls.endpoint, RUNTIME_IMAGE,
            retain=lambda name, raw: write_raw(cls.artifacts.root / name, raw), label="class-runtime")

    @classmethod
    def save_census(cls):
        write_json(cls.artifacts.root / "completion-census.json", {"protocol": PROTOCOL,
            "scope": FIXTURE_LIMIT, "controls": cls.completed,
            "attempted_controls": len(cls.completed),
            "qualified_controls": sum(row["status"] == "qualified" for row in cls.completed),
            "attempted_product_history_controls": sum("case_id" in row for row in cls.completed),
            "qualified_product_history_controls": sum("case_id" in row and row["status"] == "qualified"
                                                        for row in cls.completed),
            "attempted_nonproduct_mechanism_controls": sum("control_id" in row for row in cls.completed),
            "scientific_samples": 0, "whole_project_acceptance": False,
            "automatic_retries": 0, "physical_execution_reuse": False})

    def make_owner(self, stack, root, variant, case_id):
        files, provenance = candidate_files(variant)
        write_json(root / "source-provenance.json", provenance)
        store = make_store(root / "candidate.git", files)
        value = profiles.profile_for(case_id, PURPOSE)
        plan = workflow_plan(files, store, value)
        write_json(root / "source-plan.json", plan.record())
        journal, enrollment = fixture_delivery(stack, root / "mechanism-review", plan.request(),
            references=source_references(files, variant))
        authority = review.WorkflowReviewAuthority(journal, journal.commitment, enrollment)
        policy = execution.WorkflowPolicy()
        binding = execution.binding_for(files, value, policy, self.runtime, plan, review_authority=authority)
        subject = registry.Subject(PROTOCOL, COHORT[0], "M4",
            sha(encoded({"case": case_id, "variant": variant, "scope": FIXTURE_LIMIT})),
            exposure.BASE_SHA256, admission.source_sha256(files))
        gate = execution.gate_for(subject, binding, gate_id="qualification-" + case_id)
        registration = execution.WorkflowRegistration(binding, store.head(), plan.tree_oid,
            "fresh-" + variant, gate, COHORT)
        admitted, _ = registered_admission(root, execution.observation_registration(registration))
        raw, delta = root / "raw", root / "delta"
        head = ExternalHead.create(root / "head", journal_roots=(raw, delta))
        stack.callback(head.close)
        owner = execution.CandidateWorkflowExecution(raw, store, registration, policy,
            value=value, plan=plan, review_authority=authority, admission_authority=admitted,
            checkpoint_authority=head, delta_root=delta, cleanup_root=root / "cleanup",
            endpoint=self.endpoint, mode="physical")
        stack.callback(owner.close)
        return owner

    def assert_original_session(self, owner, terminal):
        self.assertEqual(terminal["status"], "completed")
        self.assertEqual(terminal["infrastructure"], [])
        self.assertEqual(terminal["missing_step_ids"], [])
        self.assertIs(terminal["cleanup_verified"], True)
        self.assertIs(terminal["container_cleanup"], True)
        self.assertIs(terminal["volume_cleanup"], True)
        session = profiles.decode(owner.read_authenticated("session.json"))
        self.assertIs(session["natural_exit"], True)
        self.assertIs(session["capture_complete"], True)
        self.assertIs(session["timed_out"], False)
        self.assertEqual(session["exit_code"], 0)
        self.assertEqual(session["errors"], [])
        self.assertEqual(session["requests"], list(owner.profile.phases))
        for kind in ("stdout", "stderr"):
            raw = owner.read_blob("session-" + kind + ".bin")
            stream = session[kind]
            self.assertEqual((stream["bytes"], stream["observed_bytes"], stream["sha256"]),
                             (len(raw), len(raw), sha(raw)))
            self.assertIs(stream["truncated"], False)
        intent = profiles.decode(owner.read_authenticated("intent.json"))
        container = owner.read_blob("container-create-stdout.bin").strip().decode("ascii")
        self.assertEqual(owner.read_blob("container-remove-stdout.bin").strip(), container.encode())
        self.assertEqual(owner.read_blob("container-after-stdout.bin"), b"")
        self.assertEqual(owner.read_blob("volume-remove-stdout.bin").strip(), intent["volume"].encode())
        self.assertEqual(owner.read_blob("volume-after-stdout.bin"), b"")
        # Corroborate the actual solve exec, distinct from the paused keeper PID.
        ready = profiles.decode(owner.read_authenticated("session-ready-exec-verified.json"))
        final = profiles.decode(owner.read_authenticated("session-final-exec-verified.json"))
        self.assertEqual(ready["exec_id"], final["exec_id"])
        self.assertEqual(ready["pid"], final["pid"])
        self.assertGreater(ready["pid"], 0)
        self.assertIs(ready["completed"], False)
        self.assertIs(final["completed"], True)
        answers, originals = [], []
        for phase in owner.profile.phases:
            raw = owner.read_authenticated(phase + "-response.bin")
            frame = profiles.decode(raw)
            self.assertEqual(set(frame), {"kind", "phase", "value"})
            self.assertEqual((frame["kind"], frame["phase"]), ("result", phase))
            self.assertTrue(raw.endswith(b"\n"))
            self.assertLessEqual(len(raw) - 1, 131072)
            answers.append(frame["value"])
            originals.append({"phase": phase, "raw_sha256": sha(raw), "raw_bytes": len(raw),
                              "value_sha256": sha(encoded(frame["value"]))})
            live = profiles.decode(owner.read_authenticated(phase + "-result-exec-verified.json"))
            self.assertEqual((live["exec_id"], live["pid"]), (ready["exec_id"], ready["pid"]))
        return answers, originals

    def run_history(self, case_id, variant="reference"):
        root = self.artifacts.root / (variant + "-" + case_id)
        root.mkdir()
        census = {"case_id": case_id, "variant": variant, "status": "attempted-not-qualified",
                  "qualification_verified": False, "scope": FIXTURE_LIMIT}
        self.completed.append(census)
        owner = None
        try:
            with ExitStack() as stack:
                owner = self.make_owner(stack, root, variant, case_id)
                terminal = owner.execute_once()
                # Persist a terminal and verifier before any semantic assertion.
                write_json(root / "terminal-copy.json", terminal)
                prefix = observation.publish_verifier(owner)
                result = observation.WorkflowObservationSource(owner, prefix).observation(owner.registration.gate, None)
                original = profiles.decode(owner.read_authenticated(observation.VERIFIER_FILE))
                write_json(root / "physical-observation.json", asdict(result))
                write_json(root / "original-prefix.json", asdict(prefix))
                if variant == "rollback-orphan":
                    # Reopening a deliberately corrupt DB may prevent a complete
                    # solve answer. The exact post-fault blob comparison must
                    # still fail; missing later evidence never detects a defect.
                    self.assertIs(terminal["cleanup_verified"], True)
                    self.assertEqual(owner.read_blob("container-after-stdout.bin"), b"")
                    self.assertEqual(owner.read_blob("volume-after-stdout.bin"), b"")
                    answers, originals = [], []
                else:
                    answers, originals = self.assert_original_session(owner, terminal)
                self.assertEqual(tuple(row.case_id for row in result.execution.outcomes),
                                 owner.registration.gate.ordered_case_ids)
                if variant == "reference":
                    self.assertTrue(all(row.status == "passed" for row in result.execution.outcomes),
                                    [(row.case_id, row.status) for row in result.execution.outcomes])
                    expected = [call["expected"] for call in owner.profile.calls]
                    self.assertTrue(typed_equal(answers, expected), "Exact original complete answers differ")
                    self.assert_reference_storage(owner, original)
                else:
                    self.assert_declared_defect(owner, result, original, answers, variant, root)
                self.assertEqual(owner.checkpoint(), prefix)
                census.update(status="qualified", qualification_verified=True,
                    call_count=len(answers), operation_count=sum(len(call["input"]["operations"]) for call in owner.profile.calls),
                    execution_id=result.execution.execution_id, receipt_sha256=result.execution.receipt_sha256,
                    source_sha256=owner.binding.source_sha256, commit_oid=owner.registration.commit_oid,
                    tree_oid=owner.registration.tree_oid, originals=originals)
        except BaseException as error:
            census.update(status="failed" if isinstance(error, AssertionError) else "unavailable",
                qualification_verified=False, error_type=type(error).__name__, error=str(error)[:1500])
            raise
        finally:
            write_json(root / "control-outcome.json", census)

    def assert_reference_storage(self, owner, original):
        assert_reference_storage(self, owner, original)

    def assert_declared_defect(self, owner, result, original, answers, variant, root):
        assert_declared_defect(self, owner, result, original, answers, variant, root)


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateWorkflowPhysicalPositiveTests(_PhysicalWorkflowControl, unittest.TestCase):
    def test_01_inherited_text_persistence(self):
        self.run_history(HISTORIES[0])

    def test_02_identity_and_replay(self):
        self.run_history(HISTORIES[1])

    def test_03_errors_preserve_catalog(self):
        self.run_history(HISTORIES[2])

    def test_04_cancel_fences_old_token(self):
        self.run_history(HISTORIES[3])

    def test_05_atomic_conflict_after_prepare(self):
        self.run_history(HISTORIES[4])

    def test_06_per_source_provenance(self):
        self.run_history(HISTORIES[5])

    def test_07_invalid_member_rolls_back(self):
        self.run_history(HISTORIES[6])

    def test_08_injected_transaction_failure(self):
        self.run_history(HISTORIES[7])

    def test_09_empty_modes(self):
        self.run_history(HISTORIES[8])

    def test_10_defaults_export_selection(self):
        self.run_history(HISTORIES[9])

    def test_11_v0_unsupported_continuation(self):
        self.run_history(HISTORIES[10])

    def test_12_mixed_job_state_reopen(self):
        self.run_history(HISTORIES[11])

    def test_13_domain_error_continuation(self):
        self.run_history(HISTORIES[12])

    def test_14_exact_64_operations(self):
        self.run_history(HISTORIES[13])

    def test_15_existing_job_token_domain(self):
        self.run_history(HISTORIES[14])

    def test_16_absent_token_hook_distinction(self):
        self.run_history(HISTORIES[15])

    def test_17_actual_61446_answer_growth(self):
        # This product history supports 61446 bytes. Only WQ-NORMALIZED-61824
        # below qualifies the distinct maximum expected-value support boundary.
        self.assertEqual(profiles.case_definition(HISTORIES[16])["calls"][0]["normalized_expected_bytes"], 61446)
        self.run_history(HISTORIES[16])

    def test_18_three_calls_one_actual_exec_isolated_databases(self):
        self.run_history(HISTORIES[17])

    def test_19_complete_fault_rollback_source_corroboration(self):
        self.run_history(HISTORIES[18])

    def test_20_invalid_query_options_continue(self):
        self.run_history(HISTORIES[19])


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateWorkflowPhysicalDefectTests(_PhysicalWorkflowControl, unittest.TestCase):
    def test_reused_database_is_physical_identity_and_state_bleed(self):
        self.run_history(HISTORIES[17], "reused-root")

    def test_actual_reopen_discards_prior_catalog(self):
        self.run_history(HISTORIES[0], "fresh-reopen")

    def test_epoch_overflow_uses_old_domain_error(self):
        self.run_history(HISTORIES[14], "unbounded-epoch")

    def test_absent_lookup_precedes_token_validation(self):
        self.run_history(HISTORIES[15], "lookup-before-token")

    def test_old_valid_epoch_is_not_fenced(self):
        self.run_history(HISTORIES[3], "old-epoch-comparator")

    def test_empty_export_incorrectly_exports_all(self):
        self.run_history(HISTORIES[9], "empty-export-is-all")

    def test_v0_incorrectly_dispatches_m1_jobs(self):
        self.run_history(HISTORIES[10], "v0-job-dispatch")

    def test_mixed_final_jobs_are_reverse_sorted(self):
        self.run_history(HISTORIES[11], "reverse-final-jobs")

    def test_first_domain_error_stops_later_operations(self):
        self.run_history(HISTORIES[12], "stop-after-error")

    def test_injected_fault_after_commit_retains_documents_blobs_receipt_and_job(self):
        self.run_history(HISTORIES[18], "after-commit-fault")

    def test_rollback_leaks_hidden_blob_despite_error_response(self):
        self.run_history(HISTORIES[18], "rollback-orphan")


def retained_storage(test, owner, phase_fact, boundary):
    event = boundary["event"]
    label = phase_fact["phase"] + "-boundary-%03d" % event["ordinal"]
    raw = owner.read_blob(label + "-capture-stdout.bin")
    captured = execution.b02.parse_capture(raw)
    state = observation.captured_state(captured, event["paths"], owner.plan)
    test.assertTrue(typed_equal(state, boundary["storage"]))
    test.assertIsNone(boundary["storage_unavailable"])
    test.assertEqual(state["schema_sha256"], owner.plan.schema_sha256)
    for kind in ("root", "database"):
        facts = boundary["path_facts"][kind]
        test.assertIs(facts["exists"], True)
        test.assertEqual(facts["path"], event["paths"][kind])
        test.assertGreater(facts["inode"], 0)
    return state, {"path": label + "-capture-stdout.bin", "sha256": sha(raw), "bytes": len(raw),
                   "event_sha256": sha(owner.read_authenticated(label + "-event.json"))}


def retained_role(test, owner, original, role):
    """Reread one explicitly enrolled role; absence/ambiguity cannot pass."""
    points = [point for point in owner.plan.capture_points if point.role == role]
    test.assertEqual(len(points), 1, "Exactly one enrolled fixture role is required")
    point = points[0]
    fact = original["phase_facts"][point.call_index]
    matches = [row for row in fact["boundary_facts"]
        if row["event"]["boundary"] == point.boundary_id
        and row["event"]["occurrence"] == point.occurrence]
    test.assertEqual(len(matches), 1, "Missing or ambiguous role is unavailable, never a detected defect")
    state, pin = retained_storage(test, owner, fact, matches[0])
    pin.update(role=role, call_index=point.call_index, boundary_id=point.boundary_id,
               occurrence=point.occurrence, ordinal=matches[0]["event"]["ordinal"])
    return state, pin


def expected_blobs(documents):
    unique = {doc["blob_id"]: doc["text"].encode("utf-8") for doc in documents}
    return [{"blob_id": key, "bytes": len(raw), "sha256": sha(raw)}
            for key, raw in sorted(unique.items())]


def assert_reference_storage(test, owner, original):
    test.assertEqual(original["mechanics"]["status"], "passed")
    test.assertEqual(len(original["phase_facts"]), len(owner.profile.calls))
    roots = []
    for call, fact in zip(owner.profile.calls, original["phase_facts"], strict=True):
        test.assertIs(fact["response_authenticated"], True)
        test.assertIs(fact["capture_authenticated"], True)
        entries = [row for row in fact["boundary_facts"] if row["event"]["boundary"] == "constructed-store"]
        closes = [row for row in fact["boundary_facts"] if row["event"]["boundary"] == "closed-store"]
        test.assertEqual(len(entries), 1)
        test.assertEqual(len(closes), 1 + sum(op["op"] == "reopen" for op in call["input"]["operations"]))
        initial, _ = retained_storage(test, owner, fact, entries[0])
        test.assertEqual(initial["data"], {"blobs": [], "documents": [], "jobs": []})
        final, _ = retained_storage(test, owner, fact, closes[-1])
        expected = call["expected"]
        test.assertTrue(typed_equal(final["data"]["documents"], expected["documents"]))
        test.assertTrue(typed_equal([row["public"] for row in final["data"]["jobs"]], expected.get("jobs", [])))
        test.assertEqual(final["data"]["blobs"], expected_blobs(expected["documents"]))
        for job in final["data"]["jobs"]:
            if job["public"]["state"] != "completed":
                test.assertIsNone(job["receipt"])
                test.assertIsNone(final["persisted_strings"][job["public"]["job_id"]]["receipt"])
            else:
                # The durable receipt is independently compared to the specified
                # exact earlier commit return, not to candidate final job output.
                receipts = [result for result in expected["results"] if type(result) is dict
                    and set(result) == {"job", "documents"}
                    and result["job"]["job_id"] == job["public"]["job_id"]]
                test.assertTrue(receipts, "Completed fixture job must have an explicit expected receipt")
                test.assertTrue(typed_equal(job["receipt"], receipts[0]))
        path = entries[0]["event"]["paths"]["root"]
        roots.append(path)
        # All source-qualified closed-store captures precede directory removal.
        # The complete result capture retains original tar entries, so directory
        # absence is checked too, not inferred merely from absent regular files.
        raw = owner.read_blob(fact["phase"] + "-result-capture-stdout.bin")
        execution.b02.parse_capture(raw)
        prefix = "tmp/" + path.removeprefix("/tmp/")
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
            test.assertFalse(any(member.name.rstrip("/") == prefix or member.name.startswith(prefix + "/")
                                 for member in archive))
        if owner.profile.case_id in (HISTORIES[7], HISTORIES[18]):
            # Before the real reopen and the later successful commit: the failed
            # transaction must leave only keep.txt, its blob, and a running job
            # with no persisted receipt. Final state legitimately differs after
            # that later successful commit and is checked above.
            test.assertEqual(len(closes), 2)
            rollback, _ = retained_storage(test, owner, fact, closes[0])
            test.assertEqual([doc["source"] for doc in rollback["data"]["documents"]], ["keep.txt"])
            test.assertEqual(rollback["data"]["blobs"], expected_blobs(rollback["data"]["documents"]))
            test.assertEqual(len(rollback["data"]["jobs"]), 1)
            job = rollback["data"]["jobs"][0]
            test.assertEqual((job["public"]["job_id"], job["public"]["state"], job["public"]["completed"]),
                             ("fault", "running", 0))
            test.assertIsNone(job["receipt"])
            test.assertIsNone(rollback["persisted_strings"]["fault"]["receipt"])
        if owner.profile.case_id == HISTORIES[18]:
            captured = {role: retained_role(test, owner, original, role)
                        for role in ("initial", "post_fault", "reopened", "final")}
            test.assertEqual(captured["initial"][0]["data"], {"blobs": [], "documents": [], "jobs": []})
            # The exact error-handler event and successful Store construction
            # both precede the later successful commit. The real close before
            # reopen supplies a separate corroborating snapshot, not a proxy
            # for the authenticated reopened role.
            for role in ("post_fault", "reopened"):
                test.assertTrue(typed_equal(captured[role][0]["data"], rollback["data"]))
                test.assertTrue(typed_equal(captured[role][0]["persisted_strings"], rollback["persisted_strings"]))
            test.assertTrue(typed_equal(captured["final"][0]["data"], final["data"]))
            ordinals = [captured[role][1]["ordinal"] for role in ("initial", "post_fault", "reopened", "final")]
            test.assertTrue(all(left < right for left, right in zip(ordinals, ordinals[1:])))
            test.assertEqual(len({captured[role][0]["paths"]["database"] for role in captured}), 1)
    test.assertEqual(len(roots), len(set(roots)), "Separate solve calls reused an actual captured temporary root")


def assert_declared_defect(test, owner, result, original, answers, variant, root):
    failed = {row.case_id for row in result.execution.outcomes if row.status == "failed"}
    witness = {"variant": variant, "scope": "exact authored defect discrimination only",
        "qualified": False, "generic_unavailable_is_detection": False,
        "raw_originals": [], "workflow_gate_failed_selectors": sorted(failed),
        "workflow_gate_rejects_physical_conservation": False}
    case_id = owner.profile.case_id
    def failed_at(suffix, call=0):
        selector = case_id + ":call-%03d:" % call + suffix
        test.assertIn(selector, failed)
        witness["specific_failed_selector"] = selector
    try:
        if variant == "unbounded-epoch":
            test.assertEqual(answers[0]["results"][3], {"error": "stale_epoch"})
            test.assertEqual(owner.profile.calls[0]["expected"]["results"][3], {"error": "invalid_request"})
            failed_at("result-003")
        elif variant == "lookup-before-token":
            test.assertEqual(answers[0]["results"][2], {"error": "not_found"})
            test.assertEqual(owner.profile.calls[0]["expected"]["results"][2], {"error": "invalid_request"})
            failed_at("result-002")
        elif variant == "old-epoch-comparator":
            test.assertEqual(answers[0]["results"][4], {"error": "job_state"})
            test.assertEqual(owner.profile.calls[0]["expected"]["results"][4], {"error": "stale_epoch"})
            failed_at("result-004")
        elif variant == "empty-export-is-all":
            test.assertTrue(typed_equal(answers[0]["results"][5], owner.profile.calls[0]["expected"]["results"][4]))
            test.assertFalse(typed_equal(answers[0]["results"][5], owner.profile.calls[0]["expected"]["results"][5]))
            failed_at("result-005")
        elif variant == "v0-job-dispatch":
            test.assertEqual(answers[0]["results"][0],
                {"job_id": "unused", "epoch": 1, "state": "queued", "total": 0, "completed": 0, "error": None})
            failed_at("result-000")
        elif variant == "reverse-final-jobs":
            expected = owner.profile.calls[0]["expected"]["jobs"]
            test.assertTrue(typed_equal(answers[0]["jobs"], list(reversed(expected))))
            test.assertNotEqual(answers[0]["jobs"], expected)
            failed_at("jobs")
        elif variant == "stop-after-error":
            test.assertEqual(answers[0]["results"], [{"error": "not_found"}])
            test.assertEqual(len(owner.profile.calls[0]["input"]["operations"]), 7)
            failed_at("shape")
        elif variant == "fresh-reopen":
            test.assertEqual(answers[0]["results"][1], {"reopened": True})
            test.assertEqual(answers[0]["results"][3], {"error": "not_found"})
            test.assertEqual(answers[0]["documents"], [])
            failed_at("documents")
            fact = original["phase_facts"][0]
            closes = [row for row in fact["boundary_facts"] if row["event"]["boundary"] == "closed-store"]
            test.assertEqual(len(closes), 2)
            before, pin = retained_storage(test, owner, fact, closes[0]); witness["raw_originals"].append(pin)
            after, pin = retained_storage(test, owner, fact, closes[-1]); witness["raw_originals"].append(pin)
            test.assertEqual([doc["source"] for doc in before["data"]["documents"]], ["notes/a.txt"])
            test.assertEqual(after["data"]["documents"], [])
            witness["before_reopen_document_sources"] = ["notes/a.txt"]
            witness["after_reopen_document_sources"] = []
        elif variant == "reused-root":
            facts = original["phase_facts"]
            test.assertEqual(len(facts), 3)
            entries = [[row for row in fact["boundary_facts"] if row["event"]["boundary"] == "constructed-store"][0]
                       for fact in facts]
            paths = [entry["event"]["paths"]["root"] for entry in entries]
            test.assertEqual(len(set(paths)), 1)
            second, pin = retained_storage(test, owner, facts[1], entries[1]); witness["raw_originals"].append(pin)
            test.assertEqual([doc["source"] for doc in second["data"]["documents"]], ["first.txt"])
            test.assertEqual(answers[1]["results"][0]["job_id"], "shared")
            test.assertEqual(answers[1]["results"][0]["state"], "completed")
            failed_at("result-000", call=1)
            witness["same_captured_roots"] = paths
            witness["second_call_old_document"] = "first.txt"
        elif variant in ("after-commit-fault", "rollback-orphan"):
            fact = original["phase_facts"][0]
            state, pin = retained_role(test, owner, original, "post_fault"); witness["raw_originals"].append(pin)
            failed_at("capture:post_fault:blobs")
            data = state["data"]
            if variant == "after-commit-fault":
                test.assertEqual(answers[0]["results"][3], {"error": "injected_failure"})
                test.assertEqual([doc["source"] for doc in data["documents"]], ["keep.txt", "new.txt"])
                test.assertEqual(data["blobs"], expected_blobs(data["documents"]))
                job = data["jobs"][0]
                test.assertEqual((job["public"]["state"], job["public"]["completed"]), ("completed", 1))
                test.assertIsNotNone(job["receipt"])
                test.assertIsNotNone(state["persisted_strings"]["fault"]["receipt"])
                reopened, pin = retained_role(test, owner, original, "reopened"); witness["raw_originals"].append(pin)
                test.assertTrue(typed_equal(reopened["data"], data), "Actual successful reopen must preserve the leaked commit")
                test.assertTrue(typed_equal(reopened["persisted_strings"], state["persisted_strings"]))
                for role in ("post_fault", "reopened"):
                    for component in ("blobs", "documents", "jobs"):
                        failed_at("capture:" + role + ":" + component)
                failed_at("result-004")
                witness["proven_post_failure_state"] = {"document_sources": ["keep.txt", "new.txt"],
                    "job_state": "completed", "completed": 1, "receipt_present": True, "reopen_persisted": True}
            else:
                orphan = b"workflow-orphan-after-rollback"
                expected = {"blob_id": "blob-" + sha(orphan), "bytes": len(orphan), "sha256": sha(orphan)}
                test.assertIn(expected, data["blobs"])
                test.assertNotIn(expected["blob_id"], {doc["blob_id"] for doc in data["documents"]})
                test.assertEqual([doc["source"] for doc in data["documents"]], ["keep.txt"])
                test.assertEqual(data["jobs"][0]["public"]["state"], "running")
                test.assertIsNone(data["jobs"][0]["receipt"])
                # The real Store validates blob reachability and rejects this
                # orphan before its constructor returns. Do not manufacture a
                # reopened role from an earlier close or later finally block.
                reopened_point = next(point for point in owner.plan.capture_points if point.role == "reopened")
                test.assertFalse(any(row["event"]["boundary"] == reopened_point.boundary_id
                    and row["event"]["occurrence"] == reopened_point.occurrence for row in fact["boundary_facts"]))
                outcomes = {row.case_id: row.status for row in result.execution.outcomes}
                absent = [case_id + ":call-000:capture:reopened:" + component
                          for component in ("blobs", "documents", "jobs", "persisted_fields")]
                test.assertEqual([outcomes[selector] for selector in absent], ["infrastructure_error"] * 4)
                for component in ("documents", "jobs", "persisted_fields"):
                    test.assertEqual(outcomes[case_id + ":call-000:capture:post_fault:" + component], "passed")
                witness["exact_unreferenced_blob"] = expected
                witness["incomplete_solve_is_not_the_detection"] = True
                witness["reopen_success_claimed"] = False
                witness["unavailable_reopened_selectors"] = absent
                witness["later_failures_are_not_additional_defects"] = True
            witness["physical_conservation_discrepancy"] = True
            witness["workflow_gate_rejects_physical_conservation"] = True
        else:
            raise AssertionError("Closed physical defect required")
        witness["qualified"] = True
    finally:
        write_json(root / "declared-defect-witness.json", witness)


def independent_mechanism_bytes(control_id):
    """Authored expected wire inventory; never derived from captured output."""
    if control_id not in MECHANISMS:
        raise ValueError("Closed seven physical mechanism controls required")
    ready = b'{"kind":"ready","protocol":"candidate-workflow-wire-v1"}\n'
    payload = (b'{"kind":"result","phase":"call-000","value":"' + b'x' * 61822 + b'"}'
               if control_id == MECHANISMS[0] else b'{"kind":"result","phase":"call-000","value":{}}')
    if control_id in (MECHANISMS[1], MECHANISMS[2]):
        payload += b' ' * ((131072 if control_id == MECHANISMS[1] else 131073) - len(payload))
    stdout, stderr = ready + payload + b'\n', b''
    if control_id in (MECHANISMS[3], MECHANISMS[4]):
        length = 524288 if control_id == MECHANISMS[3] else 524289
        stdout = ready + b'q' * (length - len(ready))
    if control_id in (MECHANISMS[5], MECHANISMS[6]):
        stderr = b'e' * (65536 if control_id == MECHANISMS[5] else 65537)
    return stdout, stderr


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateWorkflowPhysicalMechanismTests(_PhysicalWorkflowControl, unittest.TestCase):
    """Seven real Engine producers, zero workflow/history/semantic credit."""

    def run_mechanism(self, control_id):
        root = self.artifacts.root / control_id
        root.mkdir()
        census = {"control_id": control_id, "status": "attempted-not-qualified",
            "qualification_verified": False, "purpose": "harness_qualification",
            "product_histories": 0, "scientific_samples": 0, "scope": FIXTURE_LIMIT}
        self.completed.append(census)
        wanted = independent_mechanism_bytes(control_id)
        write_json(root / "authored-byte-contract.json", {"control_id": control_id,
            "stdout": {"bytes": len(wanted[0]), "sha256": sha(wanted[0])},
            "stderr": {"bytes": len(wanted[1]), "sha256": sha(wanted[1])},
            "framing": "one LF outside payload byte bound", "purpose": "harness_qualification",
            "product_credit": False})
        try:
            with ExitStack() as stack:
                value = execution.WorkflowQualificationProfile(control_id)
                self.assertEqual(value.original_definition_purpose, "harness_qualification")
                self.assertIs(value.record()["product_acceptance_authority"], False)
                self.assertEqual(execution.qualification_bytes(control_id), wanted)
                files = execution.qualification_source_files(control_id)
                write_json(root / "producer-source.json", {name: raw.decode("ascii") for name, raw in files.items()})
                store = make_store(root / "producer.git", files)
                tree, actual = execution.capture_git_source(store, store.head())
                self.assertEqual(actual, files)
                policy = execution.WorkflowPolicy()
                binding = execution.qualification_binding_for(files, value, policy, self.runtime)
                subject = registry.Subject(PROTOCOL, COHORT[0], "M4", sha(encoded({"qualifier": control_id})),
                                           exposure.BASE_SHA256, admission.source_sha256(files))
                gate = execution.qualification_gate_for(subject, binding, gate_id="qualifier-" + control_id)
                registration = execution.WorkflowQualificationRegistration(binding, store.head(), tree,
                    "fresh-" + control_id, gate, COHORT)
                admitted, _ = registered_admission(root, execution.qualification_observation_registration(registration))
                raw, delta = root / "raw", root / "delta"
                head = ExternalHead.create(root / "head", journal_roots=(raw, delta))
                stack.callback(head.close)
                owner = execution.CandidateWorkflowQualificationExecution(raw, store, registration, policy,
                    value=value, admission_authority=admitted, checkpoint_authority=head, delta_root=delta,
                    cleanup_root=root / "cleanup", endpoint=self.endpoint, mode="physical")
                stack.callback(owner.close)
                # The separate qualifier type may not dispatch as a product.
                before = owner.checkpoint()
                with self.assertRaises(execution.ExecutionError):
                    owner.execute_once()
                self.assertEqual(owner.checkpoint(), before)
                terminal = owner.execute_qualification_once()
                write_json(root / "terminal-copy.json", terminal)
                original = observation.reconstruct_qualification(owner)
                write_json(root / "original-qualification.json", original)
                prefix = owner.checkpoint()
                write_json(root / "original-prefix.json", asdict(prefix))
                self.assertEqual(original["status"], "passed", original)
                self.assertEqual(original["unavailable"], [])
                self.assertTrue(all(check is True for check in original["checks"].values()))
                self.assertEqual(original["original_definition_purpose"], "harness_qualification")
                session = profiles.decode(owner.read_authenticated("session.json"))
                self.assertIs(session["natural_exit"], True)
                self.assertEqual(session["exit_code"], 0)
                self.assertIs(session["timed_out"], False)
                for index, kind in enumerate(("stdout", "stderr")):
                    limit = 524288 if kind == "stdout" else 65536
                    stream = session[kind]
                    actual = owner.read_blob("session-" + kind + ".bin")
                    self.assertEqual(actual, wanted[index][:limit])
                    self.assertEqual(stream["observed_bytes"], len(wanted[index]))
                    self.assertEqual(stream["bytes"], min(limit, len(wanted[index])))
                    self.assertEqual(stream["sha256"], sha(actual))
                    self.assertIs(stream["truncated"], len(wanted[index]) > limit)
                errors = {
                    MECHANISMS[0]: set(), MECHANISMS[1]: set(), MECHANISMS[2]: {"frame-limit"},
                    MECHANISMS[3]: {"frame-limit", "truncated-frame"},
                    MECHANISMS[4]: {"frame-limit", "truncated-frame", "stdout-limit"},
                    MECHANISMS[5]: set(), MECHANISMS[6]: {"stderr-limit"},
                }[control_id]
                self.assertEqual(set(session["errors"]), errors)
                self.assertIs(session["capture_complete"], not errors)
                if control_id in (MECHANISMS[0], MECHANISMS[1], MECHANISMS[5]):
                    payload = owner.read_blob("session-stdout.bin").split(b'\n', 1)[1][:-1]
                    value = profiles.decode(payload)
                    self.assertEqual(set(value), {"kind", "phase", "value"})
                    self.assertEqual((value["kind"], value["phase"]), ("result", "call-000"))
                    expected = "x" * 61822 if control_id == MECHANISMS[0] else {}
                    self.assertTrue(typed_equal(value["value"], expected))
                    if control_id == MECHANISMS[0]:
                        self.assertEqual(len(json.dumps(value["value"], ensure_ascii=True, allow_nan=False).encode("ascii")), 61824)
                    elif control_id == MECHANISMS[1]:
                        self.assertEqual(len(payload), 131072)
                if control_id == MECHANISMS[2]:
                    payload = wanted[0].split(b'\n', 1)[1][:-1]
                    self.assertEqual(len(payload), 131073)
                    self.assertEqual(profiles.decode(payload)["value"], {})
                    self.assertFalse(session["capture_complete"], "Over-limit serialization must never count as accepted semantics")
                self.assertIs(terminal["cleanup_verified"], True)
                self.assertEqual(owner.read_blob("container-after-stdout.bin"), b"")
                self.assertEqual(owner.read_blob("volume-after-stdout.bin"), b"")
                with self.assertRaises(observation.AuthorityError):
                    observation.WorkflowObservationSource(owner, prefix)
                self.assertEqual(owner.checkpoint(), prefix)
                census.update(status="qualified", qualification_verified=True,
                    execution_id=original["execution_id"], prefix=asdict(prefix),
                    source_sha256=binding.source_sha256, declared_capture_errors=sorted(errors),
                    normalized_support_bytes=61824 if control_id == MECHANISMS[0] else None,
                    accepted_product_output=False)
        except BaseException as error:
            census.update(status="failed" if isinstance(error, AssertionError) else "unavailable",
                qualification_verified=False, error_type=type(error).__name__, error=str(error)[:1500])
            raise
        finally:
            write_json(root / "control-outcome.json", census)

    def test_01_normalized_61824_is_actually_emitted_captured_and_compared(self):
        self.run_mechanism(MECHANISMS[0])

    def test_02_exact_131072_frame_payload_plus_separate_delimiter(self):
        self.run_mechanism(MECHANISMS[1])

    def test_03_131073_frame_is_explicitly_incomplete(self):
        self.run_mechanism(MECHANISMS[2])

    def test_04_exact_524288_stdout_raw_retention_without_product_credit(self):
        self.run_mechanism(MECHANISMS[3])

    def test_05_524289_stdout_retains_bounded_prefix_and_total_observed(self):
        self.run_mechanism(MECHANISMS[4])

    def test_06_exact_65536_stderr_raw_retention(self):
        self.run_mechanism(MECHANISMS[5])

    def test_07_65537_stderr_is_incomplete_without_a_product_verdict(self):
        self.run_mechanism(MECHANISMS[6])
