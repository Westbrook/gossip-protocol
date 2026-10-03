"""Cheap pilot boundary checks; no provider, Docker, or candidate execution.

Role checks use disposable journals and synthetic meshes. Repair continuation
also exercises real V3 finance with injected transport and actual worker parsing.
These checks do not qualify the twenty-process project flow.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gossip_harness.ledger import Lease, Ledger
from gossip_harness.peer_library_contract_v4 import (
    BUILDERS, REVIEWERS, ROLES, builder_prompt, call_limit, exact_scopes,
    package_for, selector_prompt, work_key,
)
from gossip_harness.peer_library_project_v4 import (
    ActionBudget, COHORT, EvidenceRegistryV4, FrozenFrontier, PilotError, PilotStop, ProjectConfig,
    ProjectRoleLoopV4, ProjectRun, _format_correction, action_roster,
    accounting_snapshot, allowed, canonical_payload, execution_contract, enrollment,
    fixture_permit, repair_packages_from_gate, run_project, sanitized_child_environment,
)
from gossip_harness.peer_project_contract_v2 import (
    ActionRequest, CandidateOffer, Context, DispatchBinding, DispatchReply, EvidenceRef,
    ReleaseTarget, SelectedOffer, SelectionManifest, decode, encode, from_dict, identity, to_dict,
    worker_request_digest,
)
from gossip_harness.peer_candidate_v2 import named_sources, result_payload_bytes
from gossip_harness.peer_coding_dispatch_v1 import source_digest
from gossip_harness.peer_project_evidence_v3 import ContributionSlot, EvidenceError
from gossip_harness.peer_project_repair_v4 import (
    RepairTopic, TopicRepairPlan, UnchangedTopic, publish_repair_directive, repair_frontier,
)
from gossip_harness.peer_project_views_v3 import materialize_project
from gossip_harness.peer_review_release_v2 import (
    ReviewGateReceipt, ReviewPolicy, ReviewerProfile, ScopeRule, SLOTS, materialize_verdicts,
)
from gossip_harness.peer_role_loop_v2 import RoleError, WorkDirective, directive_id
from gossip_harness.worker import OpenAIWorker, WorkerResult
from tests.test_peer_role_loop_v2 import FakeFinance, FakeMesh


def full_budget_sequence():
    """A feasible maximum: sixteen proposals, two four-topic repair rounds.

    Each selector and reviewer requires one format correction in this synthetic
    sequence. Real schema/provider provenance is qualified separately.
    """
    actions = [(actor, "build", 0, False) for actor in BUILDERS]
    for generation in range(3):
        if generation:
            actions.extend((actor, "repair", generation, False)
                           for actor in ("B01", "B05", "B09", "B13"))
        actions.extend(("R1", "select_source", generation, correction)
                       for correction in (False, True))
        actions.extend((actor, "review", generation, correction)
                       for actor in REVIEWERS for correction in (False, True))
    return actions


def synthetic_offer(actor, *, generation=0, token=1):
    """Typed source identity only; no claimed financial or Git authenticity."""
    context = Context("a" * 64, "fixture", "membership-only", 1, "b" * 64)
    request_ref = EvidenceRef(f"{token:064x}", actor, "worker-request", "c" * 64)
    action = ActionRequest(context, f"action-{token}", f"dispatch-{token}", actor,
        "build" if generation == 0 else "repair",
        work_key(actor, "build" if generation == 0 else "repair", generation), "mini", request_ref, "d" * 64)
    binding = DispatchBinding(action, Lease(f"task-{token}", actor, 1, 200),
        "e" * 64, "f" * 64, "a" * 64, f"call-{token}", f"reservation-{token}", 100)
    return CandidateOffer(binding, f"{token:040x}", "b" * 64, "c" * 64, "d" * 64,
        EvidenceRef(f"{token + 100:064x}", actor, "candidate-bundle", "e" * 64))


def accounting_database(path, records):
    """Disposable query fixture with deliberate missing-reservation corruption.

    This creates only the columns consumed by the reporting query. It is not an
    authority receipt, and does not bypass or test actual financial admission.
    """
    Ledger(path, 100_000)
    with sqlite3.connect(path) as db:
        db.execute("""CREATE TABLE financial_actions_v2 (
            actor TEXT, request_id TEXT, call_id TEXT, reservation_id TEXT,
            state TEXT, cohort TEXT)""")
        for record in records:
            call = record["call"]
            # The real V2 authority uses the same stable identity for the call
            # and its reservation; keep that invariant in this reporting fixture.
            reservation = record.get("reservation", call)
            db.execute("INSERT INTO financial_actions_v2 VALUES (?,?,?,?,?,?)",
                (record.get("actor", "B01"), "request-" + call, call, reservation,
                 record.get("action_state", "completed"), record.get("cohort", "current")))
            if not record.get("missing_reservation", False):
                db.execute("INSERT INTO reservations VALUES (?,?,?,?,?,?)",
                    (reservation, "task-" + call, 1, 100, record.get("spent", 7), record.get("state", "settled")))


def completion(call, reservation=None):
    return {"reply": {"binding": {"call_id": call,
                                  "reservation_id": reservation or call}}}


def synthetic_selector():
    """Complete cognitive evidence with explicitly synthetic candidate identities."""
    mesh = FakeMesh("R1")
    mesh.node_id = "receiver"
    offers = tuple(synthetic_offer(actor, token=index) for index, actor in
                   enumerate(("B01", "B05", "B09", "B13"), 1))
    context, policy = offers[0].dispatch.action.context, "c" * 64
    files = {"README.md": "Synthetic public requirement\n", **{
        f"library/{package}/main.py": f"value = '{package}'\n"
        for package in ("catalog", "ingestion", "query", "clients")}}
    source = mesh.arrive("seed", "project-source", canonical_payload({"files": files, "base_sha": "a" * 40}))
    entries, materials = [], []
    for offer in offers:
        work = offer.dispatch.action.work
        offer_ref = mesh.arrive("receiver", "candidate-offer", encode(offer))
        contribution = mesh.arrive("receiver", "candidate-contribution", canonical_payload({
            "protocol": "peer-project-contribution-v3", "offer_sha256": identity(offer),
            "commit_oid": offer.commit_oid,
            "files": {path: body for path, body in files.items() if path.startswith("library/" + work.package_id + "/")}}))
        entries.append({"slot": to_dict(work), "actor": offer.dispatch.action.actor,
                        "offer_sha256": identity(offer), "offer_ref": to_dict(offer_ref),
                        "contribution_ref": to_dict(contribution)})
        materials.extend((offer_ref, contribution))
    entries.sort(key=lambda item: (item["slot"]["package_id"], item["slot"]["slot_id"], item["actor"]))
    frontier = mesh.arrive("receiver", "selection-frontier", canonical_payload({
        "protocol": "peer-project-frontier-v3", "context": to_dict(context),
        "eligibility_policy_sha256": policy, "offers": entries, "missing_slots": []}))
    directive = WorkDirective(context, work_key("R1", "select_source", 0), "strong", "select_source",
        source, (frontier, *materials), ("selection.json",), selector_prompt())
    return mesh, policy, directive, files


def synthetic_terminal(mesh, directive, policy, changes, *, actor="R1"):
    """Real encodings over an injected accounting capability, no actual charge."""
    request, view = materialize_project(directive, mesh, policy)
    request_ref = mesh.arrive(actor, "worker-request", canonical_payload({
        "worker_request": asdict(request), "view_manifest_sha256": identity(view)}))
    action = ActionRequest(directive.context, "action-" + directive_id(directive),
        "dispatch-" + directive_id(directive), actor, directive.kind, directive.work,
        directive.profile_id, request_ref, identity(view))
    binding = DispatchBinding(action, Lease(request.task_id, actor, 1, 200),
        worker_request_digest(request), "f" * 64, "a" * 64,
        "call-" + directive_id(directive), "reservation-" + directive_id(directive), 100)
    result = WorkerResult(changes, "Synthetic complete terminal response", 3, {})
    result_ref = mesh.arrive("finance", "financial-result", result_payload_bytes(result))
    reply = DispatchReply(action.request_id, identity(action), "completed", "synthetic-accounted-outcome",
                          binding, result_ref.payload_sha256, 3)
    role_ref = mesh.arrive(actor, "role-result", canonical_payload({
        "protocol": "peer-role-loop-v2", "actor": actor, "action": to_dict(action), "reply": to_dict(reply),
        "view_manifest": to_dict(view), "result_ref": to_dict(result_ref)}))
    proof = {"binding": binding, "action": action, "reply": reply, "worker_request": request,
             "result": result, "result_payload": {"kind": "result", "payload": asdict(result)}}
    return binding, role_ref, proof


def synthetic_review_round(output, *, malformed=False, malformed_correction=False, failed_provider=False):
    """Real review scheduling/parser with in-memory finance and promotion adapters.

    This tests counted correction behavior, not physical execution, Git release,
    or the authenticity of the explicitly authored financial capability.
    """
    mesh, eligibility, selector, files = synthetic_selector()
    selecting_binding, _, _ = synthetic_terminal(mesh, selector, eligibility, {"selection.json": "{}"})
    frontier = json.loads(mesh.resolve(selector.evidence_refs[0]))
    offers = tuple(decode(mesh.resolve(ref), CandidateOffer,
                         expected_contract_sha256=selector.context.execution_contract_sha256)
                   for ref in selector.evidence_refs if ref.kind == "candidate-offer")
    packages = ("catalog", "ingestion", "query", "clients")
    selection = SelectionManifest(selector.context, eligibility, selecting_binding,
        selecting_binding.action.view_manifest_sha256, tuple(item["offer_sha256"] for item in frontier["offers"]), (),
        tuple(SelectedOffer(package, identity(next(offer for offer in offers
            if offer.dispatch.action.work.package_id == package))) for package in packages))
    selection_ref = mesh.arrive("seed", "selection-manifest", encode(selection))
    execution_ref = mesh.arrive("seed", "execution-receipt", b'{"fixture":"not-physical"}')
    target = ReleaseTarget(selector.context, 0, "synthetic-project", "refs/heads/accepted", "b" * 40,
        "sha1", "a" * 40, "c" * 40, named_sources(files), identity(selection), "d" * 64, "e" * 64,
        (execution_ref,))
    refs_by_offer = {item["offer_sha256"]: tuple(from_dict(EvidenceRef, item[key])
        for key in ("offer_ref", "contribution_ref")) for item in frontier["offers"]}
    registry = SimpleNamespace(selection_ref=lambda _selection: selection_ref,
                               normalized_refs=lambda offer: refs_by_offer[identity(offer)])
    requirements = ("public-fixture-requirement",)
    generations = tuple((package, 0) for package in packages)
    controller = ProjectRun.__new__(ProjectRun)
    controller.output, controller.seed, controller.context = output, mesh, selector.context
    controller.deadline = time.monotonic() + 60
    (output / "corrections").mkdir()
    controller.protected = SimpleNamespace(read_files=lambda _commit: files)
    controller.policy = ReviewPolicy(selector.context,
        tuple(ScopeRule(actor, scope, requirements) for actor, scope in SLOTS),
        tuple(ReviewerProfile(actor, "strong", "f" * 64) for actor in REVIEWERS),
        requirements, requirements, requirements, eligibility, "d" * 64, "e" * 64, "a" * 64, generations)
    controller.requirements, controller.transport = requirements, None
    controller.registry_for_action, controller.corrections = {}, {}
    controller.action_budget = ActionBudget()
    proofs, responses, rounds, recorded = {}, {}, [], []
    controller.finance = SimpleNamespace(verified_terminal=lambda binding: proofs[identity(binding)])

    def dispatch(actor, directive, policy, *, generation, correction=False, fixture_changes=None):
        controller.action_budget.reserve(actor, "review", generation, correction)
        artifact = json.loads(directive.instructions.rsplit("\n", 1)[1])
        for item in artifact["verdicts"]:
            item.update(verdict="request_changes", rationale="Concrete synthetic defect; retain this valid rejection.")
        text = "{malformed" if actor == "R2" and ((malformed and not correction)
            or (malformed_correction and correction)) else json.dumps(artifact)
        binding, role_ref, proof = synthetic_terminal(mesh, directive, policy, {"review.json": text}, actor=actor)
        if failed_provider and actor == "R2":
            proof = {**proof, "reply": replace(proof["reply"], state="failed")}
        proofs[identity(binding)] = proof
        key = directive_id(directive)
        responses[key] = {"reply": to_dict(proof["reply"]), "publication_ref": to_dict(role_ref)}
        return key

    def begin(target, *, round_number, requests):
        rounds.append((round_number, requests))

    reviews = {}

    def record(target, bindings):
        for binding in bindings:
            verdicts = materialize_verdicts(binding, target, controller.finance.verified_terminal)
            reviews.setdefault(identity(target), {})[binding.action.actor] = verdicts
            recorded.append(binding)

    controller.dispatch, controller.collect = dispatch, lambda _actor, key: responses[key]
    controller.promotion = SimpleNamespace(begin_review_round=begin, record_reviews=record, reviews=reviews,
        promote=lambda _target: SimpleNamespace(status="synthetic-rejection"))
    publication = SimpleNamespace(receipt_ref=execution_ref, check_results=(("public-case", "failed"),))
    return controller, (0, registry, offers, selection, target, publication, generations), rounds, recorded


def admission_wait_fixture(root, reason, records, *, cohort_cap=1000, global_cap=1000):
    """Read-only admission-wait query fixture, not a live authority journal."""
    controller = ProjectRun.__new__(ProjectRun)
    controller.output, controller.ledger_path = root, root / "accounting.sqlite"
    accounting_database(controller.ledger_path, records)
    journal = root / "roles" / "B01" / "journal" / "role.sqlite"
    journal.parent.mkdir(parents=True)
    with sqlite3.connect(journal) as db:
        db.execute("CREATE TABLE actions (id TEXT, body BLOB)")
        db.execute("INSERT INTO actions VALUES (?,?)", ("action-key", canonical_payload({
            "reply": {"state": "waiting", "reason": reason}})))
    controller.directives = {"dispatch-action-key": SimpleNamespace(profile_id="mini")}
    controller.finance = SimpleNamespace(workers={"mini": SimpleNamespace(
        profile_manifest=lambda: {"reservation_units": 100})})
    controller.permit = {"incremental_cap_micro_usd": cohort_cap, "expected_global_cap": global_cap}
    return controller


def real_repair_failure(test, *, malformed=False):
    """Real V3 charged failure plus arrived evidence; old Git registration is synthetic.

    A transport returns unchanged file contents through the actual OpenAIWorker
    parser. No WorkerFailure or terminal proof is fabricated. The small selected
    topic/failed-public-gate fixture is trusted input for this receiver seam;
    separate topic tests qualify its real Git provenance.
    """
    from tests import test_peer_financial_authority_v3 as authority_fixture
    from gossip_harness.peer_financial_authority_v3 import digest
    from gossip_harness.worker import HTTPResponse

    class RepeatedSourceTransport(authority_fixture.FixtureTransport):
        def __call__(self, request, timeout, maximum):
            original = super().__call__(request, timeout, maximum)
            task = json.loads(json.loads(request.data)["input"])
            path = task["allowed_paths"][0]
            proposal = {"changes": [] if malformed else [{"path": path, "content": task["files"][path]}],
                        "summary": "Explicit unchanged-source fixture"}
            body = json.loads(original.body)
            body["output"][0]["content"][0]["text"] = json.dumps(proposal)
            return HTTPResponse(original.status, original.headers, json.dumps(body).encode())

    fixture = authority_fixture.PeerFinancialAuthorityV3Tests()
    test.addCleanup(fixture.doCleanups)
    fixture.setUp()
    actor = "B05"
    fixture.transport = RepeatedSourceTransport()
    fixture.worker = OpenAIWorker("fixture-never-a-real-credential", max_output_tokens=64, timeout=2,
                                  transport=fixture.transport)
    fixture.work = work_key(actor, "repair", 1)
    owned_path = "library/ingestion/main.py"
    fixture.contract["task_specs"] = [{"context": to_dict(fixture.context), "work": to_dict(fixture.work),
        "actors": [actor], "kinds": ["repair"], "profiles": ["mini"], "allowed_paths": [owned_path],
        "max_reserved_units": 900_000}]
    fixture.permit = fixture.make_permit()
    design = fixture.permit["execution_design"]
    design["action_limits"] = {"total": 1, "by_kind": {"repair": 1},
        "by_kind_generation": {"repair": {"1": 1}}, "by_actor": {actor: 1}}
    fixture.permit["execution_design_sha256"] = digest(design)
    finance = fixture.open()
    profile = digest(finance.profiles["mini"])
    files = {"README.md": "Synthetic public topic interfaces\n", **{
        f"library/{package}/main.py": f"value = '{package}'\n"
        for package in ("catalog", "ingestion", "query", "clients")}}
    offers = []
    for index, builder in enumerate(("B01", "B05", "B09", "B13"), 1):
        prior = synthetic_offer(builder, token=index)
        dispatch = replace(prior.dispatch, action=replace(prior.dispatch.action, context=fixture.context),
                           authority_config_sha256=finance.config_sha256, profile_sha256=profile)
        offers.append(replace(prior, dispatch=dispatch, source_sha256=source_digest(files)))
    offers = tuple(offers)
    mesh, policy = FakeMesh("seed"), "c" * 64
    selector_action = ActionRequest(fixture.context, "synthetic-selector", "synthetic-selection", "R1",
        "select_source", work_key("R1", "select_source", 0), "strong",
        mesh.arrive("R1", "worker-request", b"synthetic old selector"), "d" * 64)
    selector_dispatch = DispatchBinding(selector_action, Lease("selector-task", "R1", 1, 200),
        "e" * 64, "f" * 64, finance.config_sha256, "old-selector-call", "old-selector-reservation", 10)
    selection = SelectionManifest(fixture.context, policy, selector_dispatch, "d" * 64,
        tuple(identity(offer) for offer in offers), (),
        tuple(SelectedOffer(offer.dispatch.action.work.package_id, identity(offer)) for offer in offers))
    execution_ref = mesh.arrive("seed", "execution-receipt", b'{"fixture":"failed-public-check"}')
    target = ReleaseTarget(fixture.context, 0, "synthetic-project", "refs/heads/accepted", "f" * 40,
        "sha1", "9" * 40, "8" * 40, named_sources(files), identity(selection), "d" * 64, "e" * 64,
        (execution_ref,))
    gate = ReviewGateReceipt(identity(target), policy, target.required_suite_sha256,
        (), (), (), (("R2", "ingestion"),), ("required_execution_not_passed",), False)
    topics = tuple(RepairTopic(offer.dispatch.action.work.package_id, offer, tuple(sorted(files.items())),
        (f"library/{offer.dispatch.action.work.package_id}/main.py",),
        offer.dispatch.action.work.package_id == "ingestion", 1) for offer in offers)
    plan = TopicRepairPlan(target.expected_head, target, selection, gate, topics, tuple(sorted(files.items())),
                           (execution_ref,))
    directive = publish_repair_directive(plan, "ingestion", mesh, work=fixture.work, profile_id="mini",
        instructions=builder_prompt(actor, repair=True), current_failure=lambda: (target, gate))
    request, view = materialize_project(directive, mesh, policy)
    raw = canonical_payload({"worker_request": asdict(request), "view_manifest_sha256": identity(view)})
    payload_sha = fixture.payloads.put_owned(actor, raw)
    request_ref = mesh.arrive(actor, "worker-request", raw)
    test.assertEqual(request_ref.payload_sha256, payload_sha)
    key = directive_id(directive)
    action = ActionRequest(fixture.context, "action-" + key, "dispatch-" + key, actor, "repair", fixture.work,
                           "mini", request_ref, identity(view))
    lease = finance.claim(actor, fixture.context, fixture.work)
    finance.submit(actor, action, lease)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        reply = finance.lookup(actor, action.request_id)
        if reply is not None and reply.state not in ("waiting", "pending", "publication_pending"):
            break
        time.sleep(0.005)
    test.assertIsNotNone(reply)
    test.assertEqual(reply.state, "failed")
    proof = finance.verified_known_failure(reply.binding)
    result_raw = fixture.payloads.read_owned(actor, reply.result_payload_sha256)
    test.assertEqual(result_raw, canonical_payload(proof["result_payload"]))
    result_ref = mesh.arrive("finance", "financial-result", result_raw)
    role_ref = mesh.arrive(actor, "role-result", canonical_payload({
        "protocol": "peer-role-loop-v2", "actor": actor, "action": to_dict(action), "reply": to_dict(reply),
        "view_manifest": to_dict(view), "result_ref": to_dict(result_ref)}))
    result = {"actor": actor, "pid": os.getpid(), "directive_id": key, "state": "published",
        "reason": "exact_result_reference_published", "reply": to_dict(reply), "view": to_dict(view),
        "publication_ref": to_dict(role_ref), "result_ref": to_dict(result_ref), "candidate": None,
        "git_path": None, "exact_terminal_replay": True,
        "request_sha256": reply.binding.normalized_worker_request_sha256}
    controller = ProjectRun.__new__(ProjectRun)
    controller.output = fixture.root / "controller"
    (controller.output / "unchanged-repairs").mkdir(parents=True)
    controller.frozen, controller.finance, controller.seed = False, finance, mesh
    controller.context = fixture.context
    controller.processes = {actor: SimpleNamespace(pid=os.getpid())}
    controller.completions = {action.request_id: result}
    controller.directives, controller.directive_policies = {action.request_id: directive}, {action.request_id: policy}
    controller.repair_plans, controller.corrections = {action.request_id: plan}, {}
    controller.unchanged_repairs, controller.pending_unchanged_repairs = {}, ()
    controller.frontier_seal = FrozenFrontier()
    controller.current_failure = lambda: (target, gate)
    contributions = {identity(topic.offer): named_sources({path: body for path, body in topic.base_files
                                                         if path in topic.scope}) for topic in topics}
    controller.verified_contribution = lambda offer: contributions[identity(offer)]
    return SimpleNamespace(fixture=fixture, controller=controller, plan=plan, actor=actor, result=result,
        proof=proof, action=action, lease=lease, view=view, result_ref=result_ref, role_ref=role_ref, mesh=mesh)


class PeerLibraryProjectV4Tests(unittest.TestCase):
    def test_real_empty_repair_retains_exact_old_offer_and_charged_failed_call(self):
        case = real_repair_failure(self)
        run, finance = case.controller, case.controller.finance
        failure, binding = case.proof["result"], case.proof["binding"]
        self.assertEqual(str(failure), "Proposal contains no effective changes")
        self.assertEqual(failure.metadata["failure_kind"], "empty")
        self.assertEqual(failure.usage_units, 166)
        before = case.fixture.ledger.budget()
        self.assertEqual(before["spent_or_reserved"], 1166)
        retained = run.retain_unchanged_repair(case.plan, case.actor, case.result)
        self.assertEqual(retained.dispatch, binding)
        self.assertEqual(retained.result_sha256, case.proof["reply"].result_payload_sha256)
        self.assertEqual(retained.original_offer_sha256, identity(case.plan.topic("ingestion").offer))
        self.assertEqual(run.verified_unchanged_repair(retained), retained)
        frontier = repair_frontier(case.plan, (), current_failure=run.current_failure,
            unchanged=(retained,), verified_unchanged=run.verified_unchanged_repair)
        self.assertEqual(frontier, tuple(topic.offer for topic in case.plan.topics))
        self.assertIs(frontier[1], case.plan.topic("ingestion").offer)
        self.assertEqual(frontier[1].dispatch.action.work.generation, 0)
        self.assertEqual(retained.dispatch.action.work.generation, 1)
        self.assertIsNone(case.result["candidate"])
        run.pending_repair_plan, run.pending_unchanged_repairs = case.plan, (retained,)
        self.assertEqual(run.successor_generations(1, frontier),
                         (("catalog", 0), ("ingestion", 0), ("query", 0), ("clients", 0)))
        self.assertEqual(finance.submit(case.actor, case.action, case.lease), case.proof["reply"])
        self.assertEqual(finance.lookup(case.actor, case.action.request_id).state, "failed")
        self.assertEqual(case.fixture.transport.calls, 1)
        self.assertEqual(case.fixture.ledger.budget(), before)
        path = run.output / "unchanged-repairs" / (case.action.request_id + ".json")
        self.assertEqual(json.loads(path.read_text()), retained.body())
        with self.assertRaises(PilotError):
            run.retain_unchanged_repair(case.plan, case.actor, case.result)
        # A charged failed call never becomes successful candidate/release proof.
        with self.assertRaises(ValueError):
            finance.verified_terminal(binding)
        with self.assertRaises(ValueError):
            run.verified_view(binding)
        self.assertEqual(case.fixture.ledger.budget(), before)

    def test_known_malformed_repair_is_not_unchanged_source_authority(self):
        case = real_repair_failure(self, malformed=True)
        self.assertEqual(case.proof["result"].metadata["failure_kind"], "proposal")
        with self.assertRaises(PilotStop) as stopped:
            case.controller.retain_unchanged_repair(case.plan, case.actor, case.result)
        self.assertEqual(stopped.exception.outcome, "missing_repair_candidate")
        self.assertEqual(case.controller.unchanged_repairs, {})
        self.assertEqual(list((case.controller.output / "unchanged-repairs").iterdir()), [])
        self.assertEqual(case.fixture.transport.calls, 1)
        self.assertEqual(case.fixture.ledger.budget()["spent_or_reserved"], 1166)

    def test_unchanged_repair_requires_arrived_exact_evidence_and_current_registration(self):
        case = real_repair_failure(self)
        run = case.controller
        prove = lambda: run._unchanged_repair_proof(case.plan, case.actor, case.result)
        refs = (case.role_ref, case.result_ref, case.action.worker_payload_ref,
                run.directives[case.action.request_id].source_ref)
        for ref in refs:
            with self.subTest(missing=ref.kind):
                original = list(case.mesh.refs)
                case.mesh.refs.remove(ref)
                try:
                    with self.assertRaises((ValueError, PilotError, PilotStop)):
                        prove()
                finally:
                    case.mesh.refs[:] = original
        for ref in (case.role_ref, case.result_ref):
            with self.subTest(forged=ref.kind):
                original = case.mesh.blobs[ref.event_id]
                case.mesh.blobs[ref.event_id] = b'{"forged":true}'
                try:
                    with self.assertRaises((ValueError, PilotError, PilotStop)):
                        prove()
                finally:
                    case.mesh.blobs[ref.event_id] = original
        mutations = {"pid": -1, "state": "locally_failed", "exact_terminal_replay": False,
                     "request_sha256": "0" * 64, "candidate_error": "incomplete publication",
                     "git_path": "/untrusted", "result_ref": to_dict(case.role_ref), "view": {},
                     "reply": {**case.result["reply"], "usage_units": 0}}
        for key, bad in mutations.items():
            with self.subTest(completion=key):
                present, original = key in case.result, case.result.get(key)
                case.result[key] = bad
                try:
                    with self.assertRaises((ValueError, PilotError, PilotStop)):
                        prove()
                finally:
                    if present:
                        case.result[key] = original
                    else:
                        del case.result[key]
        key = case.action.request_id
        registered = run.repair_plans.pop(key)
        with self.assertRaises(PilotError):
            prove()
        run.repair_plans[key] = registered
        current = run.current_failure
        changed_gate = replace(case.plan.failure_receipt, blockers=("new_public_failure",))
        run.current_failure = lambda: (case.plan.failed_target, changed_gate)
        with self.assertRaisesRegex(ValueError, "stale"):
            prove()
        run.current_failure = current
        original_contribution = run.verified_contribution
        run.verified_contribution = lambda offer: ()
        with self.assertRaisesRegex(PilotError, "source"):
            prove()
        run.verified_contribution = original_contribution
        retained = run.retain_unchanged_repair(case.plan, case.actor, case.result)
        path = run.output / "unchanged-repairs" / (key + ".json")
        raw = path.read_bytes()
        changed = retained.body()
        changed["result_sha256"] = "0" * 64
        path.write_text(json.dumps(changed))
        with self.assertRaisesRegex(PilotError, "durable"):
            run.verified_unchanged_repair(retained)
        path.write_bytes(raw)
        run.unchanged_repairs.clear()
        with self.assertRaisesRegex(PilotError, "durable"):
            run.verified_unchanged_repair(retained)
        run.unchanged_repairs[key] = retained
        self.assertEqual(run.verified_unchanged_repair(retained), retained)
        self.assertEqual(case.fixture.transport.calls, 1)

    def test_unchanged_repair_cannot_arrive_after_successor_seal_or_cohort_freeze(self):
        case = real_repair_failure(self)
        run = case.controller
        run.frozen = True
        with self.assertRaisesRegex(PilotError, "freeze"):
            run.retain_unchanged_repair(case.plan, case.actor, case.result)
        run.frozen = False
        run.frontier_seal.seal(1, tuple(topic.offer for topic in case.plan.topics))
        with self.assertRaisesRegex(PilotError, "seal"):
            run.retain_unchanged_repair(case.plan, case.actor, case.result)
        self.assertEqual(run.unchanged_repairs, {})
        self.assertEqual(list((run.output / "unchanged-repairs").iterdir()), [])
        self.assertEqual(case.fixture.transport.calls, 1)
        self.assertEqual(case.fixture.ledger.budget()["spent_or_reserved"], 1166)

    def test_successor_registry_distinguishes_old_repair_from_current_round_replacement(self):
        # Registry/Git authenticity is supplied by the host; this tests the real
        # work-loop filter and exact final generation policy, with typed offers.
        case = real_repair_failure(self)
        prior = case.plan.topic("catalog").offer
        catalog = replace(prior, dispatch=replace(prior.dispatch,
            action=replace(prior.dispatch.action, kind="repair", work=work_key("B01", "repair", 1))))
        topics = tuple(replace(topic, offer=catalog if topic.package_id == "catalog" else topic.offer,
                               repair=topic.package_id in ("catalog", "ingestion"),
                               repair_generation=2) for topic in case.plan.topics)
        target = replace(case.plan.failed_target, generation=1)
        gate = replace(case.plan.failure_receipt, target_sha256=identity(target))
        plan = replace(case.plan, failed_target=target, failure_receipt=gate, topics=topics)
        prior = plan.topic("ingestion").offer
        replacement = replace(prior, commit_oid="7" * 40, dispatch=replace(prior.dispatch,
            action=replace(prior.dispatch.action, kind="repair", work=work_key("B05", "repair", 2))))
        offers = tuple(replacement if topic.package_id == "ingestion" else topic.offer for topic in topics)
        repair_dispatch = replace(catalog.dispatch, action=replace(catalog.dispatch.action,
            work=work_key("B01", "repair", 2), action_id="current-catalog-action",
            request_id="current-catalog-request"), call_id="current-catalog-call")
        retained = UnchangedTopic("catalog", plan.sha256, identity(catalog), repair_dispatch, "3" * 64)
        run = case.controller
        run.pending_repair_plan, run.pending_unchanged_repairs = plan, (retained,)
        # This fixture isolates the work-loop filter; the preceding tests use
        # actual finance and arrived evidence for the verifier itself.
        def trusted_registered_disposition(value):
            self.assertEqual(value, retained)
            return retained
        run.verified_unchanged_repair = trusted_registered_disposition
        run.current_failure = lambda: (target, gate)
        self.assertEqual(run.successor_generations(2, offers),
                         (("catalog", 1), ("ingestion", 2), ("query", 0), ("clients", 0)))
        for wrong_round, bad in ((1, offers), (2, (*offers, replacement)),
                                (2, (*offers[:3], offers[2])),
                                (2, (replace(catalog, commit_oid="6" * 40), *offers[1:]))):
            with self.subTest(round=wrong_round, offer_ids=tuple(identity(item) for item in bad)):
                with self.assertRaises((ValueError, PilotError)):
                    run.successor_generations(wrong_round, bad)

    def test_config_has_finite_deadline_and_fixed_executor_and_request_limits(self):
        self.assertEqual(ProjectConfig(), ProjectConfig(5400, 0.08, 4, 120))
        invalid = {
            "deadline_seconds": (True, 239, 7201, 5400.0, float("nan"), float("inf")),
            "interval": (True, 0.039, 0.251, float("nan"), float("inf")),
            "executor_slots": (True, 4.0, 0, 20),
            "worker_timeout": (True, 0, 119, 121, float("nan"), float("inf")),
        }
        for field, values in invalid.items():
            for value in values:
                with self.subTest(field=field, value=value), self.assertRaises(PilotError):
                    ProjectConfig(**{field: value})

    def test_complete_maximum_sequence_fits_without_using_all_prospective_slots(self):
        budget = ActionBudget()
        for action in full_budget_sequence():
            budget.reserve(*action)
        self.assertEqual(len(budget.actions), 54)
        self.assertEqual(Counter("builder" if kind in ("build", "repair") else kind
                                 for _, kind, _, _ in budget.actions),
                         {"builder": 24, "select_source": 6, "review": 24})
        self.assertEqual(sum(actor == "R1" for actor, _, _, _ in budget.actions), 12)
        self.assertEqual(len(action_roster()), 78)
        self.assertEqual(len(set(action_roster())), 78)
        self.assertGreater(sum(call_limit(actor) for actor in ROLES), 54)

    def test_admission_rejects_boolean_and_float_aliases_without_charging(self):
        for action in (("B01", "build", False, False), ("B01", "build", 0.0, False),
                       ("B01", "build", 0, 0), ("R1", "review", True, False),
                       ("R1", "select_source", 0, 1)):
            budget = ActionBudget()
            with self.subTest(action=action), self.assertRaises(PilotError):
                budget.reserve(*action)
            self.assertEqual(budget.actions, [])

    def test_duplicate_action_and_second_same_package_repair_do_not_consume_allowance(self):
        budget = ActionBudget()
        budget.reserve("B01", "build", 0)
        budget.reserve("B01", "repair", 1)
        expected = budget.actions.copy()
        for action in (("B01", "build", 0, False), ("B02", "repair", 1, False),
                       ("B01", "repair", 3, False), ("R2", "select_source", 0, False)):
            with self.subTest(action=action), self.assertRaises(PilotError):
                budget.reserve(*action)
            self.assertEqual(budget.actions, expected)
        budget.reserve("B05", "repair", 1)
        budget.reserve("B01", "repair", 2)
        self.assertEqual(len(budget.actions), 4)

    def test_correction_requires_original_action_and_cannot_reset_per_target(self):
        for kind in ("review", "select_source"):
            budget = ActionBudget()
            with self.subTest(kind=kind), self.assertRaises(PilotError):
                budget.reserve("R1", kind, 0, True)
            self.assertEqual(budget.actions, [])
            budget.reserve("R1", kind, 0, False)
            budget.reserve("R1", kind, 0, True)
            with self.assertRaises(PilotError):
                budget.reserve("R1", kind, 0, True)
            with self.assertRaises(PilotError):
                budget.reserve("R1", kind, 1, True)
            self.assertEqual(len(budget.actions), 2)

    def test_candidate_scopes_do_not_grant_selection_or_review_writes(self):
        scopes = exact_scopes()
        owned = [path for paths in scopes.values() for path in paths]
        self.assertEqual(len(owned), len(set(owned)))
        for actor in BUILDERS:
            self.assertEqual(allowed(actor, "build"), scopes[package_for(actor)])
            self.assertNotIn("review.json", allowed(actor, "build"))
            self.assertNotIn("selection.json", allowed(actor, "build"))
        self.assertEqual(allowed("R1", "select_source"), ("selection.json",))
        for actor in REVIEWERS:
            self.assertEqual(allowed(actor, "review"), ("review.json",))

    def test_contract_preparation_is_mode_and_credential_independent(self):
        with patch.object(OpenAIWorker, "run", side_effect=AssertionError("No calls during design")), \
                patch("gossip_harness.peer_library_project_v4.fingerprints", return_value={"module.py": "a" * 64}), \
                patch("gossip_harness.peer_library_project_v4.runtime_identity", return_value={"runtime": "fixture"}):
            before = execution_contract()
            with patch.dict("os.environ", {"OPENAI_API_KEY": "test-canary-not-a-credential",
                                          "OPENAI_PROJECT_ID": "test-project"}):
                after = execution_contract()
        self.assertEqual(before, after)
        self.assertNotIn("test-canary", json.dumps(after))
        self.assertNotIn("mode", before["execution_design"])
        self.assertEqual(before["sources"], before["execution_design"]["sources"])
        self.assertFalse(before["execution_design"]["prospective"]["policy"]["live_enabled"])

    def test_fixture_permit_cannot_claim_live_qualification_or_a_provider_transport(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            ledger_path = root / "disposable.sqlite"
            Ledger(ledger_path, 100_000_000)
            with patch.object(OpenAIWorker, "run", side_effect=AssertionError("No calls during enrollment")):
                contract = execution_contract()
                fixture = enrollment(root / "fixture", contract, ledger_path, mode="fixture")
                live = enrollment(root / "live", contract, ledger_path, mode="live")
                permit = fixture_permit(fixture, contract)
            self.assertNotEqual(fixture["transport_identity"], live["transport_identity"])
            self.assertEqual(fixture["task_specs"], live["task_specs"])
            identities = [json.dumps(spec["work"], sort_keys=True) for spec in fixture["task_specs"]]
            self.assertEqual(len(identities), 78)
            self.assertEqual(len(set(identities)), 78)
            self.assertEqual(permit["mode"], "fixture")
            self.assertIsNone(permit["qualification"])
            self.assertEqual(permit["expected_opening_usage"], 0)
            with self.assertRaises(PilotError):
                enrollment(root / "ambiguous", contract, ledger_path, mode="auto")

    def test_child_environment_removes_provider_keys_and_preserves_runtime_tools(self):
        original = {"PATH": "/usr/bin", "HOME": "/tmp/home", "LANG": "en_US.UTF-8",
            "OPENAI_API_KEY": "canary", "OPENAI_PROJECT_ID": "canary", "OPENAI_ORG_ID": "canary",
            "AWS_ACCESS_KEY_ID": "canary", "AWS_SECRET_ACCESS_KEY": "canary", "AWS_SESSION_TOKEN": "canary",
            "custom_API_KEY": "canary", "service_access_token": "canary", "SERVICE_SECRET_KEY": "canary"}
        self.assertEqual(sanitized_child_environment(original),
                         {"PATH": "/usr/bin", "HOME": "/tmp/home", "LANG": "en_US.UTF-8"})
        self.assertIn("OPENAI_API_KEY", original)

    def test_import_does_not_load_authored_candidate_implementations_or_private_acceptance(self):
        # A clean process avoids imported fixtures from other test modules. This
        # exercises Python imports only; no pilot function or transport runs.
        code = ("import json,sys; import gossip_harness.peer_library_project_v4; "
                "print(json.dumps(sorted(name for name in sys.modules if "
                "name.startswith('gossip_harness.library_m1_'))))")
        result = subprocess.run([sys.executable, "-c", code], cwd=Path(__file__).resolve().parents[1],
            env=sanitized_child_environment(dict(os.environ)),
            capture_output=True, text=True, timeout=30, check=True)
        self.assertEqual(json.loads(result.stdout), [])

    def test_public_run_defaults_to_fixture_and_passes_no_external_capabilities(self):
        # Only the outer admission boundary is exercised. The real project run
        # is replaced explicitly, so this is not physical pilot evidence.
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary).resolve() / "fresh-output"
            sentinel = {"fixture_boundary_only": True}
            with patch("gossip_harness.peer_library_project_v4.ProjectRun") as project:
                project.return_value.run.return_value = sentinel
                self.assertEqual(run_project(output), sentinel)
                project.assert_called_once_with(output, ProjectConfig(), "fixture", None, None, None, None)
                project.return_value.run.assert_called_once_with()
            self.assertTrue(output.is_dir())

    def test_ambiguous_or_unqualified_live_start_has_no_output_or_runner_side_effect(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            updates = ({"mode": "live"}, {"mode": "auto"}, {"mode": "fixture", "workers": {}},
                {"mode": "fixture", "ledger_path": root / "external.sqlite"},
                {"mode": "fixture", "permit": {}},
                {"mode": "fixture", "expected_permit_sha256": "a" * 64})
            with patch("gossip_harness.peer_library_project_v4.ProjectRun") as project:
                for index, update in enumerate(updates):
                    output = root / str(index)
                    with self.subTest(update=update), self.assertRaises(PilotError):
                        run_project(output, **update)
                    self.assertFalse(output.exists())
                project.assert_not_called()

    def test_existing_or_indirect_output_is_preserved_and_never_reused(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            existing = root / "retained-observation"
            existing.mkdir()
            (existing / "evidence.txt").write_text("retained failure\n")
            indirect = root / "alias"
            indirect.symlink_to(existing, target_is_directory=True)
            with patch("gossip_harness.peer_library_project_v4.ProjectRun") as project:
                for output in (existing, indirect / "new-observation"):
                    with self.subTest(output=output), self.assertRaises(PilotError):
                        run_project(output)
                project.assert_not_called()
            self.assertEqual((existing / "evidence.txt").read_text(), "retained failure\n")
            self.assertFalse((existing / "new-observation").exists())

    def role_fixture(self, root, *, policies=("c" * 64, "d" * 64), mesh=None):
        mesh = mesh or FakeMesh("B01")
        finance = FakeFinance(mesh, lambda: 100.0)
        source = mesh.arrive("seed", "project-source", canonical_payload({
            "files": {"library/catalog/store.py": "old source\n", "README.md": "public requirement\n"},
            "base_sha": "a" * 40}))
        directive = WorkDirective(Context("a" * 64, "fixture", "registration", 1, "b" * 64),
            work_key("B01", "build", 0), "mini", "build", source, (),
            ("library/catalog/store.py",), builder_prompt("B01"))
        loop = ProjectRoleLoopV4(root, "B01", mesh, finance, call_limit=3, policy_sha256="e" * 64,
            allowed_policies=policies, result_producer="finance", clock=lambda: 100.0)
        return loop, mesh, finance, directive

    def test_dynamic_policy_must_be_registered_before_full_materialization(self):
        with tempfile.TemporaryDirectory() as temporary:
            loop, mesh, finance, directive = self.role_fixture(Path(temporary))
            try:
                with self.assertRaises(PilotError):
                    loop._materialize(directive)
                with self.assertRaises(PilotError):
                    loop.register(directive, "f" * 64)
                self.assertEqual(loop.snapshots(), ())
                key = loop.register(directive, "c" * 64)
                self.assertEqual(key, directive_id(directive))
                request, view = loop._materialize(directive)
                self.assertEqual(view.policy_sha256, "c" * 64)
                self.assertEqual(request.files["README.md"], "public requirement\n")
                self.assertEqual(request.files["library/catalog/store.py"], "old source\n")
                self.assertEqual(finance.invocations, 0)
                self.assertEqual(loop.register(directive, "c" * 64), key)
                self.assertEqual(len(loop.snapshots()), 1)
                with self.assertRaises(PilotError):
                    loop.register(directive, "d" * 64)
            finally:
                loop.close()

    def test_registered_policy_cannot_change_when_the_same_journal_is_reopened(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            loop, mesh, _, directive = self.role_fixture(root)
            loop.register(directive, "c" * 64)
            loop.close()
            reopened, _, _, same = self.role_fixture(root, mesh=mesh)
            try:
                self.assertEqual(same, directive)
                with self.assertRaises(PilotError):
                    reopened.register(same, "d" * 64)
                self.assertEqual(reopened.register(same, "c" * 64), directive_id(same))
            finally:
                reopened.close()

    def test_changing_closed_policy_allowlist_cannot_reuse_an_existing_role_journal(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            loop, mesh, _, directive = self.role_fixture(root)
            loop.register(directive, "c" * 64)
            loop.close()
            with self.assertRaises(RoleError):
                changed, _, _, _ = self.role_fixture(root, policies=("c" * 64, "f" * 64), mesh=mesh)
                changed.close()

    def test_failed_policy_open_releases_journal_ownership_for_repair(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            loop, mesh, _, directive = self.role_fixture(root)
            key = loop.register(directive, "c" * 64)
            path = loop.policy_root / (key + ".json")
            retained = path.read_bytes()
            loop.close()
            path.unlink()  # Deliberate damage to this disposable fixture only.
            try:
                self.role_fixture(root, mesh=mesh)
            except PilotError as failure:
                # Keep the failed constructor's traceback alive: cleanup must
                # not depend on garbage collection releasing its file lock.
                self.assertIn("policy", str(failure).lower())
                path.write_bytes(retained)
                restored, _, _, same = self.role_fixture(root, mesh=mesh)
                try:
                    self.assertEqual(restored.register(same, "c" * 64), key)
                finally:
                    restored.close()
            else:
                self.fail("Missing retained policy registration was accepted")

    def test_corrected_selector_slot_retains_actual_view_and_receiver_terminal_authentication(self):
        mesh, policy, original, files = synthetic_selector()
        request, _ = materialize_project(original, mesh, policy)
        corrected, outcome, validation = _format_correction(original, request, "R1", "{malformed", ("Invalid JSON",))
        binding, role_ref, proof = synthetic_terminal(mesh, corrected, policy, {"selection.json": "{}"})
        controller = ProjectRun.__new__(ProjectRun)
        controller.seed = mesh
        controller.directives = {binding.action.request_id: corrected}
        controller.directive_policies = {binding.action.request_id: policy}
        controller.repair_plans = {}
        controller.corrections = {binding.action.request_id: (request, "R1", outcome, validation)}
        selector = ContributionSlot("R1", original.work, "strong", "f" * 64, ("selection.json",),
            "a" * 40, hashlib.sha256(original.instructions.encode()).hexdigest())
        slots = tuple(ContributionSlot(actor, work_key(actor, "build", 0), "mini", "f" * 64,
            (f"library/{package_for(actor)}/main.py",), "a" * 40, "e" * 64)
            for actor in ("B01", "B05", "B09", "B13"))
        proofs = {identity(binding): proof}
        with tempfile.TemporaryDirectory() as temporary:
            registry = EvidenceRegistryV4(Path(temporary).resolve() / "receiver", mesh,
                context=original.context, authority_config_sha256="a" * 64,
                package_scopes={package: ("library/" + package,) for package in
                                ("catalog", "ingestion", "query", "clients")},
                slots=slots, selector=selector, bases={"a" * 40: files},
                eligibility_policy_sha256=policy, view_policies={("select_source", 0): policy},
                verified_terminal=lambda candidate: proofs[identity(candidate)], verified_view=controller.pure_view)
            try:
                actual, result, view = registry._terminal(binding, role_ref, selector)
                self.assertEqual(actual.files, request.files)
                self.assertEqual(actual.instructions, request.instructions)
                self.assertEqual(view.evidence_refs, original.evidence_refs)
                self.assertEqual(result.changes, {"selection.json": "{}"})
                # This authenticates a complete response; {} remains an invalid
                # selection and has not become release or candidate evidence.
                self.assertEqual(registry._records(), {})
                proofs[identity(binding)] = {**proof, "reply": replace(proof["reply"], state="failed")}
                with self.assertRaises(EvidenceError):
                    registry._terminal(binding, role_ref, selector)
                proofs[identity(binding)] = proof
                controller.corrections[binding.action.request_id] = (
                    replace(request, files={**request.files, "README.md": "changed context"}), "R1", outcome, validation)
                with self.assertRaises(ValueError):
                    registry._terminal(binding, role_ref, selector)
            finally:
                registry.close()

    def test_review_round_corrects_only_malformed_peer_and_preserves_valid_rejections(self):
        with tempfile.TemporaryDirectory() as temporary:
            controller, args, rounds, recorded = synthetic_review_round(Path(temporary), malformed=True)
            result, verdicts = controller.review(*args)
            self.assertEqual(result.status, "synthetic-rejection")
            self.assertEqual(len(verdicts), 8)
            self.assertTrue(all(verdict.verdict == "request_changes" for verdict in verdicts))
            self.assertEqual(Counter(actor for actor, _, _, _ in controller.action_budget.actions),
                             {"R1": 1, "R2": 2, "R3": 1, "R4": 1})
            self.assertEqual([action for action in controller.action_budget.actions if action[3]],
                             [("R2", "review", 0, True)])
            self.assertEqual([number for number, _ in rounds], [0, 1])
            self.assertEqual([actor for actor, _ in rounds[1][1]], ["R2"])
            self.assertEqual({binding.action.actor for binding in recorded}, set(REVIEWERS))
            self.assertEqual(len(recorded), 4)
            self.assertEqual(len(tuple((Path(temporary) / "corrections").glob("*.json"))), 1)

    def test_review_round_has_no_third_format_attempt_or_retry_of_provider_failure(self):
        for updates, expected, calls in (({"malformed": True, "malformed_correction": True}, "format_exhausted", 5),
                                        ({"failed_provider": True}, "review_provider_failure", 4)):
            with self.subTest(updates=updates), tempfile.TemporaryDirectory() as temporary:
                controller, args, _, _ = synthetic_review_round(Path(temporary), **updates)
                with self.assertRaises(PilotStop) as stopped:
                    controller.review(*args)
                self.assertEqual(stopped.exception.outcome, expected)
                self.assertEqual(len(controller.action_budget.actions), calls)
                self.assertLessEqual(sum(action[3] for action in controller.action_budget.actions), 1)

    def test_sealed_frontier_rejects_late_candidates_removal_reordering_and_generation_aliases(self):
        offers = tuple(synthetic_offer(actor, token=index) for index, actor in
                       enumerate(("B01", "B05", "B09", "B13"), 1))
        frontier = FrozenFrontier()
        expected = tuple(identity(offer) for offer in offers)
        self.assertEqual(frontier.seal(0, offers), expected)
        self.assertEqual(frontier.seal(0, offers), expected)
        for altered in (offers[:-1], offers[::-1], (*offers, synthetic_offer("B02", token=5))):
            with self.subTest(altered=len(altered)), self.assertRaises(PilotError):
                frontier.seal(0, altered)
            self.assertEqual(frontier.generations[0], expected)
        for generation, altered in ((False, offers), (0.0, offers), (3, offers),
                                    (1, ()), (1, list(offers)), (1, (offers[0], offers[0]))):
            with self.subTest(generation=generation), self.assertRaises(PilotError):
                frontier.seal(generation, altered)
        successor = tuple(synthetic_offer(actor, generation=1, token=index + 10)
                          for index, actor in enumerate(("B01", "B05", "B09", "B13"), 1))
        self.assertEqual(frontier.seal(1, successor), tuple(identity(offer) for offer in successor))
        self.assertEqual(frontier.generations[0], expected)

    def test_repair_owners_follow_public_failures_and_current_scoped_gate(self):
        scoped = SimpleNamespace(rejected_slots=(("R1", "clients"), ("R3", "query")),
                                 missing_slots=(("R2", "catalog"),))
        self.assertEqual(repair_packages_from_gate(scoped, ()), ("catalog", "query", "clients"))
        self.assertEqual(repair_packages_from_gate(scoped, ("actual-public-provenance-failure",)),
                         ("catalog", "ingestion", "query", "clients"))
        clear = SimpleNamespace(rejected_slots=(), missing_slots=())
        self.assertEqual(repair_packages_from_gate(clear, ()), ())

    def test_negative_partial_cohort_retains_unknown_and_excludes_previous_spend(self):
        with tempfile.TemporaryDirectory() as temporary:
            ledger = Path(temporary) / "accounting.sqlite"
            accounting_database(ledger, [
                {"call": "old", "cohort": "previous", "spent": 90},
                {"call": "failed", "action_state": "failed", "spent": 7},
                {"call": "unknown", "actor": "B05", "action_state": "unknown", "state": "reserved", "spent": None},
            ])
            completed = {"one": completion("failed"), "two": completion("unknown")}
            observed = accounting_snapshot(ledger, "current", completed, "fixture")
            self.assertEqual(observed["admitted_calls"], 2)
            self.assertEqual(observed["settled_reservations"], 1)
            self.assertEqual(observed["unsettled_call_ids"], ["unknown"])
            self.assertEqual(observed["known_spend_micro_usd"], 7)
            self.assertTrue(observed["complete_bindings_reconciled"])
            self.assertFalse(observed["api_spend"])
            self.assertEqual({row["action_state"] for row in observed["calls"]}, {"failed", "unknown"})
            with sqlite3.connect("file:" + str(ledger) + "?mode=ro", uri=True) as db:
                self.assertEqual(db.execute("SELECT spent,state FROM reservations WHERE id=?",
                                           ("unknown",)).fetchone(), (None, "reserved"))

    def test_accounting_detects_missing_wrong_and_extra_completion_bindings(self):
        with tempfile.TemporaryDirectory() as temporary:
            ledger = Path(temporary) / "accounting.sqlite"
            accounting_database(ledger, [{"call": "known"}])
            for completed in ({}, {"one": completion("known", "wrong-reservation")},
                              {"one": completion("known"), "two": completion("extra")}):
                with self.subTest(completed=completed):
                    observed = accounting_snapshot(ledger, "current", completed, "fixture")
                    self.assertFalse(observed["complete_bindings_reconciled"])
                    self.assertEqual(observed["admitted_calls"], 1)

    def test_orphan_financial_action_is_not_reported_as_an_empty_reconciled_cohort(self):
        with tempfile.TemporaryDirectory() as temporary:
            ledger = Path(temporary) / "accounting.sqlite"
            accounting_database(ledger, [{"call": "orphan", "missing_reservation": True}])
            with self.assertRaises(PilotError):
                accounting_snapshot(ledger, "current", {}, "fixture")

    def test_duplicate_completion_call_identity_cannot_collapse_into_one_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            ledger = Path(temporary) / "accounting.sqlite"
            accounting_database(ledger, [{"call": "known"}])
            with self.assertRaises(PilotError):
                accounting_snapshot(ledger, "current", {"one": completion("known"),
                                                       "two": completion("known")}, "fixture")

    def test_waiting_budget_is_not_terminal_while_known_pending_settlement_can_free_headroom(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            controller = admission_wait_fixture(root, "cohort_budget", [
                {"call": "pending", "cohort": COHORT, "action_state": "pending", "spent": None, "state": "reserved"}],
                cohort_cap=100, global_cap=100)
            self.assertIsNone(controller.check_admission_wait("B01", "action-key"))
            with sqlite3.connect(controller.ledger_path) as db:
                db.execute("UPDATE financial_actions_v2 SET state='completed'")
                db.execute("UPDATE reservations SET state='settled',spent=80")
            with self.assertRaises(PilotStop) as stopped:
                controller.check_admission_wait("B01", "action-key")
            self.assertEqual(stopped.exception.outcome, "budget_exhausted")

    def test_budget_wait_respects_global_prior_spend_without_falsely_stopping_when_headroom_exists(self):
        for prior, expected_stop in ((900, True), (100, False)):
            with self.subTest(prior=prior), tempfile.TemporaryDirectory() as temporary:
                controller = admission_wait_fixture(Path(temporary).resolve(), "global_budget", [
                    {"call": "prior", "cohort": "previous-cohort", "spent": prior},
                    {"call": "current", "cohort": COHORT, "spent": 50}])
                if expected_stop:
                    with self.assertRaises(PilotStop) as stopped:
                        controller.check_admission_wait("B01", "action-key")
                    self.assertEqual(stopped.exception.outcome, "budget_exhausted")
                else:
                    self.assertIsNone(controller.check_admission_wait("B01", "action-key"))

    def test_admission_call_limit_and_unknown_halt_are_terminal_without_a_new_invocation(self):
        for reason, outcome in (("cohort_call_limit", "call_limit_exhausted"),
                                ("cohort_halted", "unknown_provider_outcome")):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as temporary:
                controller = admission_wait_fixture(Path(temporary).resolve(), reason, [])
                with self.assertRaises(PilotStop) as stopped:
                    controller.check_admission_wait("B01", "action-key")
                self.assertEqual(stopped.exception.outcome, outcome)

    def test_capacity_stop_is_retained_as_capacity_even_if_role_process_already_exited(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            marker = root / "roles" / "B01" / "capacity-stop.json"
            marker.parent.mkdir(parents=True)
            marker.write_text(json.dumps({"outcome": "context_capacity_exhausted", "detail": "Full view exceeds bound"}))
            controller = ProjectRun.__new__(ProjectRun)
            controller.output, controller.deadline = root, time.monotonic() + 60
            controller.processes = {"B01": SimpleNamespace(poll=lambda: 0)}
            with self.assertRaises(PilotStop) as stopped:
                controller.wait(lambda: False)
            self.assertEqual(stopped.exception.outcome, "context_capacity_exhausted")
            self.assertIn("Full view", stopped.exception.detail)

    def test_whole_cohort_freeze_blocks_later_model_dispatch_without_consuming_allowance(self):
        mesh, policy, directive, _ = synthetic_selector()
        controller = ProjectRun.__new__(ProjectRun)
        controller.seed, controller.deadline, controller.frozen = mesh, time.monotonic() + 60, True
        controller.action_budget = ActionBudget()
        with self.assertRaisesRegex(PilotError, "freeze"):
            controller.dispatch("R1", directive, policy, generation=0)
        self.assertEqual(controller.action_budget.actions, [])


class PeerLibraryProjectV4TCPTests(unittest.TestCase):
    """Production financial-service composition over real loopback TCP.

    This uses a real disposable V3 authority and its injected offline worker.
    Candidate code, Docker, credentials and the production ledger are unused.
    """

    def test_production_service_accepts_v3_finance_and_preserves_guard_settlement_and_replay(self):
        # Reuse only setup/cleanup and fixtures; do not inherit or rediscover the
        # separate financial authority regression class.
        from tests import test_peer_financial_authority_v3 as authority_fixture
        from gossip_harness.peer_financial_rpc_v2 import FinancialClient

        fixture = authority_fixture.PeerFinancialAuthorityV3Tests()
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        repository = Path(__file__).resolve().parents[1]
        for name in ("gossip_harness/peer_financial_rpc_v2.py", "gossip_harness/peer_financial_rpc_v3.py"):
            fixture.permit["sources"][name] = hashlib.sha256((repository / name).read_bytes()).hexdigest()
        finance = fixture.open()
        controller = ProjectRun.__new__(ProjectRun)
        controller.finance, controller.server, controller.server_thread = finance, None, None
        arrived, guarded = set(), []

        def exact_guard(action):
            guarded.append(action)
            if action.worker_payload_ref not in arrived:
                raise FileNotFoundError("Exact worker reference has not arrived in this fixture")
            self.assertEqual(action.context, fixture.context)
            self.assertEqual(action.actor, "B01")
            self.assertEqual(action.worker_payload_ref.producer, action.actor)
            raw = fixture.payloads.read_owned(action.actor, action.worker_payload_ref.payload_sha256)
            self.assertEqual(hashlib.sha256(raw).hexdigest(), action.worker_payload_ref.payload_sha256)

        controller.payloads = SimpleNamespace(request_guard=exact_guard,
            request_guard_sha256=hashlib.sha256(b"project-v4-tcp-exact-reference-fixture-guard-v1").hexdigest())

        def stop_service():
            # Register before startup, so partial construction is cleaned too.
            if controller.server is not None:
                if controller.server_thread is not None and controller.server_thread.is_alive():
                    controller.server.shutdown()
                controller.server.server_close()
            if controller.server_thread is not None:
                controller.server_thread.join(3)
                self.assertFalse(controller.server_thread.is_alive(), "Owned RPC server thread did not stop")

        self.addCleanup(stop_service)
        capability = hashlib.sha256(b"project-v4-local-fixture-B01-capability").hexdigest()
        controller.start_financial_service({"B01": capability})
        self.assertTrue(controller.server_thread.is_alive())
        self.assertIs(controller.server.rpc.finance, finance)
        self.assertEqual(controller.server.rpc.config["financial_config_sha256"], finance.config_sha256)
        self.assertEqual(finance.config["mode"], "fixture")
        self.assertEqual(finance.config["observation_kind"], "simulated")
        client = FinancialClient(controller.server.server_address[1], "B01", capability,
                                 fixture.context.execution_contract_sha256)
        self.assertIsNone(client.lookup("unknown-action"))
        lease = client.claim(fixture.context, fixture.work, "tcp-claim", ttl=60)
        self.assertEqual(client.claim(fixture.context, fixture.work, "tcp-claim", ttl=60), lease)
        action = fixture.action(number="tcp-action")
        waiting = client.submit(action, lease)
        self.assertEqual((waiting.state, waiting.reason), ("waiting", "payload_unavailable"))
        self.assertEqual(fixture.transport.calls, 0)
        self.assertEqual(fixture.ledger.budget()["spent_or_reserved"], 1000)
        arrived.add(action.worker_payload_ref)
        admitted = client.submit(action, lease)
        self.assertIn(admitted.state, ("pending", "publication_pending", "completed"))
        deadline = time.monotonic() + 5
        terminal = None
        while time.monotonic() < deadline:
            terminal = client.lookup(action.request_id)
            if terminal is not None and terminal.state not in ("waiting", "pending", "publication_pending"):
                break
            time.sleep(0.005)
        self.assertIsNotNone(terminal, "Injected TCP operation never returned a financial outcome")
        self.assertEqual(terminal.state, "completed")
        self.assertEqual(terminal.usage_units, 166)
        self.assertEqual(client.submit(action, lease), terminal)
        self.assertEqual(client.lookup(action.request_id), terminal)
        self.assertEqual(fixture.transport.calls, 1)
        self.assertEqual(fixture.ledger.budget()["spent_or_reserved"], 1166)
        self.assertGreaterEqual(len(guarded), 2)
        self.assertTrue(all(candidate == action for candidate in guarded))
        proof = finance.verified_terminal(terminal.binding)
        self.assertEqual(proof["result"].changes, {"src/a.py": "fixed\n"})


if __name__ == "__main__":
    unittest.main()
