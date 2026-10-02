"""Offline v2 candidate bindings and real Git crash recovery; no candidate execution."""
from __future__ import annotations

from dataclasses import replace
import hashlib
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.gitstore import GitStore
from gossip_harness.ledger import Lease
from gossip_harness.peer_candidate_v2 import (
    PROTOCOL, CandidateError, CandidatePublisher, bundle_manifest_digest, named_sources,
    result_payload_bytes, strictdecode_candidate_notice, validate_receipt_sources,
)
from gossip_harness.peer_coding_dispatch_v1 import source_digest
from gossip_harness.peer_git_bundle_v1 import BundleError, PROFILE, PROTOCOL as BUNDLE_PROTOCOL, import_bundle
from gossip_harness.peer_project_contract_v2 import (
    ActionRequest, CandidateOffer, Context, DispatchBinding, DispatchReply, EvidenceRef,
    NamedSource, WorkKey, identity, to_dict, worker_request_digest,
)
from gossip_harness.peer_store_v1 import canonical_bytes, strict_loads
from gossip_harness.worker import WorkerRequest, WorkerResult

CONTRACT = "c" * 64
FILES = {"catalog/index.py": "value = 'initial'\n", "README.md": "Read-only context.\n",
         "query/run.py": "query = 'initial'\n"}
SCOPES = {"catalog": ("catalog",), "query": ("query",)}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def binding(request, *, actor="catalog-builder", generation=0, action_id="build-catalog", kind="build"):
    action = ActionRequest(Context(CONTRACT, "cohort", "trajectory", 1, "e" * 64), action_id,
                           action_id + ":request", actor, kind,
                           WorkKey("catalog", "catalog-api", "implementation", generation), "fixture-mini",
                           EvidenceRef("a" * 64, actor, "worker-request", "b" * 64), "d" * 64)
    return DispatchBinding(action, Lease(request.task_id, actor, 1, 10.0),
                           worker_request_digest(request), "f" * 64, "1" * 64, "call", "reservation", 10)


def completed(dispatch, result, result_sha):
    return DispatchReply(dispatch.action.request_id, identity(dispatch.action), "completed", "",
                         dispatch, result_sha, result.usage_units)


def descriptor():
    return {"protocol": BUNDLE_PROTOCOL, "profile": PROFILE, "object_format": "sha1",
            "base_sha": "1" * 40, "offered_sha": "2" * 40, "offered_ref": "refs/harness/proposals/" + "2" * 40,
            "bundle_sha256": "0" * 64, "bundle_bytes": 300, "object_count": 3, "commit_count": 2,
            "expanded_bytes": 100, "objects_sha256": "0" * 64, "tree_sha": "3" * 40,
            "files_sha256": "0" * 64, "file_count": 1, "source_bytes": 10}


def metadata_offer(files):
    request = WorkerRequest("task", "Change catalog", ("catalog/index.py",), FILES, "1" * 40, 1)
    manifest = descriptor()
    offer = CandidateOffer(binding(request), "2" * 40, source_digest(files), "5" * 64,
                           bundle_manifest_digest(manifest),
                           EvidenceRef("6" * 64, "catalog-builder", "candidate-bundle", "0" * 64))
    return offer, manifest


class MemoryTransport:
    """Test fixture with exact durable command semantics retained across adapter restart."""
    node_id = "catalog-builder"

    def __init__(self):
        self.commands = {}
        self.payloads = {}
        self.physical_publications = 0

    def publish(self, kind, payload, command_id):
        expected = (kind, payload)
        if command_id in self.commands:
            prior, ref = self.commands[command_id]
            if prior != expected:
                raise ValueError("Changed idempotent publication")
            return ref
        ref = EvidenceRef(sha(command_id.encode()), self.node_id, kind, sha(payload))
        self.commands[command_id] = expected, ref
        self.payloads[ref] = payload
        self.physical_publications += 1
        return ref

    def resolve(self, ref):
        return self.payloads.get(ref)


class CandidateMetadataV2Tests(unittest.TestCase):
    def test_canonical_notice_round_trip_and_explicit_manifest_digest(self):
        offer, manifest = metadata_offer(FILES)
        raw = canonical_bytes({"protocol": PROTOCOL, "offer": to_dict(offer), "bundle_manifest": manifest})
        self.assertEqual(strictdecode_candidate_notice(raw, expected_contract_sha256=CONTRACT,
                                                      expected_actor="catalog-builder"), (offer, manifest))
        self.assertEqual(bundle_manifest_digest(manifest), sha(canonical_bytes(manifest)))

    def test_notice_rejects_extra_duplicate_noncanonical_and_wrong_contract(self):
        offer, manifest = metadata_offer(FILES)
        value = {"protocol": PROTOCOL, "offer": to_dict(offer), "bundle_manifest": manifest}
        raw = canonical_bytes(value)
        for changed in (raw + b"\n", raw.replace(b'"protocol":', b'"protocol":"extra","protocol":', 1),
                        canonical_bytes({**value, "model_proof": "approved"})):
            with self.subTest(changed=changed[:50]), self.assertRaises(ValueError):
                strictdecode_candidate_notice(changed, expected_contract_sha256=CONTRACT)
        with self.assertRaises(CandidateError):
            strictdecode_candidate_notice(raw, expected_contract_sha256="e" * 64)
        with self.assertRaises(CandidateError):
            strictdecode_candidate_notice(raw, expected_contract_sha256=CONTRACT, expected_actor="another-role")

    def test_notice_rejects_manifest_commit_and_payload_substitution(self):
        offer, manifest = metadata_offer(FILES)
        for field, value in (("offered_sha", "4" * 40), ("bundle_sha256", "5" * 64), ("file_count", 2)):
            changed = {**manifest, field: value}
            raw = canonical_bytes({"protocol": PROTOCOL, "offer": to_dict(offer), "bundle_manifest": changed})
            with self.subTest(field=field), self.assertRaises(ValueError):
                strictdecode_candidate_notice(raw, expected_contract_sha256=CONTRACT)

    def test_named_sources_use_raw_utf8_and_require_exact_membership(self):
        files = {"catalog/index.py": "value = 'café'\n", "README.md": "context"}
        offer, _ = metadata_offer(files)
        sources = named_sources(files)
        self.assertEqual(sources[1].sha256, sha(files["catalog/index.py"].encode("utf-8")))
        self.assertNotEqual(sources[1].sha256, source_digest({"catalog/index.py": files["catalog/index.py"]}))
        validate_receipt_sources(offer, files, sources, require_complete=True)
        validate_receipt_sources(offer, files, (sources[1],))
        for changed in ((sources[1], sources[1]), (NamedSource("absent.py", sources[1].sha256),),
                        (NamedSource(sources[1].path, "0" * 64),)):
            with self.subTest(changed=changed), self.assertRaises(CandidateError):
                validate_receipt_sources(offer, files, changed)
        with self.assertRaises(CandidateError):
            validate_receipt_sources(offer, files, (sources[1],), require_complete=True)
        with self.assertRaises(CandidateError):
            validate_receipt_sources(offer, {**files, "README.md": "changed"}, sources)

    def test_result_hash_covers_financial_envelope_and_strict_usage(self):
        result = WorkerResult({"catalog/index.py": "changed"}, "Exact result", 1, {})
        raw = result_payload_bytes(result)
        self.assertEqual(strict_loads(raw)["kind"], "result")
        self.assertNotEqual(sha(raw), sha(canonical_bytes(strict_loads(raw)["payload"])))
        with self.assertRaises(ValueError):
            result_payload_bytes(replace(result, usage_units=True))

    def test_named_sources_reject_unsafe_or_binary_paths(self):
        for files in ({"../outside": "x"}, {"catalog/index.py": "nul\0"}, {"catalog/é.py": "x"}):
            with self.subTest(files=files), self.assertRaises(CandidateError):
                named_sources(files)


class CandidateGitV2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("peer-candidate-v2-git", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.root = cls.artifacts.root.resolve()
        cls.seed = GitStore.create(cls.root / "seed.git", FILES)
        cls.base = cls.seed.head()

    def setUp(self):
        self.case_number = 0

    def case(self, **kwargs):
        self.case_number += 1
        root = self.root / (self._testMethodName + "-" + str(self.case_number))
        root.mkdir()
        store = GitStore.fork(self.seed, root / "role" / "repository.git")
        request = WorkerRequest("catalog-task", "Implement catalog only.", ("catalog/index.py",),
                                dict(FILES), self.base, 1)
        result = WorkerResult({"catalog/index.py": "value = 'café'\n"}, "Implemented catalog.", 1, {})
        dispatch = binding(request, **kwargs)
        case = SimpleNamespace(root=root, store=store, request=request, result=result, dispatch=dispatch,
                               transport=MemoryTransport(), proof_calls=0)
        case.result_sha = sha(result_payload_bytes(result))
        case.reply = completed(dispatch, result, case.result_sha)

        def proof(received_dispatch, received_request, received_result, received_sha):
            case.proof_calls += 1
            if (received_dispatch != case.dispatch or received_request != case.request
                    or received_result != case.result or received_sha != case.result_sha):
                raise CandidateError("Retained local journal differs")
            return case.reply

        case.proof = proof
        return case

    def publisher(self, case, **kwargs):
        return CandidatePublisher(case.root / "role" / "candidate", case.transport, case.store,
                                  actor=case.transport.node_id, baseline_sha=self.base,
                                  package_scopes=SCOPES, execution_contract_sha256=CONTRACT,
                                  completed_proof=case.proof, **kwargs)

    def publish(self, case, publisher=None):
        return (publisher or self.publisher(case)).publish(case.dispatch, case.request, case.result, case.result_sha)

    def action_directory(self, case):
        return case.root / "role" / "candidate" / sha(case.dispatch.action.action_id.encode())

    def test_exact_private_git_bundle_and_received_source_membership(self):
        case = self.case(generation=3)
        publication = self.publish(case)
        offer, manifest = strictdecode_candidate_notice(case.transport.resolve(publication.offer_ref),
                                                       expected_contract_sha256=CONTRACT)
        received = import_bundle(case.transport.resolve(offer.bundle_ref), manifest, case.root / "receiver-quarantine")
        expected = {**FILES, **case.result.changes}
        self.assertEqual(received.read_files(offer.commit_oid), expected)
        self.assertEqual(offer.source_sha256, source_digest(expected))
        self.assertEqual(offer.dispatch.action.work.generation, 3)
        validate_receipt_sources(offer, expected, named_sources(expected), require_complete=True)
        self.assertEqual(case.store.head(), self.base)
        self.assertEqual(self.seed.head(), self.base)
        self.assertEqual(case.transport.physical_publications, 2)
        self.assertFalse((case.store.path / "objects" / "info" / "alternates").exists())
        self.assertFalse((received.path / "refs" / "heads" / "accepted").exists())

    def test_restart_recovers_original_proposal_without_patching_twice(self):
        for point in ("after_intent", "after_proposal"):
            with self.subTest(point=point):
                case = self.case()

                def crash(name):
                    if name == point:
                        raise RuntimeError("Injected restart")

                with patch.object(case.store, "propose", wraps=case.store.propose) as propose:
                    with self.assertRaises(RuntimeError):
                        self.publish(case, self.publisher(case, crash_hook=crash))
                    publication = self.publish(case)
                    self.assertEqual(propose.call_count, 1)
                again = self.publish(case)
                self.assertEqual(again, publication)
                self.assertEqual(case.transport.physical_publications, 2)
                self.assertEqual(case.store._git("for-each-ref", "--format=%(objectname)", "refs/harness/proposals/"),
                                 publication.offer.commit_oid)

    def test_restart_reuses_bundle_and_idempotent_publication(self):
        for point in ("after_bundle", "after_bundle_publish", "after_publish"):
            with self.subTest(point=point):
                case = self.case()

                def crash(name):
                    if name == point:
                        raise RuntimeError("Injected restart")

                with self.assertRaises(RuntimeError):
                    self.publish(case, self.publisher(case, crash_hook=crash))
                bundle_before = (self.action_directory(case) / "bundle.json").read_bytes()
                with patch("gossip_harness.peer_candidate_v2.export_bundle", side_effect=AssertionError("Re-exported")), \
                        patch.object(case.store, "propose", side_effect=AssertionError("Re-proposed")):
                    publication = self.publish(case)
                    self.assertEqual(self.publish(case), publication)
                self.assertEqual((self.action_directory(case) / "bundle.json").read_bytes(), bundle_before)
                self.assertEqual(case.transport.physical_publications, 2)

    def test_exact_writable_paths_package_scope_and_full_base_files(self):
        cases = (
            (replace(WorkerRequest("catalog-task", "Implement", ("catalog",), dict(FILES), self.base, 1)),
             {"catalog/index.py": "changed"}),
            (WorkerRequest("catalog-task", "Implement", ("query/run.py",), dict(FILES), self.base, 1),
             {"query/run.py": "changed"}),
            (WorkerRequest("catalog-task", "Implement", ("catalog/index.py",),
                           {**FILES, "README.md": "false context"}, self.base, 1), {"catalog/index.py": "changed"}),
            (WorkerRequest("catalog-task", "Implement", ("catalog/index.py",), dict(FILES), self.base, 1),
             {"catalog/index.py": FILES["catalog/index.py"]}),
        )
        for request, changes in cases:
            with self.subTest(request=request):
                case = self.case()
                case.request, case.result = request, replace(case.result, changes=changes)
                case.dispatch = binding(request)
                case.result_sha = sha(result_payload_bytes(case.result))
                case.reply = completed(case.dispatch, case.result, case.result_sha)
                with patch.object(case.store, "propose", side_effect=AssertionError("Unexpected Git mutation")), \
                        self.assertRaises(CandidateError):
                    self.publish(case)
                self.assertEqual(case.transport.physical_publications, 0)

    def test_binding_mismatch_and_model_metadata_are_not_completion_proof(self):
        for corruption in ("request_digest", "result_digest", "actor", "kind", "base", "proof", "usage", "halt"):
            with self.subTest(corruption=corruption):
                case = self.case()
                if corruption == "request_digest":
                    case.dispatch = replace(case.dispatch, normalized_worker_request_sha256="0" * 64)
                elif corruption == "result_digest":
                    case.result_sha = "0" * 64
                elif corruption == "actor":
                    case.dispatch = binding(case.request, actor="another-role")
                elif corruption == "kind":
                    case.dispatch = binding(case.request, kind="review")
                elif corruption == "base":
                    case.request = replace(case.request, base_sha="0" * 40)
                    case.dispatch = binding(case.request)
                elif corruption == "proof":
                    case.proof = lambda *_: {"state": "completed", "model_says": "yes"}
                elif corruption == "usage":
                    case.reply = replace(case.reply, usage_units=2)
                else:
                    case.result = replace(case.result, metadata={"halt": True})
                    case.result_sha = sha(result_payload_bytes(case.result))
                    case.reply = completed(case.dispatch, case.result, case.result_sha)
                with patch.object(case.store, "propose", side_effect=AssertionError("Unexpected Git mutation")), \
                        self.assertRaises(CandidateError):
                    self.publish(case)

    def test_same_action_cannot_change_generation_or_retained_result(self):
        case = self.case()
        first = self.publish(case)
        intent_path = self.action_directory(case) / "intent.json"
        before = intent_path.read_bytes()
        case.dispatch = binding(case.request, generation=1)
        case.reply = completed(case.dispatch, case.result, case.result_sha)
        with self.assertRaises(CandidateError):
            self.publish(case)
        case.dispatch = binding(case.request)
        case.result = replace(case.result, changes={"catalog/index.py": "second patch"})
        case.result_sha = sha(result_payload_bytes(case.result))
        case.reply = completed(case.dispatch, case.result, case.result_sha)
        with self.assertRaises(CandidateError):
            self.publish(case)
        self.assertEqual(intent_path.read_bytes(), before)
        self.assertEqual(case.transport.physical_publications, 2)
        self.assertEqual(case.store._git("for-each-ref", "--format=%(objectname)", "refs/harness/proposals/"),
                         first.offer.commit_oid)

    def test_caller_mutation_after_capture_cannot_change_the_git_patch(self):
        case = self.case()
        original_proof = case.proof
        expected = {**FILES, **case.result.changes}

        def mutate_caller(*args):
            reply = original_proof(*args)
            case.result.changes["query/run.py"] = "outside allowed scope"
            case.request.files["README.md"] = "outside retained snapshot"
            return reply

        case.proof = mutate_caller
        publication = self.publish(case)
        self.assertEqual(case.store.read_files(publication.offer.commit_oid), expected)
        self.assertEqual(publication.offer.source_sha256, source_digest(expected))
        self.assertEqual(case.store.head(), self.base)

    def test_same_files_with_foreign_proposal_message_cannot_replace_original(self):
        case = self.case()

        def crash(name):
            if name == "after_bundle":
                raise RuntimeError("Injected restart")

        with self.assertRaises(RuntimeError):
            self.publish(case, self.publisher(case, crash_hook=crash))
        forged = case.store.propose(case.result.changes, message="Unrelated candidate action")
        receipt_path = self.action_directory(case) / "proposal.json"
        receipt = strict_loads(receipt_path.read_bytes())
        receipt["commit_oid"] = forged
        receipt_path.write_bytes(canonical_bytes(receipt))
        with self.assertRaisesRegex(CandidateError, "exact action"):
            self.publish(case)
        self.assertEqual(case.transport.physical_publications, 0)

    def test_failed_export_is_retained_and_never_automatically_retried(self):
        case = self.case()
        with patch("gossip_harness.peer_git_bundle_v1._inspect", side_effect=BundleError("Injected inspection failure")), \
                self.assertRaises(BundleError):
            self.publish(case)
        failures = list((case.store.path.parent / "peer-bundle-exports").glob("*/failure.json"))
        self.assertEqual(len(failures), 1)
        retained = {path: path.read_bytes() for path in failures[0].parent.iterdir() if path.is_file()}
        with patch("gossip_harness.peer_candidate_v2.export_bundle", side_effect=AssertionError("Retried failed export")), \
                self.assertRaisesRegex(CandidateError, "Interrupted bundle"):
            self.publish(case)
        self.assertEqual({path: path.read_bytes() for path in retained}, retained)
        self.assertEqual(case.transport.physical_publications, 0)

    def test_missing_proposal_after_started_intent_fails_closed(self):
        case = self.case()
        with patch.object(case.store, "propose", side_effect=RuntimeError("Interrupted before Git completion")), \
                self.assertRaises(RuntimeError):
            self.publish(case)
        with patch.object(case.store, "propose", side_effect=AssertionError("Repatched")), \
                self.assertRaisesRegex(CandidateError, "no unique retained result"):
            self.publish(case)
        self.assertEqual(case.transport.physical_publications, 0)

    def test_corrupt_retained_bundle_and_moved_base_are_rejected(self):
        case = self.case()

        def crash(name):
            if name == "after_bundle":
                raise RuntimeError("Injected restart")

        with self.assertRaises(RuntimeError):
            self.publish(case, self.publisher(case, crash_hook=crash))
        bundle_path = self.action_directory(case) / "bundle.json"
        bundle = strict_loads(bundle_path.read_bytes())
        bundle["payload_base64"] = "UEFDS2NvcnJ1cHQ="
        bundle_path.write_bytes(canonical_bytes(bundle))
        with self.assertRaises(CandidateError):
            self.publish(case)
        self.assertEqual(case.transport.physical_publications, 0)
        changed = case.store.propose({"catalog/index.py": "moved base"})
        case.store._git("update-ref", "refs/heads/accepted", changed, self.base)
        with self.assertRaises(CandidateError):
            self.publisher(case)


if __name__ == "__main__":
    unittest.main()
