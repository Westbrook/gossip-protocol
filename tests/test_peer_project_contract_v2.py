"""Offline boundary tests for evidence records, not runtime or release authority."""
from __future__ import annotations

from dataclasses import FrozenInstanceError, fields, replace
import hashlib
import json
import unittest

from gossip_harness.ledger import Lease
from gossip_harness import peer_project_contract_v2 as contract
from gossip_harness.worker import WorkerRequest


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


CONTRACT_SHA = digest("offline-execution-contract")
PAYLOAD = b'{"message":"local evidence"}'


def context() -> contract.Context:
    return contract.Context(CONTRACT_SHA, "cohort-real20", "trajectory-1", 1,
                            digest("requirements"))


def work() -> contract.WorkKey:
    return contract.WorkKey("catalog", "requirement-1", "slot-1", 2)


def evidence(label: str = "source", *, producer: str = "builder-1",
             kind: str = "source") -> contract.EvidenceRef:
    return contract.EvidenceRef(digest(label), producer, kind,
                                hashlib.sha256(PAYLOAD).hexdigest())


def local_view() -> contract.LocalViewManifest:
    source, receipt = evidence(), evidence("execution", kind="execution-receipt")
    return contract.LocalViewManifest(context(), digest("view-policy"), source,
                                      (receipt,), (source.event_id, receipt.event_id),
                                      (), digest("materialized-content"))


def action(kind: str = "build", actor: str = "builder-1") -> contract.ActionRequest:
    return contract.ActionRequest(context(), "action-1", "request-1", actor, kind,
                                  work(), "bounded-profile",
                                  evidence("worker-payload", producer=actor,
                                           kind="worker-request"),
                                  contract.identity(local_view()))


def binding(kind: str = "build", actor: str = "builder-1") -> contract.DispatchBinding:
    return contract.DispatchBinding(action(kind, actor), Lease("task-1", actor, 1, 1000.0),
                                    digest("normalized-worker-request"), digest("profile"),
                                    digest("finance-authority-config"), "call-1",
                                    "reservation-1", 100)


def reply(state: str = "completed") -> contract.DispatchReply:
    dispatch = binding()
    outcomes = {
        "waiting": (None, None, None),
        "pending": (dispatch, None, None),
        "publication_pending": (dispatch, digest("durable-result"), None),
        "completed": (dispatch, digest("durable-result"), 40),
        "failed": (dispatch, digest("durable-result"), 40),
        "unknown": (dispatch, None, None),
    }
    admission, result, usage = outcomes[state]
    return contract.DispatchReply(dispatch.action.request_id,
                                  contract.identity(dispatch.action), state,
                                  "offline fixture", admission, result, usage)


def offer() -> contract.CandidateOffer:
    return contract.CandidateOffer(binding(), "a" * 40, digest("candidate-source"),
                                   digest("durable-result"), digest("bundle-manifest"),
                                   evidence("bundle", kind="candidate-bundle"))


def selection() -> contract.SelectionManifest:
    selected_sha = contract.identity(offer())
    return contract.SelectionManifest(context(), digest("eligibility-policy"),
                                      binding("select_source"),
                                      contract.identity(local_view()), (selected_sha,),
                                      (), (contract.SelectedOffer("catalog", selected_sha),))


def target() -> contract.ReleaseTarget:
    return contract.ReleaseTarget(
        context(), 3, "repository-1", "refs/heads/public", "b" * 40,
        "sha1", "c" * 40, "d" * 40,
        (contract.NamedSource("catalog/catalog.py", digest("catalog-source")),),
        contract.identity(selection()), digest("ordered-required-suite"),
        digest("evaluator"), (evidence("execution", kind="execution-receipt"),),
    )


def verdict() -> contract.ScopeVerdict:
    return contract.ScopeVerdict(binding("review", "reviewer-1"), digest("review-result"),
                                 contract.identity(target()), "catalog-behavior",
                                 ("requirement-1",),
                                 (evidence("execution", kind="execution-receipt"),),
                                 "approve", "Named requirement and target examined.")


def roster() -> contract.RoleRoster:
    builders = tuple(contract.RoleSpec(f"builder-{index + 1}", "builder", "builder-profile",
                                       contract.PACKAGES[index // 4]) for index in range(16))
    reviewers = tuple(contract.RoleSpec(f"reviewer-{index + 1}", "reviewer", "review-profile",
                                        "all") for index in range(4))
    return contract.RoleRoster(CONTRACT_SHA, "cohort-real20", builders + reviewers,
                               80, 24, 3)


def record_samples() -> tuple[contract.Record, ...]:
    return (context(), work(), evidence(), local_view(), action(), binding(), reply(),
            offer(), selection().selected[0], selection(), target().sources[0],
            target(), verdict(), roster().roles[0], roster())


class ProjectRecordCodecTests(unittest.TestCase):
    def test_every_record_roundtrips_with_explicit_envelope_contract(self):
        records = record_samples()
        self.assertEqual({type(record) for record in records}, set(contract.RECORDS))
        for record in records:
            with self.subTest(kind=record.KIND):
                encoded = contract.encode(record, execution_contract_sha256=CONTRACT_SHA)
                for representation in (encoded, encoded.decode("utf-8")):
                    restored = contract.decode(representation, type(record),
                                               expected_contract_sha256=CONTRACT_SHA)
                    self.assertEqual(restored, record)
                    self.assertEqual(contract.identity(restored), contract.identity(record))
                self.assertEqual(contract.from_dict(type(record), contract.to_dict(record)), record)

    def test_every_constructor_rejects_wrong_types_in_every_field(self):
        for record in record_samples():
            for field in fields(record):
                with self.subTest(kind=record.KIND, field=field.name):
                    with self.assertRaises(contract.ContractError):
                        replace(record, **{field.name: object()})

    def test_every_record_rejects_unknown_and_missing_fields(self):
        for record in record_samples():
            raw = contract.to_dict(record)
            for mutation in ({**raw, "unrecognized": "value"},
                             {name: value for name, value in raw.items()
                              if name != next(iter(raw))}):
                with self.subTest(kind=record.KIND, fields=tuple(mutation)):
                    with self.assertRaises(contract.ContractError):
                        contract.from_dict(type(record), mutation)

    def test_envelope_rejects_wrong_contract_type_protocol_schema_and_fields(self):
        envelope = json.loads(contract.encode(action()))
        mutations = (
            {**envelope, "execution_contract_sha256": digest("another-contract")},
            {**envelope, "kind": contract.Context.KIND},
            {**envelope, "protocol": "peer-project-contract-v1"},
            {**envelope, "schema_version": True},
            {**envelope, "schema_version": 2.0},
            {**envelope, "schema_version": 3},
            {**envelope, "extra": 1},
            {key: value for key, value in envelope.items() if key != "body"},
        )
        for mutation in mutations:
            with self.subTest(envelope=mutation):
                with self.assertRaises(contract.ContractError):
                    contract.decode(json.dumps(mutation), contract.ActionRequest,
                                    expected_contract_sha256=CONTRACT_SHA)
        with self.assertRaises(contract.ContractError):
            contract.decode(contract.encode(action()), contract.Context,
                            expected_contract_sha256=CONTRACT_SHA)

    def test_nested_contract_must_match_envelope_on_encode_and_decode(self):
        changed = replace(action(), context=replace(context(),
                          execution_contract_sha256=digest("different-contract")))
        with self.assertRaises(contract.ContractError):
            contract.encode(changed, execution_contract_sha256=CONTRACT_SHA)
        envelope = json.loads(contract.encode(action()))
        envelope["body"]["context"]["execution_contract_sha256"] = digest("different-contract")
        with self.assertRaises(contract.ContractError):
            contract.decode(json.dumps(envelope), contract.ActionRequest,
                            expected_contract_sha256=CONTRACT_SHA)

    def test_context_free_records_need_explicit_contract_for_encoding(self):
        with self.assertRaises(contract.ContractError):
            contract.encode(work())
        self.assertEqual(contract.decode(contract.encode(work(),
                         execution_contract_sha256=CONTRACT_SHA), contract.WorkKey,
                         expected_contract_sha256=CONTRACT_SHA), work())

    def test_duplicate_members_are_rejected_at_envelope_and_nested_depths(self):
        for raw in ('{"body":{},"body":{}}',
                    '{"body":{"context":{"milestone":1,"milestone":2}}}'):
            with self.subTest(raw=raw), self.assertRaises(contract.ContractError):
                contract.strict_loads(raw)

    def test_nested_unknown_fields_and_mutable_constructor_collections_fail(self):
        raw = contract.to_dict(binding())
        raw["lease"]["global_head"] = "e" * 40
        with self.assertRaises(contract.ContractError):
            contract.from_dict(contract.DispatchBinding, raw)
        for record, field in ((local_view(), "evidence_refs"), (selection(), "selected"),
                              (target(), "sources"), (verdict(), "covered_requirement_ids"),
                              (roster(), "roles")):
            with self.subTest(kind=record.KIND), self.assertRaises(contract.ContractError):
                replace(record, **{field: list(getattr(record, field))})

    def test_nested_collections_are_immutable_and_decoded_inputs_are_detached(self):
        original = target()
        raw = contract.to_dict(original)
        restored = contract.from_dict(contract.ReleaseTarget, raw)
        raw["sources"][0]["path"] = "changed.py"
        raw["execution_receipt_refs"].clear()
        self.assertEqual(restored, original)
        self.assertIsInstance(restored.sources, tuple)
        self.assertIsInstance(restored.execution_receipt_refs, tuple)
        with self.assertRaises(FrozenInstanceError):
            restored.sources[0].path = "changed.py"
        with self.assertRaises(FrozenInstanceError):
            restored.context.milestone = 2
        with self.assertRaises(TypeError):
            restored.sources[0] = original.sources[0]
        with self.assertRaises(FrozenInstanceError):
            binding().lease.epoch = 2

    def test_public_record_helpers_fail_closed_on_unrecognized_record_types(self):
        for value in (None, "not-a-record", object(), contract.Record()):
            with self.subTest(value=value):
                with self.assertRaises(contract.ContractError):
                    contract.identity(value)
                with self.assertRaises(contract.ContractError):
                    contract.to_dict(value)
        with self.assertRaises(contract.ContractError):
            contract.from_dict(contract.Record, {})
        with self.assertRaises(contract.ContractError):
            contract.decode(contract.encode(action()), contract.Record,
                            expected_contract_sha256=CONTRACT_SHA)


class ProjectJsonBoundaryTests(unittest.TestCase):
    def test_bool_is_not_an_integer_or_lease_expiration(self):
        for record, field in ((context(), "milestone"), (work(), "generation"),
                              (binding(), "reserved_units"), (reply(), "usage_units"),
                              (target(), "generation"), (roster(), "max_builder_calls"),
                              (roster(), "max_reviewer_calls"), (roster(), "max_concurrent_calls")):
            for value in (True, False, 1.0):
                with self.subTest(kind=record.KIND, field=field, value=value):
                    with self.assertRaises(contract.ContractError):
                        replace(record, **{field: value})
        for field in ("epoch", "expires_at"):
            with self.subTest(lease_field=field), self.assertRaises(contract.ContractError):
                replace(binding(), lease=replace(binding().lease, **{field: True}))

    def test_integer_bounds_and_bounded_text_apply_on_construction(self):
        for record, field, invalid in (
            (context(), "milestone", (-1, 65)),
            (work(), "generation", (-1, 1_000_001)),
            (binding(), "reserved_units", (-1, 1 << 63)),
            (target(), "generation", (-1, 1_000_001)),
            (reply(), "reason", ("x" * 4097, "\ud800")),
            (verdict(), "rationale", ("x" * 4097, "\ud800")),
        ):
            for value in invalid:
                with self.subTest(kind=record.KIND, field=field, value=repr(value)[:100]):
                    with self.assertRaises(contract.ContractError):
                        replace(record, **{field: value})
        for value in (0, -1, 10 ** 1000, float("nan"), float("inf"), float("-inf")):
            with self.subTest(expiration=value), self.assertRaises(contract.ContractError):
                contract.lease_digest(replace(binding().lease, expires_at=value))

    def test_oversize_lease_expiration_fails_closed_in_nested_constructor_and_json(self):
        lease = replace(binding().lease, expires_at=10 ** 1000)
        with self.assertRaises(contract.ContractError):
            replace(binding(), lease=lease)
        raw = contract.to_dict(binding())
        raw["lease"]["expires_at"] = 10 ** 1000
        with self.assertRaises(contract.ContractError):
            contract.from_dict(contract.DispatchBinding, raw)

    def test_nonfinite_numbers_invalid_utf8_and_non_json_types_are_rejected(self):
        for value in (float("nan"), float("inf"), float("-inf"), b"bytes", (1, 2),
                      {1: "nonstring-key"}, {"nested": object()}, "\ud800"):
            with self.subTest(value=repr(value)), self.assertRaises(contract.ContractError):
                contract.canonical_bytes(value)
        for raw in ('{"value":NaN}', '{"value":Infinity}', '{"value":-Infinity}',
                    '{"value":1e10000}', b'"\xff"', '{', bytearray(b"{}")):
            with self.subTest(raw=repr(raw)), self.assertRaises(contract.ContractError):
                contract.strict_loads(raw)

    def test_json_byte_collection_and_depth_limits_include_boundary(self):
        allowed = "a" * (contract.MAX_RECORD_BYTES - 2)
        self.assertEqual(len(contract.canonical_bytes(allowed)), contract.MAX_RECORD_BYTES)
        for raw in ('"' + allowed + 'a"', " " * (contract.MAX_RECORD_BYTES + 1)):
            with self.assertRaises(contract.ContractError):
                contract.strict_loads(raw)
        for value in ([0] * (contract.MAX_COLLECTION + 1),
                      {str(i): 0 for i in range(contract.MAX_COLLECTION + 1)}):
            with self.assertRaises(contract.ContractError):
                contract.canonical_bytes(value)
        nested = 0
        for _ in range(contract.MAX_DEPTH):
            nested = [nested]
        self.assertEqual(contract.strict_loads(contract.canonical_bytes(nested)), nested)
        with self.assertRaises(contract.ContractError):
            contract.canonical_bytes([nested])
        with self.assertRaises(contract.ContractError):
            contract.strict_loads("[" * (contract.MAX_DEPTH + 1) + "0" + "]" * (contract.MAX_DEPTH + 1))

    def test_canonical_json_is_order_independent_and_keeps_unicode(self):
        self.assertEqual(contract.canonical_bytes({"z": "é", "a": [1, False]}),
                         '{"a":[1,false],"z":"é"}'.encode("utf-8"))
        self.assertEqual(contract.canonical_bytes({"z": 1, "a": 2}),
                         contract.canonical_bytes({"a": 2, "z": 1}))

    def test_identifiers_and_hashes_reject_ambiguous_or_unbounded_values(self):
        for value in ("", "actor/name", "actor name", "x" * 97, "é", "actor\n", True):
            with self.subTest(identifier=value), self.assertRaises(contract.ContractError):
                contract.identifier(value)
        for value in ("a" * 63, "a" * 65, "A" * 64, "g" * 64, "a" * 64 + "\n", False):
            with self.subTest(sha256=value), self.assertRaises(contract.ContractError):
                contract.sha256(value)


class ProjectDispatchStateTests(unittest.TestCase):
    def test_all_states_roundtrip_with_their_explicit_known_outcomes(self):
        for state in ("waiting", "pending", "publication_pending", "completed", "failed", "unknown"):
            record = reply(state)
            with self.subTest(state=state):
                self.assertEqual(contract.decode(contract.encode(record,
                    execution_contract_sha256=CONTRACT_SHA), contract.DispatchReply,
                    expected_contract_sha256=CONTRACT_SHA), record)

    def test_waiting_cannot_reserve_or_claim_result_or_even_zero_usage(self):
        waiting = reply("waiting")
        for field, value in (("binding", binding()), ("result_payload_sha256", digest("result")),
                             ("usage_units", 0), ("usage_units", 1)):
            with self.subTest(field=field, value=value), self.assertRaises(contract.ContractError):
                replace(waiting, **{field: value})

    def test_admitted_states_require_exact_request_action_and_binding(self):
        for state in ("pending", "publication_pending", "completed", "failed", "unknown"):
            for change in ({"binding": None}, {"request_id": "different-request"},
                           {"action_sha256": digest("different-action")}):
                with self.subTest(state=state, change=change), self.assertRaises(contract.ContractError):
                    replace(reply(state), **change)
        with self.assertRaises(contract.ContractError):
            replace(reply(), state="accepted")

    def test_pending_cannot_claim_outcomes_and_known_terminal_requires_both(self):
        for change in ({"result_payload_sha256": digest("result")}, {"usage_units": 0}):
            with self.subTest(change=change), self.assertRaises(contract.ContractError):
                replace(reply("pending"), **change)
        for state in ("completed", "failed"):
            for field in ("result_payload_sha256", "usage_units"):
                with self.subTest(state=state, field=field), self.assertRaises(contract.ContractError):
                    replace(reply(state), **{field: None})
            self.assertEqual(replace(reply(state), usage_units=0).usage_units, 0)

    def test_unknown_keeps_usage_unknown_and_publication_pending_needs_result(self):
        unknown = reply("unknown")
        self.assertIsNone(unknown.usage_units)
        self.assertEqual(replace(unknown, result_payload_sha256=digest("partial-result")).state,
                         "unknown")
        with self.assertRaises(contract.ContractError):
            replace(unknown, usage_units=0)
        with self.assertRaises(contract.ContractError):
            replace(reply("publication_pending"), result_payload_sha256=None)
        self.assertEqual(replace(reply("publication_pending"), usage_units=40).usage_units, 40)

    def test_usage_must_fit_reservation_and_lease_actor_must_match(self):
        for usage in (-1, 101):
            with self.subTest(usage=usage), self.assertRaises(contract.ContractError):
                replace(reply(), usage_units=usage)
        self.assertEqual(replace(reply(), usage_units=100).usage_units, 100)
        with self.assertRaises(contract.ContractError):
            replace(binding(), lease=replace(binding().lease, worker_id="other-actor"))


class ProjectLocalEvidenceTests(unittest.TestCase):
    def test_missing_required_evidence_is_explicit_and_blocks_complete_view(self):
        complete = local_view()
        complete.require_complete()
        missing_id = digest("did-not-arrive")
        incomplete = replace(complete, required_evidence_ids=(*complete.required_evidence_ids, missing_id),
                             omitted_required_ids=(missing_id,))
        self.assertEqual(contract.from_dict(contract.LocalViewManifest, contract.to_dict(incomplete)),
                         incomplete)
        with self.assertRaises(contract.ContractError):
            incomplete.require_complete()
        for change in ({"omitted_required_ids": ()},
                       {"omitted_required_ids": (digest("different-omission"),)}):
            with self.subTest(change=change), self.assertRaises(contract.ContractError):
                replace(incomplete, **change)
        with self.assertRaises(contract.ContractError):
            replace(complete, omitted_required_ids=(complete.source_ref.event_id,))

    def test_local_view_rejects_duplicate_or_ambiguous_event_identities(self):
        view = local_view()
        for refs in ((view.source_ref,), (view.evidence_refs[0],) * 2,
                     (replace(view.source_ref, producer="another-producer"),)):
            with self.subTest(refs=refs), self.assertRaises(contract.ContractError):
                replace(view, evidence_refs=refs)
        for field in ("required_evidence_ids", "omitted_required_ids"):
            with self.subTest(field=field), self.assertRaises(contract.ContractError):
                replace(view, **{field: (view.source_ref.event_id,) * 2})

    def test_resolve_local_requires_arrival_exact_producer_and_exact_payload(self):
        ref = evidence()
        self.assertEqual(contract.resolve_local(ref, (ref,), PAYLOAD), PAYLOAD)
        for arrived, payload in (((), PAYLOAD), ((ref,), None), ((ref,), b"wrong bytes"),
                                 ((ref, ref), PAYLOAD),
                                 ((replace(ref, producer="another-producer"),), PAYLOAD),
                                 ((replace(ref, kind="other-kind"),), PAYLOAD),
                                 ((replace(ref, payload_sha256=digest("other-payload")),), PAYLOAD),
                                 ([ref], PAYLOAD)):
            with self.subTest(arrived=arrived, payload=payload), self.assertRaises(contract.ContractError):
                contract.resolve_local(ref, arrived, payload)

    def test_action_payload_and_candidate_bundle_provenance_are_actor_owned(self):
        for ref in (replace(action().worker_payload_ref, producer="other-actor"),
                    replace(action().worker_payload_ref, kind="candidate-bundle")):
            with self.assertRaises(contract.ContractError):
                replace(action(), worker_payload_ref=ref)
        for ref in (replace(offer().bundle_ref, producer="other-actor"),
                    replace(offer().bundle_ref, kind="worker-request")):
            with self.assertRaises(contract.ContractError):
                replace(offer(), bundle_ref=ref)
        with self.assertRaises(contract.ContractError):
            replace(offer(), dispatch=binding("plan"))
        self.assertEqual(replace(offer(), dispatch=binding("repair")).dispatch.action.kind, "repair")

    def test_local_arrivals_must_be_exact_evidence_records_not_duck_objects(self):
        ref = evidence()

        class PretendEvidence:
            event_id = ref.event_id

            def __eq__(self, other):
                return True

        for arrived in ((object(),), (PretendEvidence(),), (ref, object())):
            with self.subTest(arrived=arrived), self.assertRaises(contract.ContractError):
                contract.resolve_local(ref, arrived, PAYLOAD)


class ProjectIdentityAndSelectionTests(unittest.TestCase):
    def test_binding_identity_changes_with_lease_payload_source_and_authority(self):
        original = binding()
        variants = [replace(original, lease=replace(original.lease, **{field: value}))
                    for field, value in (("task_id", "task-2"), ("epoch", 2), ("expires_at", 1001.0))]
        variants.extend(replace(original, **{field: value}) for field, value in (
            ("normalized_worker_request_sha256", digest("other-request")),
            ("profile_sha256", digest("other-profile")),
            ("authority_config_sha256", digest("other-authority")),
            ("call_id", "call-2"), ("reservation_id", "reservation-2"), ("reserved_units", 101)))
        variants.extend((replace(original, action=replace(original.action,
                            work=replace(work(), generation=3))),
                         replace(original, action=replace(original.action,
                            view_manifest_sha256=digest("different-local-source-view")))))
        for changed in variants:
            with self.subTest(changed=changed):
                self.assertNotEqual(contract.identity(original), contract.identity(changed))
        source_changed = replace(offer(), source_sha256=digest("changed-source"))
        self.assertNotEqual(contract.identity(offer()), contract.identity(source_changed))

    def test_identity_is_domain_separated_and_has_no_implicit_global_branch_input(self):
        record = action()
        public_body = {"protocol": contract.PROTOCOL, "schema_version": contract.SCHEMA_VERSION,
                       "kind": record.KIND, "body": contract.to_dict(record)}
        expected = hashlib.sha256(b"gossip-project-record\0" + contract.canonical_bytes(public_body)).hexdigest()
        self.assertEqual(contract.identity(record), expected)
        for field in ("global_head", "current_branch_head"):
            with self.subTest(field=field), self.assertRaises(contract.ContractError):
                contract.from_dict(contract.ActionRequest, {**contract.to_dict(record), field: "f" * 40})
        self.assertNotEqual(contract.identity(target()),
                            contract.identity(replace(target(), expected_head="e" * 40)))

    def test_selection_cannot_escape_eligible_information_set_or_change_local_view(self):
        selected = selection()
        changes = (
            {"selected": (contract.SelectedOffer("catalog", digest("did-not-arrive")),)},
            {"local_view_sha256": digest("another-view")},
            {"context": replace(context(), trajectory_id="another-trajectory")},
            {"selecting_dispatch": binding("build")},
            {"eligible_offer_sha256s": ()},
            {"selected": (selected.selected[0], selected.selected[0])},
            {"eligible_offer_sha256s": selected.eligible_offer_sha256s * 2},
            {"missing_slots": ("slot-2", "slot-2")},
        )
        for change in changes:
            with self.subTest(change=change), self.assertRaises(contract.ContractError):
                replace(selected, **change)
        second_offer = digest("second-eligible-offer")
        with self.assertRaises(contract.ContractError):
            replace(selected, eligible_offer_sha256s=(*selected.eligible_offer_sha256s, second_offer),
                    selected=(*selected.selected, contract.SelectedOffer("catalog", second_offer)))
        self.assertEqual(replace(selected, selecting_dispatch=binding("select_tests")).context, context())

    def test_release_identity_binds_exact_source_git_target_suite_and_generation(self):
        original = target()
        changes = (
            {"generation": 4}, {"repository_id": "repository-2"},
            {"protected_ref": "refs/heads/other-public"}, {"expected_head": "e" * 40},
            {"commit_oid": "e" * 40}, {"tree_oid": "e" * 40},
            {"sources": (replace(original.sources[0], sha256=digest("changed-source")),)},
            {"sources": (replace(original.sources[0], path="catalog/renamed.py"),)},
            {"selection_sha256": digest("changed-selection")},
            {"required_suite_sha256": digest("changed-ordered-suite")},
            {"evaluator_sha256": digest("changed-evaluator")},
            {"execution_receipt_refs": (evidence("different-execution", kind="execution-receipt"),)},
            {"context": replace(context(), milestone=2)},
        )
        original_review = verdict()
        for change in changes:
            with self.subTest(change=change):
                changed_target = replace(original, **change)
                self.assertNotEqual(contract.identity(changed_target), contract.identity(original))
                rebound_review = replace(original_review, target_sha256=contract.identity(changed_target))
                self.assertNotEqual(contract.identity(rebound_review), contract.identity(original_review))

    def test_worker_request_digest_normalizes_tuple_and_binds_every_request_field(self):
        request = WorkerRequest("task-1", "Implement named requirement.", ("catalog.py",),
                                {"catalog.py": "VALUE = 1\n"}, "a" * 40, 1)
        expected_body = {"task_id": "task-1", "instructions": "Implement named requirement.",
                         "allowed_paths": ["catalog.py"], "files": {"catalog.py": "VALUE = 1\n"},
                         "base_sha": "a" * 40, "attempt": 1, "feedback": ""}
        expected = hashlib.sha256(b"gossip-project-worker-request\0" +
                                  contract.canonical_bytes(expected_body)).hexdigest()
        self.assertEqual(contract.worker_request_digest(request), expected)
        for field, value in (("task_id", "task-2"), ("instructions", "Changed instruction."),
                             ("allowed_paths", ("helper.py",)),
                             ("files", {"catalog.py": "VALUE = 2\n"}), ("base_sha", "b" * 40),
                             ("attempt", 2), ("feedback", "Repair required.")):
            with self.subTest(field=field):
                self.assertNotEqual(contract.worker_request_digest(replace(request, **{field: value})),
                                    expected)
        with self.assertRaises(contract.ContractError):
            contract.worker_request_digest(replace(request, allowed_paths=["catalog.py"]))

    def test_lease_digest_is_separate_from_record_and_changes_with_fencing_epoch(self):
        lease = binding().lease
        expected = hashlib.sha256(b"gossip-project-lease\0" + contract.canonical_bytes({
            "task_id": lease.task_id, "worker_id": lease.worker_id,
            "epoch": lease.epoch, "expires_at": lease.expires_at})).hexdigest()
        self.assertEqual(contract.lease_digest(lease), expected)
        self.assertNotEqual(expected, contract.lease_digest(replace(lease, epoch=2)))
        for value in (0, -1, 1 << 63, True):
            with self.subTest(epoch=value), self.assertRaises(contract.ContractError):
                contract.lease_digest(replace(lease, epoch=value))

    def test_worker_request_digest_rejects_malformed_fields_before_hashing(self):
        request = WorkerRequest("task-1", "Implement named requirement.", ("catalog.py",),
                                {"catalog.py": "VALUE = 1\n"}, "a" * 40, 1)
        changes = (
            {"task_id": None}, {"task_id": ""}, {"instructions": []}, {"instructions": ""},
            {"feedback": False}, {"base_sha": 17}, {"base_sha": ""},
            {"attempt": True}, {"attempt": 0}, {"attempt": -1},
            {"allowed_paths": ()}, {"allowed_paths": (True,)},
            {"allowed_paths": ("catalog.py", "catalog.py")},
            {"allowed_paths": ("../outside.py",)}, {"allowed_paths": (".git/config",)},
            {"files": []}, {"files": {"catalog.py": None}},
            {"files": {"../outside.py": "content"}},
        )
        for change in changes:
            with self.subTest(change=change), self.assertRaises(contract.ContractError):
                contract.worker_request_digest(replace(request, **change))
        with self.assertRaises(contract.ContractError):
            contract.worker_request_digest(object())

    def test_worker_request_digest_rejects_cycles_but_allows_readonly_context_files(self):
        request = WorkerRequest("task-1", "Implement named requirement.", ("catalog.py",),
                                {"catalog.py": "VALUE = 1\n", "context.txt": "Read-only context."},
                                "a" * 40, 1)
        self.assertRegex(contract.worker_request_digest(request), r"^[0-9a-f]{64}$")
        cyclic = {}
        cyclic["catalog.py"] = cyclic
        with self.assertRaises(contract.ContractError):
            contract.worker_request_digest(replace(request, files=cyclic))


class ProjectReleaseAndRosterTests(unittest.TestCase):
    def test_public_release_never_claims_independent_final_acceptance(self):
        release = target()
        self.assertEqual(release.purpose, "public_release")
        self.assertIs(release.independent_final_project_acceptance, False)
        for change in ({"purpose": "final_acceptance"}, {"purpose": "independent_acceptance"},
                       {"independent_final_project_acceptance": True},
                       {"independent_final_project_acceptance": 0}):
            with self.subTest(change=change), self.assertRaises(contract.ContractError):
                replace(release, **change)

    def test_release_rejects_missing_duplicate_and_unsafe_named_sources(self):
        for path in ("", "/absolute.py", "../outside.py", "folder/../outside.py", "./file.py",
                     "folder//file.py", "folder/", "folder\\file.py", ".git/config",
                     "folder/.GIT/config", "line\nfeed.py", "x" * 513):
            with self.subTest(path=repr(path)), self.assertRaises(contract.ContractError):
                contract.NamedSource(path, digest("source"))
        release = target()
        for sources in ((), release.sources * 2,
                        (*release.sources, replace(release.sources[0], sha256=digest("other-source")))):
            with self.subTest(sources=sources), self.assertRaises(contract.ContractError):
                replace(release, sources=sources)
        with self.assertRaises(contract.ContractError):
            replace(release, execution_receipt_refs=())
        with self.assertRaises(contract.ContractError):
            replace(release, execution_receipt_refs=release.execution_receipt_refs * 2)

    def test_exact_git_objects_and_protected_ref_are_validated(self):
        for field in ("expected_head", "commit_oid", "tree_oid"):
            for value in ("a" * 39, "a" * 64, "A" * 40, "g" * 40):
                with self.subTest(field=field, value=value), self.assertRaises(contract.ContractError):
                    replace(target(), **{field: value})
        for ref in ("main", "refs/tags/release", "refs/heads/", "refs/heads/a//b", "refs/heads/a..b"):
            with self.subTest(ref=ref), self.assertRaises(contract.ContractError):
                replace(target(), protected_ref=ref)
        with self.assertRaises(contract.ContractError):
            replace(target(), git_object_format="sha256")
        with self.assertRaises(contract.ContractError):
            replace(offer(), commit_oid="a" * 64)

    def test_unknown_package_and_action_names_are_rejected(self):
        for record in (work(), selection().selected[0]):
            with self.subTest(kind=record.KIND), self.assertRaises(contract.ContractError):
                replace(record, package_id="infrastructure")
        with self.assertRaises(contract.ContractError):
            replace(action(), kind="promote")

    def test_scope_verdict_is_one_structural_record_not_quorum_or_release_authority(self):
        record = verdict()
        # These are independently serializable observations. Quorum, reviewer
        # eligibility, evidence sufficiency and promotion require the actual gate.
        for semantic_verdict in contract.VERDICTS:
            observation = replace(record, verdict=semantic_verdict)
            self.assertEqual(contract.decode(contract.encode(observation), contract.ScopeVerdict,
                             expected_contract_sha256=CONTRACT_SHA), observation)
        insufficient = replace(record, verdict="insufficient_evidence", evidence_refs=())
        self.assertEqual(insufficient.evidence_refs, ())
        for change in ({"review_dispatch": binding("build")}, {"covered_requirement_ids": ()},
                       {"covered_requirement_ids": ("requirement-1",) * 2},
                       {"verdict": "accepted"}):
            with self.subTest(change=change), self.assertRaises(contract.ContractError):
                replace(record, **change)

    def test_real20_roster_separates_cognitive_roles_from_call_and_concurrency_caps(self):
        record = roster()
        self.assertEqual(len(record.roles), 20)
        self.assertEqual(sum(role.role == "builder" for role in record.roles), 16)
        self.assertEqual(sum(role.role == "reviewer" for role in record.roles), 4)
        self.assertEqual(len({role.actor for role in record.roles}), 20)
        for package in contract.PACKAGES:
            self.assertEqual(sum(role.role == "builder" and role.package_affinity == package
                                 for role in record.roles), 4)
        self.assertEqual((record.max_builder_calls, record.max_reviewer_calls,
                          record.max_concurrent_calls), (80, 24, 3))
        throttled = replace(record, max_builder_calls=1, max_reviewer_calls=1,
                            max_concurrent_calls=1)
        self.assertEqual(throttled.roles, record.roles)
        self.assertEqual(replace(record, max_concurrent_calls=20).roles, record.roles)

    def test_roster_rejects_infrastructure_roles_duplicate_actors_and_bad_caps(self):
        for role in ("finance", "relay", "gate", "broker"):
            with self.subTest(role=role), self.assertRaises(contract.ContractError):
                replace(roster().roles[0], role=role)
        with self.assertRaises(contract.ContractError):
            replace(roster().roles[0], package_affinity="unknown-package")
        for roles in ((), roster().roles * 2,
                      (roster().roles[0], replace(roster().roles[0], profile_id="other-profile"))):
            with self.subTest(roles=roles), self.assertRaises(contract.ContractError):
                replace(roster(), roles=roles)
        for field, values in (("max_builder_calls", (0, -1, 1_000_001)),
                              ("max_reviewer_calls", (0, -1, 1_000_001)),
                              ("max_concurrent_calls", (0, -1, 21))):
            for value in values:
                with self.subTest(field=field, value=value), self.assertRaises(contract.ContractError):
                    replace(roster(), **{field: value})
        too_many = tuple(replace(roster().roles[0], actor=f"actor-{index}")
                         for index in range(contract.MAX_ROLES + 1))
        with self.assertRaises(contract.ContractError):
            replace(roster(), roles=too_many)


if __name__ == "__main__":
    unittest.main()
