"""Offline policy regressions; normalized fixtures are not execution evidence."""
from dataclasses import FrozenInstanceError, replace
import hashlib
import unittest

from gossip_harness import project_acceptance_registry_v1 as a


def sha(value):
    return hashlib.sha256(value.encode()).hexdigest()


def subject(name="s16-healthy", source="source", contract="execution-contract"):
    return a.Subject("six-trajectory-cohort", name, "M1", sha(contract), sha("requirements"), sha(source))


def binding(purpose="public_release", owner=None):
    return a.Binding(owner or subject(), sha("ordered-"+purpose), sha("evaluator"), sha("runtime-image"),
                     sha("environment"), sha("limits"), sha("seed"), "declared-executor-v1", purpose)


def registry(owner=None):
    owner = owner or subject()
    return a.Registry(owner, sha("complete-inventory"), ("M1-identity", "M1-atomicity"),
        ("s4-healthy", "s16-healthy", "o16-healthy", "s4-fault", "s16-fault", "o16-fault"), (
            a.Gate("public", ("M1-identity",), ("identity", "inherited"), binding(owner=owner)),
            a.Gate("private", ("M1-identity", "M1-atomicity"), ("lifecycle", "rollback"),
                   binding("independent_acceptance", owner))))


def freeze(reg=None):
    reg = reg or registry()
    return a.CohortFreeze(tuple(reg.subject if name == reg.subject.trajectory_id else
        replace(reg.subject, trajectory_id=name, source_sha256=sha(name))
        for name in reg.cohort_trajectory_ids), sha("irreversible-freeze"), sha("freeze-verification"), True)


def promotion(reg=None, status="promoted"):
    reg = reg or registry()
    known = status in ("promoted", "rejected")
    return a.Promotion(reg.subject, status, sha("promotion") if known else None,
                       sha("promotion-verification") if known else None)


def observation(gate):
    return a.Observation(gate.gate_id, gate.binding, a.PhysicalExecution(gate.binding,
        gate.gate_id+"-execution", sha(gate.gate_id+"-original-receipt"), sha(gate.gate_id+"-verification"),
        "completed", tuple(a.CaseResult(name, "passed") for name in gate.ordered_case_ids),
        sha("irreversible-freeze") if gate.binding.purpose != "public_release" else None))


def observed(reg=None):
    return tuple(observation(gate) for gate in (reg or registry()).gates)


def changed(item, **fields):
    return replace(item, execution=replace(item.execution, **fields))


def assess(reg=None, items=None, promoted=None, frozen=None, *, no_freeze=False,
           expected_design=None, expected_contract=None):
    reg = reg or registry()
    return a.assess(reg, observed(reg) if items is None else items,
        promotion(reg) if promoted is None else promoted,
        None if no_freeze else freeze(reg) if frozen is None else frozen,
        expected_design_sha256=expected_design or a.design_fingerprint(reg),
        expected_execution_contract_sha256=expected_contract or reg.subject.execution_contract_sha256)


class AcceptanceRegistryTests(unittest.TestCase):
    def test_complete_bound_six_member_observation_accepts_only_against_registry(self):
        result = assess()
        self.assertTrue(result.accepted_against_registry)
        self.assertEqual(result.status, "accepted_against_registry")
        self.assertEqual(tuple(item.status for item in result.requirements), ("passed", "passed"))
        self.assertEqual(result.physical_gates, ("public", "private"))
        self.assertEqual(result.reused_gates, ())
        self.assertEqual(result.registry_sha256, a.fingerprint(registry()))

    def test_observation_arrival_order_does_not_change_assessment(self):
        self.assertEqual(assess(), assess(items=tuple(reversed(observed()))))

    def test_registry_mapping_cannot_omit_or_invent_a_mandatory_requirement(self):
        original = registry()
        for requirements in (("M1-identity",), original.requirement_ids + ("M1-new",),
                             (), ("M1-identity", "M1-identity")):
            with self.subTest(requirements=requirements), self.assertRaises(a.AcceptanceError):
                replace(original, requirement_ids=requirements)
        with self.assertRaises(a.AcceptanceError):
            replace(original, gates=(original.gates[0],))
        with self.assertRaises(a.AcceptanceError):
            replace(original.gates[0], ordered_case_ids=())
        with self.assertRaises(a.AcceptanceError):
            replace(original.gates[0], requirement_ids=())

    def test_public_evidence_only_cannot_create_whole_project_registry(self):
        original = registry()
        gate = replace(original.gates[0], requirement_ids=original.requirement_ids)
        with self.assertRaises(a.AcceptanceError):
            replace(original, gates=(gate,))

    def test_missing_gate_and_missing_case_remain_explicit(self):
        public, private = observed()
        result = assess(items=(public,))
        self.assertEqual(result.status, "incomplete")
        self.assertEqual(result.missing_gates, ("private",))
        partial = changed(private, outcomes=private.execution.outcomes[:1])
        result = assess(items=(public, partial))
        self.assertFalse(result.accepted_against_registry)
        self.assertEqual(result.missing_gates, ("private",))
        empty = changed(private, outcomes=())
        self.assertEqual(assess(items=(public, empty)).status, "incomplete")

    def test_failure_does_not_hide_missing_skipped_and_infrastructure(self):
        public, private = observed()
        public = changed(public, outcomes=(a.CaseResult("identity", "failed"),))
        private = changed(private, terminal_status="infrastructure_error", outcomes=(
            a.CaseResult("lifecycle", "skipped"), a.CaseResult("rollback", "infrastructure_error")))
        result = assess(items=(public, private))
        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failed_gates, ("public",))
        self.assertEqual(result.missing_gates, ("public",))
        self.assertEqual(result.skipped_gates, ("private",))
        self.assertEqual(result.infrastructure_gates, ("private",))
        self.assertEqual(result.requirements[0].status, "failed")
        self.assertEqual(result.requirements[1].status, "incomplete")

    def test_transport_failure_cannot_be_overridden_by_passing_case_list(self):
        public, private = observed()
        result = assess(items=(changed(public, terminal_status="infrastructure_error"), private))
        self.assertEqual(result.status, "incomplete")
        self.assertEqual(result.infrastructure_gates, ("public",))

    def test_every_execution_binding_field_is_enforced(self):
        public, private = observed()
        for field in ("ordered_suite_sha256", "evaluator_sha256", "runtime_image_sha256",
                      "environment_sha256", "limits_sha256", "seed_sha256", "execution_protocol"):
            value = "other-protocol" if field == "execution_protocol" else sha("other-"+field)
            altered = replace(public.binding, **{field: value})
            item = replace(public, binding=altered, execution=replace(public.execution, binding=altered))
            with self.subTest(field=field), self.assertRaises(a.AcceptanceError):
                assess(items=(item, private))
        altered = replace(private.binding, purpose="repeatability")
        item = replace(private, binding=altered, execution=replace(private.execution, binding=altered))
        with self.assertRaises(a.AcceptanceError):
            assess(items=(public, item))

    def test_source_generation_and_requirement_revision_must_match(self):
        public, private = observed()
        for field in ("source_sha256", "requirements_sha256", "execution_contract_sha256",
                      "cohort_id", "trajectory_id", "milestone"):
            value = sha("other") if field.endswith("sha256") else "other"
            owner = replace(public.binding.subject, **{field: value})
            altered = replace(public.binding, subject=owner)
            item = replace(public, binding=altered, execution=replace(public.execution, binding=altered))
            with self.subTest(field=field), self.assertRaises(a.AcceptanceError):
                assess(items=(item, private))

    def test_duplicate_unknown_and_reordered_case_evidence_is_invalid(self):
        public, private = observed()
        invalid = (
            (public, public, private),
            (replace(public, gate_id="undeclared"), private),
            (changed(public, outcomes=tuple(reversed(public.execution.outcomes))), private),
            (changed(public, outcomes=(a.CaseResult("undeclared", "passed"),)), private),
        )
        for items in invalid:
            with self.subTest(items=items), self.assertRaises(a.AcceptanceError):
                assess(items=items)
        with self.assertRaises(a.AcceptanceError):
            changed(public, outcomes=(public.execution.outcomes[0],)*2)

    def test_partial_cohort_or_open_barrier_cannot_admit_private_observations(self):
        complete = freeze()
        for partial in (replace(complete, subjects=complete.subjects[:-1]),
                        replace(complete, no_further_model_actions=False)):
            with self.subTest(freeze=partial), self.assertRaises(a.AcceptanceError):
                assess(frozen=partial)
            self.assertEqual(assess(items=observed()[:1], frozen=partial).status, "incomplete")
        with self.assertRaises(a.AcceptanceError):
            assess(no_freeze=True)
        self.assertEqual(assess(items=(), no_freeze=True).status, "incomplete")

    def test_freeze_must_bind_current_subject_and_complete_declared_roster(self):
        complete = freeze()
        for subjects in (
            complete.subjects + (subject("rogue"),),
            tuple(replace(s, source_sha256=sha("wrong")) if s.trajectory_id == subject().trajectory_id else s
                  for s in complete.subjects),
            tuple(replace(s, cohort_id="wrong") for s in complete.subjects),
            tuple(replace(s, execution_contract_sha256=sha("wrong")) for s in complete.subjects),
        ):
            with self.subTest(subjects=subjects), self.assertRaises(a.AcceptanceError):
                assess(frozen=replace(complete, subjects=subjects))
        public, private = observed()
        with self.assertRaises(a.AcceptanceError):
            assess(items=(public, changed(private, cohort_freeze_sha256=sha("wrong-freeze"))))

    def test_case_passes_do_not_replace_exact_protected_promotion(self):
        for status in ("missing", "unknown"):
            with self.subTest(status=status):
                self.assertEqual(assess(promoted=promotion(status=status)).status, "incomplete")
        self.assertEqual(assess(promoted=promotion(status="rejected")).status, "rejected")
        with self.assertRaises(a.AcceptanceError):
            assess(promoted=replace(promotion(), subject=subject(source="stale-source")))

    def test_valid_public_reuse_keeps_original_physical_reference(self):
        public, private = observed()
        reused = replace(public, mode="reused", reuse_receipt_sha256=sha("reuse-decision"))
        result = assess(items=(reused, private))
        self.assertTrue(result.accepted_against_registry)
        self.assertEqual(result.reused_gates, ("public",))
        self.assertEqual(result.physical_gates, ("private",))
        self.assertEqual(reused.execution.receipt_sha256, public.execution.receipt_sha256)
        self.assertEqual(reused.execution.execution_id, public.execution.execution_id)

    def test_independent_and_repeatability_evidence_can_never_be_reused(self):
        for purpose in ("independent_acceptance", "repeatability"):
            gate = replace(registry().gates[1], binding=binding(purpose))
            with self.subTest(purpose=purpose), self.assertRaises(a.AcceptanceError):
                replace(observation(gate), mode="reused", reuse_receipt_sha256=sha("reuse"))

    def test_reuse_rejects_partial_infrastructure_skipped_and_changed_contract(self):
        public, private = observed()
        for update in ({"terminal_status": "infrastructure_error"}, {"outcomes": ()},
                       {"outcomes": (a.CaseResult("identity", "skipped"),)}):
            with self.subTest(update=update), self.assertRaises(a.AcceptanceError):
                replace(changed(public, **update), mode="reused", reuse_receipt_sha256=sha("reuse"))
        partial = changed(public, outcomes=public.execution.outcomes[:1])
        reused = replace(partial, mode="reused", reuse_receipt_sha256=sha("reuse"))
        with self.assertRaises(a.AcceptanceError):
            assess(items=(reused, private))
        with self.assertRaises(a.AcceptanceError):
            replace(public, binding=replace(public.binding, seed_sha256=sha("different")))

    def test_known_product_failure_may_be_reused_without_becoming_success(self):
        public, private = observed()
        public = changed(public, outcomes=(a.CaseResult("identity", "failed"), a.CaseResult("inherited", "passed")))
        reused = replace(public, mode="reused", reuse_receipt_sha256=sha("reuse"))
        result = assess(items=(reused, private))
        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failed_gates, ("public",))

    def test_repeated_gates_require_distinct_original_physical_observations(self):
        original = registry()
        extra = replace(original.gates[1], gate_id="private-repeat")
        reg = replace(original, gates=original.gates+(extra,))
        public, private = observed(original)
        duplicate = replace(private, gate_id=extra.gate_id)
        with self.assertRaises(a.AcceptanceError):
            assess(reg, (public, private, duplicate))
        separate = observation(extra)
        self.assertTrue(assess(reg, (public, private, separate)).accepted_against_registry)
        with self.assertRaises(a.AcceptanceError):
            assess(reg, (public, private, changed(separate, receipt_sha256=private.execution.receipt_sha256)))

    def test_frozen_design_excludes_only_unknown_source_and_containing_contract(self):
        original = registry()
        changed_source = registry(subject(source="future-terminal-source"))
        changed_contract = registry(subject(contract="new-containing-contract"))
        self.assertEqual(a.design_fingerprint(original), a.design_fingerprint(changed_source))
        self.assertEqual(a.design_fingerprint(original), a.design_fingerprint(changed_contract))
        self.assertNotEqual(a.fingerprint(original), a.fingerprint(changed_source))
        self.assertTrue(assess(changed_source, expected_design=a.design_fingerprint(original)).accepted_against_registry)
        with self.assertRaises(a.AcceptanceError):
            assess(changed_contract, expected_design=a.design_fingerprint(original),
                   expected_contract=original.subject.execution_contract_sha256)
        for owner in (replace(subject(), milestone="M2"), replace(subject(), requirements_sha256=sha("other")),
                      replace(subject(), cohort_id="other")):
            self.assertNotEqual(a.design_fingerprint(original), a.design_fingerprint(registry(owner)))

    def test_caller_cannot_remove_cases_or_change_inventory_under_frozen_design(self):
        original = registry()
        for altered in (replace(original, inventory_sha256=sha("narrower-inventory")),
                        replace(original, gates=(replace(original.gates[0], ordered_case_ids=("identity",)), original.gates[1]))):
            with self.subTest(registry=altered), self.assertRaises(a.AcceptanceError):
                assess(altered, expected_design=a.design_fingerprint(original))

    def test_value_inputs_are_strict_and_immutable(self):
        original = registry()
        with self.assertRaises(FrozenInstanceError):
            original.inventory_sha256 = sha("changed")
        for value in (True, 1, [], "bad hash"):
            with self.subTest(value=value), self.assertRaises(a.AcceptanceError):
                replace(original, inventory_sha256=value)
        with self.assertRaises(a.AcceptanceError):
            replace(freeze(), no_further_model_actions=1)
        with self.assertRaises(a.AcceptanceError):
            replace(original, requirement_ids=list(original.requirement_ids))
        with self.assertRaises(a.AcceptanceError):
            replace(observed()[0].execution, terminal_status="passed")


if __name__ == "__main__":
    unittest.main()
