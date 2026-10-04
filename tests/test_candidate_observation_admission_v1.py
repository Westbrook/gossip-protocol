"""Shared admission checks with explicitly simulated host capabilities.

These tests create no production authority, candidate execution or study result.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest

from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import project_acceptance_registry_v1 as registry

TRAJECTORIES = ('s4-healthy', 's4-recovery', 's16-healthy', 's16-recovery', 'o16-healthy', 'o16-recovery')


def sha(value):
    return admission.digest(value)


def registration(purpose='public_release'):
    subject = registry.Subject('full-six', TRAJECTORIES[0], 'M1', sha('contract'), sha('requirements'), sha('source'))
    binding = registry.Binding(subject, sha('suite'), sha('evaluator'), sha('image'), sha('environment'),
        sha('limits'), sha('seed'), 'candidate-example-execution-v5', purpose)
    gate = registry.Gate('declared-client-profile', ('M1-CLIENT-01',), ('cell-one', 'cell-two'), binding)
    return admission.ObservationRegistration(gate, 'a' * 40, 'b' * 40, 'first-execution', TRAJECTORIES,
        sha('original-definition'), sha('new-prospective-profile'), 'harness_qualification', sha('complete-original-binding'))


def frozen(reg):
    subject = reg.gate.binding.subject
    return registry.CohortFreeze(tuple(replace(subject, trajectory_id=key,
        source_sha256=subject.source_sha256 if key == subject.trajectory_id else sha(key))
        for key in TRAJECTORIES), sha('durable-stop'), sha('stop-verifier'), True)


class SimulatedHost:
    """Only a test double, never proof of independently retained registration."""
    def __init__(self, reg):
        self.registered = reg
        self.frozen = frozen(reg)
        self.calls = []
        self.after_freeze = None

    def registration(self):
        self.calls.append('registration')
        return self.registered

    def freeze(self):
        self.calls.append('freeze')
        if self.after_freeze is not None:
            self.registered = self.after_freeze
        return self.frozen

    def capability(self, reg=None):
        return admission.ObservationAdmission(reg or self.registered,
            verify_registration=self.registration, verify_cohort=self.freeze)


@dataclass(frozen=True)
class SimulatedExecutionBinding:
    source_sha256: str
    requirements_sha256: str
    milestone: str
    purpose: str
    protocol: str
    runtime_sha256: str
    nested_profile: tuple[str, ...]


def original_binding(reg):
    binding = reg.gate.binding
    subject = binding.subject
    return SimulatedExecutionBinding(subject.source_sha256, subject.requirements_sha256,
        subject.milestone, binding.purpose, binding.execution_protocol, sha('full-runtime'), ('every', 'field'))


class CandidateObservationAdmissionTests(unittest.TestCase):
    def test_public_profile_preserves_original_purpose_and_needs_no_freeze(self):
        reg = registration()
        host = SimulatedHost(reg)
        cap = host.capability()
        self.assertIsNone(cap.before_intent(reg))
        cap.check_current(reg, None)
        self.assertEqual(host.calls, ['registration'] * 4)
        self.assertEqual(reg.original_definition_purpose, 'harness_qualification')
        self.assertEqual(reg.gate.binding.purpose, 'public_release')

    def test_independent_and_repeatability_check_full_freeze_each_time(self):
        for purpose in ('independent_acceptance', 'repeatability'):
            with self.subTest(purpose=purpose):
                reg = registration(purpose)
                host = SimulatedHost(reg)
                cap = host.capability()
                proof = cap.before_intent(reg)
                self.assertEqual(proof, host.frozen)
                for _ in range(3):
                    cap.check_current(reg, proof)
                self.assertEqual(host.calls, ['registration', 'freeze', 'registration'] * 4)

    def test_constructor_requires_exact_registration_and_capabilities(self):
        reg = registration()
        for value in (None, asdict(reg), True):
            with self.subTest(value=type(value).__name__), self.assertRaises(admission.AdmissionError):
                admission.ObservationAdmission(value, verify_registration=lambda: reg)
        with self.assertRaises(admission.AdmissionError):
            admission.ObservationAdmission(reg, verify_registration=True)
        with self.assertRaises(admission.AdmissionError):
            admission.ObservationAdmission(reg, verify_registration=lambda: reg, verify_cohort=True)

    def test_hashes_and_boolean_claims_are_not_registration_capabilities(self):
        reg = registration()
        for value in (True, sha(asdict(reg)), asdict(reg)):
            with self.subTest(value=type(value).__name__), self.assertRaises(admission.AdmissionError):
                admission.ObservationAdmission(reg, verify_registration=lambda: value).before_intent(reg)

    def test_missing_registration_is_unavailable_not_product_failure(self):
        reg = registration()
        with self.assertRaises(admission.AdmissionUnavailable):
            admission.ObservationAdmission(reg, verify_registration=lambda: None).before_intent(reg)

    def test_authority_rejection_propagates_without_product_verdict(self):
        reg = registration()
        def rejected():
            raise admission.AdmissionError('External checkpoint rollback')
        cap = admission.ObservationAdmission(reg, verify_registration=rejected)
        with self.assertRaisesRegex(admission.AdmissionError, 'rollback'):
            cap.before_intent(reg)

    def test_complete_registration_substitution_rejected_before_freeze(self):
        reg = registration('independent_acceptance')
        host = SimulatedHost(reg)
        cap = host.capability()
        changes = [replace(reg, **{name: value}) for name, value in (
            ('commit_oid', 'c' * 40), ('tree_oid', 'd' * 40), ('repetition_id', 'second-execution'),
            ('cohort_trajectory_ids', tuple(reversed(TRAJECTORIES))), ('definition_sha256', sha('other-definition')),
            ('profile_sha256', sha('other-profile')), ('original_definition_purpose', 'public_development'),
            ('original_binding_sha256', sha('other-binding')))]
        changes.extend((replace(reg, gate=replace(reg.gate, gate_id='other-gate')),
            replace(reg, gate=replace(reg.gate, requirement_ids=('OTHER-REQUIREMENT',))),
            replace(reg, gate=replace(reg.gate, ordered_case_ids=tuple(reversed(reg.gate.ordered_case_ids))))))
        for changed in changes:
            with self.subTest(changed=changed), self.assertRaises(admission.AdmissionError):
                cap.before_intent(changed)
        self.assertEqual(host.calls, [])

    def test_subject_and_complete_registry_binding_substitution_rejected(self):
        reg = registration('independent_acceptance')
        cap = SimulatedHost(reg).capability()
        binding = reg.gate.binding
        for name in ('ordered_suite_sha256', 'evaluator_sha256', 'runtime_image_sha256',
                     'environment_sha256', 'limits_sha256', 'seed_sha256'):
            changed = replace(reg, gate=replace(reg.gate, binding=replace(binding, **{name: sha('other')})))
            with self.subTest(name=name), self.assertRaises(admission.AdmissionError):
                cap.before_intent(changed)
        for name in ('source_sha256', 'requirements_sha256', 'execution_contract_sha256'):
            changed_subject = replace(binding.subject, **{name: sha('other')})
            changed = replace(reg, gate=replace(reg.gate, binding=replace(binding, subject=changed_subject)))
            with self.subTest(name=name), self.assertRaises(admission.AdmissionError):
                cap.before_intent(changed)
        for name, value in (('milestone', 'M4'), ('cohort_id', 'other-cohort')):
            changed_subject = replace(binding.subject, **{name: value})
            with self.subTest(name=name), self.assertRaises(admission.AdmissionError):
                cap.before_intent(replace(reg, gate=replace(reg.gate, binding=replace(binding, subject=changed_subject))))

    def test_independently_registered_original_cannot_be_substituted(self):
        reg = registration()
        host = SimulatedHost(reg)
        host.registered = replace(reg, profile_sha256=sha('other'))
        with self.assertRaisesRegex(admission.AdmissionError, 'Authenticated'):
            host.capability(reg).before_intent(reg)

    def test_registered_definition_changed_during_freeze_is_rejected(self):
        reg = registration('independent_acceptance')
        host = SimulatedHost(reg)
        host.after_freeze = replace(reg, definition_sha256=sha('changed'))
        with self.assertRaisesRegex(admission.AdmissionError, 'Authenticated'):
            host.capability().before_intent(reg)

    def test_registration_revocation_detected_after_dispatch(self):
        reg = registration()
        host = SimulatedHost(reg)
        cap = host.capability()
        proof = cap.before_intent(reg)
        host.registered = None
        with self.assertRaises(admission.AdmissionUnavailable):
            cap.check_current(reg, proof)

    def test_registration_requires_exact_six_distinct_typed_trajectories(self):
        reg = registration()
        for keys in (list(TRAJECTORIES), TRAJECTORIES[:-1], (*TRAJECTORIES, 'seventh'),
                     (TRAJECTORIES[0],) * 6, (True, *TRAJECTORIES[1:]),
                     ('not-own', *TRAJECTORIES[1:])):
            with self.subTest(keys=keys), self.assertRaises(admission.AdmissionError):
                replace(reg, cohort_trajectory_ids=keys)

    def test_registration_rejects_malformed_git_and_identity_fields(self):
        reg = registration()
        for name, value in (('commit_oid', 'HEAD'), ('tree_oid', 42), ('repetition_id', ''),
                            ('definition_sha256', 'a'), ('profile_sha256', True),
                            ('original_binding_sha256', None), ('original_definition_purpose', False)):
            with self.subTest(name=name), self.assertRaises(admission.AdmissionError):
                replace(reg, **{name: value})

    def test_qualification_cannot_be_a_product_gate_purpose(self):
        for purpose in ('harness_qualification', 'registry_qualification', 'public_development'):
            with self.subTest(purpose=purpose), self.assertRaises(registry.AcceptanceError):
                registration(purpose)

    def test_missing_cohort_capability_or_original_is_unavailable(self):
        reg = registration('independent_acceptance')
        for callback in (None, lambda: None):
            with self.subTest(callback=callback), self.assertRaises(admission.AdmissionUnavailable):
                admission.ObservationAdmission(reg, verify_registration=lambda: reg,
                    verify_cohort=callback).before_intent(reg)

    def test_self_declared_boolean_or_dict_is_not_typed_freeze(self):
        reg = registration('independent_acceptance')
        for value in (True, asdict(frozen(reg)), sha('receipt')):
            with self.subTest(value=type(value).__name__), self.assertRaises(admission.AdmissionError):
                admission.ObservationAdmission(reg, verify_registration=lambda: reg,
                    verify_cohort=lambda: value).before_intent(reg)

    def test_partial_nonterminal_or_foreign_cohort_rejected(self):
        reg = registration('independent_acceptance')
        proof = frozen(reg)
        subject = proof.subjects[1]
        variants = [replace(proof, subjects=proof.subjects[:-1]), replace(proof, no_further_model_actions=False)]
        for name, value in (('trajectory_id', 'foreign'), ('cohort_id', 'other'), ('milestone', 'M4'),
                            ('requirements_sha256', sha('other')), ('execution_contract_sha256', sha('other'))):
            variants.append(replace(proof, subjects=(proof.subjects[0], replace(subject, **{name: value}), *proof.subjects[2:])))
        variants.append(replace(proof, subjects=(replace(proof.subjects[0], source_sha256=sha('other-final-source')), *proof.subjects[1:])))
        for value in variants:
            with self.subTest(value=value), self.assertRaises(admission.AdmissionError):
                admission.ObservationAdmission(reg, verify_registration=lambda: reg,
                    verify_cohort=lambda: value).before_intent(reg)

    def test_entire_original_freeze_is_rechecked_not_just_receipt(self):
        reg = registration('repeatability')
        host = SimulatedHost(reg)
        cap = host.capability()
        proof = cap.before_intent(reg)
        for value in (replace(proof, receipt_sha256=sha('other')), replace(proof, verifier_receipt_sha256=sha('other')),
                      replace(proof, subjects=tuple(reversed(proof.subjects))),
                      replace(proof, subjects=(proof.subjects[0], replace(proof.subjects[1], source_sha256=sha('other')), *proof.subjects[2:]))):
            host.frozen = value
            with self.subTest(value=value), self.assertRaisesRegex(admission.AdmissionError, 'barrier changed'):
                cap.check_current(reg, proof)

    def test_public_cannot_claim_independent_freeze_or_switch_purpose(self):
        reg = registration()
        cap = SimulatedHost(reg).capability()
        with self.assertRaises(admission.AdmissionError):
            cap.check_current(reg, frozen(reg))
        other = replace(reg, gate=replace(reg.gate, binding=replace(reg.gate.binding, purpose='independent_acceptance')))
        with self.assertRaises(admission.AdmissionError):
            cap.before_intent(other)

    def test_retained_freeze_requires_exact_type(self):
        reg = registration('independent_acceptance')
        cap = SimulatedHost(reg).capability()
        for value in (asdict(frozen(reg)), True):
            with self.subTest(value=type(value).__name__), self.assertRaises(admission.AdmissionError):
                cap.check_current(reg, value)

    def test_original_binding_digest_includes_every_field(self):
        reg = registration()
        original = original_binding(reg)
        self.assertEqual(admission.binding_sha256(original, gate=reg.gate), admission.digest(asdict(original)))
        for changed in (replace(original, runtime_sha256=sha('other-runtime')),
                        replace(original, nested_profile=('every', 'field', 'changed'))):
            self.assertNotEqual(admission.binding_sha256(original, gate=reg.gate),
                                admission.binding_sha256(changed, gate=reg.gate))

    def test_old_binding_cannot_be_hashed_as_new_product_purpose(self):
        reg = registration()
        original = original_binding(reg)
        for name, value in (('source_sha256', sha('other')), ('requirements_sha256', sha('other')),
                            ('milestone', 'M4'), ('purpose', 'harness_qualification'),
                            ('protocol', 'candidate-old-execution-v4')):
            with self.subTest(name=name), self.assertRaises(admission.AdmissionError):
                admission.binding_sha256(replace(original, **{name: value}), gate=reg.gate)
        for value in (asdict(original), SimulatedExecutionBinding, True):
            with self.subTest(value=type(value).__name__), self.assertRaises(admission.AdmissionError):
                admission.binding_sha256(value, gate=reg.gate)

    def test_common_source_identity_is_complete_order_independent_and_binary_safe(self):
        one = {'library/main.py': b'code', 'assets/binary': b'\xff\0\xfe'}
        self.assertEqual(admission.source_sha256(one), admission.source_sha256(dict(reversed(tuple(one.items())))))
        for changed in ({**one, 'library/main.py': b'changed'}, {'library/main.py': b'code'},
                        {**one, 'extra.py': b''}):
            self.assertNotEqual(admission.source_sha256(one), admission.source_sha256(changed))

    def test_source_inventory_rejects_nonbytes_alias_and_unsafe_paths(self):
        for files in ({}, [], {'a.py': 'text'}, {'a.py': bytearray(b'x')}, {'/absolute': b'x'},
                      {'a/../b': b'x'}, {'a//b': b'x'}, {'a\\b': b'x'}, {True: b'x'}):
            with self.subTest(files=files), self.assertRaises(admission.AdmissionError):
                admission.source_sha256(files)

    def test_freeze_record_exact_roundtrip_does_not_authenticate(self):
        proof = frozen(registration('independent_acceptance'))
        self.assertEqual(admission.freeze_from_record(json.loads(admission.encoded(asdict(proof)))), proof)
        self.assertEqual(admission.freeze_from_record(asdict(proof)), proof)
        self.assertIsNone(admission.freeze_from_record(None))

    def test_freeze_record_rejects_extra_missing_and_wrong_typed_fields(self):
        original = json.loads(admission.encoded(asdict(frozen(registration('independent_acceptance')))))
        values = [{**original, 'trusted': True}, {key: value for key, value in original.items() if key != 'receipt_sha256'},
                  {**original, 'no_further_model_actions': 1}, {**original, 'subjects': True},
                  {**original, 'subjects': [dict(original['subjects'][0], extra=True), *original['subjects'][1:]]},
                  {**original, 'subjects': [{**original['subjects'][0], 'source_sha256': False}, *original['subjects'][1:]]}]
        for value in values:
            with self.subTest(value=value), self.assertRaises(admission.AdmissionError):
                admission.freeze_from_record(value)

    def test_independent_verification_cannot_supply_a_missing_original_intent_freeze(self):
        reg = registration('independent_acceptance')
        cap = SimulatedHost(reg).capability()
        with self.assertRaisesRegex(admission.AdmissionError, 'barrier changed'):
            cap.check_current(reg, None)


class CandidateLoadedEvaluatorSourceTests(unittest.TestCase):
    """Synthetic modules exercise compilation comparison, never product grading."""
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="gossip-loaded-code-")
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "fixture.py"
        self.module_name = "gossip_loaded_source_fixture"
        self.addCleanup(lambda: sys.modules.pop(self.module_name, None))

    def loaded(self, source):
        raw = source.encode()
        self.path.write_bytes(raw)
        module = types.ModuleType(self.module_name)
        module.__file__ = str(self.path)
        sys.modules[self.module_name] = module
        # Only authored inert fixture source is executed, to simulate an earlier
        # import. The production verifier compiles source and never executes it.
        exec(compile(raw, str(self.path), "exec", dont_inherit=True), vars(module))
        return module, raw

    def test_same_registered_file_hash_does_not_hide_preimported_stale_code(self):
        original = "def grade(value):\n    return value + 1\n"
        module, _ = self.loaded(original)
        current = original.replace("+ 1", "+ 2").encode()
        self.path.write_bytes(current)
        registered = hashlib.sha256(current).hexdigest()
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), registered)
        self.assertEqual(module.grade(1), 2)
        with self.assertRaisesRegex(admission.AdmissionError, "Previously imported evaluator code differs"):
            admission._loaded_definitions_match(module, self.path.read_bytes())

    def test_true_class_static_property_accessors_wrappers_and_dataclass_methods(self):
        source = """from dataclasses import dataclass
from functools import wraps

def decorated(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        return function(*args, **kwargs)
    return wrapper

@decorated
def ordinary(value):
    return value

@dataclass
class Example:
    value: int

    @classmethod
    def make(cls, value):
        return cls(value)

    @staticmethod
    def identity(value):
        return value

    @property
    def number(self):
        return self.value

    @number.setter
    def number(self, value):
        self.value = value

    @number.deleter
    def number(self):
        self.value = 0

    class Nested:
        def method(self):
            return 3
"""
        module, raw = self.loaded(source)
        admission._loaded_definitions_match(module, raw)
        self.assertEqual(module.Example.make(4).number, 4)

    def test_property_setter_or_source_method_replacement_is_rejected(self):
        source = """class Example:
    @property
    def value(self):
        return 1
    @value.setter
    def value(self, value):
        self.changed = value
"""
        module, raw = self.loaded(source)
        original = module.Example.value
        module.Example.value = property(original.fget, lambda self, value: None)
        with self.assertRaises(admission.AdmissionError):
            admission._loaded_definitions_match(module, raw)

    def test_replacing_function_with_class_does_not_skip_code_comparison(self):
        module, raw = self.loaded("def grade():\n    return 1\n")
        module.grade = type("ForgedGrade", (), {})
        with self.assertRaises(admission.AdmissionError):
            admission._loaded_definitions_match(module, raw)

    def test_compilation_does_not_execute_module_top_level(self):
        module, _ = self.loaded("def grade():\n    return 1\n")
        raw = b"raise AssertionError('must not execute')\ndef grade():\n    return 1\n"
        # Function code still matches. The top-level AssertionError would escape
        # if verification executed source instead of merely compiling it.
        admission._loaded_definitions_match(module, raw)

    def test_live_code_rechecked_when_expected_compilation_is_cached(self):
        module, raw = self.loaded("def grade():\n    return 1\n")
        admission._loaded_definitions_match(module, raw)
        module.grade = lambda: 2
        with self.assertRaises(admission.AdmissionError):
            admission._loaded_definitions_match(module, raw)

    def test_wrapper_cycle_fails_closed(self):
        module, raw = self.loaded("def grade():\n    return 1\n")
        module.grade.__wrapped__ = module.grade
        with self.assertRaisesRegex(admission.AdmissionError, "Cyclic"):
            admission._loaded_definitions_match(module, raw)

    def test_registered_aliases_hashes_and_actual_loaded_shared_module(self):
        name = "candidate_observation_admission_v1.py"
        pin = hashlib.sha256(Path(admission.__file__).read_bytes()).hexdigest()
        admission.verify_loaded_sources({name: pin, "http/" + name: pin, "finite/" + name: pin})
        with self.assertRaises(admission.AdmissionError):
            admission.verify_loaded_sources({name: "0" * 64})
        with self.assertRaises(admission.AdmissionError):
            admission.verify_loaded_sources({name: pin, "http/" + name: "0" * 64})

    def test_registered_sources_reject_traversal_and_nonhashes(self):
        for values in ({}, {"../outside.py": "a" * 64}, {"/outside.py": "a" * 64},
                       {"finite/../outside.py": "a" * 64}, {"candidate_observation_admission_v1.py": True}):
            with self.subTest(values=values), self.assertRaises(admission.AdmissionError):
                admission.verify_loaded_sources(values)

    def test_current_new_executor_profiles_match_already_loaded_definitions(self):
        from gossip_harness import candidate_client_execution_v5 as cli
        from gossip_harness import candidate_http_execution_v4 as http
        admission.verify_loaded_sources(cli.evaluator_sources())
        admission.verify_loaded_sources(http.evaluator_sources())


    def test_known_import_time_source_pins_are_not_replaced_by_current_file_hash(self):
        from unittest.mock import patch
        name = "candidate_observation_admission_v1.py"
        pin = hashlib.sha256(Path(admission.__file__).read_bytes()).hexdigest()
        with patch.object(admission, "LOADED_SOURCE_SHA256", "0" * 64, create=True):
            with self.assertRaisesRegex(admission.AdmissionError, "source hash differs"):
                admission.verify_loaded_sources({name: pin})
        with patch.object(admission, "_LOADED_SOURCES", {name: "0" * 64}, create=True):
            with self.assertRaisesRegex(admission.AdmissionError, "own source pin differs"):
                admission.verify_loaded_sources({name: pin})


    def test_invalid_registered_source_is_an_admission_error(self):
        module, _ = self.loaded("def grade():\n    return 1\n")
        with self.assertRaisesRegex(admission.AdmissionError, "cannot compile"):
            admission._loaded_definitions_match(module, b"def broken(:\n")
