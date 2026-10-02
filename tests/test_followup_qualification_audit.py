"""Independent retained-evidence attacks; no fixture, candidate, Docker or API work.

The forty-two source rows and their raw observations are authored here rather
than produced by the runner or qualifier being audited. Passing these checks
establishes auditor behavior on synthetic evidence, not physical qualification.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from typing import Any
import unittest

from analysis.audit_continuation_followup import (
    audit_qualification, check_model_profiles, qualified_baseline_proof,
)


PROTOCOL = "continuation-followup-fixture-qualification-v2"
IMAGE = "sha256:" + "a" * 64
ADAPTER = "b" * 64
PROJECTS = ("warehouse", "job-queue")


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def file_sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def case(name):
    return dict(id=name, requirement="R-" + name, input=dict(command=name), expected=True)


def raw_receipt(row):
    """Create typed observations independently of the fixture implementation."""
    outcomes = []
    for index, item in enumerate(row["cases"]):
        fails = row["kind"] == "mutant" and index in row["groups"]["witness"]
        outcomes.append(dict(index=index, id=item["id"], requirement=item["requirement"],
            passed=not fails, status="wrong_answer" if fails else "passed",
            actual=False if fails else deepcopy(item["expected"])))
    passed = all(item["passed"] for item in outcomes)
    return dict(protocol="gossip-blackbox-v1", schema_version=1,
        status="passed" if passed else "failed", passed=passed, cleanup_verified=True,
        source_sha256=digest(row["files"]), suite_sha256=digest(row["cases"]),
        image_id=IMAGE, adapter_sha256=ADAPTER, case_timeout_seconds=3,
        timeout_seconds=90, case_count=len(row["cases"]), exit_code=0,
        timed_out=False, input_delivery_failed=False, output_truncated=False,
        container_name=f"gossip-blackbox-synthetic-{row['index']:04d}", outcomes=outcomes)


def classification(row, receipt):
    """State the classifier's public contract without importing its implementation."""
    outcomes, groups = receipt["outcomes"], row["groups"]
    runnable = all(item["status"] in {"passed", "wrong_answer"} for item in outcomes)
    public = all(outcomes[index]["passed"] for index in groups["public"])
    wrong = [index for index in groups["witness"] if outcomes[index]["status"] == "wrong_answer"]
    result = {key: row[key] for key in
              ("index", "project_id", "stage_index", "source_id", "kind", "family", "fixture_id")}
    result.update(runnable=runnable, public_surviving=public and runnable,
        witness_wrong_answers=wrong, all_cases_passed=receipt["passed"],
        qualified=(runnable and public and bool(wrong) if row["kind"] == "mutant"
                   else runnable and receipt["passed"]))
    return result


class FollowupQualificationAuditTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.projects: dict[str, Any] = {}
        self.rows: list[dict[str, Any]] = []
        for pid in PROJECTS:
            stages = []
            for stage in range(2):
                public = ([case(pid + "-base-public")] if stage == 0 else []) + [case(f"{pid}-public-{stage}")]
                private = ([case(pid + "-base-private")] if stage == 0 else []) + [
                    case(f"{pid}-private-{stage}-{number}") for number in range(7)]
                stages.append(dict(known_files={"solution.py": f"# {pid} golden {stage}\n"},
                    visible_cases=public, hidden_cases=private))
            project = dict(id=pid, initial_files={"solution.py": f"# {pid} baseline\n"}, stages=stages)
            self.projects[pid] = project
            for stage in range(2):
                public = [item for part in stages[:stage + 1] for item in part["visible_cases"]]
                private = [item for part in stages[:stage + 1] for item in part["hidden_cases"]]
                cases = deepcopy(public + private)
                kinds = [("golden", "golden", None), ("control", "correct", None),
                         ("control", "alternate", None)] + [
                    ("mutant", f"mutant-{number}", f"family-{number}") for number in range(7)]
                for kind, source_id, family in kinds:
                    files = (deepcopy(stages[stage]["known_files"]) if source_id in {"golden", "correct"}
                             else {"solution.py": f"# {pid} {stage} {source_id}\n"})
                    witness = [index for index, item in enumerate(cases)
                               if item["id"] == f"{pid}-private-{stage}-{source_id.removeprefix('mutant-')}"] if kind == "mutant" else list(range(len(public), len(cases)))
                    identity = f"{pid}/stage-{stage}/{kind}/{source_id}"
                    self.rows.append(dict(index=len(self.rows), project_id=pid, stage_index=stage,
                        source_id=source_id, kind=kind, family=family, fixture_id=identity,
                        purpose=PROTOCOL + "/" + identity, files=files, cases=deepcopy(cases),
                        groups=dict(public=list(range(len(public))),
                                    private=list(range(len(public), len(cases))), witness=witness)))
        for pid in PROJECTS:
            identity = pid + "/baseline/initial"
            self.rows.append(dict(index=len(self.rows), project_id=pid, stage_index=-1,
                source_id="initial", kind="baseline", family=None, fixture_id=identity,
                purpose=PROTOCOL + "/" + identity, files=deepcopy(self.projects[pid]["initial_files"]),
                cases=[case(pid + "-base-public"), case(pid + "-base-private")],
                groups=dict(public=[0], private=[1], witness=[])))
        self.matrix = dict(stages=[[pid, stage] for pid in PROJECTS for stage in range(2)],
                           baseline_projects=list(PROJECTS), rows=self.rows)
        self.runtime: dict[str, Any] = dict(server_sha256="1" * 64, daemon_id_sha256="2" * 64,
            configuration_sha256="3" * 64, context_sha256="4" * 64,
            server=dict(Version="27.5.1", ApiVersion="1.47", Os="linux", Arch="arm64"),
            image=dict(Id=IMAGE, Os="linux", Architecture="arm64", Variant=None))
        self.validation = dict(image=IMAGE, adapter_sha256=ADAPTER,
                               case_timeout_seconds=3, suite_timeout_seconds=90)
        self.receipts: list[dict[str, Any]] = []
        self.executions: list[dict[str, Any]] = []
        self.classifications: list[dict[str, Any]] = []
        for row in self.rows:
            self.receipts.append({})
            self.executions.append({})
            self.classifications.append({})
            self.rebuild_row(row["index"])
        inputs = {"continuation-followup-study-plan.json": b'{"synthetic":true}\n',
                  "gossip_harness/frozen_source.py": b"# Synthetic frozen source, never executed.\n",
                  "analysis/frozen_auxiliary.py": b"# Synthetic auxiliary, never executed.\n"}
        for name, content in inputs.items():
            path = self.root / "inputs" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
        hashes = {name: hashlib.sha256(content).hexdigest() for name, content in inputs.items()}
        self.contract: dict[str, Any] = dict(project_ids=list(PROJECTS), image=IMAGE,
            plan_sha256=hashes["continuation-followup-study-plan.json"],
            sources={"frozen_source.py": hashes["gossip_harness/frozen_source.py"]},
            extra_sources={"analysis/frozen_auxiliary.py": hashes["analysis/frozen_auxiliary.py"]},
            qualification_protocol=PROTOCOL, qualification_matrix_sha256=digest(self.matrix))
        environment = dict(python="synthetic-3.11", operating_system="synthetic-linux")
        self.manifest = dict(protocol=PROTOCOL, contract=self.contract, matrix=self.matrix,
            inputs=[dict(path=name, sha256=hashes[name]) for name in sorted(hashes)],
            execution_environment=environment, execution_environment_sha256=digest(environment),
            adapter_sha256=ADAPTER, api_calls=0, candidate_execution_on_host=False,
            minimum_surviving_families=4)
        self.report = dict(protocol=PROTOCOL, status="qualified", api_calls=0,
            execution_environment_sha256=digest(environment), runtime_identity=self.runtime,
            runtime_identity_sha256=digest(self.runtime), executions=self.executions,
            classifications=self.classifications,
            conclusion=dict(qualified=True, complete=True,
                stages=[dict(project_id=pid, stage_index=stage, qualified=True, source_count=10,
                    public_surviving_families=[f"family-{number}" for number in range(7)],
                    public_surviving_family_count=7) for pid in PROJECTS for stage in range(2)],
                baselines=[dict(project_id=pid, qualified=True, source_count=1) for pid in PROJECTS]))

    def rebuild_row(self, index):
        row = self.rows[index]
        row.update(source_sha256=digest(row["files"]), suite_sha256=digest(row["cases"]),
                   receipt=f"receipts/{index:04d}.json")
        receipt = raw_receipt(row)
        self.receipts[index] = receipt
        self.executions[index] = {key: row[key] for key in
            ("index", "purpose", "fixture_id", "receipt", "source_sha256", "suite_sha256")}
        self.executions[index].update(physically_executed=True, reused=False, verified=True,
                                      runtime_identity_sha256=digest(self.runtime))
        self.classifications[index] = classification(row, receipt)

    def persist(self, *, rebind_matrix=False):
        """Rewrite retained bytes; frozen matrix identity changes only explicitly."""
        if rebind_matrix:
            self.contract["qualification_matrix_sha256"] = digest(self.matrix)
        self.manifest["matrix_sha256"] = digest(self.matrix)
        self.manifest["contract_sha256"] = digest(self.contract)
        self.report["contract_sha256"] = digest(self.contract)
        for index, receipt in enumerate(self.receipts):
            path = self.root / f"receipts/{index:04d}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(encoded(receipt))
            self.executions[index]["receipt_sha256"] = file_sha(path)
        path = self.root / "manifest.json"
        path.write_bytes(encoded(self.manifest))
        self.report["manifest_sha256"] = file_sha(path)
        path = self.root / "results.json"
        path.write_bytes(encoded(self.report))
        return dict(path=str(path), sha256=file_sha(path))

    def audit(self, **kwargs):
        return audit_qualification(self.persist(**kwargs), self.contract, self.projects, self.validation)

    def test_truthful_full_matrix_is_accepted_as_retained_evidence(self):
        result = self.audit()
        self.assertEqual(len(result["containers"]), 42)
        self.assertEqual(set(result["baselines"]), set(PROJECTS))
        self.assertEqual([row["kind"] for row in self.rows].count("mutant"), 28)
        self.assertTrue(all(row["qualified"] for row in result["report"]["classifications"]))

    def test_baseline_proof_binds_exact_separate_source_and_execution(self):
        qualification = self.audit()
        proof = qualified_baseline_proof(qualification, "warehouse")
        row, execution = self.rows[40], self.executions[40]
        expected = dict(protocol=PROTOCOL, contract_sha256=digest(self.contract),
            manifest_sha256=file_sha(self.root / "manifest.json"),
            execution_environment_sha256=self.report["execution_environment_sha256"],
            runtime_identity_sha256=digest(self.runtime), project_id="warehouse", stage_index=-1,
            fixture_id="warehouse/baseline/initial", source_id="initial", index=40,
            purpose=PROTOCOL + "/warehouse/baseline/initial", source_sha256=digest(row["files"]),
            suite_sha256=digest(row["cases"]), receipt="receipts/0040.json",
            receipt_sha256=execution["receipt_sha256"], physically_executed=True, reused=False)
        self.assertEqual(proof, expected)
        self.assertNotEqual(proof["source_sha256"], self.rows[0]["source_sha256"])
        self.assertNotEqual(proof["receipt_sha256"], self.executions[0]["receipt_sha256"])
        self.assertEqual(qualified_baseline_proof(qualification, "job-queue")["index"], 41)

    def test_self_rehashed_omission_of_baseline_case_breaks_frozen_matrix(self):
        row = self.rows[40]
        row["cases"] = row["cases"][:1]
        row["groups"] = dict(public=[0], private=[0], witness=[])
        self.rebuild_row(40)
        with self.assertRaisesRegex(ValueError, "forty-two-source qualification matrix"):
            self.audit()

    def test_self_rehashed_changed_case_expectation_breaks_frozen_matrix(self):
        self.rows[0]["cases"][0]["expected"] = "forged expected value"
        self.rebuild_row(0)
        with self.assertRaisesRegex(ValueError, "forty-two-source qualification matrix"):
            self.audit()

    def test_missing_baseline_execution_is_not_complete_matrix(self):
        self.rows.pop()
        self.receipts.pop()
        self.executions.pop()
        self.classifications.pop()
        with self.assertRaisesRegex(ValueError, "forty-two-source qualification matrix"):
            self.audit(rebind_matrix=True)

    def test_golden_cannot_substitute_for_shipped_baseline_source(self):
        self.rows[40]["files"] = deepcopy(self.rows[0]["files"])
        self.rebuild_row(40)
        with self.assertRaisesRegex(ValueError, "Baseline eligibility"):
            self.audit(rebind_matrix=True)

    def test_golden_must_match_frozen_project_source(self):
        self.rows[0]["files"] = {"solution.py": "# source from an unrelated golden\n"}
        self.rebuild_row(0)
        with self.assertRaisesRegex(ValueError, "golden source"):
            self.audit(rebind_matrix=True)

    def test_correct_controls_need_a_distinct_source_variant(self):
        self.rows[2]["files"] = deepcopy(self.rows[0]["files"])
        self.rebuild_row(2)
        with self.assertRaises(ValueError):
            self.audit(rebind_matrix=True)

    def test_execution_source_suite_and_purpose_must_match_bound_row(self):
        original = deepcopy(self.executions[0])
        for key in ("source_sha256", "suite_sha256", "purpose", "fixture_id", "receipt"):
            with self.subTest(key=key):
                self.executions[0] = dict(original, **{key: "forged"})
                with self.assertRaisesRegex(ValueError, "row identity"):
                    self.audit()
        self.executions[0] = original

    def test_physical_execution_flags_cannot_claim_reuse(self):
        original = deepcopy(self.executions[0])
        for key, value in (("physically_executed", False), ("reused", True), ("verified", False),
                           ("physically_executed", 1), ("reused", 0)):
            with self.subTest(key=key, value=value):
                self.executions[0] = dict(original, **{key: value})
                with self.assertRaisesRegex(ValueError, "row identity"):
                    self.audit()
        self.executions[0] = original

    def test_wrong_typed_actual_cannot_pass(self):
        self.receipts[0]["outcomes"][0]["actual"] = 1
        with self.assertRaisesRegex(ValueError, "wrong typed value"):
            self.audit()

    def test_wrong_answer_equal_to_expectation_is_not_a_witness(self):
        index = self.rows[3]["groups"]["witness"][0]
        self.receipts[3]["outcomes"][index]["actual"] = True
        with self.assertRaisesRegex(ValueError, "wrong-answer outcome equals expectation"):
            self.audit()

    def test_crashing_mutant_cannot_qualify_from_a_red_test(self):
        index = self.rows[3]["groups"]["witness"][0]
        outcome = self.receipts[3]["outcomes"][index]
        outcome.pop("actual")
        outcome.update(status="error", error="synthetic crash")
        self.classifications[3] = classification(self.rows[3], self.receipts[3])
        self.assertFalse(self.classifications[3]["runnable"])
        self.assertFalse(self.classifications[3]["qualified"])
        with self.assertRaisesRegex(ValueError, "actual runnable outputs"):
            self.audit()

    def test_public_failing_mutant_cannot_qualify(self):
        self.receipts[3]["outcomes"][0].update(passed=False, status="wrong_answer", actual=False)
        self.classifications[3] = classification(self.rows[3], self.receipts[3])
        with self.assertRaisesRegex(ValueError, "actual runnable outputs"):
            self.audit()

    def test_mutant_without_wrong_answer_witness_cannot_qualify(self):
        index = self.rows[3]["groups"]["witness"][0]
        self.receipts[3]["outcomes"][index].update(passed=True, status="passed", actual=True)
        self.receipts[3].update(passed=True, status="passed")
        self.classifications[3] = classification(self.rows[3], self.receipts[3])
        with self.assertRaisesRegex(ValueError, "actual runnable outputs"):
            self.audit()

    def test_infrastructure_failure_cannot_qualify(self):
        self.receipts[0]["cleanup_verified"] = False
        with self.assertRaisesRegex(ValueError, "configuration/cleanup mismatch"):
            self.audit()

    def test_forged_qualified_classification_does_not_override_observations(self):
        self.classifications[3]["witness_wrong_answers"] = []
        with self.assertRaisesRegex(ValueError, "actual runnable outputs"):
            self.audit()

    def test_missing_and_duplicate_containers_are_rejected(self):
        original = self.receipts[1]["container_name"]
        for name in (None, "", "foreign-container", self.receipts[0]["container_name"]):
            with self.subTest(name=name):
                if name is None:
                    self.receipts[1].pop("container_name", None)
                else:
                    self.receipts[1]["container_name"] = name
                with self.assertRaisesRegex(ValueError, "fresh containers"):
                    self.audit()
        self.receipts[1]["container_name"] = original

    def test_receipt_source_suite_and_adapter_binding_cannot_be_rehashed_away(self):
        original = deepcopy(self.receipts[0])
        for key in ("source_sha256", "suite_sha256", "adapter_sha256", "image_id"):
            with self.subTest(key=key):
                self.receipts[0] = dict(original, **{key: "c" * 64})
                with self.assertRaises(ValueError):
                    self.audit()
        self.receipts[0] = original

    def test_retained_scientific_dependency_bytes_are_checked(self):
        (self.root / "inputs/gossip_harness/frozen_source.py").write_text("# changed\n")
        with self.assertRaisesRegex(ValueError, "dependency bytes changed"):
            self.audit()

    def test_omitted_or_reordered_dependency_inventory_is_rejected(self):
        original = deepcopy(self.manifest["inputs"])
        for inventory in (original[:-1], list(reversed(original))):
            with self.subTest(inventory=inventory):
                self.manifest["inputs"] = inventory
                with self.assertRaisesRegex(ValueError, "omitted or reordered"):
                    self.audit()
        self.manifest["inputs"] = original

    def test_missing_acceptance_case_is_rejected_even_with_new_matrix_binding(self):
        row = self.rows[0]
        row["groups"]["private"] = row["groups"]["private"][:-1]
        row["groups"]["witness"] = row["groups"]["witness"][:-1]
        self.rebuild_row(0)
        with self.assertRaisesRegex(ValueError, "omitted an acceptance scenario"):
            self.audit(rebind_matrix=True)

    def test_witness_cannot_be_an_extra_case_outside_private_acceptance(self):
        row = self.rows[3]
        row["cases"].append(case("synthetic-extra-red-test"))
        row["groups"]["witness"] = [len(row["cases"]) - 1]
        self.rebuild_row(3)
        with self.assertRaisesRegex(ValueError, "held-out acceptance case"):
            self.audit(rebind_matrix=True)

    def test_no_fewer_than_four_independent_fault_families_per_stage(self):
        for index in range(3, 10):
            self.rows[index]["family"] = "same-family"
            self.rebuild_row(index)
        with self.assertRaisesRegex(ValueError, "fault families"):
            self.audit(rebind_matrix=True)

    def test_four_distinct_fault_families_meet_registered_minimum(self):
        for index in range(3, 10):
            self.rows[index]["family"] = f"family-{(index - 3) % 4}"
            self.rebuild_row(index)
        self.report["conclusion"]["stages"][0].update(
            public_surviving_families=[f"family-{number}" for number in range(4)],
            public_surviving_family_count=4)
        self.assertEqual(len(self.audit(rebind_matrix=True)["containers"]), 42)

    def test_qualification_minimum_cannot_be_changed_or_float_aliased(self):
        for minimum in (1, 5, 4.0, True):
            with self.subTest(minimum=minimum):
                self.manifest["minimum_surviving_families"] = minimum
                with self.assertRaises(ValueError):
                    self.audit()

    def test_conclusion_must_match_independently_reconstructed_results(self):
        original = deepcopy(self.report["conclusion"])
        mutations = [dict(original, complete=False), dict(original, qualified=False)]
        for field, value in (("source_count", 9), ("source_count", 10.0),
                             ("public_surviving_family_count", 8), ("qualified", False),
                             ("public_surviving_families", ["invented-family"])):
            changed = deepcopy(original)
            changed["stages"][0][field] = value
            mutations.append(changed)
        changed = deepcopy(original)
        changed["baselines"].pop()
        mutations.append(changed)
        for conclusion in mutations:
            with self.subTest(conclusion=conclusion):
                self.report["conclusion"] = conclusion
                with self.assertRaises(ValueError):
                    self.audit()

    def test_runtime_identity_cannot_change_for_one_execution(self):
        self.executions[0]["runtime_identity_sha256"] = "e" * 64
        with self.assertRaisesRegex(ValueError, "row identity"):
            self.audit()

    def test_rehashed_runtime_cannot_omit_daemon_identity_or_change_image(self):
        original = deepcopy(self.runtime)
        for runtime in ({}, {key: value for key, value in original.items() if key != "daemon_id_sha256"},
                        dict(original, image=dict(original["image"], Id="sha256:" + "e" * 64))):
            with self.subTest(runtime=runtime):
                self.report["runtime_identity"] = runtime
                self.report["runtime_identity_sha256"] = digest(runtime)
                for execution in self.executions:
                    execution["runtime_identity_sha256"] = digest(runtime)
                with self.assertRaises(ValueError):
                    self.audit()


PROFILE_SOURCE = '''
MODEL = "cheap-pinned"
STRONG_MODEL = "strong-pinned"
CONTEXT_TOKENS = 8
MODEL_MAX_OUTPUT_TOKENS = 4
ENDPOINT = "https://api.openai.com/v1/responses"
PRICE_VERIFIED_ON = "2026-10-02"
MAX_REQUEST_BYTES = 500
MODEL_PROFILES: Mapping[str, ModelProfile] = MappingProxyType({
    MODEL: ModelProfile(MODEL, 8, 4, 750000, 4500000, "https://example.test/cheap"),
    STRONG_MODEL: ModelProfile(STRONG_MODEL, 12, 4, 2500000, 15000000,
        "https://example.test/strong", long_context_above_input_tokens=10,
        long_input_micro_usd_per_million=5000000, long_output_micro_usd_per_million=22500000),
})
'''


class FollowupModelProfileAuditTests(unittest.TestCase):
    def setUp(self):
        common = dict(max_output_tokens=4, price_verified_on="2026-10-02",
            endpoint="https://api.openai.com/v1/responses", service_tier="default", reasoning_effort="low",
            configured_max_output_tokens=2, max_request_bytes=500, cached_input_accounting="uncached_rate",
            reservation_policy="full_context_input_plus_configured_max_output")
        cheap = dict(common, model="cheap-pinned", context_tokens=8,
            input_micro_usd_per_million=750000, output_micro_usd_per_million=4500000,
            source_url="https://example.test/cheap", long_context_above_input_tokens=None,
            long_input_micro_usd_per_million=None, long_output_micro_usd_per_million=None,
            reservation_units=15)
        strong = dict(common, model="strong-pinned", context_tokens=12,
            input_micro_usd_per_million=2500000, output_micro_usd_per_million=15000000,
            source_url="https://example.test/strong", long_context_above_input_tokens=10,
            long_input_micro_usd_per_million=5000000, long_output_micro_usd_per_million=22500000,
            reservation_units=105)
        self.contract: dict[str, Any] = dict(output_tokens=2, models=dict(cheap=cheap, strong=strong),
            call_envelope=dict(maximum_physical_calls=256, maximum_calls_by_model=dict(cheap=160, strong=120),
                reservation_micro_usd_per_call=dict(cheap=15, strong=105),
                conservative_full_cohort_micro_usd=14640))

    def test_literal_registry_profiles_and_reservation_envelope_are_accepted(self):
        check_model_profiles(self.contract, PROFILE_SOURCE)

    def test_profile_read_does_not_execute_source(self):
        check_model_profiles(self.contract, PROFILE_SOURCE + '\nraise AssertionError("never execute")\n')

    def test_identity_pricing_context_and_cost_fields_are_checked(self):
        for field, value in (("model", "unpriced-model"), ("context_tokens", 9),
                             ("input_micro_usd_per_million", 1), ("reservation_units", 1),
                             ("price_verified_on", "forged"), ("endpoint", "https://example.test"),
                             ("configured_max_output_tokens", 3), ("max_request_bytes", 501)):
            with self.subTest(field=field):
                contract = deepcopy(self.contract)
                contract["models"]["cheap"][field] = value
                with self.assertRaisesRegex(ValueError, "pricing, model identity or reservation profile"):
                    check_model_profiles(contract, PROFILE_SOURCE)

    def test_long_context_reservation_uses_long_context_rates(self):
        self.contract["models"]["strong"]["reservation_units"] = 60
        self.contract["call_envelope"]["reservation_micro_usd_per_call"]["strong"] = 60
        self.contract["call_envelope"]["conservative_full_cohort_micro_usd"] = 9240
        with self.assertRaisesRegex(ValueError, "pricing, model identity or reservation profile"):
            check_model_profiles(self.contract, PROFILE_SOURCE)

    def test_model_maxima_are_not_additive_call_capacity(self):
        self.contract["call_envelope"]["maximum_physical_calls"] = 280
        with self.assertRaisesRegex(ValueError, "commitment envelope"):
            check_model_profiles(self.contract, PROFILE_SOURCE)

    def test_cost_envelope_respects_shared_256_call_limit(self):
        self.contract["call_envelope"]["conservative_full_cohort_micro_usd"] = 15000
        with self.assertRaisesRegex(ValueError, "commitment envelope"):
            check_model_profiles(self.contract, PROFILE_SOURCE)

    def test_typed_model_maxima_cannot_use_float_aliases(self):
        self.contract["call_envelope"]["maximum_calls_by_model"]["cheap"] = 160.0
        with self.assertRaisesRegex(ValueError, "commitment envelope"):
            check_model_profiles(self.contract, PROFILE_SOURCE)

    def test_nonliteral_profile_registry_is_rejected_without_evaluation(self):
        source = PROFILE_SOURCE[:PROFILE_SOURCE.index("MODEL_PROFILES:")] + "MODEL_PROFILES: dict = build_profiles()\n"
        with self.assertRaisesRegex(ValueError, "not literal"):
            check_model_profiles(self.contract, source)

    def test_profile_parser_rejects_unchecked_constructor_shapes(self):
        mutations = (
            PROFILE_SOURCE.replace("MappingProxyType({", "ReplacementWrapper({"),
            PROFILE_SOURCE.replace("ModelProfile(MODEL,", "ReplacementProfile(MODEL,"),
            PROFILE_SOURCE.replace("\n})", "\n}, unexpected=True)"),
            PROFILE_SOURCE.replace('"https://example.test/cheap"),',
                                   '"https://example.test/cheap", endpoint="ignored"),'),
        )
        for source in mutations:
            with self.subTest(source=source):
                with self.assertRaises(ValueError):
                    check_model_profiles(self.contract, source)

    def test_profile_parser_rejects_later_registry_or_constant_rebinding(self):
        for statement in ('MODEL_PROFILES = {}', 'ENDPOINT = choose_endpoint()',
                          'ENDPOINT = "https://changed.test"'):
            with self.subTest(statement=statement):
                with self.assertRaises(ValueError):
                    check_model_profiles(self.contract, PROFILE_SOURCE + "\n" + statement + "\n")

    def test_reservation_money_requires_integer_units(self):
        self.contract["call_envelope"]["conservative_full_cohort_micro_usd"] = 14640.0
        with self.assertRaisesRegex(ValueError, "commitment envelope"):
            check_model_profiles(self.contract, PROFILE_SOURCE)

    def test_fractional_micro_unit_reservation_rounds_up(self):
        source = PROFILE_SOURCE.replace("750000, 4500000", "750001, 4500000")
        self.contract["models"]["cheap"].update(input_micro_usd_per_million=750001, reservation_units=16)
        self.contract["call_envelope"]["reservation_micro_usd_per_call"]["cheap"] = 16
        self.contract["call_envelope"]["conservative_full_cohort_micro_usd"] = 14776
        check_model_profiles(self.contract, source)

    def test_long_context_threshold_is_strictly_greater_than(self):
        for context, reservation, full_cost in ((10, 55, 8640), (11, 100, 14040)):
            with self.subTest(context=context):
                contract = deepcopy(self.contract)
                source = PROFILE_SOURCE.replace("STRONG_MODEL, 12, 4", f"STRONG_MODEL, {context}, 4")
                contract["models"]["strong"].update(context_tokens=context, reservation_units=reservation)
                contract["call_envelope"]["reservation_micro_usd_per_call"]["strong"] = reservation
                contract["call_envelope"]["conservative_full_cohort_micro_usd"] = full_cost
                check_model_profiles(contract, source)


if __name__ == "__main__":
    unittest.main()
