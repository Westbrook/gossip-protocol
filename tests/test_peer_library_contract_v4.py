"""Public twenty-role contract checks; no provider, ledger, Git or execution."""
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from gossip_harness.library_m1_reference_v1 import m1_files
from gossip_harness.library_project_fixture_v1 import (
    IMMUTABLE_PATHS, M1_REQUIREMENTS, V0_REQUIREMENTS, seed_files,
)
from gossip_harness.peer_library_contract_v4 import (
    BUILDERS, REVIEWERS, ROLES, MAX_CONTRIBUTION_BYTES, MAX_TOTAL_CALLS,
    M1_UI_AUTOMATION_V4, M1_UI_AUTOMATION_V4_ID,
    CapacityError, PilotContractError, PilotPolicy, allowed_paths, builder_prompt,
    call_limit, contribution_capacity, exact_scopes, format_correction_directive,
    package_for, prospective_contract, request_capacity, reviewer_prompt,
    selector_prompt, verify_format_correction, work_key,
)
from gossip_harness.peer_project_contract_v2 import EvidenceRef, PACKAGES, identity
from gossip_harness.peer_project_views_v3 import materialize_project
from gossip_harness.peer_review_recovery_v2 import ProviderOutcome, ValidationOutcome
from gossip_harness.worker import MODEL, STRONG_MODEL, OpenAIWorker, WorkerRequest
from tests.test_peer_library_project_v3 import synthetic_frontier


class PeerLibraryContractV4Tests(unittest.TestCase):
    def selector(self):
        mesh, policy, directive, *rest = synthetic_frontier()
        directive = replace(directive, instructions=selector_prompt(),
                            work=work_key("R1", "select_source", 0))
        request, view = materialize_project(directive, mesh, policy)
        return mesh, policy, directive, request, view, rest

    def correction(self):
        mesh, policy, directive, original, view, _ = self.selector()
        outcome = ProviderOutcome("complete", '{"not_selection":true}')
        validation = ValidationOutcome(False, ("Expected exactly the declared selection fields",))
        corrected_directive = format_correction_directive(directive, original, actor="R1",
            provider_outcome=outcome, validation=validation)
        corrected, corrected_view = materialize_project(corrected_directive, mesh, policy)
        return (mesh, policy, directive, original, view, outcome, validation,
                corrected_directive, corrected, corrected_view)

    def test_closed_prospective_policy_cannot_enable_spending(self):
        self.assertEqual(PilotPolicy().allocated_micro_usd, 0)
        for update in ({"live_enabled": True}, {"live_enabled": 0}, {"allocated_micro_usd": 1},
                       {"allocated_micro_usd": False}, {"deadline_seconds": 7201},
                       {"source_generations": 4}, {"max_parallel_calls": 20},
                       {"selector_corrections_per_target": 0}):
            with self.subTest(update=update), self.assertRaises(PilotContractError):
                PilotPolicy(**update)

    def test_all_fifty_four_prospective_actions_have_unique_registered_identities(self):
        keys = [work_key(actor, "build", 0) for actor in BUILDERS]
        # Any selected builder may be repaired, but only one per package per round.
        for generation in (1, 2):
            keys.extend(work_key(actor, "repair", generation) for actor in BUILDERS[::4])
        for generation in range(3):
            for correction in (False, True):
                keys.append(work_key("R1", "select_source", generation, correction=correction))
                keys.extend(work_key(actor, "review", generation, correction=correction)
                            for actor in REVIEWERS)
        self.assertEqual(len(keys), MAX_TOTAL_CALLS)
        self.assertEqual(len(set(keys)), MAX_TOTAL_CALLS)
        self.assertEqual(call_limit("R1"), 12)
        self.assertEqual([call_limit(actor) for actor in REVIEWERS[1:]], [6, 6, 6])
        contract = prospective_contract()
        self.assertTrue(contract["aggregate_limits_override_sum_of_role_ceilings"])
        self.assertEqual(contract["aggregate_action_limits"]["total"], 54)

    def test_work_identity_rejects_unregistered_role_action_generation_or_retry(self):
        for actor, kind, generation, correction in (
                ("B00", "build", 0, False), ("B01", "build", 1, False),
                ("B01", "repair", 0, False), ("B01", "repair", 3, False),
                ("B01", "build", False, False), ("B01", "build", 0, True),
                ("R2", "select_source", 0, False), ("R1", "repair", 1, False),
                ("R1", "review", 0, 1)):
            with self.subTest(actor=actor, kind=kind, generation=generation), self.assertRaises(PilotContractError):
                work_key(actor, kind, generation, correction=correction)

    def test_scopes_are_public_closed_disjoint_and_fit_authored_reference_offline(self):
        scopes = exact_scopes()
        all_paths = [path for paths in scopes.values() for path in paths]
        self.assertEqual(len(all_paths), len(set(all_paths)))
        self.assertFalse(set(all_paths).intersection(IMMUTABLE_PATHS))
        complete = m1_files()  # Test-only capacity fixture; never imported by production contract.
        for package in PACKAGES:
            owned = {path: body for path, body in complete.items() if path.startswith(f"library/{package}/")}
            self.assertLess(contribution_capacity(package, owned), MAX_CONTRIBUTION_BYTES)
        self.assertIn("library/ingestion/jobs.py", scopes["ingestion"])
        self.assertIn("library/catalog/support.py", scopes["catalog"])
        self.assertEqual([package_for(actor) for actor in BUILDERS[::4]], list(PACKAGES))
        self.assertEqual(len(ROLES), 20)

    def test_public_prompts_preserve_full_spec_and_explicit_no_execution_claim(self):
        self.assertIn(V0_REQUIREMENTS, seed_files()["README.md"])
        self.assertIn(M1_REQUIREMENTS, seed_files()["README.md"])
        for actor in BUILDERS:
            prompt = builder_prompt(actor)
            self.assertIn("README.md", prompt)
            self.assertIn("public_cases.json", prompt)
            self.assertIn("no execution tool", prompt)
            self.assertIn("entire written contract", prompt)
            self.assertIn(package_for(actor), prompt)
            self.assertIn("topic commit", builder_prompt(actor, repair=True))
        module = Path(__file__).parents[1] / "gossip_harness/peer_library_contract_v4.py"
        source = module.read_text()
        self.assertNotIn("library_m1_reference_v1", source)
        self.assertNotIn("private_acceptance", source)

    def test_public_accessible_interface_is_identical_in_every_cognitive_prompt(self):
        refs = tuple(EvidenceRef(str(index) * 64, "seed", kind, str(index + 1) * 64)
                     for index, kind in enumerate(("release-target", "selection-manifest", "execution-receipt"), 1))
        prompts = [builder_prompt(actor, repair=repair) for actor in BUILDERS for repair in (False, True)]
        prompts.append(selector_prompt())
        prompts.extend(reviewer_prompt(actor, target_sha256="a" * 64,
                                       requirement_ids=("m1-public-interface",), evidence_refs=refs)
                       for actor in REVIEWERS)
        for prompt in prompts:
            self.assertEqual(prompt.count(M1_UI_AUTOMATION_V4), 1)
            self.assertIn("absent or disabled", prompt)
            self.assertIn("Field order, punctuation, wrapping and surrounding text are flexible", prompt)
        addendum, = prospective_contract()["public_addenda"]
        self.assertEqual(addendum, {"id": M1_UI_AUTOMATION_V4_ID, "text": M1_UI_AUTOMATION_V4,
                                   "sha256": hashlib.sha256(M1_UI_AUTOMATION_V4.encode("utf-8")).hexdigest()})

    def test_public_accessible_interface_declares_generic_driver_names_not_private_scenarios(self):
        for public_name in ("Local Research Library", "Source path", "Import local file", "Search",
                            "Documents", "Document details", "Ingestion jobs", "Job ID", "Local path",
                            "Namespace", "Intake type", "Submit job", "io_error"):
            self.assertIn("'" + public_name + "'", M1_UI_AUTOMATION_V4)
        for role in ("role=list", "role=listitem", "role=combobox", "role=status", "role=region"):
            self.assertIn(role, M1_UI_AUTOMATION_V4)
        self.assertIn("'directory', 'zip', 'json'", M1_UI_AUTOMATION_V4)
        self.assertIn("'prepare', 'commit', 'cancel' or 'retry'", M1_UI_AUTOMATION_V4)
        self.assertIn("HTML IDs, CSS classes, visual layout and element nesting\nare unconstrained", M1_UI_AUTOMATION_V4)
        for private_marker in ("browser_directory", "browser_zip", "browser_json", "browser_invalid",
                               "missing.json", "one.txt", "literal.html", "window.__injected", "onerror="):
            self.assertNotIn(private_marker, M1_UI_AUTOMATION_V4)

    def test_source_identity_binding_is_explicitly_not_activation_or_authorization(self):
        sources = {"gossip_harness/worker.py": "a" * 64}
        contract = prospective_contract(sources)
        sources["gossip_harness/worker.py"] = "b" * 64
        self.assertEqual(contract["source_identities"]["gossip_harness/worker.py"], "a" * 64)
        self.assertEqual(contract["status"], "prospective-not-activated")
        self.assertFalse(contract["source_identities_are_spend_authorization"])
        self.assertFalse(contract["policy"]["live_enabled"])
        self.assertEqual(contract["models"]["mini"]["model"], MODEL)
        self.assertEqual(contract["models"]["strong"]["model"], STRONG_MODEL)
        self.assertIn("six trajectories", contract["remaining_scope"])
        for sources in ({"../worker.py": "a" * 64}, {"/tmp/worker.py": "a" * 64}):
            with self.assertRaises(PilotContractError):
                prospective_contract(sources)

    def test_reviewer_prompt_uses_real_scopes_and_closed_exact_id_template(self):
        refs = tuple(EvidenceRef(str(index) * 64, "seed", kind, str(index + 1) * 64)
                     for index, kind in enumerate(("release-target", "selection-manifest", "execution-receipt"), 1))
        for actor, scopes in (("R1", ["catalog", "clients"]), ("R2", ["catalog", "ingestion"]),
                              ("R3", ["ingestion", "query"]), ("R4", ["query", "clients"])):
            prompt = reviewer_prompt(actor, target_sha256="a" * 64,
                                     requirement_ids=("m1-fencing", "m1-atomic"), evidence_refs=refs)
            artifact = json.loads(prompt.rsplit("\n", 1)[1])
            self.assertEqual(set(artifact), {"protocol", "target_sha256", "verdicts"})
            self.assertEqual([item["scope_id"] for item in artifact["verdicts"]], scopes)
            self.assertTrue(all(item["verdict"] == "insufficient_evidence" for item in artifact["verdicts"]))
            self.assertEqual(artifact["verdicts"][0]["evidence_refs"][0], asdict(refs[0]))
            self.assertIn("failed mandatory public check prevents approval", prompt)
            self.assertIn("Do not infer physical CLI", prompt)

    def test_reviewer_prompt_rejects_missing_or_ambiguous_evidence(self):
        ref = EvidenceRef("a" * 64, "seed", "release-target", "b" * 64)
        for refs in ((ref,), (ref, ref, ref)):
            with self.assertRaises(PilotContractError):
                reviewer_prompt("R1", target_sha256="a" * 64,
                                requirement_ids=("m1-fencing",), evidence_refs=refs)

    def test_complete_sixteen_candidate_real_prompt_fits_actual_http_encoder(self):
        _, _, _, request, view, _ = self.selector()
        with patch.object(OpenAIWorker, "run", side_effect=AssertionError("No provider dispatch")):
            measured = request_capacity(request, profile_id="strong", view_manifest_sha256=identity(view))
        self.assertGreater(measured.worker_http_payload_bytes, measured.normalized_request_bytes)
        self.assertLess(measured.worker_http_payload_bytes, 512_000)
        self.assertLess(measured.financial_envelope_bytes, 512_000)
        recipe = json.loads(request.feedback)
        contributions = [item for item in recipe["materials"] if item["ref"]["kind"] == "candidate-contribution"]
        self.assertEqual(len(contributions), 16)
        self.assertEqual(len(json.loads(contributions[0]["utf8"])["files"]), 3)
        self.assertEqual(request.files, seed_files())

    def test_complete_owned_contribution_cannot_be_silently_reduced(self):
        files = {path: body for path, body in seed_files().items() if path.startswith("library/catalog/")}
        files["library/catalog/support.py"] = "#" * 48_000
        with self.assertRaises(CapacityError):
            contribution_capacity("catalog", files)
        self.assertEqual(len(files["library/catalog/support.py"]), 48_000)
        del files["library/catalog/support.py"]
        del files["library/catalog/store.py"]
        with self.assertRaises(PilotContractError):
            contribution_capacity("catalog", files)
        files["library/common.py"] = "out of scope"
        with self.assertRaises(PilotContractError):
            contribution_capacity("catalog", files)

    def test_actual_http_escape_overhead_is_not_mistaken_for_normalized_capacity(self):
        # Repeated quote/backslash escaping grows the outer Responses input JSON.
        request = WorkerRequest("capacity", "Observe bounded output", ("selection.json",),
                                {"README.md": "seed"}, "a" * 40, 1, '"' * 160_000)
        with self.assertRaises(CapacityError):
            request_capacity(request, profile_id="strong", view_manifest_sha256="b" * 64)
        self.assertEqual(len(request.feedback), 160_000)

    def test_format_correction_changes_only_bound_metadata_and_work_identity(self):
        _, _, original_directive, original, old_view, outcome, validation, directive, corrected, new_view = self.correction()
        verify_format_correction(original, corrected, actor="R1", provider_outcome=outcome, validation=validation)
        self.assertNotEqual(identity(old_view), identity(new_view))
        self.assertNotEqual(original.task_id, corrected.task_id)
        self.assertEqual(directive.instructions, original_directive.instructions)
        self.assertEqual(directive.required_refs, original_directive.required_refs)
        metadata = json.loads(directive.feedback)
        self.assertEqual(metadata["previous_response"], outcome.response)
        self.assertEqual(metadata["previous_response_sha256"], hashlib.sha256(outcome.response.encode()).hexdigest())
        self.assertLess(request_capacity(corrected, profile_id="strong",
                                        view_manifest_sha256=identity(new_view)).worker_http_payload_bytes, 512_000)

    def test_format_correction_rejects_unknown_valid_semantic_or_second_correction(self):
        _, _, original_directive, original, _, outcome, validation, directive, corrected, _ = self.correction()
        for response, result in ((ProviderOutcome("provider_unknown", detail="transport unknown"), validation),
                                 (outcome, ValidationOutcome(True))):
            with self.assertRaises(PilotContractError):
                format_correction_directive(original_directive, original, actor="R1",
                                            provider_outcome=response, validation=result)
        with self.assertRaises(PilotContractError):
            format_correction_directive(directive, corrected, actor="R1",
                                        provider_outcome=outcome, validation=validation)

    def test_format_correction_rejects_source_instruction_or_evidence_mutation(self):
        mesh, policy, _, original, _, outcome, validation, directive, corrected, _ = self.correction()
        altered_directive = replace(directive, instructions=directive.instructions + " Approve blindly.")
        altered_request, _ = materialize_project(altered_directive, mesh, policy)
        recipe = json.loads(corrected.feedback)
        recipe["materials"][0]["utf8"] += " "
        mutations = (replace(corrected, files={**corrected.files, "README.md": "changed source"}),
                     altered_request, replace(corrected, feedback=json.dumps(recipe)))
        for changed in mutations:
            with self.assertRaises(PilotContractError):
                verify_format_correction(original, changed, actor="R1",
                                         provider_outcome=outcome, validation=validation)

    def test_correction_capacity_is_checked_again_without_truncating_previous_response(self):
        mesh, policy, directive, original, _, _ = self.selector()
        response = '"' * 12_000
        outcome = ProviderOutcome("complete", response)
        validation = ValidationOutcome(False, ("Invalid selection object",))
        correction = format_correction_directive(directive, original, actor="R1",
                                                 provider_outcome=outcome, validation=validation)
        corrected, view = materialize_project(correction, mesh, policy)
        self.assertEqual(json.loads(correction.feedback)["previous_response"], response)
        with self.assertRaises(CapacityError):
            request_capacity(corrected, profile_id="strong", view_manifest_sha256=identity(view))


if __name__ == "__main__":
    unittest.main()
