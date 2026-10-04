"""Prospective batch-capture qualification of eight process histories and four mutants.

This new source/protocol contract requires fresh physical execution. The original
D03 incomplete observation is preserved and grants no credit here. History, CLI,
probe and cleanup bounds are unchanged; batch capture adds its fixed 60/5 policy.

The corrected reference is an authored candidate fixture, never a scientific
sample or whole-project acceptance. Host code handles its inert source bytes;
only fresh containers execute candidate code. Public expected answers stay in
the host profile and never enter input staging, setup argv or helper requests.

Each explicit Docker class owns one history and fresh journals/resources. This
permits separate evidence, but the central runner may cancel queued classes on
an unexpected failure. Planned, completed, failed and unrun remain distinct.
"""
from __future__ import annotations

import ast
from collections import Counter
from contextlib import closing
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import time
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_checkpoint_head_v1 as head
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import candidate_product_process_cases_v1 as declarations
from gossip_harness import candidate_product_process_core_v1 as core
from gossip_harness import candidate_product_process_execution_v1 as execution
from gossip_harness import candidate_product_process_observation_v1 as observer
from gossip_harness import candidate_product_process_reader_v1 as reader
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.candidate_scope_consumer_v1 import AuthorityError
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
from gossip_harness.library_v2_json_reference_v2 import (
    corrected_v2_binary_files, corrected_v2_files, corrected_v2_source_inputs,
)
from tests import candidate_engine_control_framing_v1 as control_framing
from tests.test_candidate_clients_docker_v4 import make_store

PROTOCOL = "candidate-product-process-physical-qualification-batch-v1"
COHORT = ("fixture-original", *("fixture-reserved-" + str(i) for i in range(1, 6)))
CASE_IDS = (
    "process-worker-explicit-enrollment-order",
    "process-worker-terminal-retry-fencing",
    "process-reindex-resume-and-generation-restart",
    "process-export-canonical-selection-and-byte-limit",
    "process-backup-root-explicit-adoption-and-cas",
    "process-backup-restore-fences-edits-and-removed-id",
    "process-public-v0-migration-and-restart",
    "process-public-m2-migration-and-restart",
)
# Every defect changes one generated source file at an exact, unique seam.
# No input, expected answer, evaluator, setup or product definition is changed.
MUTATIONS = {
    "export-newline": (
        "library/query/legacy_m3_service.py",
        "return 200, canonical_export_bytes(self.export_bundle(**value))",
        "return 200, canonical_export_bytes(self.export_bundle(**value)) + b'\\n'",
    ),
    "retry-drops-enrollment": (
        "library/catalog/m4_control.py",
        '            self.db.execute("UPDATE jobs SET epoch=?,state=\'queued\',completed=0,error=NULL,receipt=NULL WHERE job_id=?",(epoch,job_id))\n',
        '            self.db.execute("UPDATE jobs SET epoch=?,state=\'queued\',completed=0,error=NULL,receipt=NULL WHERE job_id=?",(epoch,job_id))\n'
        '            self.db.execute("UPDATE job_control SET enrolled=0 WHERE job_id=?",(job_id,))\n',
    ),
    "reimport-loses-high-water": (
        "library/catalog/m4_store.py",
        "edit = increment(high[0]) if high is not None else 1",
        "edit = 1",
    ),
    "migration-loses-notes": (
        "library/catalog/m4_store.py",
        '            self.db.execute("INSERT INTO metadata VALUES (\'schema\',\'4\') ON CONFLICT(key) DO UPDATE SET value=\'4\'")',
        '            self.db.execute("UPDATE document_state SET notes=\'\'")\n'
        '            self.db.execute("INSERT INTO metadata VALUES (\'schema\',\'4\') ON CONFLICT(key) DO UPDATE SET value=\'4\'")',
    ),
}
DEFECTS = (
    ("D01", CASE_IDS[3], "export-newline", (
        ("s005-http", "canonical_wire_bytes"), ("s008-http", "canonical_wire_bytes"))),
    ("D02", CASE_IDS[1], "retry-drops-enrollment", (
        ("s010-cli", "json_exact"), ("s014-http", "json_exact"),
        ("s015-http", "json_field:generation"), ("s015-http", "json_field:documents"),
        ("s015-http", "json_field:revisions"), ("s015-http", "json_field:blobs"))),
    ("D03", CASE_IDS[5], "reimport-loses-high-water", (("s018-cli", "json_exact"),)),
    ("D04", CASE_IDS[7], "migration-loses-notes", (
        ("s002-http", "json_exact"), ("s003-http", "json_exact"),
        ("s013-cli", "json_exact"), ("s016-http", "json_exact"))),
)


def controls():
    return tuple({"control_id": "P" + str(i + 1).zfill(2), "case_id": case,
                  "candidate_variant": "reference", "expected_failed_facets": []}
                 for i, case in enumerate(CASE_IDS)) + tuple(
        {"control_id": identifier, "case_id": case, "candidate_variant": variant,
         "expected_failed_facets": [list(pair) for pair in facets]}
        for identifier, case, variant, facets in DEFECTS)


def qualification_policy():
    # Existing executor divides this into 1500s work and 300s cleanup reserve.
    # Wire 10s and helper/CLI 30s remain defaults, fixed before any execution.
    return execution.HttpPolicy(RUNTIME_IMAGE, lifetime_seconds=1800)


def candidate_files(variant):
    inputs = corrected_v2_source_inputs()
    texts, binaries = corrected_v2_files(), corrected_v2_binary_files()
    if set(texts).intersection(binaries):
        raise ValueError("Reference text/binary collision")
    original = {name: value.encode("utf-8") for name, value in texts.items()} | binaries
    result = dict(original)
    mutation = None
    if variant != "reference":
        path, before, after = MUTATIONS[variant]
        raw = result[path].decode("utf-8")
        if raw.count(before) != 1:
            raise ValueError("Declared unique mutation seam changed: " + variant)
        result[path] = raw.replace(before, after, 1).encode("utf-8")
        ast.parse(result[path], filename=path)
        if {name for name in original if original[name] != result[name]} != {path}:
            raise ValueError("Mutation changed an unexpected file set")
        mutation = {"path": path, "before_literal": before, "after_literal": after,
                    "original_sha256": execution.sha256(original[path]),
                    "mutated_sha256": execution.sha256(result[path])}
    return result, {"variant": variant, "generator_inputs": inputs,
        "original_source_manifest": execution.source_manifest(original),
        "candidate_source_manifest": execution.source_manifest(result), "mutation": mutation,
        "authored_candidate_fixture": True, "scientific_samples": 0}


def write_new(path, value):
    raw = execution.encoded(value)
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return raw


def planned_census():
    cases = {case["id"]: case for case in declarations.acceptance_cases()}
    rows = []
    for row in controls():
        actions = cases[row["case_id"]]["input"]["actions"]
        rows.append({**row, "planned_actions": len(actions),
                     "planned_by_kind": dict(Counter(item["op"] for item in actions))})
    return {"protocol": PROTOCOL, "controls": rows,
        "public_definition_histories": 8, "positive_candidate_histories": 8, "positive_actions": 150,
        "defect_candidate_histories": 4, "defect_actions": 69,
        "total_candidate_histories": 12, "total_actions": 219,
        "purpose": "public_release", "original_definition_purpose": "public_product_definition",
        "fixture_authority": "synthetic qualification registration; no production scope or study authority",
        "scientific_samples": 0, "whole_project_acceptance": False,
        "policy": asdict(qualification_policy()), "cleanup_reserve_seconds": execution.CLEANUP_SECONDS,
        "runner_may_cancel_queued_controls": True, "adaptive_deadlines": False,
        "execution_protocol": execution.BATCH_PROTOCOL,
        "source_capture": execution.source_capture.BatchCapturePolicy().record(),
        "physical_execution_reuse": False, "prior_protocol_results_reusable": False}


class _PhysicalProductControl:
    """Shared trusted host setup/assertions; no test methods or shared containers."""
    CONTROL_ID = ""

    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("product-process-physical-batch-v1-" + cls.CONTROL_ID.lower(), retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.outcome = {"control_id": cls.CONTROL_ID, "status": "not-run", "scientific_samples": 0,
                       "whole_project_acceptance": False, "qualification_verified": False,
                       "terminal_census": None, "cleanup_verified": None,
                       "initial_state_setup_verified": None, "authenticated_action_count": None}
        cls.addClassCleanup(cls.save_census)
        cls.definition = next(row for row in controls() if row["control_id"] == cls.CONTROL_ID)
        write_new(cls.artifacts.root / "prospective-controls.json", planned_census())
        cls.endpoint = execution.engine.EngineEndpoint.from_environment()
        def retain_runtime(name, raw):
            with (cls.artifacts.root / name).open("xb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
        cls.runtime = execution.engine.runtime_identity(cls.endpoint, RUNTIME_IMAGE,
            retain=retain_runtime, label="class-runtime")

    @classmethod
    def save_census(cls):
        write_new(cls.artifacts.root / "completion-census.json", {"protocol": PROTOCOL,
            "outcome": cls.outcome, "this_class_planned_histories": 1,
            "this_class_qualified_histories": int(cls.outcome["qualification_verified"]),
            "scope": "authored fixture observation qualification", "scientific_samples": 0,
            "whole_project_acceptance": False})

    def descriptor_bytes(self, owner, descriptor):
        raw = owner.read_authenticated(descriptor["path"])
        self.assertEqual(len(raw), descriptor["bytes"])
        self.assertEqual(execution.sha256(raw), descriptor["sha256"])
        return raw

    def command(self, owner, label, argv):
        record = owner.json_authenticated(label + ".json")
        self.assertEqual(record["argv"], ["docker", "--host", "unix://" + self.endpoint.socket_path, *argv])
        self.assertIs(type(record["exit_code"]), int)
        self.assertEqual(record["exit_code"], 0)
        self.assertIs(record["capture_complete"], True)
        self.assertIs(record["timed_out"], False)
        raw = {}
        for channel in ("stdout", "stderr"):
            part = record[channel]
            self.assertEqual(part["path"], label + "-" + channel + ".bin")
            self.assertIs(part["truncated"], False)
            raw[channel] = self.descriptor_bytes(owner, part)
            self.assertEqual(part["observed_bytes"], len(raw[channel]))
        return raw

    def inspect(self, owner, label):
        # This independent framing helper reads retained bytes only; capture EOF
        # remains the original producer's separate authenticated temporal fact.
        framed = control_framing.decode_control_frame(owner.read_authenticated(label + "-response.bin"))
        self.assertEqual(framed["status"], 200)
        self.assertIs(framed["framing_complete"], True)
        return execution.engine.strict_json_loads(framed["body"])

    def position(self, owner, name):
        self.assertIsNotNone(owner.journal)
        return owner.journal._chain.position(name)

    def assert_setup(self, owner):
        recipe = owner.recipe
        setup = execution.setup_definition(recipe)
        intent = owner.json_authenticated("intent.json")
        keeper = self.inspect(owner, "keeper-running")
        raw_intent = owner.json_authenticated("initial-state-intent.json")
        self.assertEqual(raw_intent["setup"], setup)
        self.assertEqual(raw_intent["setup_sha256"], execution.digest(setup))
        self.assertEqual(raw_intent["keeper_id"], keeper["Id"])
        self.assertEqual(raw_intent["volume"], intent["volume"])
        argv = execution.setup_argv(recipe, keeper["Id"])
        self.assertEqual(raw_intent["argv"], argv)
        streams = self.command(owner, "initial-state-bootstrap", argv[1:])
        self.assertEqual(streams["stdout"], execution.setup_expected_output(recipe))
        self.assertEqual(streams["stderr"], b"")
        complete = owner.json_authenticated("initial-state-complete.json")
        self.assertEqual(complete["ack_sha256"], execution.sha256(streams["stdout"]))
        self.assertEqual(complete["setup_sha256"], execution.digest(setup))
        self.assertIs(setup["candidate_code_executed"], False)
        self.assertEqual(setup["directories"], ["/tmp/backups", "/tmp/next"])
        self.assertEqual({row["Destination"] for row in keeper["Mounts"]}, {"/tmp"})
        volume = keeper["Mounts"][0]
        self.assertEqual(volume["Name"], intent["volume"])
        self.assertIs(volume["RW"], True, "Only this versioned seeded keeper permits fixed bootstrap writes")
        self.assertEqual(keeper["HostConfig"]["NetworkMode"], "none")
        self.assertEqual(keeper["Path"], "python")
        self.assertEqual(keeper["Config"]["Entrypoint"], ["python"])
        self.assertEqual(keeper["Args"], ["-I", "-c", "import time;time.sleep(" + str(owner.policy.lifetime_seconds) + ")"])
        self.assertEqual(keeper["Config"]["User"], "65534:65534")
        if recipe.seed_database_fixture is None:
            self.assertIsNone(setup["seed"])
        else:
            self.assertEqual(recipe.seed_database_fixture, "initial.sqlite")
            raw = dict(recipe.fixtures)[recipe.seed_database_fixture]
            original = json.loads(owner.profile.case.original_json)["input"]["snapshot"]
            self.assertEqual(setup["seed"], {"fixture_path": "initial.sqlite", "bytes": len(raw),
                                           "sha256": execution.sha256(raw)})
            self.assertEqual(setup["seed"]["sha256"], original["sha256"])
            self.assertEqual(setup["seed"]["bytes"], original["bytes"])
            self.assertEqual(recipe.steps[0].kind, "cli")
            self.assertEqual(recipe.steps[0].argv[-1], "migrate")
        setup_position = self.position(owner, "initial-state-complete.json")
        self.assertLess(self.position(owner, "initial-state-intent.json"), setup_position)
        for index, step in enumerate(recipe.steps):
            if step.kind in ("start", "cli", "probe"):
                label = "step-" + str(index).zfill(3) + ("-cli" if step.kind == "cli" else "-probe" if step.kind == "probe" else "")
                self.assertLess(setup_position, self.position(owner, label + "-create-intent.json"))
        return {"setup_sha256": execution.digest(setup), "seed": setup["seed"],
                "setup_before_every_candidate_creation": True, "keeper_id": keeper["Id"]}

    def assert_cleanup_and_order(self, owner, terminal):
        names = [path.name for path in owner.root.glob("*-intent.json") if owner.has_authenticated(path.name)]
        creates, removals = [], {}
        for name in names:
            record = owner.json_authenticated(name)
            if name.endswith("-create-intent.json"):
                creates.append((name.removesuffix("-create-intent.json"), record))
            elif {"container_id", "spec", "force", "inspection_sha256"} <= set(record):
                self.assertNotIn(record["spec"]["name"], removals)
                removals[record["spec"]["name"]] = name.removesuffix("-intent.json"), record
        self.assertEqual(terminal["create_intents"], len(creates))
        self.assertEqual(terminal["unconfirmed_creates"], 0)
        self.assertEqual(set(terminal["cleanup"]), set(removals))
        self.assertTrue(all(value is True for value in terminal["cleanup"].values()))
        self.assertEqual(set(removals), {creation["spec"]["name"] for _, creation in creates})
        ids = []
        for prefix, creation in creates:
            spec = creation["spec"]
            created = self.command(owner, prefix + "-create", creation["create_argv"][1:])["stdout"].strip().decode("ascii")
            self.assertRegex(created, r"^[0-9a-f]{64}$")
            ids.append(created)
            label, removal = removals[spec["name"]]
            self.assertEqual(removal["container_id"], created)
            self.assertEqual(execution.encoded(removal["spec"]), execution.encoded(spec))
            owned = execution.engine.strict_json_loads(self.command(owner, label + "-inspect",
                ["inspect", "--format", "{{json .}}", spec["name"]])["stdout"])
            self.assertEqual(owned["Id"], created)
            self.assertEqual(owned["Name"], "/" + spec["name"])
            self.assertEqual(owned["Image"], RUNTIME_IMAGE)
            self.assertEqual(owned["Config"]["Labels"], dict(spec["labels"]))
            self.assertEqual(owned["Config"]["Labels"]["gossip.source"], owner.binding.source_sha256)
            self.assertEqual(owned["Config"]["Labels"]["gossip.fixture"], owner.binding.fixture_sha256)
            self.assertEqual(execution.digest(owned), removal["inspection_sha256"])
            self.command(owner, label + "-remove", ["rm", *(["--force"] if removal["force"] else []), created])
            absent = self.command(owner, label + "-absence", ["container", "ls", "--all", "--quiet",
                "--filter", "name=^/" + spec["name"] + "$"])["stdout"]
            self.assertEqual(absent, b"")
            self.assertLess(self.position(owner, prefix + "-create-intent.json"), self.position(owner, label + "-intent.json"))
            self.assertLess(self.position(owner, label + "-intent.json"), self.position(owner, label + "-remove.json"))
            self.assertLess(self.position(owner, label + "-remove.json"), self.position(owner, label + "-absence.json"))
            self.assertLess(self.position(owner, label + "-absence.json"), self.position(owner, "volume-cleanup-inspect.json"))
        self.assertEqual(len(ids), len(set(ids)))
        intent = owner.json_authenticated("intent.json")
        volume_args = ["volume", "inspect", "--format", "{{json .}}", intent["volume"]]
        volume_created = execution.engine.strict_json_loads(self.command(owner, "volume-created", volume_args)["stdout"])
        volume_owned = execution.engine.strict_json_loads(self.command(owner, "volume-cleanup-inspect", volume_args)["stdout"])
        self.assertEqual(execution.encoded(volume_created), execution.encoded(owner.json_authenticated("volume-baseline.json")))
        self.assertEqual(execution.encoded(volume_owned), execution.encoded(volume_created))
        self.assertEqual(volume_owned["Name"], intent["volume"])
        self.assertEqual(volume_owned["Driver"], "local")
        self.assertEqual(volume_owned["Options"], execution.VOLUME_OPTIONS)
        self.assertEqual(volume_owned["Labels"], {"gossip.execution": intent["execution_id"],
                                               "gossip.snapshot": execution.SNAPSHOT_PROTOCOL})
        self.assertLess(self.position(owner, "volume-intent.json"), self.position(owner, "volume-created.json"))
        self.assertLess(self.position(owner, "volume-created.json"), self.position(owner, "keeper-create-intent.json"))
        self.assertLess(self.position(owner, "volume-cleanup-inspect.json"), self.position(owner, "volume-remove.json"))
        self.command(owner, "volume-remove", ["volume", "rm", intent["volume"]])
        self.assertEqual(self.command(owner, "volume-absence", ["volume", "ls", "--quiet", "--filter",
            "name=^" + intent["volume"] + "$"])["stdout"], b"")
        self.assertIs(terminal["volume_cleanup"], True)
        self.assertLess(self.position(owner, "volume-remove.json"), self.position(owner, "volume-absence.json"))
        self.assertLess(self.position(owner, "volume-absence.json"), self.position(owner, "terminal.json"))
        last_retirement = None
        for index, step in enumerate(owner.recipe.steps):
            prefix = "step-" + str(index).zfill(3)
            if step.kind == "stop":
                last_retirement = prefix + "-retire-absence.json"
            elif step.kind == "cli" and last_retirement:
                self.assertLess(self.position(owner, last_retirement), self.position(owner, prefix + "-cli-create-intent.json"))
            elif step.kind == "start" and last_retirement:
                self.assertLess(self.position(owner, last_retirement), self.position(owner, prefix + "-create-intent.json"))
        return {"distinct_owned_containers": len(ids), "verified_container_absence": len(ids),
                "owned_volume_absence_verified": True, "normal_retirement_before_successors": True}

    def assert_complete_census(self, owner, original, terminal, history):
        expected = Counter(step.kind for step in owner.recipe.steps)
        self.assertEqual(original.status, "completed", original)
        self.assertEqual(original.infrastructure, ())
        self.assertEqual(original.missing_step_ids, ())
        self.assertIs(original.cleanup_verified, True)
        self.assertEqual(terminal["entered_step_indices"], list(range(len(owner.recipe.steps))))
        self.assertEqual(terminal["unentered_step_ids"], [])
        self.assertEqual(terminal["created_helpers"], expected["probe"])
        self.assertEqual(terminal["created_cli"], expected["cli"])
        self.assertEqual(terminal["created_servers"], expected["start"])
        self.assertEqual(terminal["created_keepers"], 1)
        for field in ("planned_requests", "observed_probe_records", "observed_requests", "attempted_probes"):
            self.assertEqual(terminal[field], expected["probe"], field)
        for field in ("planned_cli", "observed_cli_records", "attempted_cli"):
            self.assertEqual(terminal[field], expected["cli"], field)
        self.assertEqual(terminal["unknown_request_outcomes"], 0)
        self.assertEqual(terminal["unavailable_cli_attempts"], 0)
        self.assertEqual(len(history.steps), len(owner.recipe.steps))
        self.assertTrue(all(step.state == "authenticated" for step in history.steps))
        self.assertTrue(all(not step.limitations for step in history.steps))
        self.assertEqual(len(terminal["start_response_records"]), 1 + expected["start"] + expected["probe"])
        self.assertTrue(all(row["status"] == 204 for row in terminal["start_response_records"]))
        epochs = [json.loads(self.descriptor_bytes(owner, item)) for item in terminal["epochs"]]
        self.assertEqual([item["epoch"] for item in epochs], list(range(1, expected["start"] + 1)))
        self.assertEqual(len({item["server_id"] for item in epochs}), expected["start"])
        for field in ("keeper_id", "keeper_started_at", "database_volume", "database_path", "source_sha256", "fixture_sha256"):
            self.assertEqual(len({item[field] for item in epochs}), 1, field)
        self.assertEqual(epochs[0]["source_sha256"], owner.binding.source_sha256)
        self.assertEqual(epochs[0]["fixture_sha256"], owner.binding.fixture_sha256)
        self.assertEqual(epochs[0]["database_volume"], owner.json_authenticated("intent.json")["volume"])
        self.assertEqual(epochs[0]["database_path"], owner.recipe.database_path)
        self.assertEqual(owner.binding.source_sha256, admission.source_sha256(owner.files))
        self.assertEqual(owner.actual_registration, owner.registration.observation)
        self.assertEqual(owner.profile.original_definition_purpose, "public_product_definition")
        self.assertEqual(owner.actual_registration.gate.binding.purpose, "public_release")

    def run_control(self):
        root = self.artifacts.root.resolve()
        started = time.monotonic()
        self.outcome.update(status="started-not-qualified", definition=self.definition)
        try:
            profile = execution.HttpProductProfile(core.case_definition(self.definition["case_id"]),
                capture_policy=execution.source_capture.BatchCapturePolicy())
            recipe, policy = execution.recipe_from_case(profile.case), qualification_policy()
            files, candidate = candidate_files(self.definition["candidate_variant"])
            store = make_store(root / "candidate.git", files)
            commit = store.head()
            tree, captured = execution.capture_source(store, commit, policy=profile.capture_policy)
            self.assertEqual(captured, files)
            write_new(root / "candidate-definition.json", candidate)
            binding = execution.binding_for(files, recipe, policy, self.runtime,
                requirements_sha256=core.CONTRACT_SHA256, profile=profile, purpose="public_release")
            subject = registry.Subject("product-process-qualification-batch-v1", COHORT[0], profile.milestone,
                execution.digest({"qualification": PROTOCOL, "control": self.definition, "policy": asdict(policy),
                    "source_capture": profile.capture_policy.record()}),
                core.CONTRACT_SHA256, binding.source_sha256)
            prospective = execution.observation_registration_for(binding, profile, policy, subject=subject,
                gate_id="product-process-" + self.CONTROL_ID.lower(), commit_oid=commit, tree_oid=tree,
                repetition_id="physical-batch-v1-1", cohort_trajectory_ids=COHORT)
            registration = execution.HttpRegistration(binding, commit, tree, "physical-batch-v1-1", prospective)
            retained = write_new(root / "prospective-registration.json", {"registration": asdict(prospective),
                "source_registration": asdict(registration), "profile": profile.record(), "recipe": recipe.record(),
                "candidate": candidate, "policy": asdict(policy), "runtime": self.runtime,
                "authority_is_synthetic_fixture": True, "whole_project_acceptance": False})
            def authenticate_registration():
                admission.require((root / "prospective-registration.json").read_bytes() == retained,
                                  "Prospective physical fixture registration changed")
                return prospective
            authority = admission.ObservationAdmission(prospective,
                verify_registration=authenticate_registration, verify_cohort=lambda: None)
            journal, deltas, cleanup = root / "journal", root / "deltas", root / "cleanup"
            with closing(head.ExternalHead.create(root / "external-head", journal_roots=(journal, deltas))) as anchor, \
                 execution.CandidateHttpExecution(journal, store, registration, recipe, policy,
                    profile=profile, observation_admission=authority, checkpoint_authority=anchor,
                    delta_root=deltas, cleanup_root=cleanup, endpoint=self.endpoint) as owner:
                original = owner.execute_once()
                terminal = owner.json_authenticated("terminal.json")
                # Save available terminal counts before semantic/test assertions.
                self.outcome.update(execution_status=original.status, terminal_sha256=original.terminal_sha256,
                    original_execution_id=original.execution_id, cleanup_verified=original.cleanup_verified,
                    infrastructure=list(original.infrastructure), missing_step_ids=list(original.missing_step_ids),
                    terminal_census={key: terminal[key] for key in (
                        "planned_requests", "observed_probe_records", "observed_requests", "unknown_request_outcomes",
                        "planned_cli", "observed_cli_records", "attempted_probes", "attempted_cli", "unavailable_cli_attempts",
                        "entered_step_indices", "unentered_step_ids", "create_intents", "unconfirmed_creates",
                        "created_helpers", "created_cli", "created_servers", "created_keepers", "cleanup", "volume_cleanup")})
                write_new(root / "physical-census.json", self.outcome)
                checkpoint = owner.checkpoint()
                self.assertEqual(anchor.read(), checkpoint)
                write_new(root / "original-checkpoint.json", asdict(checkpoint))
                history = reader.observe_execution(owner, checkpoint)
                projection = observer.project_outcomes(profile, history)
                write_new(root / "observed-diagnostics.json", asdict(projection))
                self.outcome["diagnostic_outcomes"] = [asdict(item) for item in projection.decisive_outcomes]
                self.assert_complete_census(owner, original, terminal, history)
                self.outcome["authenticated_action_count"] = len(history.steps)
                self.outcome["setup_proof"] = self.assert_setup(owner)
                self.outcome["initial_state_setup_verified"] = True
                self.outcome["cleanup_proof"] = self.assert_cleanup_and_order(owner, terminal)
                source = observer.HttpObservationSource(owner, checkpoint,
                    receipt_path=owner.root / "semantic-verifier.json")
                observed = source.observation(prospective.gate, None)
                self.assertEqual(anchor.read(), source.checkpoint)
                self.assertGreater(source.checkpoint.sequence, checkpoint.sequence)
                self.assertEqual(observed.execution.receipt_sha256, original.terminal_sha256)
                self.assertEqual(observed.execution.outcomes, projection.decisive_outcomes)
                self.assertEqual(observed.execution.binding, prospective.gate.binding)
                self.assertEqual(observed.execution.terminal_status, "completed")
                self.assertEqual(observed.mode, "physical")
                verifier = owner.json_authenticated("semantic-verifier.json")
                self.assertEqual(verifier["protocol"], observer.PROTOCOL + "-git-source-batch-v1")
                self.assertEqual(verifier["source_capture"], profile.capture_policy.record())
                self.assertEqual(verifier["execution_protocol"], execution.BATCH_PROTOCOL)
                self.assertEqual(verifier["original_registration"], json.loads(execution.encoded(asdict(prospective))))
                self.assertEqual(verifier["original_terminal_sha256"], original.terminal_sha256)
                self.assertEqual(verifier["original_binding_sha256"], prospective.original_binding_sha256)
                self.assertEqual(verifier["decisive_outcomes"], [asdict(item) for item in projection.decisive_outcomes])
                self.assertFalse(verifier["held_out_claim"] or verifier["whole_project_acceptance"] or verifier["physical_execution_reused"])
                self.assertEqual(observed.execution.verifier_receipt_sha256,
                                 execution.sha256(owner.read_authenticated("semantic-verifier.json")))
                # Wrong gate/source binding cannot consume this valid original.
                with self.assertRaises(AuthorityError):
                    source.observation(replace(prospective.gate, gate_id="foreign-product-control"), None)
                foreign_binding = replace(prospective.gate.binding,
                    subject=replace(subject, source_sha256="f" * 64))
                with self.assertRaises(AuthorityError):
                    source.observation(replace(prospective.gate, binding=foreign_binding), None)
                self.assertEqual(owner.checkpoint(), source.checkpoint)
                write_new(root / "verified-checkpoint.json", asdict(source.checkpoint))
                write_new(root / "registry-observation.json", asdict(observed))
                self.assertEqual(projection.mechanics_guard.status, "passed")
                failed = {(item.step_id, facet.name) for item in projection.diagnostics for facet in item.facets
                          if facet.name in item.required_facets and facet.disposition == "fail"}
                unavailable = {(item.step_id, facet.name) for item in projection.diagnostics for facet in item.facets
                               if facet.name in item.required_facets and facet.disposition == "unavailable"}
                self.outcome.update(failed_required_facets=sorted(failed), unavailable_required_facets=sorted(unavailable),
                    verifier_sha256=observed.execution.verifier_receipt_sha256,
                    mechanics_guard=asdict(projection.mechanics_guard))
                self.assertEqual(unavailable, set())
                self.assertEqual(failed, {tuple(pair) for pair in self.definition["expected_failed_facets"]})
                expected_failed_steps = {step for step, _ in failed}
                self.assertEqual({item.step_id for item in projection.diagnostics if item.status == "failed"}, expected_failed_steps)
                self.assertTrue(all(item.status == ("failed" if item.step_id in expected_failed_steps else "passed")
                                    for item in projection.diagnostics))
                if self.CONTROL_ID == "D01":
                    for item in projection.diagnostics:
                        if item.step_id in expected_failed_steps:
                            facets = {facet.name: facet.disposition for facet in item.facets}
                            self.assertEqual(facets["json_exact"], "pass")
                            self.assertEqual(facets["status"], "pass")
                            actual = history.steps[item.step_index]
                            wanted = profile.case.steps[item.step_index].expectation.record()["body_utf8"].encode("utf-8")
                            self.assertEqual(actual.facts.body, wanted + b"\n")
                self.assertEqual(execution.capture_source(store, commit, policy=profile.capture_policy), (tree, files))
            self.outcome.update(status="qualified", qualification_verified=True)
        except BaseException as error:
            self.outcome.update(status="failed-not-qualified", failure_type=type(error).__name__, failure=str(error)[:2048])
            raise
        finally:
            self.outcome["elapsed_seconds"] = time.monotonic() - started
            write_new(root / "control-outcome.json", self.outcome)


_DOCKER = unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")


@_DOCKER
class ProductProcessWorkerOrderBatchV1DockerTests(_PhysicalProductControl, unittest.TestCase):
    CONTROL_ID = "P01"
    def test_complete_worker_order_history(self):
        self.run_control()


@_DOCKER
class ProductProcessWorkerTerminalBatchV1DockerTests(_PhysicalProductControl, unittest.TestCase):
    CONTROL_ID = "P02"
    def test_complete_worker_terminal_history(self):
        self.run_control()


@_DOCKER
class ProductProcessReindexBatchV1DockerTests(_PhysicalProductControl, unittest.TestCase):
    CONTROL_ID = "P03"
    def test_complete_reindex_history(self):
        self.run_control()


@_DOCKER
class ProductProcessExportBatchV1DockerTests(_PhysicalProductControl, unittest.TestCase):
    CONTROL_ID = "P04"
    def test_complete_export_history(self):
        self.run_control()


@_DOCKER
class ProductProcessBackupRootBatchV1DockerTests(_PhysicalProductControl, unittest.TestCase):
    CONTROL_ID = "P05"
    def test_complete_backup_root_history(self):
        self.run_control()


@_DOCKER
class ProductProcessRestoreBatchV1DockerTests(_PhysicalProductControl, unittest.TestCase):
    CONTROL_ID = "P06"
    def test_complete_restore_history(self):
        self.run_control()


@_DOCKER
class ProductProcessV0MigrationBatchV1DockerTests(_PhysicalProductControl, unittest.TestCase):
    CONTROL_ID = "P07"
    def test_complete_v0_migration_history(self):
        self.run_control()


@_DOCKER
class ProductProcessM2MigrationBatchV1DockerTests(_PhysicalProductControl, unittest.TestCase):
    CONTROL_ID = "P08"
    def test_complete_m2_migration_history(self):
        self.run_control()


@_DOCKER
class ProductProcessExportDefectBatchV1DockerTests(_PhysicalProductControl, unittest.TestCase):
    CONTROL_ID = "D01"
    def test_exact_json_does_not_hide_wrong_export_bytes(self):
        self.run_control()


@_DOCKER
class ProductProcessEnrollmentDefectBatchV1DockerTests(_PhysicalProductControl, unittest.TestCase):
    CONTROL_ID = "D02"
    def test_retry_enrollment_loss_is_detected(self):
        self.run_control()


@_DOCKER
class ProductProcessHighWaterDefectBatchV1DockerTests(_PhysicalProductControl, unittest.TestCase):
    CONTROL_ID = "D03"
    def test_reimport_high_water_loss_is_detected(self):
        self.run_control()


@_DOCKER
class ProductProcessMigrationDefectBatchV1DockerTests(_PhysicalProductControl, unittest.TestCase):
    CONTROL_ID = "D04"
    def test_seeded_migration_note_loss_is_detected(self):
        self.run_control()
