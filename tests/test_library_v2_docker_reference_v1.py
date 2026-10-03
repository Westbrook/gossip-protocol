"""Fresh pinned Docker qualification of the prospective cumulative v2 reference.

All expected histories are frozen and retained before reference invocation.
These development observations are separate from candidate acceptance, the
six-trajectory cohort, browser evidence, and statistical treatment outcomes.
"""
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import unittest
import zlib

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.blackbox_validator import BlackboxValidator, SUPERVISOR_ADAPTER
from gossip_harness import library_v2_acceptance_cases_v1 as v2_oracle
from gossip_harness import library_v2_inherited_cases_v1 as inherited
from gossip_harness.library_v2_reference_v1 import v2_files, v2_binary_files, schema3_v2_files
from gossip_harness.pilot import DEFAULT_IMAGE
from gossip_harness.sandbox import DockerValidator
from tests import test_library_m4_docker_reference_v1 as prior_delivery

PROTOCOL = "library-v2-docker-development-qualification-v1"
PRIOR_DELIVERY_SOURCE_SHA256 = '98e651c997b65c63384608b8e0da7b2b86c6715fe1b0efb2f4e18899f05b8f1b'
LEGACY_SOURCE_LIMIT = 1_048_576
LEGACY_WRAPPER = '''import runpy
import sys
sys.argv = ["/checks/observe.py", "/workspace", "/checks/legacy-v2"]
runpy.run_path("/checks/observe.py", run_name="__main__")
'''


def _save(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, ensure_ascii=True, indent=2)
        stream.write("\n")


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _legacy_checks(files, adapter):
    """Mount bounded public predecessor code read-only; never execute it on host."""
    if (type(files) is not dict or not files or len(files) > 256
            or any(type(name) is not str or not name or "\\" in name or "\0" in name
                   or PurePosixPath(name).is_absolute()
                   or any(part in ("", ".", "..") for part in name.split("/"))
                   or type(source) is not str for name, source in files.items())
            or sum(len(source.encode("utf-8")) for source in files.values()) > LEGACY_SOURCE_LIMIT):
        raise ValueError("Unsupported predecessor source map")
    return {"supervisor.py": SUPERVISOR_ADAPTER, "child.py": LEGACY_WRAPPER,
            "observe.py": adapter, **{"legacy-v2/" + name: source for name, source in files.items()}}


def _host_verdict(raw, cases, registry, raw_sha256, oracle_sha256, scorer):
    verdict = prior_delivery._host_verdict(raw, cases, registry, raw_sha256, oracle_sha256, scorer)
    return {**verdict, "schema": "library-v2-host-oracle-verdict-v1", "protocol": PROTOCOL,
            "purpose": "development_qualification", "inherited_host_verdict_source_sha256": PRIOR_DELIVERY_SOURCE_SHA256}


def _release_case(files, binaries):
    """Expected exact public deliverable inventory, independent of builder output."""
    if set(files).intersection(binaries):
        raise ValueError("Binary fixture shadows a text source")
    complete = {name: source.encode("utf-8") for name, source in files.items()} | binaries
    records = [{"path": name, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
               for name, raw in sorted(complete.items())]
    source_hash = hashlib.sha256(_canonical(records)).hexdigest()
    manifest = {"format": "local-research-library-release-manifest-v1", "files": records,
                "source_sha256": source_hash, "runtime": {"image": DEFAULT_IMAGE, "python": "3.12"},
                "product_contract_sha256": inherited.CONTRACT_SHA256,
                "api_versions": ["v0", "lifecycle-v2", "maintenance-v3", "v1"], "storage_version": 4}
    manifest_raw = _canonical(manifest)
    installed_records = sorted(records + [{"path": "release-manifest.json", "bytes": len(manifest_raw),
        "sha256": hashlib.sha256(manifest_raw).hexdigest()}], key=lambda row: row["path"])
    response = {"format": "local-research-library-release-v1", "manifest": "release-manifest.json",
                "files": len(records), "source_sha256": source_hash}
    return {"id": "v2-release-install-public-workflow", "input": {"fixtures": [
        {"path": name, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw),
         "data": base64.b64encode(zlib.compress(raw, 9)).decode("ascii")}
        for name, raw in sorted(binaries.items())]}, "expected": {
            "python": [3, 12], "first": {"exit": 0, "value": response, "other_stream_empty": True},
            "second": {"exit": 0, "value": response, "other_stream_empty": True},
            "manifest": manifest, "installed_records": installed_records,
            "second_identical": True, "source_unchanged": True,
            "repeat": {"exit": 2, "value": {"error": "already_exists"}, "other_stream_empty": True},
            "repeat_unchanged": True, "public": {"exit": 0, "value": {
                "tests": 3, "failures": [], "errors": [], "skipped": [],
                "expected_failures": [], "unexpected_successes": []}, "other_stream_empty": True},
            "after_public_records": installed_records}}


class LibraryV2DockerHostOracleTests(unittest.TestCase):
    def test_inherited_host_oracle_is_frozen_and_does_not_promote_infrastructure(self):
        self.assertEqual(_sha(prior_delivery.__file__), PRIOR_DELIVERY_SOURCE_SHA256)
        raw = {"status": "failed", "cleanup_verified": True, "exit_code": 0, "timed_out": False,
               "output_truncated": False, "input_delivery_failed": False, "case_count": 1,
               "outcomes": [{"index": 0, "id": "case", "status": "wrong_answer", "actual": {"value": 2}}]}
        original = deepcopy(raw)
        cases = [{"id": "case", "expected": {"value": 2}}]
        verdict = _host_verdict(raw, cases, {}, "a" * 64, "b" * 64, lambda c, a: True)
        self.assertTrue(verdict["passed"])
        self.assertEqual(verdict["outcomes"][0]["raw_status"], "wrong_answer")
        self.assertEqual(raw, original)
        for change in ({"status": "sandbox_error"}, {"cleanup_verified": False}, {"exit_code": 1},
                       {"timed_out": True}, {"output_truncated": True}, {"input_delivery_failed": True},
                       {"case_count": True}, {"outcomes": []}):
            failed = _host_verdict(raw | change, cases, {}, "a" * 64, "b" * 64,
                                   lambda c, a: self.fail("Infrastructure reached the scorer"))
            self.assertFalse(failed["passed"])
        self.assertFalse(_host_verdict(raw, cases, {}, "a" * 64, "b" * 64, lambda c, a: 1)["passed"])

    def test_legacy_predecessor_is_readonly_check_input_with_strict_paths_and_bounds(self):
        files = {"library/__init__.py": "", "library/catalog/store.py": "class Store: pass\n"}
        checks = _legacy_checks(files, "print('observation')")
        self.assertEqual(checks["legacy-v2/library/catalog/store.py"], files["library/catalog/store.py"])
        self.assertEqual(checks["observe.py"], "print('observation')")
        compile(checks["child.py"], "legacy-wrapper", "exec")
        for bad in ({}, {"../escape.py": ""}, {"/abs.py": ""}, {"a//b.py": ""},
                    {"a\\b.py": ""}, {"a.py": b"bytes"}, {"a.py": "x" * (LEGACY_SOURCE_LIMIT + 1)},
                    {str(i): "" for i in range(257)}):
            with self.subTest(bad=list(bad)[:2]), self.assertRaises(ValueError):
                _legacy_checks(bad, "")

    def test_release_expected_identity_changes_with_bytes_and_pins_v2_contract(self):
        binary = {"compatibility/v0.sqlite3": b"public-v0", "compatibility/m2.sqlite3": b"public-m2"}
        case = _release_case({"library/__init__.py": ""}, binary)
        manifest = case["expected"]["manifest"]
        self.assertEqual(manifest["product_contract_sha256"], inherited.CONTRACT_SHA256)
        self.assertEqual(manifest["storage_version"], 4)
        changed = _release_case({"library/__init__.py": "# changed"}, binary)
        self.assertNotEqual(manifest["source_sha256"], changed["expected"]["manifest"]["source_sha256"])
        self.assertNotIn("expected", case["input"])
        with self.assertRaises(ValueError):
            _release_case({"same": "text"}, {"same": b"binary"})


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class LibraryV2DockerReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if _sha(prior_delivery.__file__) != PRIOR_DELIVERY_SOURCE_SHA256:
            raise RuntimeError("Frozen delivery observer changed")
        ready, detail = BlackboxValidator(DEFAULT_IMAGE).preflight()
        if not ready:
            raise RuntimeError(detail)

    def evaluate(self, label, *, milestone="v2", defect=None):
        module = v2_oracle if milestone == "v2" else inherited
        cases = v2_oracle.acceptance_cases() if milestone == "v2" else inherited.acceptance_cases(milestone)
        registry = v2_oracle.registry_manifest() if milestone == "v2" else inherited.registry_manifest(milestone)
        scorer = v2_oracle.score_case if milestone == "v2" else lambda c, a: inherited.score_case(milestone, c, a)
        oracle_source = Path(module.__file__).read_bytes()
        files = v2_files()
        predecessor = schema3_v2_files() if milestone == "v2" else None
        checks = (_legacy_checks(predecessor, v2_oracle.CHILD_ADAPTER) if predecessor is not None else
                  {"supervisor.py": SUPERVISOR_ADAPTER, "child.py": inherited.child_adapter(milestone)})
        original_source_sha256 = hashlib.sha256(_canonical(files)).hexdigest()
        if defect == "root-binding":
            if milestone != "v2":
                raise ValueError("Root defect belongs to the separately authored v2 history")
            cases = [c for c in cases if c["id"] == "v2-explicit-initial-adoption-and-cas"]
            self.assertEqual(len(cases), 1)
            path = "library/catalog/v2_maintenance.py"
            original = "            raise LibraryError('backup_root_unbound')"
            replacement = "            return str(self.backup_dir)"
            self.assertEqual(files[path].count(original), 1, "Frozen deliberate defect seam changed")
            files[path] = files[path].replace(original, replacement)
        elif defect is not None:
            raise ValueError("Unknown deliberate qualification defect")
        validator = BlackboxValidator(DEFAULT_IMAGE, timeout_seconds=600, case_timeout_seconds=30)
        validator._sandbox = DockerValidator(DEFAULT_IMAGE, checks,
            command=("python", "-I", "/checks/supervisor.py"), timeout_seconds=600)
        with ArtifactDirectory(label, retain_success=True) as artifacts:
            _save(artifacts.root / "case-definitions.json", cases)
            _save(artifacts.root / "authored-source.json", files)
            _save(artifacts.root / "observer-sources.json", checks)
            with (artifacts.root / "host-oracle-source.py").open("xb") as stream:
                stream.write(oracle_source)
            _save(artifacts.root / "pre-execution-freeze.json", {
                "protocol": PROTOCOL, "purpose": "development_qualification",
                "not_experimental_candidate_acceptance": True, "milestone": milestone,
                "inherited_expectations_adapted": milestone in ("m3", "m4"),
                "cases_registry": registry, "host_oracle_source_sha256": hashlib.sha256(oracle_source).hexdigest(),
                "definitions_file_sha256": _sha(artifacts.root / "case-definitions.json"),
                "source_file_sha256": _sha(artifacts.root / "authored-source.json"),
                "observer_sources_sha256": _sha(artifacts.root / "observer-sources.json"),
                "supported_predecessor_staged_readonly": predecessor is not None,
                "supported_predecessor_source_sha256": (hashlib.sha256(_canonical(predecessor)).hexdigest()
                                                        if predecessor is not None else None),
                "supported_predecessor_source_count": len(predecessor) if predecessor is not None else 0,
                "supported_predecessor_source_bytes": (sum(len(s.encode("utf-8")) for s in predecessor.values())
                                                       if predecessor is not None else 0),
                "original_unmutated_source_sha256": original_source_sha256,
                "executed_source_sha256": hashlib.sha256(_canonical(files)).hexdigest(),
                "selected_case_ids": [c["id"] for c in cases],
                "image": DEFAULT_IMAGE, "adapter_sha256": validator._sandbox.checks_sha256,
                "timeout_seconds": 600, "case_timeout_seconds": 30, "deliberate_defect": defect,
                "claims_excluded": ["browser journeys", "full v2 coverage", "held-out acceptance", "statistical comparison"],
            })
            result = validator.evaluate(files, cases)
            _save(artifacts.root / "execution-receipt.json", result)
            self.assertEqual(Path(module.__file__).read_bytes(), oracle_source, "Oracle changed during execution")
            current = v2_oracle.registry_manifest() if milestone == "v2" else inherited.registry_manifest(milestone)
            self.assertEqual(current, registry, "Case definitions changed during execution")
            verdict = _host_verdict(result, cases, registry, _sha(artifacts.root / "execution-receipt.json"),
                                   hashlib.sha256(oracle_source).hexdigest(), scorer)
            _save(artifacts.root / "host-verdict.json", verdict)
            _save(artifacts.root / "host-verdict-integrity.json", {
                "host_verdict": "host-verdict.json", "sha256": _sha(artifacts.root / "host-verdict.json"),
                "raw_receipt": "execution-receipt.json", "raw_receipt_sha256": _sha(artifacts.root / "execution-receipt.json")})
            self.assertTrue(result.get("cleanup_verified"), result.get("status"))
            return result, verdict

    def test_inherited_m1_histories_on_combined_v2(self):
        _, verdict = self.evaluate("v2-inherited-m1", milestone="m1")
        self.assertTrue(verdict["passed"], verdict["outcomes"])
        self.assertEqual(len(verdict["outcomes"]), 8)

    def test_inherited_m2_histories_on_combined_v2(self):
        _, verdict = self.evaluate("v2-inherited-m2", milestone="m2")
        self.assertTrue(verdict["passed"], verdict["outcomes"])
        self.assertEqual(len(verdict["outcomes"]), 12)

    def test_adapted_m3_histories_on_combined_v2(self):
        _, verdict = self.evaluate("v2-inherited-m3", milestone="m3")
        self.assertTrue(verdict["passed"], verdict["outcomes"])
        self.assertEqual(len(verdict["outcomes"]), 10)

    def test_adapted_m4_histories_on_combined_v2(self):
        _, verdict = self.evaluate("v2-inherited-m4", milestone="m4")
        self.assertTrue(verdict["passed"], verdict["outcomes"])
        self.assertEqual(len(verdict["outcomes"]), 18)

    def test_prospectively_authored_six_amendment_histories(self):
        _, verdict = self.evaluate("v2-six-amendments")
        self.assertTrue(verdict["passed"], verdict["outcomes"])
        self.assertEqual(len(verdict["outcomes"]), len(v2_oracle.acceptance_cases()))

    def test_independent_history_detects_root_binding_defect(self):
        _, verdict = self.evaluate("v2-root-binding-defect", defect="root-binding")
        self.assertFalse(verdict["passed"])
        self.assertEqual(len(verdict["outcomes"]), 1)
        self.assertEqual(verdict["outcomes"][0]["status"], "wrong_answer")

    def test_fresh_release_reproducible_install_and_public_workflow(self):
        files, binaries = v2_files(), v2_binary_files()
        cases = [_release_case(files, binaries)]
        checks = {"supervisor.py": SUPERVISOR_ADAPTER, "child.py": prior_delivery._RELEASE_ADAPTER,
                  "public_probe.py": prior_delivery._PUBLIC_PROBE}
        validator = BlackboxValidator(DEFAULT_IMAGE, timeout_seconds=180, case_timeout_seconds=120)
        validator._sandbox = DockerValidator(DEFAULT_IMAGE, checks,
            command=("python", "-I", "/checks/supervisor.py"), timeout_seconds=180)
        with ArtifactDirectory("v2-release-install-public", retain_success=True) as artifacts:
            _save(artifacts.root / "case-definitions.json", cases)
            _save(artifacts.root / "authored-source.json", files)
            _save(artifacts.root / "observer-sources.json", checks)
            _save(artifacts.root / "pre-execution-freeze.json", {
                "protocol": PROTOCOL, "purpose": "development_qualification",
                "not_experimental_candidate_acceptance": True,
                "product_contract_sha256": inherited.CONTRACT_SHA256,
                "inherited_delivery_observer_source_sha256": PRIOR_DELIVERY_SOURCE_SHA256,
                "image": DEFAULT_IMAGE, "adapter_sha256": validator._sandbox.checks_sha256,
                "definitions_file_sha256": _sha(artifacts.root / "case-definitions.json"),
                "source_file_sha256": _sha(artifacts.root / "authored-source.json"),
                "observer_sources_sha256": _sha(artifacts.root / "observer-sources.json"),
                "timeout_seconds": 180, "case_timeout_seconds": 120,
                "boundary": "Public binary fixtures enter only the private child source copy; shared source is read-only.",
                "claims_excluded": ["browser release journey", "held-out acceptance", "full v2 coverage", "swarm superiority"]})
            result = validator.evaluate(files, cases)
            _save(artifacts.root / "execution-receipt.json", result)
            self.assertTrue(result.get("cleanup_verified"), result.get("status"))
            self.assertTrue(result["passed"], result.get("outcomes"))
