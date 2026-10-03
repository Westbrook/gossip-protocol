"""Fresh pinned Docker qualification of the complete authored M4 application.

Inputs and exact expected histories remain outside the sandbox. Public binary
snapshots enter only the trusted observer's private workspace. None of these
reference checks is comparative study evidence or a whole-cohort acceptance.
"""
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import unittest
import zlib

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.blackbox_validator import BlackboxValidator, SUPERVISOR_ADAPTER
from gossip_harness.library_m2_acceptance_cases_v1 import (
    CHILD_ADAPTER as M2_ADAPTER, acceptance_cases as m2_cases, registry_manifest as m2_registry,
)
from gossip_harness.library_m3_acceptance_cases_v1 import (
    CHILD_ADAPTER as M3_ADAPTER, acceptance_cases as m3_cases, registry_manifest as m3_registry,
)
from gossip_harness import library_m4_acceptance_cases_v3 as m4_oracle
from gossip_harness.library_m4_acceptance_cases_v3 import (
    CHILD_ADAPTER as M4_ADAPTER, acceptance_cases as m4_cases, registry_manifest as m4_registry,
)
from gossip_harness.library_m4_reference_v1 import m4_files
from gossip_harness.library_m4_fixture_v1 import BINARY_PATHS, fixture_files, fixture_manifest
from gossip_harness.library_project_fixture_v1 import public_cases
from gossip_harness.pilot import DEFAULT_IMAGE
from gossip_harness.sandbox import DockerValidator


def _save(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, ensure_ascii=True, indent=2)
        stream.write("\n")


def _host_verdict(raw, cases, registry, raw_sha256, oracle_source_sha256, scorer):
    """Score observed JSON on the host; never upgrade infrastructure failures."""
    outcomes = raw.get("outcomes")
    eligible = (raw.get("status") in ("passed", "failed")
                and raw.get("cleanup_verified") is True
                and type(raw.get("exit_code")) is int and raw["exit_code"] == 0
                and raw.get("timed_out") is False
                and raw.get("output_truncated") is False
                and raw.get("input_delivery_failed") is False
                and type(raw.get("case_count")) is int and raw["case_count"] == len(cases)
                and type(outcomes) is list and len(outcomes) == len(cases))
    verdicts = []
    for index, case in enumerate(cases):
        outcome = outcomes[index] if eligible else None
        observed = (type(outcome) is dict and type(outcome.get("index")) is int
                    and outcome["index"] == index and outcome.get("id") == case["id"]
                    and outcome.get("status") in ("passed", "wrong_answer") and "actual" in outcome)
        passed = scorer(case, outcome["actual"]) is True if observed else False
        verdicts.append({"index": index, "id": case["id"], "observed": observed,
                         "raw_status": outcome.get("status") if type(outcome) is dict else None,
                         "passed": passed,
                         "status": "passed" if passed else "wrong_answer" if observed else "unobserved"})
    return {"schema": "library-m4-host-oracle-verdict-v2", "purpose": "authored_reference_qualification",
            "not_experimental_candidate_acceptance": True, "oracle_registry": registry,
            "oracle_source_sha256": oracle_source_sha256,
            "raw_receipt": "execution-receipt.json", "raw_receipt_sha256": raw_sha256,
            "raw_receipt_eligible": eligible, "raw_observations_rewritten": False,
            "passed": eligible and all(row["passed"] for row in verdicts), "outcomes": verdicts}


class LibraryM4DockerHostOracleTests(unittest.TestCase):
    def fixture(self):
        return ({"status": "failed", "cleanup_verified": True, "exit_code": 0, "timed_out": False,
                 "output_truncated": False, "input_delivery_failed": False, "case_count": 1,
                 "outcomes": [{"index": 0, "id": "case", "status": "wrong_answer", "actual": {"value": 2}}]},
                [{"id": "case", "expected": {"value": {"$integer_at_least": 0}}}])

    def score(self, raw, cases, scorer):
        return _host_verdict(raw, cases, {"protocol": "oracle-v2"}, "a" * 64, "b" * 64, scorer)

    def test_host_verdict_preserves_raw_wrong_answer_and_requires_boolean_true(self):
        raw, cases = self.fixture()
        before = deepcopy(raw)
        verdict = self.score(raw, cases, lambda case, actual: True)
        self.assertTrue(verdict["passed"])
        self.assertEqual(verdict["outcomes"][0]["raw_status"], "wrong_answer")
        self.assertEqual(raw, before)
        self.assertFalse(verdict["raw_observations_rewritten"])
        self.assertEqual(verdict["raw_receipt_sha256"], "a" * 64)
        self.assertFalse(self.score(raw, cases, lambda case, actual: 1)["passed"])
        self.assertEqual(self.score(raw, cases, lambda case, actual: False)["outcomes"][0]["status"], "wrong_answer")

    def test_infrastructure_receipts_never_reach_scorer(self):
        changes = [{"status": "timeout"}, {"status": "sandbox_error"}, {"cleanup_verified": False},
                   {"exit_code": 1}, {"exit_code": False}, {"timed_out": True}, {"output_truncated": True},
                   {"input_delivery_failed": True}, {"case_count": 0}, {"case_count": True}, {"outcomes": []}]
        for change in changes:
            with self.subTest(change=change):
                raw, cases = self.fixture()
                raw.update(change)
                verdict = self.score(raw, cases, lambda case, actual: self.fail("Infrastructure reached oracle"))
                self.assertFalse(verdict["passed"])
                self.assertFalse(verdict["raw_receipt_eligible"])
                self.assertEqual(verdict["outcomes"][0]["status"], "unobserved")

    def test_missing_or_mismatched_observation_never_reaches_scorer(self):
        changes = [{"status": "error"}, {"status": "timeout"}, {"id": "other"}, {"index": 1}, {"index": False}]
        for change in [*changes, None]:
            with self.subTest(change=change):
                raw, cases = self.fixture()
                if change is None:
                    del raw["outcomes"][0]["actual"]
                else:
                    raw["outcomes"][0].update(change)
                verdict = self.score(raw, cases, lambda case, actual: self.fail("Invalid observation reached oracle"))
                self.assertFalse(verdict["passed"])
                self.assertEqual(verdict["outcomes"][0]["status"], "unobserved")


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class LibraryM4DockerReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ready, detail = BlackboxValidator(DEFAULT_IMAGE).preflight()
        if not ready:
            raise RuntimeError(detail)

    def evaluate(self, label, *, milestone="m4", defect=None):
        suites = {
            "m1": (lambda: public_cases("m1"), None, None),
            "m2": (m2_cases, M2_ADAPTER, m2_registry),
            "m3": (m3_cases, M3_ADAPTER, m3_registry),
            "m4": (m4_cases, M4_ADAPTER, m4_registry),
        }
        factory, adapter, registry = suites[milestone]
        cases = factory()
        frozen_registry = None if registry is None else registry()
        oracle_source = Path(m4_oracle.__file__).read_bytes() if milestone == "m4" else None
        oracle_sha = hashlib.sha256(oracle_source).hexdigest() if oracle_source is not None else None
        files = m4_files()
        if defect == "revision_projection_omits_content":
            cases = [case for case in cases if case["id"] == "m4-fresh-revision-identity-and-numbered-adapters"]
            path = "library/catalog/m4_store.py"
            original = "revision_id(document_id, revision['revision'], revision['blob_id'])"
            self.assertEqual(files[path].count(original), 1)
            files[path] = files[path].replace(original, "revision_id(document_id, revision['revision'], '')")
        elif defect is not None:
            self.fail("Unknown deliberate qualification defect")
        if defect:
            self.assertEqual(len(cases), 1)
        validator = BlackboxValidator(DEFAULT_IMAGE, timeout_seconds=300, case_timeout_seconds=30)
        if adapter is not None:
            validator._sandbox = DockerValidator(DEFAULT_IMAGE,
                {"supervisor.py": SUPERVISOR_ADAPTER, "child.py": adapter},
                command=("python", "-I", "/checks/supervisor.py"), timeout_seconds=300)
        with ArtifactDirectory(label, retain_success=True) as artifacts:
            _save(artifacts.root / "case-definitions.json", cases)
            _save(artifacts.root / "authored-source.json", files)
            if oracle_source is not None:
                with (artifacts.root / "host-oracle-source.py").open("xb") as stream:
                    stream.write(oracle_source)
            _save(artifacts.root / "pre-execution-freeze.json", {
                "purpose": "authored_reference_qualification",
                "not_experimental_candidate_acceptance": True,
                "milestone": milestone,
                "inherited_expectations_adapted": False,
                "cases_registry": frozen_registry,
                "host_oracle_source_sha256": oracle_sha,
                "definitions_file_sha256": hashlib.sha256((artifacts.root / "case-definitions.json").read_bytes()).hexdigest(),
                "source_file_sha256": hashlib.sha256((artifacts.root / "authored-source.json").read_bytes()).hexdigest(),
                "image": DEFAULT_IMAGE, "adapter_sha256": validator._sandbox.checks_sha256,
                "timeout_seconds": 300, "case_timeout_seconds": 30, "deliberate_defect": defect,
            })
            result = validator.evaluate(files, cases)
            _save(artifacts.root / "execution-receipt.json", result)
            if milestone == "m4":
                raw_sha = hashlib.sha256((artifacts.root / "execution-receipt.json").read_bytes()).hexdigest()
                self.assertEqual(Path(m4_oracle.__file__).read_bytes(), oracle_source,
                                 "Host oracle changed during qualification")
                self.assertEqual(m4_registry(), frozen_registry, "Oracle registry changed during qualification")
                verdict = _host_verdict(result, cases, frozen_registry, raw_sha, oracle_sha, m4_oracle.score_case)
                _save(artifacts.root / "host-verdict.json", verdict)
                verdict_sha = hashlib.sha256((artifacts.root / "host-verdict.json").read_bytes()).hexdigest()
                _save(artifacts.root / "host-verdict-integrity.json", {
                    "host_verdict": "host-verdict.json", "sha256": verdict_sha,
                    "raw_receipt": "execution-receipt.json", "raw_receipt_sha256": raw_sha})
                # Preserve every raw result field; callers inspect the distinct
                # host verdict instead of altering the exact-equality receipt.
                result = {**result, "host_verdict": verdict, "host_verdict_sha256": verdict_sha}
            self.assertTrue(result.get("cleanup_verified"), result.get("status"))
            return result

    def test_inherited_m1_histories_on_combined_m4(self):
        result = self.evaluate("m4-inherited-m1", milestone="m1")
        self.assertTrue(result["passed"], [(o.get("id"), o["status"]) for o in result["outcomes"]])
        self.assertEqual(len(result["outcomes"]), 8)

    def test_inherited_independent_m2_histories_on_combined_m4(self):
        result = self.evaluate("m4-inherited-m2", milestone="m2")
        self.assertTrue(result["passed"], [(o.get("id"), o["status"]) for o in result["outcomes"]])
        self.assertEqual(len(result["outcomes"]), 12)

    def test_inherited_independent_m3_histories_on_combined_m4(self):
        result = self.evaluate("m4-inherited-m3", milestone="m3")
        self.assertTrue(result["passed"], [(o.get("id"), o["status"]) for o in result["outcomes"]])
        self.assertEqual(len(result["outcomes"]), 10)

    def test_independent_m4_histories_on_combined_m4(self):
        result = self.evaluate("m4-independent-reference-v3")
        verdict = result["host_verdict"]
        self.assertTrue(verdict["passed"], verdict["outcomes"])
        self.assertEqual(len(verdict["outcomes"]), len(m4_cases()))

    def test_independent_history_detects_revision_identity_without_content(self):
        result = self.evaluate("m4-revision-content-defect", defect="revision_projection_omits_content")
        self.assertFalse(result["passed"])
        self.assertEqual([(o["id"], o["status"]) for o in result["outcomes"]],
                         [("m4-fresh-revision-identity-and-numbered-adapters", "wrong_answer")])
        self.assertFalse(result["host_verdict"]["passed"])
        self.assertEqual([(o["id"], o["status"]) for o in result["host_verdict"]["outcomes"]],
                         [("m4-fresh-revision-identity-and-numbered-adapters", "wrong_answer")])

    def test_fresh_release_reproducible_install_and_public_workflow(self):
        files = m4_files()
        public = fixture_files()
        binaries = {name: public[name] for name in BINARY_PATHS}
        complete = {name: source.encode("utf-8") for name, source in files.items()} | binaries
        records = [{"path": name, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
                   for name, raw in sorted(complete.items())]
        canonical = lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True,
                                             separators=(",", ":"), allow_nan=False).encode("utf-8")
        source_hash = hashlib.sha256(canonical(records)).hexdigest()
        manifest = {
            "format": "local-research-library-release-manifest-v1",
            "files": records, "source_sha256": source_hash,
            "runtime": {"image": DEFAULT_IMAGE, "python": "3.12"},
            "product_contract_sha256": "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c",
            "api_versions": ["v0", "lifecycle-v2", "maintenance-v3", "v1"], "storage_version": 4,
        }
        manifest_raw = canonical(manifest)
        installed_records = sorted(records + [{"path": "release-manifest.json", "bytes": len(manifest_raw),
                                               "sha256": hashlib.sha256(manifest_raw).hexdigest()}],
                                   key=lambda row: row["path"])
        response = {"format": "local-research-library-release-v1", "manifest": "release-manifest.json",
                    "files": len(records), "source_sha256": source_hash}
        cases = [{"id": "m4-release-install-public-workflow", "input": {"fixtures": [
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
                "after_public_records": installed_records,
            }}]
        validator = BlackboxValidator(DEFAULT_IMAGE, timeout_seconds=180, case_timeout_seconds=120)
        validator._sandbox = DockerValidator(DEFAULT_IMAGE,
            {"supervisor.py": SUPERVISOR_ADAPTER, "child.py": _RELEASE_ADAPTER, "public_probe.py": _PUBLIC_PROBE},
            command=("python", "-I", "/checks/supervisor.py"), timeout_seconds=180)
        with ArtifactDirectory("m4-release-install-public", retain_success=True) as artifacts:
            _save(artifacts.root / "case-definitions.json", cases)
            _save(artifacts.root / "authored-source.json", files)
            _save(artifacts.root / "observer-sources.json", validator._sandbox.tests)
            _save(artifacts.root / "pre-execution-freeze.json", {
                "purpose": "authored_reference_qualification", "not_experimental_candidate_acceptance": True,
                "binary_fixture_manifest": fixture_manifest(), "image": DEFAULT_IMAGE,
                "adapter_sha256": validator._sandbox.checks_sha256,
                "definitions_file_sha256": hashlib.sha256((artifacts.root / "case-definitions.json").read_bytes()).hexdigest(),
                "source_file_sha256": hashlib.sha256((artifacts.root / "authored-source.json").read_bytes()).hexdigest(),
                "timeout_seconds": 180, "case_timeout_seconds": 120,
                "boundary": "Binary public fixtures staged in a private child copy; shared source mount remains read-only.",
                "claims_excluded": ["browser release journey", "held-out acceptance", "swarm superiority"],
            })
            result = validator.evaluate(files, cases)
            _save(artifacts.root / "execution-receipt.json", result)
            self.assertTrue(result.get("cleanup_verified"), result.get("status"))
            self.assertTrue(result["passed"], result.get("outcomes"))


_PUBLIC_PROBE = r'''import json
import runpy
import sys
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, sys.argv[1])
namespace = runpy.run_path(sys.argv[1] + '/test_cumulative_public.py', run_name='public_qualification')
suite = unittest.defaultTestLoader.loadTestsFromTestCase(namespace['CumulativePublicTests'])
result = unittest.TestResult()
suite.run(result)
value = {'tests': result.testsRun,
         'failures': [[test.id(), detail] for test, detail in result.failures],
         'errors': [[test.id(), detail] for test, detail in result.errors],
         'skipped': [[test.id(), reason] for test, reason in result.skipped],
         'expected_failures': [[test.id(), detail] for test, detail in result.expectedFailures],
         'unexpected_successes': [test.id() for test in result.unexpectedSuccesses]}
print(json.dumps(value, sort_keys=True, ensure_ascii=True))
'''

_RELEASE_ADAPTER = r'''import base64
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import zlib

payload = json.loads(sys.stdin.buffer.read(65537))

def records(root):
    result = []
    for path in sorted(root.rglob('*')):
        if path.is_symlink():
            raise ValueError('Unexpected source or release symlink')
        if path.is_file():
            raw = path.read_bytes()
            result.append({'path': path.relative_to(root).as_posix(), 'bytes': len(raw),
                           'sha256': hashlib.sha256(raw).hexdigest()})
    # POSIX path strings define the manifest order; pathlib instead compares
    # path components and places release/* before release-manifest.json.
    return sorted(result, key=lambda row: row['path'])

def invoke(command, cwd):
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as error:
        process = subprocess.run(command, cwd=cwd, stdin=subprocess.DEVNULL,
                                 stdout=output, stderr=error, timeout=90, check=False)
        output.seek(0); error.seek(0)
        out, err = output.read(65537), error.read(65537)
    if len(out) > 65536 or len(err) > 65536:
        raise ValueError('Release subprocess output limit')
    selected = out if process.returncode == 0 else err
    value = json.loads(selected)
    return {'exit': process.returncode, 'value': value,
            'other_stream_empty': not (err if process.returncode == 0 else out)}

with tempfile.TemporaryDirectory() as temporary:
    parent = Path(temporary)
    source = parent / 'source'
    original = records(Path('/workspace'))
    shutil.copytree('/workspace', source, copy_function=shutil.copyfile)
    seen = set()
    for fixture in payload['fixtures']:
        name = fixture['path']
        if name not in ('compatibility/v0.sqlite3', 'compatibility/m2.sqlite3') or name in seen:
            raise ValueError('Undeclared public binary fixture')
        seen.add(name)
        if type(fixture['bytes']) is not int or not 0 < fixture['bytes'] <= 1048576:
            raise ValueError('Fixture byte limit')
        decoder = zlib.decompressobj()
        raw = decoder.decompress(base64.b64decode(fixture['data'], validate=True), 1048577)
        if (len(raw) != fixture['bytes'] or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail
                or hashlib.sha256(raw).hexdigest() != fixture['sha256']):
            raise ValueError('Frozen fixture identity mismatch')
        target = source / name
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('xb') as stream:
            stream.write(raw)
    if seen != {'compatibility/v0.sqlite3', 'compatibility/m2.sqlite3'}:
        raise ValueError('Missing immutable binary fixture')
    source_before = records(source)
    bootstrap = "import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_module('library.clients.release',run_name='__main__')"
    first, second = parent / 'release-one', parent / 'release-two'
    def build(destination):
        return invoke([sys.executable, '-I', '-B', '-c', bootstrap, str(source), '--output', str(destination)], source)
    first_result = build(first)
    second_result = build(second)
    installed = records(first)
    repeat_result = build(first)
    repeat_unchanged = records(first) == installed
    # The documented container install mounts its package read-only. Isolated
    # Python ignores PYTHONDONTWRITEBYTECODE, including the public fixture's
    # own subprocesses; reproduce that mount's write protection here.
    for path in first.rglob('*'):
        path.chmod(0o555 if path.is_dir() else 0o444)
    first.chmod(0o555)
    public = invoke([sys.executable, '-I', '-B', '/checks/public_probe.py', str(first)], first)
    result = {'python': list(sys.version_info[:2]), 'first': first_result, 'second': second_result,
              'manifest': json.loads((first / 'release-manifest.json').read_bytes()),
              'installed_records': installed, 'second_identical': records(second) == installed,
              'source_unchanged': records(source) == source_before and records(Path('/workspace')) == original,
              'repeat': repeat_result, 'repeat_unchanged': repeat_unchanged, 'public': public,
              'after_public_records': records(first)}
    print(json.dumps(result, sort_keys=True, ensure_ascii=True, separators=(',', ':')))
'''
