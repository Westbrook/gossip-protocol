"""Fresh sandbox qualification of trusted M2, not a comparative trial."""
from __future__ import annotations

import hashlib
import json
import os
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.blackbox_validator import BlackboxValidator, SUPERVISOR_ADAPTER
from gossip_harness.library_m2_acceptance_cases_v1 import CHILD_ADAPTER, acceptance_cases, registry_manifest
from gossip_harness.library_m2_reference_v1 import m2_files
from gossip_harness.library_project_fixture_v1 import public_cases
from gossip_harness.pilot import DEFAULT_IMAGE
from gossip_harness.sandbox import DockerValidator


def _save(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, ensure_ascii=True, indent=2)
        stream.write("\n")


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class LibraryM2DockerReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ready, detail = BlackboxValidator(DEFAULT_IMAGE).preflight()
        if not ready:
            raise RuntimeError(detail)

    def evaluate(self, label, *, public=False, stale_token_mutant=False):
        cases = public_cases("m1") if public else acceptance_cases()
        files = m2_files()
        if stale_token_mutant:
            cases = [case for case in cases if case['id'] == 'm2-migrate-aba-receipt']
            self.assertEqual(len(cases), 1)
            path = 'library/catalog/store.py'
            original = 'SET current_revision=current_revision+1,edit_version=edit_version+1 '
            self.assertEqual(files[path].count(original), 1)
            files[path] = files[path].replace(original,
                'SET current_revision=current_revision+1,edit_version=edit_version ')
        validator = BlackboxValidator(DEFAULT_IMAGE, timeout_seconds=180, case_timeout_seconds=15)
        if not public:
            validator._sandbox = DockerValidator(DEFAULT_IMAGE,
                {'supervisor.py':SUPERVISOR_ADAPTER,'child.py':CHILD_ADAPTER},
                command=('python','-I','/checks/supervisor.py'),timeout_seconds=180)
        with ArtifactDirectory(label, retain_success=True) as artifacts:
            _save(artifacts.root/'case-definitions.json', cases)
            _save(artifacts.root/'authored-source.json', files)
            _save(artifacts.root/'pre-execution-freeze.json', {
                'purpose':'authored_reference_qualification',
                'not_experimental_candidate_acceptance':True,
                'cases_registry':None if public else registry_manifest(),
                'definitions_file_sha256':hashlib.sha256((artifacts.root/'case-definitions.json').read_bytes()).hexdigest(),
                'source_file_sha256':hashlib.sha256((artifacts.root/'authored-source.json').read_bytes()).hexdigest(),
                'image':DEFAULT_IMAGE,'adapter_sha256':validator._sandbox.checks_sha256,
                'timeout_seconds':180,'case_timeout_seconds':15,
                'deliberate_defect':'refresh_does_not_advance_edit_version' if stale_token_mutant else None})
            result = validator.evaluate(files, cases)
            _save(artifacts.root/'execution-receipt.json', result)
            self.assertTrue(result.get('cleanup_verified'), result.get('status'))
            return result

    def test_inherited_m1_histories_in_pinned_python_sandbox(self):
        result = self.evaluate('m2-inherited-public', public=True)
        self.assertTrue(result['passed'], [(o.get('id'),o['status']) for o in result['outcomes']])
        self.assertEqual(len(result['outcomes']), 8)

    def test_independently_authored_m2_histories_in_pinned_python_sandbox(self):
        result = self.evaluate('m2-independent-reference')
        self.assertTrue(result['passed'], [(o.get('id'),o['status']) for o in result['outcomes']])
        self.assertEqual(len(result['outcomes']), len(acceptance_cases()))

    def test_independent_aba_history_rejects_missing_edit_token_increment(self):
        result = self.evaluate('m2-stale-token-defect', stale_token_mutant=True)
        self.assertFalse(result['passed'])
        self.assertEqual(result['status'], 'failed')
        self.assertEqual([(o['id'],o['status']) for o in result['outcomes']],
                         [('m2-migrate-aba-receipt','wrong_answer')])
