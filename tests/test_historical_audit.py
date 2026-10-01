"""Historical runtime checks fail before importing any untrusted source."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from devtools import historical_audit as historical


class HistoricalAuditTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.run, self.runtime = self.root / 'run', self.root / 'trusted'
        self.output, self.attestation = self.root / 'audit', self.root / 'trusted.json'
        package = self.runtime / 'gossip_harness'
        package.mkdir(parents=True)
        (package / '__init__.py').write_text('')
        (package / 'core.py').write_text('raise RuntimeError("Core is data, never imported by this auditor")\n')
        (package / 'sustained_audit.py').write_text('''import argparse, json
from pathlib import Path
p = argparse.ArgumentParser()
p.add_argument('--run'); p.add_argument('--output'); p.add_argument('--accounting-ledger')
a = p.parse_args()
Path(a.output).write_text(json.dumps({'passed': True, 'audited': True, 'run': a.run}))
''')
        frozen = self.run / 'source-snapshot/gossip_harness'
        frozen.mkdir(parents=True)
        source = (package / 'core.py').read_bytes()
        (frozen / 'core.py').write_bytes(source)
        contract = dict(protocol='frozen-test', sources={'core.py': hashlib.sha256(source).hexdigest()})
        self.contract = contract
        (self.run / 'results.json').write_bytes(historical._json(dict(contract=contract,
            contract_sha=hashlib.sha256(historical._json(contract)).hexdigest())))
        (self.run / 'preregistered.json').write_text(json.dumps(dict(contract=contract)))
        # An archive-selected entrypoint is deliberately ignored.
        (self.run / 'manifest.json').write_text(json.dumps(dict(contract=contract,
            entrypoint='evil', runtime=str(self.run))))
        self.seal()

    def seal(self):
        data = historical._json(historical.attest_runtime(self.runtime, 'sustained')) + b'\n'
        self.attestation.write_bytes(data)
        self.pin = hashlib.sha256(data).hexdigest()

    def execute(self, **kwargs):
        return historical.run_audit(self.run, self.runtime, self.attestation, self.pin,
                                    self.output, auditor='sustained', **kwargs)

    def test_fixed_auditor_runs_from_complete_snapshot_without_importing_core(self):
        before = (self.run / 'results.json').read_bytes()
        receipt = self.execute()
        self.assertTrue(receipt['passed'])
        self.assertTrue(receipt['complete_runtime'])
        self.assertFalse(receipt['candidate_execution'])
        self.assertEqual(receipt['provider_requests'], 0)
        self.assertEqual((self.run / 'results.json').read_bytes(), before)
        self.assertTrue(json.loads((self.output / 'audit.json').read_text())['audited'])
        self.assertEqual(json.loads((self.output / 'receipt.json').read_text()), receipt)
        self.assertFalse(list((self.output / 'runtime').rglob('*.pyc')))

    def test_wrong_attestation_or_changed_runtime_launches_zero_processes(self):
        self.pin = '0' * 64
        with patch.object(historical, '_execute') as launch:
            with self.assertRaisesRegex(ValueError, 'pinned digest'):
                self.execute()
            launch.assert_not_called()
        self.seal()
        (self.runtime / 'gossip_harness/core.py').write_text('changed = True')
        with patch.object(historical, '_execute') as launch:
            with self.assertRaisesRegex(ValueError, 'trusted attestation'):
                self.execute()
            launch.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_missing_initializer_import_or_external_dependency_is_not_complete(self):
        package = self.runtime / 'gossip_harness'
        for content in ('from .missing import value', 'from gossip_harness import missing', 'import provider_sdk',
                        'import importlib; importlib.import_module("evil")',
                        'from importlib import import_module as load; load("evil")'):
            (package / 'sustained_audit.py').write_text(content)
            with self.assertRaises(ValueError):
                historical.attest_runtime(self.runtime, 'sustained')
        (package / '__init__.py').unlink()
        with self.assertRaisesRegex(ValueError, 'initializer'):
            historical.attest_runtime(self.runtime, 'sustained')

    def test_frozen_core_mismatch_and_symlink_are_rejected_before_launch(self):
        frozen = self.run / 'source-snapshot/gossip_harness/core.py'
        frozen.write_text('changed = True')
        with patch.object(historical, '_execute') as launch:
            with self.assertRaisesRegex(ValueError, 'frozen core differ'):
                self.execute()
            launch.assert_not_called()
        frozen.unlink()
        frozen.symlink_to(self.runtime / 'gossip_harness/core.py')
        with self.assertRaisesRegex(ValueError, 'symlinks'):
            self.execute()
        self.assertFalse(self.output.exists())

    def test_known_entrypoint_cannot_be_selected_by_an_archive(self):
        with self.assertRaisesRegex(ValueError, 'Known auditor'):
            historical.run_audit(self.run, self.runtime, self.attestation, self.pin,
                                 self.output, auditor='gossip_harness.core')
        self.assertFalse(self.output.exists())

    def test_failed_subprocess_retains_diagnostics_without_green_receipt(self):
        (self.runtime / 'gossip_harness/sustained_audit.py').write_text('raise ValueError("Rejected archived evidence")')
        self.seal()
        receipt = self.execute()
        self.assertFalse(receipt['passed'])
        self.assertNotEqual(receipt['returncode'], 0)
        self.assertIn('Rejected archived evidence', (self.output / 'stderr.log').read_text())
        self.assertTrue(receipt['inputs_unchanged'])

    def test_existing_output_and_duplicate_contract_keys_are_rejected(self):
        self.output.mkdir()
        with self.assertRaisesRegex(ValueError, 'fresh'):
            self.execute()
        self.output.rmdir()
        (self.run / 'results.json').write_text('{"contract":{},"contract":{}}')
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            self.execute()

    def test_mid_audit_source_change_invalidates_receipt(self):
        real = historical._execute
        def mutate(*args, **kwargs):
            result = real(*args, **kwargs)
            (self.runtime / 'gossip_harness/core.py').write_text('changed = True')
            return result
        with patch.object(historical, '_execute', side_effect=mutate):
            with self.assertRaisesRegex(ValueError, 'changed while'):
                self.execute()
        self.assertFalse(json.loads((self.output / 'receipt.json').read_text())['passed'])

    def test_timeout_keeps_failure_receipt_and_cli_rejects_failed_audit(self):
        (self.runtime / 'gossip_harness/sustained_audit.py').write_text('import time; time.sleep(30)')
        self.seal()
        with self.assertRaises(subprocess.TimeoutExpired):
            self.execute(timeout_seconds=0.05)
        receipt = json.loads((self.output / 'receipt.json').read_text())
        self.assertFalse(receipt['passed'])
        self.assertTrue(receipt['executed'])
        with patch.object(historical, 'run_audit', return_value={'passed': False}):
            with self.assertRaises(SystemExit) as error:
                historical.main(['--runtime', str(self.runtime), '--auditor', 'sustained',
                    '--attestation', str(self.attestation), '--trusted-runtime-sha256', self.pin,
                    '--run', str(self.run), '--output', str(self.root / 'second')])
        self.assertEqual(error.exception.code, 1)

    def test_sigterm_keeps_receipt_and_kills_child_that_outlives_auditor(self):
        child_code = ("import os,signal,sys,time; from pathlib import Path; "
                      "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                      "Path(sys.argv[1]).write_text(str(os.getpid()))\n"
                      "while True:\n Path(sys.argv[1]+'.heartbeat').write_text(str(time.monotonic_ns())); time.sleep(.01)\n")
        auditor = f'''import argparse, os, subprocess, sys, time
from pathlib import Path
p = argparse.ArgumentParser(); p.add_argument('--run'); p.add_argument('--output')
a = p.parse_args()
pid = Path(a.output).with_name('child.pid')
child = subprocess.Popen([sys.executable, '-I', '-c', {child_code!r}, str(pid)])
while not pid.exists(): time.sleep(.01)
Path(a.output).with_name('leader.pid').write_text(str(os.getpid()))
time.sleep(60)
'''
        (self.runtime / 'gossip_harness/sustained_audit.py').write_text(auditor)
        self.seal()
        command = [sys.executable, '-m', 'devtools.historical_audit', '--runtime', str(self.runtime),
            '--auditor', 'sustained', '--attestation', str(self.attestation), '--trusted-runtime-sha256', self.pin,
            '--run', str(self.run), '--output', str(self.output)]
        process = subprocess.Popen(command, cwd=Path(__file__).resolve().parents[1],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        child_pid = leader_pid = None
        try:
            deadline = time.monotonic() + 15
            while not (self.output / 'leader.pid').exists() and time.monotonic() < deadline:
                if process.poll() is not None:
                    self.fail(process.communicate()[1].decode())
                time.sleep(.02)
            self.assertTrue((self.output / 'leader.pid').exists())
            leader_pid = int((self.output / 'leader.pid').read_text())
            child_pid = int((self.output / 'child.pid').read_text())
            process.terminate()
            process.communicate(timeout=10)
            self.assertNotEqual(process.returncode, 0)
            receipt = json.loads((self.output / 'receipt.json').read_text())
            self.assertFalse(receipt['passed'])
            self.assertIn('cancelled by signal', receipt['failure'])
            heartbeat = self.output / 'child.pid.heartbeat'
            stopped = heartbeat.read_bytes()
            time.sleep(.1)
            self.assertEqual(heartbeat.read_bytes(), stopped, 'Owned child continued executing after cancellation')
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)
            if leader_pid is not None:
                try:
                    os.killpg(leader_pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def test_zero_exit_requires_an_explicit_full_pass_from_known_auditor(self):
        for index, value in enumerate(({'passed': False}, {'passed': 1}, [], {})):
            self.output = self.root / f'false-audit-{index}'
            def unsuccessful(*args):
                (self.output / 'audit.json').write_text(json.dumps(value))
                return 0
            with patch.object(historical, '_execute', side_effect=unsuccessful):
                with self.assertRaisesRegex(ValueError, 'complete passing'):
                    self.execute()
            receipt = json.loads((self.output / 'receipt.json').read_text())
            self.assertFalse(receipt['passed'])
            self.assertIn('audit_sha256', receipt)

    def test_executable_runtime_change_cannot_certify_audit(self):
        identity = historical._identity()
        with patch.object(historical, '_identity', side_effect=[identity, identity, {**identity, 'git_sha256': '0'*64}]):
            with self.assertRaisesRegex(ValueError, 'runtime changed'):
                self.execute()
        receipt = json.loads((self.output / 'receipt.json').read_text())
        self.assertFalse(receipt['passed'])
        self.assertTrue(receipt['executed'])
