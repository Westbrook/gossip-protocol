"""Offline boundary controls; these tests do not launch Docker or Chromium."""

from dataclasses import asdict, replace
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness import library_m1_browser_acceptance_v4 as browser
from gossip_harness.library_m1_acceptance_v4 import FrozenSubject, source_sha256
from gossip_harness.library_m1_clients_reference_v1 import clients_files


class LibraryM1BrowserAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.node = shutil.which("node")
        assert self.node is not None
        self.policy = browser.BrowserAcceptancePolicy("sha256:" + "a" * 64, self.node)
        self.files = {"solution.py": "def solve(value): return value\n", "library/a.py": "x = 1\n"}
        self.subject = FrozenSubject("cohort", "trajectory", "b" * 64, "c" * 40,
                                     "d" * 40, source_sha256(self.files))

    def node_script(self, script):
        result = subprocess.run([self.node, "-e", script], cwd=browser.ROOT,
            capture_output=True, text=True, timeout=15, check=False)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_network_request_policy_rejects_external_origins_and_write_confusion(self):
        self.node_script(r'''
const assert=require('node:assert/strict');
const {requestPolicy}=require('./devtools/browser/library_m1_acceptance.cjs');
for(const url of ['https://127.0.0.1:8765/','http://localhost:8765/','http://127.0.0.1:4178/',
 'http://x:y@127.0.0.1:8765/','file:///etc/passwd','http://127.0.0.1:8765.evil.test/'])
 assert.throws(()=>requestPolicy(url,'GET',null));
for(const method of ['CONNECT','PUT','DELETE','PATCH','HEAD'])
 assert.throws(()=>requestPolicy('http://127.0.0.1:8765/api/jobs',method,null));
assert.throws(()=>requestPolicy('http://127.0.0.1:8765/','POST',Buffer.from('{}'),'application/json'));
assert.throws(()=>requestPolicy('http://127.0.0.1:8765/api/jobs','POST',Buffer.from('{}'),'text/plain'));
assert.throws(()=>requestPolicy('http://127.0.0.1:8765/api/jobs','POST',Buffer.alloc(65537),'application/json'));
assert.throws(()=>requestPolicy('http://127.0.0.1:8765/api/jobs','GET',Buffer.from('{}')));
assert.equal(requestPolicy('http://127.0.0.1:8765/api/jobs','POST',Buffer.from('{}'),'application/json').body,'e30=');
''')

    def test_response_policy_rejects_redirects_cookie_injection_and_output_overflow(self):
        self.node_script(r'''
const assert=require('node:assert/strict');
const {responsePolicy}=require('./devtools/browser/library_m1_acceptance.cjs');
const good={status:200,headers:[['Content-Type','application/json']],body:'e30='};
assert.equal(responsePolicy(good).body.toString(),'{}');
for(const status of [101,199,301,307,600,200.5,true]) assert.throws(()=>responsePolicy({...good,status}));
for(const headers of [[['Set-Cookie','secret=a']],[['Refresh','0;url=https://example.com']],
 [['Content-Disposition','attachment']], [['X-Test','a\r\nb']], [['a', 'x'.repeat(16385)]]])
 assert.throws(()=>responsePolicy({...good,headers}));
for(const body of ['!!!!','e30','e30=\n',Buffer.alloc(262145).toString('base64')])
 assert.throws(()=>responsePolicy({...good,body}));
''')

    def test_http_client_is_isolated_stdlib_and_caps_raw_json_before_host_normalization(self):
        self.node_script(r'''
const assert=require('node:assert/strict');
const {HTTP_CLIENT,dockerEnvironment}=require('./devtools/browser/library_m1_acceptance.cjs');
assert(HTTP_CLIENT.includes("HTTPConnection('127.0.0.1',8765,timeout=3)"));
assert(HTTP_CLIENT.includes('response.read(262145)'));
assert(HTTP_CLIENT.includes("raw.decode('utf-8','strict')"));
assert(HTTP_CLIENT.includes('object_pairs_hook=pairs,parse_constant=invalid_constant'));
process.env.OPENAI_API_KEY='sentinel';process.env.NODE_OPTIONS='sentinel';
assert(!('OPENAI_API_KEY' in dockerEnvironment()));assert(!('NODE_OPTIONS' in dockerEnvironment()));
''')

    def test_late_browser_events_invalidate_previously_passing_verdict(self):
        self.node_script(r'''
const assert=require('node:assert/strict');
const {finalVerdict}=require('./devtools/browser/library_m1_acceptance.cjs');
for(const event of [{blockedRequestCount:1},{pageErrors:['late exception']},{downloads:1}]) {
 const receipt={passed:true,status:'passed',blockedRequestCount:0,pageErrors:[],downloads:0,...event};
 finalVerdict(receipt);assert.equal(receipt.passed,false);assert.equal(receipt.status,'failed');
}
''')

    def test_intake_locator_handles_actual_reference_wrapped_select_options(self):
        class FormLabels(HTMLParser):
            def __init__(self):
                super().__init__()
                self.current = None
                self.labels = []

            def handle_starttag(self, tag, attrs):
                if tag == "label":
                    self.current = {"text": "", "controls": [], "options": []}
                if self.current is not None:
                    if tag in ("input", "select"):
                        self.current["controls"].append({"tag": tag, "attributes": dict(attrs)})
                    if tag == "option":
                        self.current["options"].append(dict(attrs).get("value"))

            def handle_data(self, text):
                if self.current is not None:
                    self.current["text"] += text

            def handle_endtag(self, tag):
                if tag == "label":
                    self.labels.append(self.current)
                    self.current = None

        parser = FormLabels()
        # Read the actual authored reference output, not a mock of the locator.
        parser.feed(clients_files()["library/clients/index.html"])
        selects = [label for label in parser.labels
                   if any(control["tag"] == "select" for control in label["controls"])]
        self.assertEqual(len(selects), 1)
        self.assertEqual(selects[0]["options"], ["directory", "zip", "json"])
        self.assertNotEqual(selects[0]["text"].strip(), "Intake type")
        self.assertEqual([label["text"].strip() for label in parser.labels if label not in selects],
                         ["Source path", "Search", "Job ID", "Local path", "Namespace"])
        script = r'''
const assert=require('node:assert/strict');
const {INTAKE_CONTROL}=require('./devtools/browser/library_m1_acceptance.cjs');
const labels=JSON.parse(require('node:fs').readFileSync(0,'utf8'));
assert.equal(INTAKE_CONTROL.role,'combobox');
assert.equal(labels.filter(label=>INTAKE_CONTROL.name.test(label.text)).length,1);
assert(INTAKE_CONTROL.name.test('Intake type'));
assert(!INTAKE_CONTROL.name.test('Intake typewriter'));
assert(!INTAKE_CONTROL.name.test('Unrelated Intake type'));
'''
        result = subprocess.run([self.node, "-e", script], cwd=browser.ROOT,
            input=json.dumps(parser.labels), capture_output=True, text=True, timeout=15, check=False)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_job_identification_accepts_reordered_visible_text_without_id_substrings(self):
        self.node_script(r'''
const assert=require('node:assert/strict');
const {jobIdentityPattern}=require('./devtools/browser/library_m1_acceptance.cjs');
const identity=jobIdentityPattern('job_a-2');
for(const text of ['job_a-2: queued, epoch 1, 0/2','queued: job_a-2 (epoch 1) 0/2','0/2\njob_a-2'])
 assert(identity.test(text));
for(const text of ['job_a-20: queued','other_job_a-2','job_a-2-extra']) assert(!identity.test(text));
for(const id of ['job.a','job[1]','x'.repeat(65)]) assert.throws(()=>jobIdentityPattern(id));
''')

    def test_policy_rejects_unpinned_image_missing_runtime_and_unbounded_deadline(self):
        for value in ("python:3", "sha256:" + "A" * 64):
            with self.assertRaises(ValueError):
                replace(self.policy, image_id=value)
        for value in (59, 301, True, 180.0):
            with self.assertRaises(ValueError):
                replace(self.policy, timeout_seconds=value)
        with self.assertRaises(ValueError):
            replace(self.policy, node_executable="node")
        with self.assertRaises(ValueError):
            replace(self.policy, node_modules_path="relative/modules")

    def test_environment_excludes_provider_and_node_option_injection(self):
        modules = self.root / "node_modules"
        modules.mkdir()
        with patch.dict(os.environ, {"OPENAI_API_KEY": "sentinel", "NODE_OPTIONS": "--require=bad", "NODE_PATH": "/trusted/node_modules"}):
            values = browser._environment(replace(self.policy, node_modules_path=str(modules)))
        self.assertNotIn("OPENAI_API_KEY", values)
        self.assertNotIn("NODE_OPTIONS", values)
        self.assertEqual(values["NODE_PATH"], str(modules))

    def test_candidate_container_preserves_existing_sandbox_and_has_no_host_port(self):
        sandbox = browser._sandbox(self.policy)
        arguments = sandbox._arguments("owned", Path("/source"), Path("/checks"))
        for argument in ("--network=none", "--read-only", "--pull=never", "--cap-drop=ALL", "--user=65534:65534"):
            self.assertIn(argument, arguments)
        self.assertNotIn("--publish", arguments)
        self.assertEqual(sandbox.command, ("python", "-I", "/checks/bootstrap.py"))

    def test_bad_source_or_unverified_freeze_never_probes_runtime(self):
        with patch.object(browser, "_probe") as probe:
            with self.assertRaises(ValueError):
                browser.run_once(self.root / "bad", {"solution.py": "different"}, self.subject,
                    b"freeze", lambda *_: True, self.policy)
            with self.assertRaises(ValueError):
                browser.run_once(self.root / "bad", self.files, self.subject,
                    b"freeze", lambda *_: False, self.policy)
            probe.assert_not_called()

    def mocked_run(self, *, verifier=None, invoke=None, runtime=None, remove=None):
        output = self.root / "observation"
        verifier = verifier or (lambda *_: True)
        runtime = runtime or {"fixture": "mocked-runtime-not-physical-evidence"}
        completed = subprocess.CompletedProcess([], 0, stdout=b"e" * 64 + b"\n", stderr=b"")
        driver = {"passed": True, "cleanup": {"browser": "complete"}}
        with patch.object(browser, "_probe", return_value=runtime), \
             patch.object(browser, "_invoke", side_effect=invoke, return_value=driver) as dispatch, \
             patch.object(browser.subprocess, "run", return_value=completed) as command, \
             patch.object(browser.DockerValidator, "_remove", side_effect=remove, return_value=True):
            result = browser.run_once(output, self.files, self.subject,
                b"retained immutable freeze", verifier, self.policy)
        return output, result, dispatch, command

    def test_mocked_success_retains_intent_freeze_and_rechecks_barrier(self):
        calls = []
        output, result, dispatch, _ = self.mocked_run(verifier=lambda *args: calls.append(args) or True)
        self.assertEqual(len(calls), 3)
        self.assertEqual(result["status"], "passed")
        self.assertFalse(result["reused"])
        self.assertFalse(result["whole_project_acceptance"])
        self.assertEqual((output / "freeze-receipt.bin").read_bytes(), b"retained immutable freeze")
        intent = json.loads((output / "intent.json").read_bytes())
        self.assertEqual(intent["purpose"], "independent_acceptance")
        self.assertEqual(intent["subject"], asdict(self.subject))
        self.assertEqual(intent["runtime"]["fixture"], "mocked-runtime-not-physical-evidence")
        dispatch.assert_called_once()

    def test_failing_predispatch_barrier_preserves_intent_without_execution(self):
        calls = []
        _, result, dispatch, command = self.mocked_run(verifier=lambda *_: calls.append(1) or len(calls) == 1)
        self.assertFalse(result["browser_checks_passed"])
        self.assertFalse(result["physically_executed"])
        dispatch.assert_not_called(); command.assert_not_called()

    def test_postdispatch_barrier_change_invalidates_browser_pass(self):
        calls = []
        _, result, dispatch, _ = self.mocked_run(verifier=lambda *_: calls.append(1) or len(calls) < 3)
        self.assertFalse(result["browser_checks_passed"])
        self.assertEqual(result["status"], "infrastructure_failure")
        dispatch.assert_called_once()

    def test_existing_output_never_reexecutes(self):
        output, _, _, _ = self.mocked_run()
        with patch.object(browser, "_probe") as probe:
            with self.assertRaises(ValueError):
                browser.run_once(output, self.files, self.subject, b"freeze", lambda *_: True, self.policy)
            probe.assert_not_called()

    def test_browser_cleanup_failure_cannot_pass(self):
        _, result, _, _ = self.mocked_run(invoke=lambda *_: {"passed": True, "cleanup": {"browser": "unverified"}})
        self.assertFalse(result["browser_checks_passed"])
        self.assertEqual(result["status"], "cleanup_failed")

    def test_cancellation_ignores_repeated_signal_until_cleanup_and_restores_handler(self):
        previous = signal.getsignal(signal.SIGTERM)
        with self.assertRaises(KeyboardInterrupt):
            with browser._owned_signals():
                try:
                    os.kill(os.getpid(), signal.SIGTERM)
                except KeyboardInterrupt:
                    self.assertEqual(signal.getsignal(signal.SIGTERM), signal.SIG_IGN)
                    os.kill(os.getpid(), signal.SIGTERM)
                    raise
        self.assertEqual(signal.getsignal(signal.SIGTERM), previous)

    def test_interruption_retains_failed_receipt_then_propagates_after_container_cleanup(self):
        with self.assertRaises(KeyboardInterrupt):
            self.mocked_run(invoke=lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
        receipt = json.loads((self.root / "observation/receipt.json").read_bytes())
        self.assertFalse(receipt["browser_checks_passed"])
        self.assertTrue(receipt["cleanup_verified"])
        self.assertEqual(receipt["error"], "KeyboardInterrupt")

    def test_driver_interruption_terminates_and_reaps_node_before_propagating(self):
        from unittest.mock import Mock
        process = Mock()
        process.poll.return_value = None
        process.wait.side_effect = [KeyboardInterrupt(), 0]
        self.root.joinpath("driver-output").mkdir()
        with patch.object(browser.subprocess, "Popen", return_value=process):
            with self.assertRaises(KeyboardInterrupt):
                browser._invoke(self.policy, "gossip-browser-" + "f" * 32, self.root / "driver-output")
        process.terminate.assert_called_once()
        self.assertEqual(process.wait.call_count, 2)

    def test_first_signal_during_container_cleanup_is_deferred_until_receipt_fsync(self):
        completed = []
        previous = signal.getsignal(signal.SIGTERM)
        def remove(_name):
            os.kill(os.getpid(), signal.SIGTERM)
            completed.append("container removal finished")
            return True
        with self.assertRaises(KeyboardInterrupt):
            self.mocked_run(remove=remove)
        self.assertEqual(completed, ["container removal finished"])
        self.assertEqual(signal.getsignal(signal.SIGTERM), previous)
        receipt = json.loads((self.root / "observation/receipt.json").read_bytes())
        self.assertEqual(receipt["status"], "cancelled")
        self.assertTrue(receipt["cleanup_verified"])
        self.assertFalse(receipt["browser_checks_passed"])
        self.assertTrue((self.root / "observation/cancellation.json").is_file())

    def test_first_signal_during_terminal_persistence_retains_cancellation_record(self):
        original = browser._save
        def save(file, value):
            if file.name == "receipt.json":
                os.kill(os.getpid(), signal.SIGTERM)
            original(file, value)
        with patch.object(browser, "_save", side_effect=save):
            with self.assertRaises(KeyboardInterrupt):
                self.mocked_run()
        output = self.root / "observation"
        cancellation = json.loads((output / "cancellation.json").read_bytes())
        self.assertEqual(cancellation["terminal_receipt_sha256"], browser._sha((output / "receipt.json").read_bytes()))
        self.assertTrue(cancellation["cleanup_completed_before_propagation"])

    def test_source_mapping_is_detached_from_later_caller_mutations(self):
        def mutation(*_):
            self.files["solution.py"] = "changed"
            return {"passed": True, "cleanup": {"browser": "complete"}}
        # Caller dictionary changes must not alter detached staged bytes. The
        # barrier, not a mutable caller mapping, is authoritative after capture.
        _, result, _, _ = self.mocked_run(invoke=mutation)
        self.assertTrue(result["browser_checks_passed"])
        self.assertEqual(result["source_sha256"], self.subject.source_sha256)

    def test_staged_source_binding_preserves_crlf_and_unicode_bytes(self):
        directory = self.root / "source"
        directory.mkdir()
        raw = "# café\r\nx = 1\r\n"
        (directory / "solution.py").write_bytes(raw.encode("utf-8"))
        self.assertEqual(browser._files_from_directory(directory), {"solution.py": raw})
