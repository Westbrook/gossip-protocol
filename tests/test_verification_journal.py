from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from gossip_harness.ledger import Ledger
from gossip_harness.verification_journal import (
    JournalConflict, JournalCorrupt, JournalUnknownOutcome, RequestJournal,
)
from gossip_harness import verification_journal
from gossip_harness.worker import WorkerFailure, WorkerRequest, WorkerResult


class VerificationJournalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.journal = RequestJournal(self.root / "journal")
        self.request = WorkerRequest("opaque-task", "Implement", ("solution.py",),
                                     {"solution.py": "pass\n"}, "a" * 40, 1)
        self.result = WorkerResult({"solution.py": "answer = 42\n"}, "Done", 3,
                                   {"api_calls": 1, "model": "scripted-no-api"})
        self.ledger = Ledger(self.root / "budget.sqlite", budget_units=10)
        self.ledger.add_task("opaque-task")
        self.lease = self.ledger.claim("opaque-task", "worker", now=0, ttl=600)
        self.reserve = Mock(side_effect=lambda: self.ledger.reserve("reservation", self.lease, 10, now=1))
        self.settle = Mock(side_effect=lambda usage: self.ledger.settle("reservation", usage))
        self.invoke = Mock(return_value=self.result)

    def execute(self, **overrides):
        arguments = dict(call_id="call", request=self.request, reservation_id="reservation",
                         invoke=self.invoke, reserve=self.reserve, settle=self.settle)
        arguments.update(overrides)
        return self.journal.execute(**arguments)

    def reservation(self):
        with sqlite3.connect(self.ledger.path) as db:
            return db.execute("SELECT amount,spent,state FROM reservations WHERE id='reservation'").fetchone()

    def test_persists_before_settlement_and_replays_without_dispatch_or_hook(self):
        observed = []

        def persisted():
            paths = self.journal.paths("call")
            observed.append(self.reservation())
            self.assertTrue(paths["request"].is_file())
            self.assertTrue(paths["result"].is_file())
            self.assertFalse(paths["settled"].exists())

        first = self.execute(on_persisted=persisted)
        replay = self.execute(on_persisted=Mock(side_effect=AssertionError("Hook cannot replay")))
        self.assertEqual(first, self.result)
        self.assertEqual(replay, self.result)
        self.assertEqual(observed, [(10, None, "reserved")])
        self.reserve.assert_called_once_with()
        self.invoke.assert_called_once_with()
        self.assertEqual(self.settle.call_count, 2)
        self.assertEqual(self.reservation(), (10, 3, "settled"))
        self.assertTrue(self.journal.paths("call")["settled"].exists())

    def test_request_and_reservation_reuse_conflicts_before_any_new_action(self):
        self.execute()
        for change in (dict(request=replace(self.request, instructions="Different")),
                       dict(request=replace(self.request, files={"solution.py": "changed"})),
                       dict(reservation_id="different-reservation")):
            with self.subTest(change=change):
                with self.assertRaises(JournalConflict):
                    self.execute(**change)
        self.assertEqual(self.invoke.call_count, 1)
        self.assertEqual(self.reserve.call_count, 1)
        self.assertEqual(self.settle.call_count, 1)

    def test_missing_durable_response_is_unknown_and_never_reinvoked(self):
        self.invoke.side_effect = RuntimeError("Transport ended ambiguously")
        with self.assertRaises(JournalUnknownOutcome):
            self.execute()
        self.invoke.side_effect = AssertionError("Must not retry")
        with self.assertRaises(JournalUnknownOutcome):
            self.execute()
        self.assertEqual(self.invoke.call_count, 1)
        self.assertEqual(self.reserve.call_count, 1)
        self.settle.assert_not_called()
        self.assertEqual(self.reservation(), (10, None, "reserved"))

    def test_known_failure_is_persisted_settled_and_replayed_as_same_failure(self):
        self.invoke.side_effect = WorkerFailure("Sanitized known rejection", 2, {"halt": False})
        for _ in range(2):
            with self.assertRaises(WorkerFailure) as raised:
                self.execute()
            self.assertEqual(str(raised.exception), "Sanitized known rejection")
            self.assertEqual(raised.exception.usage_units, 2)
            self.assertEqual(raised.exception.metadata, {"halt": False})
        self.invoke.assert_called_once_with()
        self.assertEqual(self.settle.call_count, 2)
        self.assertEqual(self.reservation(), (10, 2, "settled"))

    def test_unknown_usage_failure_remains_reserved_on_every_replay(self):
        self.invoke.side_effect = WorkerFailure("Usage unknown", None, {"halt": True})
        for _ in range(2):
            with self.assertRaises(WorkerFailure) as raised:
                self.execute()
            self.assertIsNone(raised.exception.usage_units)
            self.assertEqual(raised.exception.metadata, {"halt": True})
        self.invoke.assert_called_once_with()
        self.settle.assert_not_called()
        self.assertEqual(self.reservation(), (10, None, "reserved"))
        self.assertFalse(self.journal.paths("call")["settled"].exists())

    def test_committed_settlement_without_marker_is_reconciled_idempotently(self):
        def interrupted_settle(usage):
            self.ledger.settle("reservation", usage)
            raise RuntimeError("Process stopped after settlement")

        with self.assertRaisesRegex(RuntimeError, "after settlement"):
            self.execute(settle=interrupted_settle)
        self.assertEqual(self.reservation(), (10, 3, "settled"))
        self.assertFalse(self.journal.paths("call")["settled"].exists())
        self.assertEqual(self.execute(), self.result)
        self.invoke.assert_called_once_with()
        self.assertTrue(self.journal.paths("call")["settled"].exists())

    def test_failed_reservation_does_not_write_dispatch_intent(self):
        with self.assertRaisesRegex(ValueError, "Reservation denied"):
            self.execute(reserve=Mock(side_effect=ValueError("Reservation denied")))
        self.assertFalse(self.journal.paths("call")["request"].exists())
        self.invoke.assert_not_called()
        self.assertEqual(self.execute(), self.result)
        self.invoke.assert_called_once_with()

    def test_reserved_before_dispatch_intent_is_safe_to_reserve_again(self):
        write = verification_journal._write

        def interrupted_write(path, value):
            if path == self.journal.paths("call")["request"]:
                raise OSError("Stopped after reservation before intent")
            return write(path, value)

        with patch.object(verification_journal, "_write", side_effect=interrupted_write):
            with self.assertRaisesRegex(OSError, "before intent"):
                self.execute()
        self.assertEqual(self.reservation(), (10, None, "reserved"))
        self.invoke.assert_not_called()
        self.assertFalse(self.journal.paths("call")["request"].exists())
        self.assertEqual(self.execute(), self.result)
        self.assertEqual(self.reserve.call_count, 2)
        self.invoke.assert_called_once_with()
        self.assertEqual(self.reservation(), (10, 3, "settled"))

    def test_corrupted_request_response_and_settlement_are_blocked(self):
        self.execute()
        paths = self.journal.paths("call")
        originals = {kind: path.read_bytes() for kind, path in paths.items()}
        for kind in paths:
            with self.subTest(kind=kind):
                for name, raw in originals.items():
                    paths[name].write_bytes(raw)
                data = json.loads(originals[kind])
                if kind == "request":
                    data["request"]["instructions"] = "Corrupted"
                    expected = JournalConflict
                elif kind == "result":
                    data["payload"]["changes"]["solution.py"] = "Corrupted"
                    expected = JournalCorrupt
                else:
                    data["usage_units"] = 1
                    expected = JournalCorrupt
                paths[kind].write_text(json.dumps(data))
                with self.assertRaises(expected):
                    self.execute()
        self.assertEqual(self.invoke.call_count, 1)
        self.assertEqual(self.settle.call_count, 1)

    def test_orphan_response_and_invalid_result_cannot_become_fresh_calls(self):
        self.execute()
        self.journal.paths("call")["request"].unlink()
        with self.assertRaises(JournalCorrupt):
            self.execute()
        self.assertEqual(self.invoke.call_count, 1)
        invalid = RequestJournal(self.root / "invalid-journal")
        invoke = Mock(return_value=WorkerResult({}, "No valid usage", True, {}))
        with self.assertRaises(JournalCorrupt):
            invalid.execute("other", self.request, "other", invoke, Mock(), Mock())
        with self.assertRaises(JournalUnknownOutcome):
            invalid.execute("other", self.request, "other", invoke, Mock(), Mock())
        invoke.assert_called_once_with()

    def test_concurrent_local_callers_dispatch_only_once(self):
        def delayed():
            time.sleep(0.03)
            return self.result

        self.invoke.side_effect = delayed
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda _: self.execute(), range(6)))
        self.assertEqual(results, [self.result] * 6)
        self.invoke.assert_called_once_with()
        self.reserve.assert_called_once_with()
        self.assertEqual(self.settle.call_count, 6)
        self.assertEqual(self.reservation(), (10, 3, "settled"))

    def test_distinct_calls_cannot_share_an_existing_reservation(self):
        self.execute()
        with self.assertRaisesRegex(JournalConflict, "Reservation ID"):
            self.execute(call_id="another-call")
        self.invoke.assert_called_once_with()
        self.reserve.assert_called_once_with()
        self.assertEqual(self.reservation(), (10, 3, "settled"))

    def test_concurrent_distinct_calls_have_one_reservation_owner(self):
        def attempt(index):
            try:
                return self.execute(call_id=f"call-{index}")
            except JournalConflict:
                return None

        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(attempt, range(6)))
        self.assertEqual(sum(result is not None for result in results), 1)
        self.invoke.assert_called_once_with()
        self.reserve.assert_called_once_with()
        self.assertEqual(self.reservation(), (10, 3, "settled"))

    def test_sigkill_after_durable_response_replays_without_duplicate_invocation(self):
        config = self.root / "config.json"
        config.write_text(json.dumps({"root": str(self.root), "lease": asdict(self.lease),
                                      "request": asdict(self.request), "result": asdict(self.result)}))
        script = self.root / "scripted_worker.py"
        script.write_text('''import json,os,signal,sys
from pathlib import Path
from gossip_harness.ledger import Ledger,Lease
from gossip_harness.verification_journal import RequestJournal
from gossip_harness.worker import WorkerRequest,WorkerResult
c=json.loads(Path(sys.argv[1]).read_text()); root=Path(c['root'])
ledger=Ledger(root/'budget.sqlite'); lease=Lease(**c['lease'])
request=WorkerRequest(**c['request']); result=WorkerResult(**c['result'])
def invoke():
    with (root/'invocations.txt').open('a') as stream:
        stream.write(str(os.getpid())+'\\n'); stream.flush(); os.fsync(stream.fileno())
    return result
def persisted():
    with (root/'persisted.marker').open('w') as stream:
        stream.write(str(os.getpid())); stream.flush(); os.fsync(stream.fileno())
    signal.pause()
journal=RequestJournal(root/'journal')
answer=journal.execute('call',request,'reservation',invoke,
    lambda: ledger.reserve('reservation',lease,10,now=1),
    lambda units: ledger.settle('reservation',units),
    persisted if sys.argv[2]=='pause' else None)
(root/'returned.json').write_text(json.dumps({'pid':os.getpid(),'changes':answer.changes,'usage':answer.usage_units}))
''')
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
        command = [sys.executable, str(script), str(config)]
        process = subprocess.Popen(command + ["pause"], env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 20
            while not (self.root / "persisted.marker").exists() and time.monotonic() < deadline:
                if process.poll() is not None:
                    break
                time.sleep(0.02)
            self.assertTrue((self.root / "persisted.marker").exists(), "Child did not reach durable response boundary")
            self.assertEqual(self.reservation(), (10, None, "reserved"))
            self.assertTrue(self.journal.paths("call")["result"].is_file())
            self.assertFalse(self.journal.paths("call")["settled"].exists())
            process.kill()
            process.communicate(timeout=10)
            self.assertEqual(process.returncode, -signal.SIGKILL)
            resumed = subprocess.run(command + ["resume"], env=environment, capture_output=True, text=True, timeout=20)
            self.assertEqual(resumed.returncode, 0, resumed.stderr)
            returned = json.loads((self.root / "returned.json").read_text())
            self.assertNotEqual(returned["pid"], process.pid)
            self.assertEqual(returned["changes"], self.result.changes)
            self.assertEqual(returned["usage"], 3)
            self.assertEqual((self.root / "invocations.txt").read_text().splitlines(), [str(process.pid)])
            self.assertEqual(self.reservation(), (10, 3, "settled"))
            self.assertTrue(self.journal.paths("call")["settled"].exists())
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=10)


if __name__ == "__main__":
    unittest.main()
