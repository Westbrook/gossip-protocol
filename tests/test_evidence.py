"""Scientific controls tested with synthetic files and deterministic workers."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace
import json
import threading
import unittest

from gossip_harness.evidence import (
    ControlledWorker, NoteReference, ResponseCache, SemanticNote,
    baseline_manifest_hash, deliver_notes,
)
from gossip_harness.worker import WorkerFailure, WorkerRequest, WorkerResult


FILES = {"parser.py": "def parse(text):\n    return text.strip()\n",
         "summary.py": "def summary(rows):\n    return len(rows)\n",
         "SPEC.txt": "Rows are strings; preserve empty rows.\n"}


def note(task="parser", **overrides):
    data = {"summary": "Check the contract at the module boundary.",
            "references": [{"path": "SPEC.txt", "quote": "preserve empty rows"}],
            "recommendation": "Verify the treatment of empty rows before integration."}
    data.update(overrides)
    return SemanticNote.parse(json.dumps(data), producer="worker-" + task, task_id=task, files=FILES)


def request(task="trial-0/team-shared/parser", **overrides):
    data = dict(task_id=task, instructions="Implement the assigned feature.",
                allowed_paths=("parser.py",), files=dict(FILES), base_sha="a" * 40,
                attempt=1, feedback="")
    data.update(overrides)
    return WorkerRequest(**data)


class CountingWorker:
    def __init__(self, failure=None):
        self.calls = []
        self.reservations = []
        self.lock = threading.Lock()
        self.failure = failure

    def reservation_units(self, req):
        with self.lock:
            self.reservations.append(req)
        return 100

    def run(self, req):
        with self.lock:
            self.calls.append(req)
        if self.failure:
            raise self.failure
        return WorkerResult({"parser.py": "# proposed source\n"}, "A candidate change.",
                            17, {"model": "synthetic", "usage": {"input_tokens": 3}})


class EvidenceTests(unittest.TestCase):
    def test_note_is_immutable_and_serialization_does_not_mutate_it(self):
        result = note()
        result.verify(FILES)
        with self.assertRaises(FrozenInstanceError):
            result.summary = "replacement"
        with self.assertRaises(FrozenInstanceError):
            result.references[0].quote = "replacement"
        serialized = result.to_dict()
        serialized["references"][0]["quote"] = "mutated"
        self.assertEqual(result.references[0].quote, "preserve empty rows")
        self.assertEqual(result.baseline_manifest_hash, baseline_manifest_hash(FILES))
        with self.assertRaises(ValueError):
            replace(result, references=list(result.references))
        with self.assertRaises(ValueError):
            NoteReference("SPEC.txt", ["mutable"])

    def test_full_baseline_and_every_identity_field_are_bound(self):
        original = note()
        changed = {**FILES, "unreferenced.txt": "new information"}
        with self.assertRaisesRegex(ValueError, "binding"):
            original.verify(changed)
        for altered in (replace(original, producer="other"), replace(original, task_id="other"),
                        replace(original, summary="changed"), replace(original, note_id="0" * 64),
                        replace(original, content_hash="0" * 64)):
            with self.subTest(altered=altered), self.assertRaises(ValueError):
                altered.verify(FILES)
        self.assertEqual(baseline_manifest_hash(FILES), baseline_manifest_hash(dict(reversed(list(FILES.items())))))

    def test_exact_quote_required_but_interpretation_is_not_certified(self):
        for refs in ([], [{"path": "missing.py", "quote": "preserve"}],
                     [{"path": "SPEC.txt", "quote": ""}],
                     [{"path": "SPEC.txt", "quote": "Preserve empty rows"}],
                     [{"path": "SPEC.txt", "quote": "preserve"}] * 2,
                     [{"path": "SPEC.txt", "quote": "preserve"}] * 9):
            with self.subTest(refs=refs), self.assertRaises(ValueError):
                note(references=refs)
        # Reference verification deliberately is not a factual correctness judge.
        accepted = note(recommendation="A possibly mistaken interpretation to be tested.")
        accepted.verify(FILES)

    def test_schema_rejects_duplicate_json_keys_nonfinite_and_unbounded_text(self):
        for body in ('{"summary":"first","summary":"second"}',
                     '{"summary":NaN}', '[]'):
            with self.subTest(body=body), self.assertRaises(ValueError):
                SemanticNote.parse(body, producer="worker", task_id="parser", files=FILES)
        for overrides in ({"summary": "x" * 4001}, {"recommendation": ""}, {"extra": True}):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                note(**overrides)

    def test_isolated_only_gets_own_and_single_gets_all_notes(self):
        notes = (note("parser"), note("summary"))
        workers = {"worker-parser": "parser", "worker-summary": "summary"}
        isolated = deliver_notes(notes, files=FILES, workers=workers, arm="isolated")
        self.assertEqual(isolated.payloads["worker-parser"], (notes[0],))
        self.assertEqual(isolated.payloads["worker-summary"], (notes[1],))
        self.assertEqual(isolated.stats["contacts"], 0)
        single = deliver_notes(notes, files=FILES, workers={"worker-whole": "whole-project"}, arm="single")
        self.assertEqual({n.note_id for n in single.payloads["worker-whole"]}, {n.note_id for n in notes})

    def test_broker_and_gossip_deliver_identical_notes_before_execution(self):
        notes = (note("parser"), note("summary"))
        workers = {"worker-parser": "parser", "worker-summary": "summary"}
        bus = deliver_notes(notes, files=FILES, workers=workers, arm="team-shared")
        gossip = deliver_notes(reversed(notes), files=FILES, workers=workers, arm="team-gossip")
        self.assertEqual(bus.payloads, gossip.payloads)
        self.assertEqual(bus.receipts[0]["included_note_ids"], gossip.receipts[0]["included_note_ids"])
        self.assertEqual(bus.receipts[0]["event_ids"], gossip.receipts[0]["event_ids"])
        self.assertTrue(bus.converged and gossip.converged)
        self.assertGreaterEqual(bus.stats["rounds"], 2)
        self.assertGreater(gossip.stats["contacts"], 0)
        self.assertNotEqual(bus.stats, gossip.stats)

    def test_delivery_bound_fails_closed_instead_of_using_partial_evidence(self):
        for arm in ("team-shared", "team-gossip"):
            with self.subTest(arm=arm), self.assertRaises(WorkerFailure) as caught:
                deliver_notes((note(),), files=FILES, workers={"other": "summary"}, arm=arm, max_rounds=0)
            self.assertEqual(caught.exception.usage_units, 0)
            self.assertTrue(caught.exception.metadata["halt"])

    def test_mutated_note_rejected_before_delivery(self):
        with self.assertRaises(ValueError):
            deliver_notes((replace(note(), summary="tampered"),), files=FILES,
                          workers={"worker-parser": "parser"}, arm="team-shared")

    def test_record_replay_have_identical_normalized_prompts_and_no_replay_calls(self):
        provider, cache, audit = CountingWorker(), ResponseCache(), []
        notes = (note("parser"), note("summary"))
        record = ControlledWorker(provider, notes_by_task={"parser": notes}, cache=cache, audit=audit.append)
        replay = ControlledWorker(provider, notes_by_task={"parser": tuple(reversed(notes))},
                                  cache=cache, mode="replay", audit=audit.append)
        first = request()
        second = replace(first, task_id="different-run/team-gossip/parser")
        self.assertEqual(record.reservation_units(first), 100)
        original = record.run(first)
        self.assertEqual(replay.reservation_units(second), 0)
        duplicate = replay.run(second)
        self.assertEqual(len(provider.calls), 1)
        self.assertEqual(len(provider.reservations), 1)
        self.assertEqual(original.changes, duplicate.changes)
        self.assertEqual(duplicate.usage_units, 0)
        self.assertEqual(duplicate.metadata["original_usage_units"], 17)
        self.assertTrue(duplicate.metadata["replay"])
        self.assertEqual(original.metadata["request_sha256"], duplicate.metadata["request_sha256"])
        self.assertEqual(provider.reservations[0], provider.calls[0])
        self.assertEqual(provider.calls[0].task_id, "parser")
        self.assertNotIn("team-shared", provider.calls[0].instructions)
        self.assertEqual([r["kind"] for r in audit], ["reservation", "result", "reservation", "result"])
        self.assertEqual([r.get("reserved_units") for r in audit if r["kind"] == "reservation"], [100, 0])
        snapshot = cache.to_dict()[record.request_hash(first)]
        self.assertEqual(snapshot["request"]["files"], FILES)
        self.assertEqual(snapshot["request"]["instructions"], provider.calls[0].instructions)

    def test_replay_mismatch_fails_without_live_fallback(self):
        provider, cache = CountingWorker(), ResponseCache()
        record = ControlledWorker(provider, notes_by_task={"parser": (note(),)}, cache=cache)
        replay = ControlledWorker(provider, notes_by_task={"parser": (note(),)}, cache=cache, mode="replay")
        record.run(request())
        for changed in (replace(request(), instructions="Changed task"),
                        replace(request(), feedback="Different feedback"),
                        replace(request(), attempt=2), replace(request(), base_sha="b" * 40),
                        replace(request(), files={**FILES, "parser.py": "different"})):
            for action in (replay.run, replay.reservation_units):
                with self.subTest(changed=changed, action=action), self.assertRaises(WorkerFailure) as caught:
                    action(changed)
                self.assertEqual(caught.exception.metadata["failure_kind"], "control_mismatch")
                self.assertEqual(caught.exception.usage_units, 0)
        self.assertEqual(len(provider.calls), 1)

    def test_known_cost_failure_is_replayed_without_an_api_call(self):
        provider = CountingWorker(WorkerFailure("Synthetic refusal", 13, {"failure_kind": "refusal"}))
        cache = ResponseCache()
        record = ControlledWorker(provider, notes_by_task={"parser": ()}, cache=cache)
        replay = ControlledWorker(None, notes_by_task={"parser": ()}, cache=cache, mode="replay")
        with self.assertRaises(WorkerFailure) as first:
            record.run(request())
        with self.assertRaises(WorkerFailure) as second:
            replay.run(request("other/team-gossip/parser"))
        self.assertEqual(first.exception.usage_units, 13)
        self.assertEqual(second.exception.usage_units, 0)
        self.assertEqual(second.exception.metadata["original_usage_units"], 13)
        self.assertEqual(str(first.exception), str(second.exception))
        self.assertEqual(len(provider.calls), 1)

    def test_cache_and_result_mutation_cannot_change_replay(self):
        provider, cache = CountingWorker(), ResponseCache()
        record = ControlledWorker(provider, notes_by_task={"parser": (note(),)}, cache=cache)
        replay = ControlledWorker(None, notes_by_task={"parser": (note(),)}, cache=cache, mode="replay")
        original = record.run(request())
        original.changes["parser.py"] = "mutated"
        original.metadata["usage"]["input_tokens"] = 999
        snapshot = cache.to_dict()
        snapshot[record.request_hash(request())]["changes"]["parser.py"] = "also mutated"
        actual = replay.run(request())
        self.assertEqual(actual.changes["parser.py"], "# proposed source\n")
        self.assertEqual(actual.metadata["usage"]["input_tokens"], 3)

    def test_concurrent_recording_is_thread_safe_and_duplicate_does_not_spend(self):
        provider, cache = CountingWorker(), ResponseCache()
        record = ControlledWorker(provider, notes_by_task={"parser": ()}, cache=cache)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(record.run, [replace(request(), attempt=i + 1) for i in range(8)]))
        self.assertEqual(len(results), 8)
        self.assertEqual(len(cache.to_dict()), 8)
        with self.assertRaises(WorkerFailure) as caught:
            record.run(request())
        self.assertEqual(caught.exception.usage_units, 0)
        self.assertEqual(len(provider.calls), 8)

    def test_audit_failure_preserves_known_cost_and_cache_receipt(self):
        provider, cache = CountingWorker(), ResponseCache()
        def broken(_):
            raise OSError("synthetic audit storage error")
        record = ControlledWorker(provider, notes_by_task={"parser": ()}, cache=cache, audit=broken)
        with self.assertRaises(WorkerFailure) as caught:
            record.run(request())
        self.assertEqual(caught.exception.usage_units, 17)
        self.assertTrue(caught.exception.metadata["halt"])
        self.assertEqual(len(cache.to_dict()), 1)


if __name__ == "__main__":
    unittest.main()
