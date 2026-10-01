"""Consumer boundary tests for the versioned study adapter; no Docker or providers."""
from copy import deepcopy
import hashlib
import json
import signal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from devtools import study_validation as adapter
from devtools.study_validation import (
    StudyValidationError, StudyValidationSession, study_signals, study_validation_contract,
)
from devtools.validation_session import ResourceBudget, digest
from tests.test_validation_session import CASES, FILES, IMAGE, FakeFactory


class StudyValidationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.counter = 0

    def session(self, factory=None, **options):
        self.counter += 1
        return StudyValidationSession(self.root / str(self.counter), image=IMAGE,
            protocol="study-test-v2", validator_factory=factory or FakeFactory(),
            runtime_identity=lambda: "fake-study-daemon", **options)

    def request(self, label, **options):
        return dict(files=deepcopy(FILES), label=label, **options)

    def test_preregistration_contract_is_pure_complete_and_matches_session(self):
        budget = ResourceBudget(workers=4, cpus=4, memory_mib=1024, outer_parallelism=2)
        with patch.object(adapter, "ValidationSession") as runtime:
            contract = study_validation_contract(image=IMAGE, protocol="study-test-v2", budget=budget)
            runtime.assert_not_called()
        self.assertEqual(list(self.root.iterdir()), [])
        self.assertEqual(contract["resource_budget"]["capacity"], 2)
        self.assertEqual(contract["per_validator_resources"], dict(cpus=1, memory_mib=256, pids=64))
        for path in ("devtools/study_validation.py", "devtools/validation_session.py",
                     "gossip_harness/blackbox_validator.py", "gossip_harness/sandbox.py"):
            self.assertEqual(contract["support_sha256"][path],
                hashlib.sha256((adapter._ROOT / path).read_bytes()).hexdigest())
        session = self.session(budget=budget)
        self.assertEqual(contract, session.contract)
        self.assertEqual(json.loads((session.session.output / "study-contract.json").read_text()), contract)

    def test_transitive_support_imports_and_dynamic_imports_fail_closed(self):
        (self.root / "pkg").mkdir()
        (self.root / "pkg/__init__.py").write_text("")
        root_file = self.root / "adapter.py"
        root_file.write_text("from pkg import helper\n")
        helper = self.root / "pkg/helper.py"
        helper.write_text("VALUE = 1\n")
        with patch.object(adapter, "_ROOT", self.root), patch.object(adapter, "__file__", str(root_file)), \
                patch.object(adapter, "support_digests", return_value={}):
            first = adapter._support_closure([])
            self.assertEqual(set(first), {"adapter.py", "pkg/__init__.py", "pkg/helper.py"})
            helper.write_text("VALUE = 2\n")
            self.assertNotEqual(first, adapter._support_closure([]))
            helper.write_text("from importlib import import_module as load\nload('anything')\n")
            with self.assertRaisesRegex(ValueError, "Dynamic imports"):
                adapter._support_closure([])

    def test_bad_resources_or_determinism_rejected_without_runtime_or_output(self):
        with patch.object(adapter, "ValidationSession") as runtime:
            for options in (dict(budget=ResourceBudget(outer_parallelism=3)),
                            dict(deterministic_visible=1), dict(timeout_seconds=-1)):
                with self.subTest(options=options), self.assertRaises(ValueError):
                    self.session(**options)
            runtime.assert_not_called()
        self.assertEqual(list(self.root.iterdir()), [])

    def test_default_visible_jobs_execute_physically_with_one_preflight(self):
        factory = FakeFactory()
        session = self.session(factory)
        self.assertTrue(session.preflight()[0])
        self.assertEqual(session.summary()["counts"]["preflights"], 1)
        rows = session.evaluate_many([self.request("first"), self.request("second")], CASES)
        self.assertEqual(factory.preflights, 1)
        self.assertEqual(len(factory.calls), 2)
        self.assertEqual([row["validation"]["label"] for row in rows], ["first", "second"])
        self.assertTrue(all(row["validation"]["physical"] for row in rows))
        self.assertTrue(all(not row["validation"]["binding"]["deterministic"] for row in rows))

    def test_explicit_visible_reuse_preserves_original_identity_and_detached_rows(self):
        factory = FakeFactory()
        session = self.session(factory, deterministic_visible=True)
        first, second = session.evaluate_many([self.request("one"), self.request("two")], CASES)
        self.assertTrue(first["validation"]["physical"])
        self.assertFalse(second["validation"]["physical"])
        self.assertEqual(second["validation"]["reuse"]["kind"], "singleflight")
        first["receipt"]["outcomes"][0]["actual"] = "caller mutation"
        later = session.evaluate(FILES, CASES, label="one")
        self.assertEqual(later["receipt"]["outcomes"][0]["actual"], 2)
        self.assertEqual(later["validation"]["reuse"]["kind"], "in_session")
        self.assertEqual(later["validation"]["execution_id"], second["validation"]["execution_id"])
        self.assertEqual(later["validation"]["logical_index"], 2)
        self.assertEqual(len(factory.calls), 1)
        self.assertEqual(later["validation"]["source_sha256"], digest(FILES))
        self.assertEqual(later["validation"]["suite_sha256"], digest(CASES))

    def test_heterogeneous_independent_observations_always_execute_with_reasons(self):
        factory = FakeFactory()
        session = self.session(factory, deterministic_visible=True)
        rows = session.evaluate_matrix([
            self.request("visible", cases=CASES),
            self.request("final", cases=CASES, purpose="final", reason="Acceptance barrier"),
            self.request("repeat", cases=[dict(input=3, expected=4)], purpose="repeatability",
                         reason="Independent final-stage historical observation", deterministic=True),
        ])
        self.assertEqual([row["validation"]["purpose"] for row in rows],
                         ["visible", "final", "repeatability"])
        self.assertEqual(len(factory.calls), 3)
        self.assertTrue(all(row["validation"]["physical"] for row in rows))
        self.assertTrue(all(row["validation"]["reuse"] is None for row in rows))
        self.assertTrue(all(not row["validation"]["binding"]["deterministic"] for row in rows[1:]))
        self.assertNotEqual(rows[1]["validation"]["suite_sha256"], rows[2]["validation"]["suite_sha256"])

    def test_behavioral_failure_remains_usable_failed_evidence(self):
        rows = self.session(FakeFactory(behavioral_failure=True)).evaluate_many([self.request("wrong")], CASES)
        self.assertEqual(rows[0]["receipt"]["status"], "failed")
        self.assertFalse(rows[0]["receipt"]["passed"])
        self.assertEqual(rows[0]["validation"]["status"], "failed")

    def test_bad_requests_reject_before_any_preflight(self):
        factory = FakeFactory()
        session = self.session(factory)
        invalid = [[], [self.request("same"), self.request("same")],
                   [self.request("wrong source", source_sha256="bad")],
                   [self.request("missing reason", purpose="final")],
                   [self.request("unexpected", surprise=True)],
                   [self.request("bad deterministic", deterministic="yes")],
                   [dict(label="no files")]]
        for requests in invalid:
            with self.subTest(requests=requests), self.assertRaises(ValueError):
                session.evaluate_many(requests, CASES)
        with self.assertRaises(ValueError):
            session.evaluate_many([self.request("two suites", cases=CASES)], CASES)
        self.assertEqual(factory.preflights, 0)
        self.assertEqual(factory.calls, [])

    def test_infrastructure_failure_retains_raw_results_and_stops_future_launches(self):
        factory = FakeFactory(fail_first=True)
        session = self.session(factory)
        with self.assertRaises(StudyValidationError) as raised:
            session.evaluate_many([self.request("broken"), self.request("healthy")], CASES)
        summary = raised.exception.summary
        self.assertEqual(summary["counts"]["logical_jobs"], 2)
        self.assertEqual(summary["counts"]["infrastructure_failed"], 1)
        for result in summary["results"]:
            self.assertTrue((Path(result["artifact_path"]) / "result.json").is_file())
        with self.assertRaises(StudyValidationError):
            session.evaluate(FILES, CASES, label="after failure")
        self.assertEqual(len(factory.calls), 2)

    def test_incomplete_reordered_or_wrong_source_results_fail_closed(self):
        mutations = {
            "missing": lambda summary: summary["results"].pop(),
            "reordered": lambda summary: summary["results"].reverse(),
            "source": lambda summary: summary["results"][0]["binding"].update(source_sha256="bad"),
            "cleanup": lambda summary: summary["results"][0]["receipt"].update(cleanup_verified=False),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                session = self.session()
                original = session.session.run
                def run(jobs):
                    summary = original(jobs)
                    mutate(summary)
                    return summary
                with patch.object(session.session, "run", side_effect=run), self.assertRaises(StudyValidationError):
                    session.evaluate_many([self.request("a"), self.request("b")], CASES)

    def test_forged_reuse_cannot_satisfy_an_independent_final(self):
        session = self.session()
        original = session.session.run
        def run(jobs):
            summary = original(jobs)
            row = summary["results"][0]
            row["physical"] = False
            row["reuse"] = dict(kind="persisted", execution_id=row["execution_id"],
                                origin_session_id="fake", artifact_path="fake")
            return summary
        with patch.object(session.session, "run", side_effect=run), self.assertRaises(StudyValidationError):
            session.evaluate(FILES, CASES, label="final", purpose="final", reason="Acceptance")

    def test_changed_contract_stops_before_and_after_execution(self):
        factory = FakeFactory()
        cache = self.root / "changed-contract-cache"
        session = self.session(factory, deterministic_visible=True, cache=cache)
        changed = dict(session.contract, study_protocol="changed")
        with patch.object(adapter, "study_validation_contract", return_value=changed), \
                self.assertRaises(StudyValidationError):
            session.evaluate(FILES, CASES, label="before")
        self.assertEqual(factory.calls, [])
        with patch.object(adapter, "study_validation_contract", side_effect=[session.contract, session.contract, changed]), \
                self.assertRaises(StudyValidationError) as raised:
            session.evaluate(FILES, CASES, label="during")
        self.assertEqual(len(factory.calls), 1)
        self.assertEqual(raised.exception.summary["counts"]["physical_executions"], 1)
        self.assertEqual(raised.exception.summary["results"][0]["status"], "infrastructure_failed")
        self.assertEqual(list(cache.glob("*.json")), [])

    def test_preregistered_no_reuse_cannot_be_escalated_by_job_override(self):
        factory = FakeFactory()
        session = self.session(factory)
        for options in (dict(deterministic=True),):
            with self.assertRaisesRegex(ValueError, "preregistered"):
                session.evaluate_many([self.request("override")], CASES, **options)
        with self.assertRaisesRegex(ValueError, "preregistered"):
            session.evaluate_many([self.request("row override", deterministic=True)], CASES)
        self.assertEqual(factory.preflights, 0)
        self.assertEqual(factory.calls, [])

    def test_signal_context_cancels_and_restores_prior_handlers(self):
        session = self.session()
        previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        with self.assertRaises(StudyValidationError), study_signals(session):
            handler = signal.getsignal(signal.SIGTERM)
            self.assertTrue(callable(handler))
            handler(signal.SIGTERM, None)
            self.assertTrue(session.cancelled)
            with self.assertRaises(StudyValidationError):
                session.raise_if_cancelled()
            with self.assertRaises(StudyValidationError):
                session.evaluate(FILES, CASES, label="cancelled")
        self.assertEqual({number: signal.getsignal(number) for number in previous}, previous)
        self.assertEqual(session.summary()["counts"]["physical_executions"], 0)

    def test_safe_names_preserve_human_labels_and_order_without_path_escape(self):
        session = self.session()
        labels = ["../../Visible tree α", "Visible/tree:β"]
        rows = session.evaluate_many([self.request(label) for label in labels], CASES)
        self.assertEqual([row["validation"]["label"] for row in rows], labels)
        for row in rows:
            path = Path(row["validation"]["artifact_path"])
            self.assertEqual(path.parent, session.session.output)
            self.assertEqual(row["validation"]["study_contract_sha256"], session.contract_sha256)


if __name__ == "__main__":
    unittest.main()
