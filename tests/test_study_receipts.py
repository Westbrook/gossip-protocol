"""Tamper rejection for retained study observations; no Docker or candidate code."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from devtools import study_receipts as audit
from devtools.study_validation import StudyValidationError, StudyValidationSession
from devtools.validation_session import digest
from tests.test_validation_session import CASES, FILES, IMAGE, FakeFactory


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True) + "\n")


class StudyReceiptTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.counter = 0

    def session(self, *, factory=None, cache=None, deterministic=False):
        self.counter += 1
        return StudyValidationSession(self.root / f"session-{self.counter}", image=IMAGE,
            protocol="receipt-fixture-v2", validator_factory=factory or FakeFactory(),
            runtime_identity=lambda: "fixture-daemon", deterministic_visible=deterministic, cache=cache)

    def populate(self, session=None, *, count=2, purpose="visible"):
        session = session or self.session()
        session.evaluate_many([dict(label=f"observation-{index}", files=FILES) for index in range(count)],
                              CASES, purpose=purpose, reason="Independent acceptance" if purpose == "final" else "")
        return session

    def check(self, session, *other):
        return audit.audit_study_sessions([item.session.output for item in (session, *other)],
                                          session.contract, evidence_root=self.root)

    def mutate(self, session, index, change):
        summary_path = session.session.output / "session.json"
        summary = json.loads(summary_path.read_text())
        row = summary["results"][index]
        change(row)
        save(Path(row["artifact_path"]) / "result.json", row)
        save(summary_path, summary)

    def test_complete_behavioral_failures_are_valid_evidence_without_execution(self):
        session = self.populate(self.session(factory=FakeFactory(behavioral_failure=True)))
        with patch("subprocess.run", side_effect=AssertionError("Candidate/runtime execution forbidden")):
            result = self.check(session)
        self.assertFalse(result["candidate_code_executed"])
        self.assertEqual(result["counts"], dict(logical_jobs=2, physical_executions=2, reused_observations=0))
        self.assertEqual([row["result"]["status"] for row in result["observations"]], ["failed", "failed"])
        self.assertEqual(result["observations"][0]["job"]["files"], FILES)

    def test_singleflight_and_in_session_reuse_link_to_one_physical_origin(self):
        session = self.populate(self.session(deterministic=True))
        session.evaluate(FILES, CASES, label="later")
        session.evaluate(FILES, CASES, label="final", purpose="final", reason="Independent final")
        result = self.check(session)
        rows = result["observations"]
        self.assertEqual([row["result"]["physical"] for row in rows], [True, False, False, True])
        self.assertEqual(rows[1]["result"]["reuse"]["kind"], "singleflight")
        self.assertEqual(rows[2]["result"]["reuse"]["kind"], "in_session")
        self.assertEqual(result["verified_origin_sessions"], 1)

    def test_persisted_reuse_recursively_audits_retained_origin_session(self):
        cache = self.root / "cache"
        first = self.populate(self.session(cache=cache, deterministic=True), count=1)
        second = self.populate(self.session(cache=cache, deterministic=True), count=1)
        result = self.check(second)
        self.assertEqual(result["verified_origin_sessions"], 2)
        self.assertEqual(result["counts"]["physical_executions"], 0)
        self.assertEqual(result["observations"][0]["result"]["reuse"]["origin_session_id"], first.session.session_id)
        origin = first.summary()["results"][0]
        Path(origin["artifact_path"], "result.json").unlink()
        with self.assertRaises(audit.StudyReceiptError):
            self.check(second)

    def test_missing_record_and_unaccounted_directory_are_rejected(self):
        session = self.populate()
        result_path = Path(session.summary()["results"][0]["artifact_path"]) / "result.json"
        original = result_path.read_bytes()
        result_path.unlink()
        with self.assertRaises(audit.StudyReceiptError):
            self.check(session)
        result_path.write_bytes(original)
        (session.session.output / "00009-unaccounted").mkdir()
        with self.assertRaisesRegex(audit.StudyReceiptError, "unaccounted"):
            self.check(session)

    def test_source_or_suite_input_tampering_is_rejected(self):
        for field, value in (("files", {"solution.py": "tampered"}),
                             ("cases", [{"input": 1, "expected": "tampered"}])):
            session = self.populate(count=1)
            path = Path(session.summary()["results"][0]["artifact_path"]) / "input.json"
            request = json.loads(path.read_text())
            request["job"][field] = value
            save(path, request)
            with self.assertRaises(audit.StudyReceiptError):
                self.check(session)

    def test_inconsistent_or_host_oracle_forged_receipt_is_rejected(self):
        for change in (lambda row: row["receipt"].update(source_sha256="0" * 64),
                       lambda row: row["receipt"]["outcomes"][0].update(actual="forged"),
                       lambda row: row["receipt"].update(cleanup_verified=False)):
            session = self.populate(count=1)
            self.mutate(session, 0, change)
            with self.assertRaises(audit.StudyReceiptError):
                self.check(session)

    def test_summary_order_counts_and_boolean_types_are_reconciled(self):
        changes = [lambda value: value["results"].reverse(),
                   lambda value: value["counts"].update(logical_jobs=99),
                   lambda value: value["counts"].update(preflights=True),
                   lambda value: value.update(passed=1),
                   lambda value: value.update(cancelled=True)]
        for change in changes:
            session = self.populate()
            path = session.session.output / "session.json"
            summary = json.loads(path.read_text())
            change(summary)
            save(path, summary)
            with self.assertRaises(audit.StudyReceiptError):
                self.check(session)

    def test_changed_study_or_runtime_contract_is_rejected(self):
        for field, value in (("image", "sha256:" + "b" * 64), ("timeout_seconds", 99),
                             ("adapter_sha256", "0" * 64), ("support_sha256", {})):
            session = self.populate(count=1)
            path = session.session.output / "session.json"
            summary = json.loads(path.read_text())
            summary["runtime_contract"][field] = value
            save(path, summary)
            with self.assertRaises(audit.StudyReceiptError):
                self.check(session)
        session = self.populate(count=1)
        altered = deepcopy(session.contract)
        altered["deterministic_visible"] = True
        save(session.session.output / "study-contract.json", altered)
        with self.assertRaises(audit.StudyReceiptError):
            self.check(session)

    def test_infrastructure_failure_never_qualifies_as_behavioral_failure(self):
        session = self.session(factory=FakeFactory(fail_first=True))
        with self.assertRaises(StudyValidationError):
            self.populate(session, count=1)
        with self.assertRaises(audit.StudyReceiptError):
            self.check(session)

    def test_observation_cannot_enable_unpreregistered_determinism(self):
        session = self.populate(count=1)
        row = session.summary()["results"][0]
        path = Path(row["artifact_path"]) / "input.json"
        request = json.loads(path.read_text())
        request["job"]["deterministic"] = True
        request["binding"]["deterministic"] = True
        save(path, request)
        def changed(result):
            result["binding"] = request["binding"]
            result["key_sha256"] = digest(result["binding"])
        self.mutate(session, 0, changed)
        with self.assertRaisesRegex(audit.StudyReceiptError, "preregistered"):
            self.check(session)

    def test_independent_final_cannot_claim_visible_origin_reuse(self):
        session = self.populate(purpose="final")
        first = session.summary()["results"][0]
        self.mutate(session, 1, lambda row: row.update(physical=False, execution_id=first["execution_id"],
            reuse=dict(kind="in_session", origin_session_id=session.session.session_id,
                       execution_id=first["execution_id"], artifact_path=first["artifact_path"])))
        with self.assertRaisesRegex(audit.StudyReceiptError, "ineligible"):
            self.check(session)

    def test_physical_execution_id_must_identify_one_observation(self):
        session = self.populate()
        execution_id = session.summary()["results"][0]["execution_id"]
        self.mutate(session, 1, lambda row: row.update(execution_id=execution_id))
        with self.assertRaisesRegex(audit.StudyReceiptError, "execution identity"):
            self.check(session)

    def test_symlink_and_outside_origin_paths_are_rejected(self):
        session = self.populate(self.session(deterministic=True))
        original = session.summary()["results"][0]
        path = Path(original["artifact_path"]) / "result.json"
        renamed = path.with_suffix(".real")
        path.rename(renamed)
        path.symlink_to(renamed)
        with self.assertRaisesRegex(audit.StudyReceiptError, "symlink"):
            self.check(session)
        path.unlink()
        renamed.rename(path)
        self.mutate(session, 1, lambda row: row["reuse"].update(artifact_path=str(self.root.parent)))
        with self.assertRaisesRegex(audit.StudyReceiptError, "escapes"):
            self.check(session)

    def test_duplicate_json_and_late_file_mutation_fail_closed(self):
        session = self.populate(count=1)
        path = session.session.output / "session.json"
        original = path.read_bytes()
        path.write_bytes(b'{"schema":1,"schema":2}')
        with self.assertRaises(audit.StudyReceiptError):
            self.check(session)
        path.write_bytes(original)
        verify = audit.verify_receipt
        def mutate(receipt, job, binding):
            result = verify(receipt, job, binding)
            path.write_bytes(original + b" ")
            return result
        with patch.object(audit, "verify_receipt", side_effect=mutate):
            with self.assertRaisesRegex(audit.StudyReceiptError, "changed during audit"):
                self.check(session)


if __name__ == "__main__":
    unittest.main()
