"""Pure predicate controls; no invocation, capture or acceptance authority.

The host execution protocol
and candidate/context/merged-source qualification are separate obligations.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import unittest

from gossip_harness import cumulative_generated_probe_values_v2 as p

FIXTURE = Path(__file__).parent / "fixtures/generated-probe-values-v1/admission.json"


def proposal(template="refresh-identity-v1", text="old", replacement="new"):
    changed = template in {"refresh-identity-v1", "completed-receipt-replay-v1"}
    return {"template_id": template,
            "requirement_id": "M3-BACKUP-RESTORE" if template == "manifest-content-hash-v1" else "M2-REFRESH",
            "parameters": {"initial_text": text, "replacement_text": replacement} if changed else {"text": text},
            "expectation": {"kind": "exact_scalar", "value": "unchanged"} if template == "refresh-noop-v1"
                           else {"kind": "contract_relation", "value": True}}


def admitted(row):
    return p.admit(row, released_requirements=("M2-REFRESH", "M3-BACKUP-RESTORE"),
                   contract_sha256=p.PRODUCT_SHA256)


def document(text):
    source = "probe/document.txt"
    return {"document_id": "doc-" + hashlib.sha256(b"document\0" + source.encode()).hexdigest(),
            "source_id": "src-" + hashlib.sha256(b"source\0" + source.encode()).hexdigest(),
            "source": source, "title": "document.txt", "text": text,
            "blob_id": "blob-" + hashlib.sha256(text.encode()).hexdigest()}


def record(text, revision=1, edit_version=1):
    return {"document": document(text), "revision": revision, "edit_version": edit_version,
            "deleted": False, "notes": "", "tags": [], "collections": []}


def values_for(row):
    template, params = row["template_id"], row["parameters"]
    text = params.get("initial_text", params.get("text"))
    job = {"job_id": "probe-job", "epoch": 1, "state": "completed", "total": 1, "completed": 1, "error": None}
    submitted = {**job, "state": "queued", "completed": 0}
    if template == "manifest-content-hash-v1":
        return {"submitted": submitted, "captured_job": {
            "manifest": json.dumps([{"source": "probe/document.txt", "text": text}]),
            "content_hashes": json.dumps([hashlib.sha256(text.encode()).hexdigest()]), "receipt": None}}
    changed = "replacement_text" in params
    result = {"before": record(text), "refresh": {"status": "refreshed" if changed else "unchanged",
              "record": record(params.get("replacement_text", text), 2 if changed else 1, 2 if changed else 1)}}
    if template == "completed-receipt-replay-v1":
        receipt = {"job": job, "documents": [document(text)]}
        result.update(submitted=submitted, prepared={"job_id": "probe-job", "epoch": 1},
                      original_receipt=deepcopy(receipt), replay=deepcopy(receipt))
    else:
        result["imported"] = {"status": "imported", "document": document(text)}
    return result


class GeneratedProbePredicateV2Tests(unittest.TestCase):
    limits = {"review_bytes": 4096, "proposal_count": 4, "json_nodes": 128, "json_depth": 8}

    def assert_disposition(self, row, values, disposition):
        result = p.evaluate_values(admitted(row), values)
        self.assertEqual(result["disposition"], disposition)
        self.assertIs(result["acceptance_authority"], False)
        self.assertIs(result["observation_authority"], False)
        return result

    def test_authored_admission_vectors(self):
        vectors = json.loads(FIXTURE.read_bytes())
        for row in vectors["admission"]:
            with self.subTest(vector=row["id"]):
                if row["expect"] == "admitted":
                    self.assertIs(admitted(row["proposal"])["acceptance_authority"], False)
                else:
                    with self.assertRaisesRegex(ValueError, row["expect"]):
                        admitted(row["proposal"])

    def test_all_templates_accept_independent_complete_values_without_authority(self):
        for template in p.TEMPLATES:
            for text in ("", "plain", "é\n\x00", "🌍"):
                row = proposal(template, text=text, replacement=text + "!")
                with self.subTest(template=template, text=text):
                    self.assert_disposition(row, values_for(row), "pass")

    def test_parser_requires_exact_host_limits(self):
        raw = p.canonical({"protocol": p.PROTOCOL, "probes": []})
        for limits in ({}, {**self.limits, "extra": 1}, {**self.limits, "json_depth": True},
                       {**self.limits, "json_depth": 0}):
            with self.subTest(limits=limits), self.assertRaisesRegex(ValueError, "missing_frozen_parser_limits"):
                p.parse_batch(raw, limits=limits)

    def test_parser_rejects_duplicate_nested_members_nonfinite_and_bad_utf8(self):
        for raw in (b'{"protocol":"x","protocol":"y","probes":[]}',
                    b'{"protocol":"x","probes":[{"parameters":{"text":"a","text":"b"}}]}',
                    b'{"protocol":"x","probes":[NaN]}', b'{"protocol":"x","probes":[1e999]}', b'\xff'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                p.parse_batch(raw, limits=self.limits)

    def test_parser_enforces_bytes_count_depth_and_nodes(self):
        raw = p.canonical({"protocol": p.PROTOCOL, "probes": [proposal()]})
        self.assertEqual(p.parse_batch(raw, limits=self.limits), [proposal()])
        for limits in ({**self.limits, "review_bytes": len(raw)-1},
                       {**self.limits, "json_depth": 1}, {**self.limits, "json_nodes": 2}):
            with self.subTest(limits=limits), self.assertRaises(ValueError):
                p.parse_batch(raw, limits=limits)
        with self.assertRaises(ValueError):
            p.parse_batch(p.canonical({"protocol": p.PROTOCOL, "probes": [proposal()] * 5}), limits=self.limits)
        self.assertEqual(p.parse_batch(raw, limits={**self.limits, "review_bytes": len(raw)}), [proposal()])

    def test_product_contract_and_release_gate_cannot_be_substituted(self):
        row = proposal()
        for released, contract in [((), p.PRODUCT_SHA256), (("M2-REFRESH",), "0" * 64)]:
            with self.assertRaises(ValueError):
                p.admit(row, released_requirements=released, contract_sha256=contract)
        row["requirement_id"] = "M1-JOBS"
        with self.assertRaises(ValueError):
            admitted(row)

    def test_text_domain_uses_utf8_bytes_not_characters(self):
        self.assertIs(admitted(proposal("refresh-noop-v1", "é" * 16384))["acceptance_authority"], False)
        for text in ("é" * 16385, "a" * 32769, True, None, "\ud800"):
            with self.subTest(text_type=type(text).__name__), self.assertRaises(ValueError):
                admitted(proposal("refresh-noop-v1", text))

    def test_admitted_identity_is_stable_and_independent_of_caller_mutation(self):
        row = proposal(); first = admitted(row); second = admitted(deepcopy(row))
        self.assertEqual(first["probe_id"], second["probe_id"])
        row["parameters"]["initial_text"] = "changed"
        self.assertEqual(first["parameters"]["initial_text"], "old")
        self.assertNotEqual(first["probe_id"], admitted(row)["probe_id"])

    def test_no_arbitrary_code_selector_or_expectation_fields(self):
        for path, extra in [((), {"code": "pass"}), (("parameters",), {"selector": "*"}),
                            (("expectation",), {"code": "True"})]:
            row = proposal(); target = row
            for key in path: target = target[key]
            target.update(extra)
            with self.assertRaises(ValueError): admitted(row)

    def test_same_wrong_identity_and_stale_blob_are_rejected(self):
        row = proposal(); values = values_for(row)
        for key in ("document_id", "source_id"):
            bad = deepcopy(values)
            bad["before"]["document"][key] = "same-wrong"
            bad["refresh"]["record"]["document"][key] = "same-wrong"
            self.assert_disposition(row, bad, "fail")
        values["refresh"]["record"]["document"]["blob_id"] = document("old")["blob_id"]
        self.assert_disposition(row, values, "fail")

    def test_noop_requires_exact_entire_record(self):
        row = proposal("refresh-noop-v1"); values = values_for(row)
        for key, bad_value in [("edit_version", 2), ("revision", 2), ("deleted", True),
                               ("notes", "changed"), ("tags", ["changed"]), ("collections", ["changed"])]:
            bad = deepcopy(values); bad["refresh"]["record"][key] = bad_value
            self.assert_disposition(row, bad, "fail")

    def test_bool_integer_aliases_and_malformed_records_fail(self):
        row = proposal("refresh-noop-v1"); values = values_for(row)
        for target in ("before", "after"):
            for key in ("revision", "edit_version"):
                bad = deepcopy(values); record_value = bad["before"] if target == "before" else bad["refresh"]["record"]
                record_value[key] = True
                self.assert_disposition(row, bad, "fail")
        for field in ("tags", "collections"):
            bad = deepcopy(values);bad["refresh"]["record"][field] = [1]
            self.assert_disposition(row, bad, "fail")

    def test_replay_requires_historical_receipt_and_exact_completed_job(self):
        row = proposal("completed-receipt-replay-v1"); values = values_for(row)
        bad = deepcopy(values);bad["replay"]["documents"] = [document("new")]
        self.assert_disposition(row, bad, "fail")
        for field in ("epoch", "total", "error"):
            bad = deepcopy(values)
            for name in ("original_receipt", "replay"):del bad[name]["job"][field]
            self.assert_disposition(row, bad, "fail")
        bad = deepcopy(values);bad["original_receipt"]["job"]["epoch"] = True
        self.assert_disposition(row, bad, "fail")

    def test_captured_hash_comes_from_parameters_and_manifest_is_exact(self):
        row = proposal("manifest-content-hash-v1"); values = values_for(row)
        correct = hashlib.sha256(b"old").hexdigest()
        for hashes in (["blob-" + correct], [correct.upper()], None, [], [correct, correct], ["0" * 64]):
            bad = deepcopy(values);bad["captured_job"]["content_hashes"] = json.dumps(hashes)
            self.assert_disposition(row, bad, "fail")
        bad = deepcopy(values);bad["captured_job"]["manifest"] = '[{"source":"other","text":"old"}]'
        self.assert_disposition(row, bad, "fail")
        bad = deepcopy(values);bad["captured_job"]["manifest"] = '[{"source":"probe/document.txt","source":"probe/document.txt","text":"old"}]'
        self.assert_disposition(row, bad, "fail")

    def test_missing_value_is_unavailable_but_known_prerequisite_failure_survives(self):
        for template in p.TEMPLATES:
            row = proposal(template); values = values_for(row)
            for name in list(values):
                missing = deepcopy(values);del missing[name]
                self.assert_disposition(row, missing, "unavailable")
            name = "submitted" if template in {"completed-receipt-replay-v1", "manifest-content-hash-v1"} else "imported"
            result = self.assert_disposition(row, {name: {"forged": True}}, "fail")
            self.assertEqual(result["reason"], "prerequisite_value_contradicted:" + name)

    def test_child_capture_marker_or_pass_receipt_is_never_evidence(self):
        self.assert_disposition(proposal("refresh-noop-v1"), {"passed": True}, "unavailable")
        self.assert_disposition(proposal("manifest-content-hash-v1"),
            {"capture_job": {"job_id": "probe-job", "database": "/tmp/cumulative-probe/library.sqlite"}}, "unavailable")

    def test_known_refresh_failure_survives_missing_replay(self):
        row = proposal("completed-receipt-replay-v1"); vals = values_for(row)
        del vals["replay"]
        vals["refresh"]["record"]["document"]["text"] = "wrong"
        self.assert_disposition(row, vals, "fail")

    def test_known_replay_failure_survives_missing_refresh(self):
        row = proposal("completed-receipt-replay-v1"); vals = values_for(row)
        self.assert_disposition(row, {"replay": {"forged": True}}, "fail")
        self.assert_disposition(row, {"replay": vals["replay"]}, "unavailable")

    def test_bad_capture_survives_missing_submit_without_manufactured_values(self):
        row = proposal("manifest-content-hash-v1"); vals = values_for(row)
        self.assert_disposition(row, {"captured_job": {}}, "fail")
        self.assert_disposition(row, {"captured_job": vals["captured_job"]}, "unavailable")


if __name__ == "__main__":
    unittest.main()
