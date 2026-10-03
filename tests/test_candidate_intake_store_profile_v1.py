"""Pure mapper controls: no candidate imports, execution, Docker, or providers."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import hashlib
import unittest
from unittest.mock import patch

from gossip_harness import candidate_intake_store_profile_v1 as profile
from gossip_harness import candidate_intake_store_cases_v1 as cases
from gossip_harness import candidate_intake_store_observer_v1 as observer

INCARNATION = "123456789abc4def8123456789abcdef"


def public_case() -> dict:
    case = cases._Case("profile-control", "profile", ["M1-T08"], "control", "Private mapper control")
    case.snapshot("before")
    job = case.put_job("new", [{"source": "alpha.txt", "text": "a"}, {"source": "beta.txt", "text": "b"}], "completed")
    # These are prospective test data, not executed candidate operations.
    case.call("after", case.op("store", "commit_job", "new", 1), job["receipt"])
    return case.finish()


def observations(case: dict, schema: str) -> tuple:
    values = []
    generations = profile._generation_by_phase(case)
    for phase in profile.PHASES:
        auxiliary = profile.expected_auxiliary(case["expected"][phase], generations[phase], INCARNATION)
        files = tuple({"path": name, "bytes": 0, "sha256": hashlib.sha256(b"").hexdigest()}
                      for name in observer.V2_STORAGE_PATHS)
        values.append(observer.Observation(deepcopy(case["expected"][phase]), {}, files,
            schema, "a" * 64, auxiliary, observer.V2_SQLITE_LAYOUT))
    return tuple(values)


class CandidateIntakeStoreProfileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema_bytes, cls.schema = profile.authored_schema_profile()

    def test_pinned_literal_schema_has_fourteen_tables_four_fences(self):
        self.assertEqual(self.schema["purpose"], "authored-v2-b02-fixture-qualification-only")
        self.assertEqual(sum(sql.startswith("CREATE TABLE ") for sql in self.schema["statements"]), 14)
        self.assertEqual(sum(sql.startswith("CREATE TRIGGER ") for sql in self.schema["statements"]), 4)
        self.assertEqual(self.schema["schema_sha256"], observer.sqlite_schema_sha256(self.schema_bytes))
        self.assertEqual(self.schema["reviewed_sources"], profile.REVIEWED_SOURCES)

    def test_changed_reviewed_source_refuses_even_unchanged_ddl(self):
        sources = profile.v2_files()
        sources["library/catalog/m4_legacy.py"] += "\n# changed\n"
        with patch.object(profile, "v2_files", return_value=sources), self.assertRaises(observer.ObservationUnavailable):
            profile.authored_schema_profile()

    def test_schema_creation_never_imports_candidate_modules(self):
        import sys
        before = {key for key in sys.modules if key == "library" or key.startswith("library.")}
        with patch("builtins.exec", side_effect=AssertionError("unexpected Python execution")):
            profile.authored_schema_profile()
        self.assertEqual(before, {key for key in sys.modules if key == "library" or key.startswith("library.")})

    def test_exact_single_document_auxiliary_rows(self):
        doc = {"document_id": "doc-one", "blob_id": "blob-two"}
        expected = profile.expected_auxiliary({"documents": [doc], "jobs": [{"public": {"job_id": "job", "epoch": 7}}]}, 3, INCARNATION)
        rid = "rev-" + hashlib.sha256(b"revision\0doc-one\0" + b"1\0blob-two").hexdigest()
        self.assertEqual(expected, {
            "backups": [], "collections": [], "maintenance_artifacts": [], "search_entries": [],
            "control": [{"key": "cleanup_cursor", "value": "null"}, {"key": "incarnation", "value": INCARNATION},
                        {"key": "last_error", "value": "null"}, {"key": "worker_generation", "value": "0"}],
            "metadata": [{"key": "catalog_generation", "value": "3"}, {"key": "schema", "value": "4"}],
            "document_control": [{"document_id": "doc-one", "edit_high_water": 1}],
            "document_revisions": [{"revision_id": rid, "document_id": "doc-one", "revision": 1, "blob_id": "blob-two"}],
            "document_state": [{"document_id": "doc-one", "head_revision_id": rid, "edit_version": 1,
                                "deleted": 0, "notes": "", "tags": "[]", "collections": "[]"}],
            "job_control": [{"job_id": "job", "epoch_high_water": 7, "enrolled": 0}],
            "search_state": [{"singleton": 1, "published_generation": None, "target_generation": None,
                              "processed": 0, "total": 0, "cursor": None}],
        })

    def test_new_batch_advances_once_replay_and_reopen_do_not(self):
        case = public_case()
        receipt = case["expected"]["results"]["after"][0]
        case["recipe"]["phases"]["after"].append(deepcopy(case["recipe"]["phases"]["after"][0]))
        case["expected"]["results"]["after"].append(deepcopy(receipt))
        self.assertEqual(profile._generation_by_phase(case), {"before": 1, "after": 2, "reopened": 2})
        checks = profile.evaluate_auxiliary(case, *observations(case, self.schema["schema_sha256"]))
        self.assertTrue(all(checks.values()), checks)
        self.assertEqual(len(checks), 40)

    def test_corruption_of_every_auxiliary_table_in_each_phase_is_detected(self):
        case = public_case()
        values = observations(case, self.schema["schema_sha256"])
        for index, phase in enumerate(profile.PHASES):
            for table in profile.TABLES:
                with self.subTest(phase=phase, table=table):
                    changed = deepcopy(values)
                    changed[index].auxiliary_tables[table].append({"unknown": "hidden-write"})
                    checks = profile.evaluate_auxiliary(case, *changed)
                    self.assertFalse(checks[phase + ".auxiliary." + table])

    def test_missing_extra_table_and_unknown_schema_are_not_silently_ignored(self):
        case = public_case()
        values = observations(case, self.schema["schema_sha256"])
        for mutate in (lambda tables: tables.pop("backups"), lambda tables: tables.update(extra=[])):
            changed = deepcopy(values)
            mutate(changed[1].auxiliary_tables)
            self.assertFalse(profile.evaluate_auxiliary(case, *changed)["after.auxiliary-table-census"])
        for changed in (replace(values[1], schema_sha256="f" * 64), replace(values[1], layout=observer.SQLITE_LAYOUT),
                        replace(values[1], registration_sha256="f" * 64)):
            with self.assertRaises(observer.ObservationUnavailable):
                profile.evaluate_auxiliary(case, values[0], changed, values[2])

    def test_only_valid_initial_uuid_is_observed_and_it_must_be_conserved(self):
        case = public_case()
        values = observations(case, self.schema["schema_sha256"])
        for index in (0, 1, 2):
            changed = deepcopy(values)
            row = next(row for row in changed[index].auxiliary_tables["control"] if row["key"] == "incarnation")
            row["value"] = "wrong" if index == 0 else "abcdef1234564def8123456789abcdef"
            self.assertFalse(all(profile.evaluate_auxiliary(case, *changed).values()))

    def test_effective_write_does_not_allow_extra_control_keys(self):
        case = public_case()
        values = observations(case, self.schema["schema_sha256"])
        values[1].auxiliary_tables["control"].append({"key": "backup_root", "value": "unasked"})
        self.assertFalse(profile.evaluate_auxiliary(case, *values)["after.auxiliary.control"])

    def test_highwater_and_generation_are_derived_from_expected_not_candidate_after(self):
        case = public_case()
        values = observations(case, self.schema["schema_sha256"])
        values[1].auxiliary_tables["job_control"][0]["epoch_high_water"] = 999
        values[1].auxiliary_tables["metadata"][0]["value"] = "999"
        checks = profile.evaluate_auxiliary(case, *values)
        self.assertFalse(checks["after.auxiliary.job_control"])
        self.assertFalse(checks["after.auxiliary.metadata"])

    def test_sqlite_integer_bool_alias_does_not_pass_typed_comparison(self):
        case = public_case()
        values = observations(case, self.schema["schema_sha256"])
        values[1].auxiliary_tables["document_state"][0]["deleted"] = False
        self.assertFalse(profile.evaluate_auxiliary(case, *values)["after.auxiliary.document_state"])

    def test_ancillary_missing_extra_nonempty_or_changed_file_fails(self):
        case = public_case()
        values = observations(case, self.schema["schema_sha256"])
        for files in (values[1].files[:1], values[1].files + ({"path": "ignored.lock", "bytes": 0, "sha256": "a" * 64},),
                      (values[1].files[0], dict(values[1].files[1], bytes=1), values[1].files[2])):
            with self.subTest(files=files):
                checks = profile.evaluate_auxiliary(case, values[0], replace(values[1], files=files), values[2])
                self.assertFalse(checks["after.auxiliary-files"])

    def test_unreviewed_mutation_method_is_observation_unavailable(self):
        case = public_case()
        values = observations(case, self.schema["schema_sha256"])
        case["recipe"]["phases"]["after"][0]["method"] = "restore"
        with self.assertRaises(observer.ObservationUnavailable):
            profile.evaluate_auxiliary(case, *values)

    def test_snapshot_not_explained_by_prospective_operations_is_unavailable(self):
        case = public_case()
        case["recipe"]["phases"]["after"] = []
        case["expected"]["results"]["after"] = []
        with self.assertRaises(observer.ObservationUnavailable):
            profile._generation_by_phase(case)

    def test_all_prospective_store_histories_fit_profile_without_candidate_execution(self):
        for case in cases._store_cases() + cases._history_cases():
            with self.subTest(case=case["case_id"]):
                generations = profile._generation_by_phase(case)
                self.assertGreaterEqual(generations["before"], 1)
                self.assertGreaterEqual(generations["after"], generations["before"])
                self.assertEqual(generations["after"], generations["reopened"])

    def test_unavailable_dependency_masks_later_checks_preserving_before_failure(self):
        case = public_case()
        values = observations(case, self.schema["schema_sha256"])
        values[0].auxiliary_tables["metadata"][0]["value"] = "bad"
        values[1].auxiliary_tables["job_control"] = []
        checks = profile.evaluate_auxiliary(case, *values, unavailable_phases=("after", "reopened"))
        self.assertFalse(checks["before.auxiliary.metadata"])
        self.assertTrue(checks["before.auxiliary.control"])
        self.assertIsNone(checks["after.auxiliary.job_control"])
        self.assertTrue(all(value is None for key, value in checks.items() if not key.startswith("before.")))
        with self.assertRaises(observer.ObservationUnavailable):
            profile.evaluate_auxiliary(case, *values, unavailable_phases=("after",))
