"""Portable controls for independent fixture derivation; no candidate execution."""
from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
import hashlib
import io
import json
import struct
import unittest
import zipfile

from gossip_harness import candidate_http_fixtures_v1 as f


class HttpFixtureDocumentTests(unittest.TestCase):
    def test_literal_document_identity_and_exact_text(self) -> None:
        self.assertEqual(f.document("Alpha.txt", "Straße [a.b]\n").as_json(), {
            "document_id": "doc-4e5111992f051db7335c4efe6f07ec35396cb58dc3b8ae76d6120adfe001ced9",
            "source_id": "src-38d3f6f3f16032a0f633c351b2cd093dea18cf51437e76934778ec4b99b2474b",
            "source": "Alpha.txt", "blob_id": "blob-5ca4f749bfa7d76933ed142a1eb3b10afc52f0f4869e4c3c3a5993128e582eb2",
            "title": "Alpha.txt", "text": "Straße [a.b]\n"})
        self.assertEqual(f.document("nested/é.md", "CAFÉ\r\n").as_json(), {
            "document_id": "doc-6558ed3404d057ccf599001d527438de0eee7be3b4883c327d292d9b1d37c106",
            "source_id": "src-96c8c686a2b9bf4222fe4fe417b6084afa6ab010afbe679969ce21ec6b6d134a",
            "source": "nested/é.md", "blob_id": "blob-fbfd55fd917263de20438bab26230265f13e118ba3c6ea938202c69d8a403b0d",
            "title": "é.md", "text": "CAFÉ\r\n"})

    def test_equal_bytes_share_blob_but_not_source_identity(self) -> None:
        a, b, _, empty = f.corpus_state().documents
        self.assertNotEqual(a.document_id, b.document_id)
        self.assertNotEqual(a.source_id, b.source_id)
        self.assertEqual(a.blob_id, b.blob_id)
        self.assertEqual(empty.blob_id, "blob-e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        self.assertNotEqual(f.document("a.txt", "x\n").blob_id, f.document("a.txt", "x\r\n").blob_id)

    def test_literal_casefold_search_and_total_before_pagination(self) -> None:
        state = f.corpus_state()
        for query in ("STRASSE", "[a.b]"):
            result = state.listing(query=query, offset=1, limit=1)
            self.assertEqual(result["total"], 2)
            self.assertEqual([d["source"] for d in result["documents"]], ["alpha.md"])
        self.assertEqual(state.listing(query="a.b", offset=2)["documents"], [])
        self.assertEqual(state.listing(query="a.*b")["total"], 0)
        self.assertEqual(state.listing(query="café")["total"], 1)
        self.assertEqual(state.listing(query="é.md\ncafé")["total"], 1)

    def test_export_selection_is_source_ordered_and_fresh(self) -> None:
        state = f.corpus_state()
        result = state.export((state.documents[-1].document_id, state.documents[0].document_id))
        self.assertEqual([d["source"] for d in result["documents"]], ["Alpha.txt", "z.txt"])
        result["documents"][0]["text"] = "changed"
        self.assertEqual(state.documents[0].text, "Straße [a.b]\n")
        self.assertEqual(state.export(())["documents"], [])
        with self.assertRaises(f.FixtureError):
            state.export((state.documents[0].document_id,) * 2)
        with self.assertRaises(f.FixtureError):
            state.export(("missing",))

    def test_query_fixture_bounds_do_not_coerce_bools(self) -> None:
        state = f.corpus_state()
        for kwargs in ({"offset": True}, {"limit": False}, {"limit": 101}, {"offset": -1}, {"query": "x" * 257}):
            with self.subTest(kwargs=kwargs), self.assertRaises(f.FixtureError):
                state.listing(**kwargs)
        self.assertEqual(state.listing(query="x" * 256, limit=100)["total"], 0)

    def test_census_coverage_uses_expected_document_count(self) -> None:
        for count, offsets in ((0, (0,)), (100, (0,)), (101, (0, 100)), (200, (0, 100)),
                               (201, (0, 100, 200)), (256, (0, 100, 200))):
            state = f.ExpectedState(tuple(f.document(f"{i:03d}.txt", "x") for i in range(count)))
            self.assertEqual(state.census_offsets(), offsets)
            self.assertEqual(sum(len(state.listing(offset=o)["documents"]) for o in offsets), count)

    def test_immutable_values_and_no_execution_authority(self) -> None:
        value = f.expected_state("completed")
        with self.assertRaises(FrozenInstanceError):
            value.documents = ()
        with self.assertRaises(f.FixtureError):
            f.ExpectedState(list(value.documents), value.jobs)
        with self.assertRaises(f.FixtureError):
            f.Receipt(value.get("subject").job, list(value.documents))
        with self.assertRaises(f.FixtureError):
            f.ExpectedState((), value.jobs)
        with self.assertRaises(f.FixtureError):
            f.ExpectedState(tuple(replace(d, text="changed") for d in value.documents), value.jobs)
        with self.assertRaises(f.FixtureError):
            f.Transition(value, error="job_state", value=value.get("subject").job)
        with self.assertRaises(f.FixtureError):
            f.KeyBoundary("mutable", [], "x.txt")
        self.assertFalse(f.EXECUTION_AUTHORITY)
        self.assertFalse(f.ACCEPTANCE_AUTHORITY)
        projection = value.jobs_json()
        projection["jobs"].clear()
        self.assertEqual(len(value.jobs), 2)


class HttpFixtureStateTests(unittest.TestCase):
    def test_base_states_and_guard_are_literal(self) -> None:
        for state, epoch, completed, error in (("queued", 1, 0, None), ("running", 1, 0, None),
                                              ("completed", 1, 2, None), ("cancelled", 2, 0, None),
                                              ("failed", 1, 0, "invalid_source")):
            with self.subTest(state=state):
                value = f.expected_state(state)
                self.assertEqual(value.get("subject").job.as_json(), {
                    "job_id": "subject", "epoch": epoch, "state": state, "total": 2,
                    "completed": completed, "error": error})
                self.assertEqual(value.get("sentinel").job.as_json(), {
                    "job_id": "sentinel", "epoch": 1, "state": "queued", "total": 0, "completed": 0, "error": None})
                self.assertEqual([d.source for d in value.documents],
                                 ["a.txt", "b.txt", "guard.txt"] if state == "completed" else ["guard.txt"])

    def test_all_twenty_state_action_cells(self) -> None:
        # Independently enumerated expected state/epoch/error, not generated by the transition routine.
        rows = (
            ("queued", "prepare", "running", 1, None), ("queued", "commit", "queued", 1, "job_state"),
            ("queued", "cancel", "cancelled", 2, None), ("queued", "retry", "queued", 1, "job_state"),
            ("running", "prepare", "running", 1, None), ("running", "commit", "completed", 1, None),
            ("running", "cancel", "cancelled", 2, None), ("running", "retry", "running", 1, "job_state"),
            ("completed", "prepare", "completed", 1, "job_state"), ("completed", "commit", "completed", 1, None),
            ("completed", "cancel", "completed", 1, "job_state"), ("completed", "retry", "completed", 1, "job_state"),
            ("cancelled", "prepare", "cancelled", 2, "job_state"), ("cancelled", "commit", "cancelled", 2, "job_state"),
            ("cancelled", "cancel", "cancelled", 2, None), ("cancelled", "retry", "queued", 3, None),
            ("failed", "prepare", "failed", 1, "job_state"), ("failed", "commit", "failed", 1, "job_state"),
            ("failed", "cancel", "failed", 1, "job_state"), ("failed", "retry", "queued", 2, None))
        completed_job = {"job_id": "subject", "epoch": 1, "state": "completed", "total": 2,
                         "completed": 2, "error": None}
        cancelled_job = {"job_id": "subject", "epoch": 2, "state": "cancelled", "total": 2,
                         "completed": 0, "error": None}
        receipt = {"job": completed_job, "documents": [
            {"document_id": "doc-e33e4f494b80b682de63a64fba092041d0cf073eb7da7bb185c6fb643346fbbb",
             "source_id": "src-82167beb034cb2ddfec8f52c2ad43a5667087fdfb7264373f61e666f930d36d6",
             "source": "a.txt", "blob_id": "blob-ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
             "title": "a.txt", "text": "a"},
            {"document_id": "doc-836a48ac21958447288e5b0be5ab074d8e046c068d0208e7fa513805d8cf507a",
             "source_id": "src-6e59573326073681a2ae3943e6e2b312d8dfc7dfa995631b451775312e3cfb58",
             "source": "b.txt", "blob_id": "blob-3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d",
             "title": "b.txt", "text": "b"}]}
        successes = {
            ("queued", "prepare"): (f.Token, {"job_id": "subject", "epoch": 1}, False),
            ("queued", "cancel"): (f.Job, cancelled_job, False),
            ("running", "prepare"): (f.Token, {"job_id": "subject", "epoch": 1}, True),
            ("running", "commit"): (f.Receipt, receipt, False),
            ("running", "cancel"): (f.Job, cancelled_job, False),
            ("completed", "commit"): (f.Receipt, receipt, True),
            ("cancelled", "cancel"): (f.Job, cancelled_job, True),
            ("cancelled", "retry"): (f.Job, {"job_id": "subject", "epoch": 3, "state": "queued",
                                            "total": 2, "completed": 0, "error": None}, False),
            ("failed", "retry"): (f.Job, {"job_id": "subject", "epoch": 2, "state": "queued",
                                         "total": 2, "completed": 0, "error": None}, False),
        }
        self.assertEqual(len(rows), 20)
        self.assertEqual({(before, action) for before, action, _, _, error in rows if error is None}, set(successes))
        for before, action, after, epoch, error in rows:
            with self.subTest(before=before, action=action):
                state = f.expected_state(before)
                result = f.apply_action(state, action, "subject", **({"epoch": state.get("subject").job.epoch}
                                                                    if action == "commit" else {}))
                self.assertEqual(result.error, error)
                subject = result.state.get("subject").job
                self.assertEqual((subject.state, subject.epoch), (after, epoch))
                self.assertEqual(subject.completed, 2 if after == "completed" else 0)
                self.assertEqual(subject.error, "invalid_source" if after == "failed" else None)
                self.assertEqual(result.state.get("sentinel"), state.get("sentinel"))
                self.assertEqual([d.source for d in result.state.documents],
                                 ["a.txt", "b.txt", "guard.txt"] if after == "completed" else ["guard.txt"])
                if error is not None:
                    self.assertEqual(result.state, state)
                    self.assertEqual(result.http_status, 409)
                    self.assertIsNone(result.value)
                    self.assertIs(result.replay, False)
                else:
                    kind, value, replay = successes[(before, action)]
                    self.assertEqual(result.http_status, 200)
                    self.assertIs(type(result.value), kind)
                    self.assertEqual(result.value.as_json(), value)
                    self.assertIs(result.replay, replay)

    def test_wrong_epoch_precedes_every_state(self) -> None:
        for name in f.STATES:
            state = f.expected_state(name)
            result = f.apply_action(state, "commit", "subject", epoch=state.get("subject").job.epoch + 1)
            self.assertEqual((result.error, result.http_status, result.state), ("stale_epoch", 409, state))
        for action in f.ACTIONS:
            result = f.apply_action(f.guarded_state(), action, "missing", **({"epoch": 1} if action == "commit" else {}))
            self.assertEqual((result.error, result.http_status), ("not_found", 404))

    def test_manifest_replay_and_conflict_in_all_states(self) -> None:
        for name in f.STATES:
            state = f.expected_state(name)
            manifest = state.get("subject").manifest
            same = f.submit(state, "subject", tuple(reversed(manifest)))
            self.assertEqual(same.state, state)
            self.assertEqual(same.value, state.get("subject").job)
            self.assertTrue(same.replay)
            changed = manifest[:-1] + (replace(manifest[-1], text="changed"),)
            conflict = f.submit(state, "subject", changed)
            self.assertEqual((conflict.error, conflict.state), ("job_conflict", state))

    def test_failed_retry_retains_original_bad_manifest_and_fails_again(self) -> None:
        failed = f.expected_state("failed")
        queued = f.apply_action(failed, "retry", "subject").state
        self.assertEqual(queued.get("subject").manifest, f.FAILED_MANIFEST)
        self.assertEqual(queued.get("subject").job.as_json(), {
            "job_id": "subject", "epoch": 2, "state": "queued", "total": 2, "completed": 0, "error": None})
        result = f.apply_action(queued, "prepare", "subject")
        self.assertEqual(result.error, "invalid_source")
        self.assertEqual(result.state.get("subject").job.epoch, 2)
        self.assertEqual(result.state.get("subject").manifest, f.FAILED_MANIFEST)
        self.assertEqual(result.state.documents, failed.documents)

    def test_old_token_fenced_after_cancel_retry_prepare_and_commit(self) -> None:
        state = f.expected_state("running")
        for action, epoch in (("cancel", None), ("retry", None), ("prepare", None), ("commit", 3)):
            state = f.apply_action(state, action, "subject", epoch=epoch).state
            result = f.apply_action(state, "commit", "subject", epoch=1)
            self.assertEqual((result.error, result.state), ("stale_epoch", state))
        self.assertEqual(state.get("subject").job.state, "completed")

    def test_intervening_import_not_revalidated_by_running_prepare_but_by_commit(self) -> None:
        manifest = (f.Entry("item.txt", "job text"), f.Entry("z.txt", "new"))
        state = f.submit(f.guarded_state(), "subject", manifest).state
        state = f.apply_action(state, "prepare", "subject").state
        for action in ("cancel", "retry", "prepare"):
            state = f.apply_action(state, action, "subject").state
        state = f.import_document(state, "item.txt", "file text").state
        replay = f.apply_action(state, "prepare", "subject")
        self.assertTrue(replay.replay)
        self.assertEqual(replay.value.as_json(), {"job_id": "subject", "epoch": 3})
        failure = f.apply_action(state, "commit", "subject", epoch=3)
        self.assertEqual((failure.error, failure.http_status), ("source_changed", 409))
        self.assertEqual(failure.state.documents, state.documents)
        self.assertEqual([d.source for d in failure.state.documents], ["guard.txt", "item.txt"])
        self.assertEqual(failure.state.get("subject").job.as_json(), {
            "job_id": "subject", "epoch": 3, "state": "failed", "total": 2, "completed": 0, "error": "source_changed"})
        self.assertEqual(failure.state.get("subject").manifest, manifest)
        self.assertEqual(f.apply_action(failure.state, "commit", "subject", epoch=1).error, "stale_epoch")
        self.assertEqual(f.apply_action(failure.state, "commit", "subject", epoch=3).error, "job_state")

    def test_receipt_replay_excludes_unrelated_catalog_documents(self) -> None:
        state = f.expected_state("running")
        state = f.import_document(state, "a.txt", "a").state
        first = f.apply_action(state, "commit", "subject", epoch=1)
        extended = f.import_document(first.state, "later.txt", "later").state
        replay = f.apply_action(f.reopen(extended), "commit", "subject", epoch=1)
        self.assertEqual(first.value, replay.value)
        self.assertEqual([d["source"] for d in replay.value.as_json()["documents"]], ["a.txt", "b.txt"])
        self.assertEqual([d.source for d in extended.documents], ["a.txt", "b.txt", "guard.txt", "later.txt"])

    def test_empty_batch_and_jobs_sorting(self) -> None:
        state = f.ExpectedState()
        for name in ("b", "aa", "a"):
            state = f.submit(state, name, ()).state
        self.assertEqual([j["job_id"] for j in state.jobs_json()["jobs"]], ["a", "aa", "b"])
        state = f.apply_action(state, "prepare", "a").state
        result = f.apply_action(state, "commit", "a", epoch=1)
        self.assertEqual(result.value.as_json()["documents"], [])
        self.assertEqual(result.state.get("a").job.state, "completed")
        self.assertEqual(result.state.get("a").job.completed, 0)

    def test_import_identity_replay_and_changed_bytes(self) -> None:
        state = f.import_document(f.ExpectedState(), "same.txt", "first").state
        self.assertTrue(f.import_document(state, "same.txt", "first").replay)
        changed = f.import_document(state, "same.txt", "second")
        self.assertEqual((changed.error, changed.state), ("source_changed", state))
        full = f.ExpectedState(tuple(f.document(f"{i:03d}.txt", "x") for i in range(256)))
        self.assertTrue(f.import_document(full, "000.txt", "x").replay)
        with self.assertRaises(f.UnspecifiedFixture):
            f.import_document(full, "new.txt", "x")

    def test_deferred_invalid_batch_and_unsupported_type(self) -> None:
        for manifest, code in (((f.Entry("a.exe", "x"),), "unsupported_type"),
                               ((f.Entry("a.txt", "x"), f.Entry("a.txt", "x")), "invalid_batch"),
                               (f.member_count_manifest(65), "invalid_batch"),
                               ((f.Entry("a.txt", "é" * 16385),), "too_large")):
            queued = f.submit(f.guarded_state(), "subject", manifest).state
            self.assertEqual(queued.get("subject").job.state, "queued")
            failed = f.apply_action(queued, "prepare", "subject")
            self.assertEqual(failed.error, code)
            self.assertEqual(failed.state.documents, queued.documents)

    def test_aggregate_member_byte_boundary_is_independent_of_per_member_limit(self) -> None:
        # Identical count and individually legal members isolate aggregate bytes.
        large = tuple(f.Entry(f"m{i:02d}.txt", "é" * 16384) for i in range(16))
        for tail, total_bytes in (("", 524288), ("x", 524289)):
            with self.subTest(total_bytes=total_bytes):
                manifest = large + (f.Entry("tail.txt", tail),)
                self.assertEqual(len(manifest), 17)
                self.assertTrue(all(len(e.text.encode("utf-8")) <= 32768 for e in manifest))
                self.assertEqual(sum(len(e.text.encode("utf-8")) for e in manifest), total_bytes)
                before = f.guarded_state()
                admitted = f.submit(before, "aggregate", manifest)
                self.assertEqual((admitted.http_status, admitted.error, admitted.replay), (200, None, False))
                self.assertEqual(admitted.value.as_json(), {"job_id": "aggregate", "epoch": 1,
                    "state": "queued", "total": 17, "completed": 0, "error": None})
                result = f.apply_action(admitted.state, "prepare", "aggregate")
                self.assertEqual(result.state.documents, before.documents)
                self.assertEqual(result.state.get("sentinel"), before.get("sentinel"))
                if total_bytes == 524289:
                    self.assertEqual((result.http_status, result.error, result.value, result.replay),
                                     (400, "too_large", None, False))
                    self.assertEqual(result.state.get("aggregate").job.as_json(), {
                        "job_id": "aggregate", "epoch": 1, "state": "failed", "total": 17,
                        "completed": 0, "error": "too_large"})
                    self.assertEqual(result.state.get("aggregate").manifest, manifest)
                else:
                    self.assertEqual((result.http_status, result.error, result.replay), (200, None, False))
                    self.assertIs(type(result.value), f.Token)
                    self.assertEqual(result.value.as_json(), {"job_id": "aggregate", "epoch": 1})
                    committed = f.apply_action(result.state, "commit", "aggregate", epoch=1)
                    self.assertEqual((committed.http_status, committed.error, committed.replay), (200, None, False))
                    self.assertIs(type(committed.value), f.Receipt)
                    self.assertEqual(committed.value.job.as_json(), {"job_id": "aggregate", "epoch": 1,
                        "state": "completed", "total": 17, "completed": 17, "error": None})
                    self.assertEqual([(d.source, d.text) for d in committed.value.documents],
                                     [(e.source, e.text) for e in manifest])
                    self.assertEqual(len(committed.state.documents), 18)
                    self.assertEqual(sum(len(d.text.encode("utf-8")) for d in committed.value.documents), 524288)
                    self.assertEqual(committed.state.get("sentinel"), before.get("sentinel"))

    def test_m1_catalog_capacity_counts_union_and_rechecks_before_commit(self) -> None:
        base_documents = tuple(f.document(f"{i:03d}.txt", "x") for i in range(256))
        # Existing identical sources consume no additional capacity but remain in the receipt.
        for initial_count, new_sources, expected_count in ((255, ("255.txt",), 256),
                                                          (255, ("255.txt", "256.txt"), 257),
                                                          (256, (), 256)):
            with self.subTest(initial_count=initial_count, new_sources=new_sources):
                before = f.submit(f.ExpectedState(base_documents[:initial_count]), "sentinel", ()).state
                manifest = (f.Entry("000.txt", "x"),) + tuple(f.Entry(source, "x") for source in new_sources)
                self.assertEqual(len({d.source for d in before.documents} | {e.source for e in manifest}), expected_count)
                admitted = f.submit(before, "capacity", manifest)
                self.assertEqual((admitted.http_status, admitted.error, admitted.replay), (200, None, False))
                result = f.apply_action(admitted.state, "prepare", "capacity")
                self.assertEqual(result.state.documents, before.documents)
                self.assertEqual(result.state.get("sentinel"), before.get("sentinel"))
                if expected_count == 257:
                    self.assertEqual((result.http_status, result.error, result.value, result.replay),
                                     (400, "capacity", None, False))
                    self.assertEqual(result.state.get("capacity").job.as_json(), {
                        "job_id": "capacity", "epoch": 1, "state": "failed", "total": 3,
                        "completed": 0, "error": "capacity"})
                    self.assertEqual(result.state.get("capacity").manifest, manifest)
                else:
                    self.assertEqual((result.http_status, result.error, result.replay), (200, None, False))
                    self.assertIs(type(result.value), f.Token)
                    self.assertEqual(result.value.as_json(), {"job_id": "capacity", "epoch": 1})
                    committed = f.apply_action(result.state, "commit", "capacity", epoch=1)
                    self.assertEqual((committed.http_status, committed.error, committed.replay), (200, None, False))
                    self.assertIs(type(committed.value), f.Receipt)
                    self.assertEqual(committed.state.documents, base_documents)
                    self.assertEqual(committed.value.job.as_json(), {"job_id": "capacity", "epoch": 1,
                        "state": "completed", "total": len(manifest), "completed": len(manifest), "error": None})
                    self.assertEqual([(d.source, d.text) for d in committed.value.documents],
                                     [(e.source, e.text) for e in manifest])
                    self.assertEqual(committed.state.get("sentinel"), before.get("sentinel"))
        # A full64-member batch remains within intake bounds while the catalog union crosses its bound.
        new_batch = tuple(f.Entry(f"n{i:03d}.txt", "x") for i in range(64))
        for initial_count, manifest, expected_count in (
                (192, new_batch, 256), (193, new_batch, 257),
                (193, (f.Entry("000.txt", "x"),) + new_batch[:63], 256)):
            with self.subTest(full_batch_initial=initial_count, first_source=manifest[0].source):
                self.assertEqual(len(manifest), 64)
                before = f.submit(f.ExpectedState(base_documents[:initial_count]), "sentinel", ()).state
                self.assertEqual(len({d.source for d in before.documents} | {e.source for e in manifest}), expected_count)
                admitted = f.submit(before, "full_batch", manifest)
                self.assertEqual((admitted.http_status, admitted.error), (200, None))
                result = f.apply_action(admitted.state, "prepare", "full_batch")
                self.assertEqual(result.state.documents, before.documents)
                self.assertEqual(result.state.get("sentinel"), before.get("sentinel"))
                if expected_count == 257:
                    self.assertEqual((result.http_status, result.error, result.value, result.replay),
                                     (400, "capacity", None, False))
                    self.assertEqual(result.state.get("full_batch").job.as_json(), {
                        "job_id": "full_batch", "epoch": 1, "state": "failed", "total": 64,
                        "completed": 0, "error": "capacity"})
                    self.assertEqual(result.state.get("full_batch").manifest, manifest)
                else:
                    self.assertEqual((result.http_status, result.error, result.replay), (200, None, False))
                    self.assertIs(type(result.value), f.Token)
                    self.assertEqual(result.value.as_json(), {"job_id": "full_batch", "epoch": 1})
                    committed = f.apply_action(result.state, "commit", "full_batch", epoch=1)
                    self.assertEqual((committed.http_status, committed.error, committed.replay), (200, None, False))
                    self.assertIs(type(committed.value), f.Receipt)
                    self.assertEqual(committed.value.job.as_json(), {"job_id": "full_batch", "epoch": 1,
                        "state": "completed", "total": 64, "completed": 64, "error": None})
                    self.assertEqual([(d.source, d.text) for d in committed.value.documents],
                                     [(e.source, e.text) for e in manifest])
                    expected_catalog = {(d.source, d.text) for d in before.documents} | {(e.source, e.text) for e in manifest}
                    self.assertEqual([(d.source, d.text) for d in committed.state.documents], sorted(expected_catalog))
                    self.assertEqual(len(committed.state.documents), 256)
                    self.assertEqual(committed.state.get("sentinel"), before.get("sentinel"))
        # A direct valid import after prepare consumes the last available catalog slot.
        before = f.submit(f.ExpectedState(base_documents[:254]), "sentinel", ()).state
        manifest = (f.Entry("000.txt", "x"), f.Entry("254.txt", "x"), f.Entry("255.txt", "x"))
        queued = f.submit(before, "capacity", manifest).state
        prepared = f.apply_action(queued, "prepare", "capacity")
        self.assertEqual((prepared.http_status, prepared.error), (200, None))
        self.assertEqual(prepared.value.as_json(), {"job_id": "capacity", "epoch": 1})
        imported = f.import_document(prepared.state, "256.txt", "outside batch")
        self.assertEqual((imported.http_status, imported.error), (200, None))
        self.assertEqual(len(imported.state.documents), 255)
        failed = f.apply_action(imported.state, "commit", "capacity", epoch=1)
        self.assertEqual((failed.http_status, failed.error, failed.value, failed.replay), (400, "capacity", None, False))
        self.assertEqual(failed.state.documents, imported.state.documents)
        self.assertNotIn("254.txt", {d.source for d in failed.state.documents})
        self.assertNotIn("255.txt", {d.source for d in failed.state.documents})
        self.assertEqual(failed.state.get("capacity").job.as_json(), {"job_id": "capacity", "epoch": 1,
            "state": "failed", "total": 3, "completed": 0, "error": "capacity"})
        self.assertEqual(failed.state.get("capacity").manifest, manifest)
        self.assertEqual(failed.state.get("sentinel"), before.get("sentinel"))

    def test_unknown_and_competing_error_codes_are_authoring_errors(self) -> None:
        for manifest in ((f.Entry(f.KEY_BOUNDARIES[1].source, "x"),), (f.Entry("/a.txt", "x"),),
                         (f.Entry("../bad.exe", "x"),)):
            queued = f.submit(f.ExpectedState(), "subject", manifest).state
            with self.assertRaises(f.UnspecifiedFixture):
                f.apply_action(queued, "prepare", "subject")
        with self.assertRaises(f.FixtureError):
            f.apply_action(f.expected_state("running"), "commit", "subject", epoch=True)
        # This is not a source claim that the HTTP service must reject bool/float epochs.
        with self.assertRaises(f.FixtureError):
            f.Job("x", 1, "running", 2, 1, None)


class HttpFixtureBytesTests(unittest.TestCase):
    def test_zip_is_literal_stored_utf8_and_has_fixed_metadata(self) -> None:
        raw = f.canonical_zip()
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            self.assertEqual(archive.namelist(), ["z.txt", "nested/a.md", "a.html"])
            self.assertEqual([archive.read(name) for name in archive.namelist()], [b"", "café\r\n".encode(), b"<b>literal</b>\n"])
            for member in archive.infolist():
                self.assertEqual(member.date_time, (1980, 1, 1, 0, 0, 0))
                self.assertEqual(member.create_system, 3)
                self.assertEqual(member.create_version, 20)
                self.assertEqual(member.extract_version, 20)
                self.assertEqual(member.compress_type, zipfile.ZIP_STORED)
                self.assertEqual(member.flag_bits, 0x800)
                self.assertEqual(member.external_attr, 0o100644 << 16)
                self.assertEqual((member.extra, member.comment), (b"", b""))
                self.assertEqual(member.file_size, member.compress_size)
            self.assertEqual(archive.comment, b"")
        self.assertEqual(f.stored_zip(()), bytes.fromhex("504b0506000000000000000000000000000000000000"))

    def test_zip_preserves_duplicate_names_and_explicit_modes(self) -> None:
        for name, modes in (("duplicate", (0o100644, 0o100644)), ("symlink", (0o120777,)), ("fifo", (0o010644,))):
            raw = next(item.data for item in f.intake_files() if item.path == name + ".zip")
            with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                self.assertEqual(tuple(info.external_attr >> 16 for info in archive.infolist()), modes)
                self.assertEqual(archive.namelist(), ["a.txt"] * len(modes))
        raw = f.stored_zip((f.ZipMember("n/", b"", 0o040755), f.ZipMember("n/é.md", b"x")))
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            self.assertTrue(archive.infolist()[0].is_dir())
            self.assertEqual(archive.infolist()[0].external_attr, (0o040755 << 16) | 0x10)
            self.assertEqual(archive.read("n/é.md"), b"x")

    def test_zip_record_offsets_are_consistent_without_extra_trailing_bytes(self) -> None:
        raw = f.canonical_zip()
        end = struct.unpack("<IHHHHIIH", raw[-22:])
        self.assertEqual(end[:5], (0x06054b50, 0, 0, 3, 3))
        size, offset = end[5:7]
        self.assertEqual(offset + size + 22, len(raw))
        self.assertEqual(raw[offset:offset + 4], b"PK\x01\x02")
        self.assertEqual(end[-1], 0)

    def test_literal_request_boundary_hashes_and_utf8_size(self) -> None:
        for length, digest in ((65536, "0eb37848fedad2a718f470eef4acdf95ab05e677208a5c7bbe826cd7dd219560"),
                               (65537, "ba1bb39465a8c8dd744116805238e3e4e3f1c9bfba1ed6101b62364110d78ca9")):
            raw = f.request_body_boundary(length)
            self.assertEqual(len(raw), length)
            self.assertEqual(hashlib.sha256(raw).hexdigest(), digest)
            self.assertEqual(len(json.loads(raw)["entries"][0]["text"].encode()), 8192)
            self.assertLess(len(raw.decode()), length)
        self.assertEqual(f.request_body_boundary(65537), f.request_body_boundary(65536) + b" ")

    def test_path_boundary_isolates_bytes_and_segments(self) -> None:
        exact, over, sixteen, seventeen = f.KEY_BOUNDARIES
        self.assertEqual((exact.source_bytes, over.source_bytes), (256, 257))
        self.assertEqual((exact.segment_bytes, over.segment_bytes), ((127, 128), (128, 128)))
        self.assertEqual((len(sixteen.segment_bytes), len(seventeen.segment_bytes)), (16, 17))
        self.assertLess(exact.source_bytes - len(exact.source), 128)
        self.assertEqual([len(source.split("/")) for name, source in f.DIRECT_SOURCE_BOUNDARIES if name.startswith("segments")], [16, 17])
        self.assertEqual([name for name, _ in f.INVALID_NAMESPACES],
                         ["empty", "absolute", "backslash", "nul", "empty-segment", "dot", "parent", "trailing", "segment129"])

    def test_count_fixtures_cross_four_intake_representations(self) -> None:
        files = {item.path: item.data for item in f.intake_files()}
        for count in (64, 65):
            entries = f.member_count_manifest(count)
            self.assertEqual(len(entries), count)
            self.assertEqual(json.loads(files[f"count{count}.json"]), {"entries": [e.as_json() for e in entries]})
            with zipfile.ZipFile(io.BytesIO(files[f"count{count}.zip"])) as archive:
                self.assertEqual(len(archive.infolist()), count)
                self.assertEqual(archive.read("m00.txt"), b"x")
                self.assertTrue(all(info.file_size == 1 for info in archive.infolist()))
            self.assertEqual(len([path for path in files if path.startswith(f"count{count}/")]), count)
        self.assertEqual(json.loads(files["empty.json"]), {"entries": []})
        self.assertEqual(f.EMPTY_DIRECTORIES, ("empty",))

    def test_fixture_names_and_fresh_results_preserve_no_mutable_shared_state(self) -> None:
        first, second = f.intake_files(), f.intake_files()
        self.assertEqual(first, second)
        self.assertEqual(len(first), len({item.path for item in first}))
        self.assertTrue(all(item.sha256 == hashlib.sha256(item.data).hexdigest() for item in first))
        with self.assertRaises(FrozenInstanceError):
            first[0].data = b"changed"
        self.assertEqual(json.loads(f.json_bundle(f.CANONICAL_BATCH))["entries"][0]["source"], "ns/z.txt")
        self.assertEqual([e.source for e in f.canonical_manifest(f.CANONICAL_BATCH)],
                         ["ns/a.html", "ns/nested/a.md", "ns/z.txt"])


if __name__ == "__main__":
    unittest.main()
