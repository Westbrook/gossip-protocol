"""Driver boundary controls; Docker classes require the explicit physical lane."""
from __future__ import annotations

import ast
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_storage_driver_v1 as driver
from gossip_harness import candidate_storage_observer_v1 as observer
from gossip_harness.library_v2_reference_v1 import v2_binary_files, v2_files
from gossip_harness.sandbox import DockerValidator


def capsule(entries: list[tuple[str, bytes | str | None]]) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:") as archive:
        for name, value in entries:
            item = tarfile.TarInfo(name)
            if value is None:
                item.type = tarfile.DIRTYPE
                archive.addfile(item)
            elif type(value) is str:
                item.type = tarfile.SYMTYPE
                item.linkname = value
                archive.addfile(item)
            else:
                item.size = len(value)
                archive.addfile(item, io.BytesIO(value))
    return output.getvalue()


class CandidateStorageDriverTests(unittest.TestCase):
    def test_full_source_identity_checks_bytes_paths_and_ancestor_collisions(self):
        first = {"library/main.py": b"first", "public/data.bin": b"\xff\x00"}
        self.assertEqual(driver.source_sha256(first), driver.source_sha256(dict(reversed(list(first.items())))))
        self.assertNotEqual(driver.source_sha256(first), driver.source_sha256(first | {"public/data.bin": b"\xff\x01"}))
        for files in ({}, {"../escape": b""}, {".env": b"key"}, {"a": b"", "a/b": b""},
                      {"path": "text"}, {"/absolute": b""}):
            with self.subTest(files=list(files)), self.assertRaises(driver.DriverError):
                driver.source_sha256(files)

    def test_capture_preserves_database_and_sidecars_without_extracting(self):
        raw = capsule([("tmp", None), ("tmp/catalog.sqlite", b"raw db"),
                       ("tmp/catalog.sqlite-wal", b"wal"), ("tmp/data", None), ("tmp/data/content", b"payload")])
        self.assertEqual(driver.parse_capture(raw), {"catalog.sqlite": b"raw db", "catalog.sqlite-wal": b"wal",
                                                     "data/content": b"payload"})
        self.assertEqual(driver.parse_capture(capsule([("tmp", None)])), {})

    def test_complete_unsafe_layout_distinguished_from_incomplete_transport(self):
        for entries in ([('tmp', None), ('tmp/link', '/etc/passwd')],
                        [('tmp', None), ('tmp/../escape', b'')],
                        [('tmp', None), ('tmp/a', b'one'), ('tmp/a', b'two')],
                        [('tmp', None), ('tmp/a', b''), ('tmp/a/b', b'')]):
            raw = capsule(entries)
            with self.subTest(entries=entries), self.assertRaises(driver.CaptureLayoutError):
                driver.parse_capture(raw)
            # Framing failure takes precedence over the candidate path verdict.
            with self.assertRaises(driver.DriverError) as caught:
                driver.parse_capture(raw[:512])
            self.assertNotIsInstance(caught.exception, driver.CaptureLayoutError)
        for raw in (b'', b'not a tar', capsule([('elsewhere', None)])):
            with self.assertRaises(driver.DriverError):
                driver.parse_capture(raw)

    def test_staging_and_docker_arguments_preserve_boundary_and_uid_split(self):
        sandbox = DockerValidator(driver.RUNTIME_IMAGE, {"check.py": ""},
                                  command=("python", "-I", "-c", "import time;time.sleep(600)"))
        arguments = driver._start_arguments(sandbox, "owned-name", Path("/source"), Path("/checks"), "owned-volume")
        for item in ("--network=none", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges:true",
                     "--pull=never", "--user=0:0", "--detach"):
            self.assertIn(item, arguments)
        self.assertNotIn("--rm", arguments)
        self.assertFalse(any(item.startswith("--tmpfs=/tmp:") for item in arguments))
        self.assertIn("type=volume,source=owned-volume,target=/tmp,volume-nocopy", arguments)
        self.assertIn("type=bind,source=/source,target=/workspace,readonly,bind-propagation=rprivate", arguments)
        compile(driver.CHILD_ADAPTER, "storage_adapter.py", "exec")

    def test_pause_and_volume_inspection_require_exact_control_plane_identity(self):
        volume = {"Name": "owned", "Driver": "local", "Scope": "local", "Options": driver.VOLUME_OPTIONS,
                  "Labels": {"gossip.execution": "execution", "gossip.snapshot": driver.SNAPSHOT_PROTOCOL}}
        self.assertTrue(driver._volume_valid(volume, "owned", "execution"))
        self.assertFalse(driver._volume_valid(volume | {"Options": {}}, "owned", "execution"))
        state = {"Image": driver.RUNTIME_IMAGE, "State": {"Running": True, "Paused": True, "Pid": 12},
                 "Mounts": [{"Destination": "/tmp", "Type": "volume", "Name": "owned", "Driver": "local", "RW": True}]}
        self.assertTrue(driver._paused(state, "owned", driver.RUNTIME_IMAGE))
        for change in ({"Image": "wrong"}, {"State": {"Running": True, "Paused": False, "Pid": 12}},
                       {"Mounts": []}, {"Mounts": state["Mounts"] * 2}):
            self.assertFalse(driver._paused(state | change, "owned", driver.RUNTIME_IMAGE))

    def test_source_mismatch_and_existing_output_reject_before_dispatch(self):
        files = {"library/__init__.py": b""}
        with tempfile.TemporaryDirectory() as temporary, patch.object(subprocess, "Popen") as process:
            root = Path(temporary)
            with self.assertRaises(driver.DriverError):
                driver.run_storage_case(files, "rollback", root / "run", expected_source_sha256="0" * 64)
            with self.assertRaises(driver.DriverError):
                driver.run_storage_case(files, "rollback", root, expected_source_sha256=driver.source_sha256(files))
            process.assert_not_called()
            self.assertFalse((root / "run").exists())

    def test_bounded_command_capture_records_truncation_and_timeout_without_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            commands = driver._Commands(Path(temporary), 1)
            record = commands.run("overflow", [sys.executable, "-I", "-c", "print('x'*10000)"], limit=16)
            self.assertFalse(driver._clean(record))
            self.assertEqual(record["stdout"]["bytes"], 16)
            self.assertGreater(record["stdout"]["observed_bytes"], 16)
            record = commands.run("deadline", [sys.executable, "-I", "-c", "import time;time.sleep(5)"])
            self.assertTrue(record["timed_out"])
            self.assertFalse(driver._clean(record))
            with self.assertRaises(driver.DriverError):
                commands.run("deadline", ["unused"])

    def test_protocol_ignores_candidate_pass_flag_and_rejects_wrong_phase(self):
        with tempfile.TemporaryDirectory() as temporary:
            commands = driver._Commands(Path(temporary), 2)
            script = "import sys;sys.stdin.readline();print('{\"phase\":\"after\",\"value\":{\"passed\":true}}',flush=True)"
            session = driver._Session(commands, [sys.executable, "-I", "-c", script])
            try:
                with self.assertRaises(driver.DriverError):
                    session.phase("before")
            finally:
                self.assertFalse(session.finish(False))


    def test_duplicate_nonfinite_and_deep_candidate_json_are_invalid_observations(self):
        payloads = ('{"phase":"before","phase":"before","value":null}',
                    '{"phase":"before","value":NaN}',
                    '{"phase":"before","value":1e9999}',
                    '{"phase":"before","value":' + '[' * 1200 + '0' + ']' * 1200 + '}')
        for payload in payloads:
            with self.subTest(size=len(payload)), tempfile.TemporaryDirectory() as temporary:
                commands = driver._Commands(Path(temporary), 2)
                script = "import sys;sys.stdin.readline();print(" + repr(payload) + ",flush=True)"
                session = driver._Session(commands, [sys.executable, "-I", "-c", script])
                try:
                    with self.assertRaises(driver.DriverError):
                        session.phase("before")
                finally:
                    self.assertFalse(session.finish(False))

    def test_excess_response_flood_retains_bounded_streams_and_unique_errors(self):
        with tempfile.TemporaryDirectory() as temporary:
            commands = driver._Commands(Path(temporary), 2)
            script = "import sys;sys.stdout.write('x\\n'*600000);sys.stdout.flush()"
            session = driver._Session(commands, [sys.executable, "-I", "-c", script])
            session.process.wait(timeout=5)
            self.assertFalse(session.finish(False))
            self.assertEqual(session.errors, {"extra-response"})
            self.assertLessEqual(len(session.streams["stdout"]), driver.MAX_STREAM_BYTES)
            self.assertGreater(session.counts["stdout"], driver.MAX_STREAM_BYTES)
            record = json.loads((Path(temporary) / "session.json").read_bytes())
            self.assertEqual(record["errors"], ["extra-response"])
            self.assertTrue(record["stdout"]["truncated"])

    def test_prospective_authored_schema_has_fourteen_tables_and_four_fences(self):
        raw, profile = authored_schema_profile()
        self.assertEqual(profile["schema_sha256"], observer.sqlite_schema_sha256(raw))
        self.assertEqual(len(profile["statements"]), 18)
        self.assertEqual(sum(value.startswith("CREATE TABLE ") for value in profile["statements"]), 14)
        self.assertEqual(sum(value.startswith("CREATE TRIGGER ") for value in profile["statements"]), 4)
        self.assertEqual(set(profile["reviewed_sources"]),
                         {"library/catalog/m4_store.py", "library/catalog/m4_control.py"})

    def test_symlink_output_ancestor_rejects_before_dispatch(self):
        files = {"library/__init__.py": b""}
        with tempfile.TemporaryDirectory() as temporary, patch.object(subprocess, "Popen") as process:
            root = Path(temporary).resolve()
            (root / "real").mkdir()
            (root / "linked").symlink_to(root / "real", target_is_directory=True)
            with self.assertRaises(driver.DriverError):
                driver.run_storage_case(files, "rollback", root / "linked" / "new",
                                        expected_source_sha256=driver.source_sha256(files))
            process.assert_not_called()


def authored_schema_profile() -> tuple[bytes, dict]:
    """Prospective schema from fixed AUTHORED SQL, never candidate execution.

    The generated reference Python is parsed as data. Only literal table DDL is
    selected, and four reviewed fence definitions are reproduced explicitly.
    No module in the candidate source map is imported or run on the host.
    This is a fixture qualification profile, not production source authority.
    """
    sources = v2_files()
    store = ast.parse(sources["library/catalog/m4_store.py"])
    declarations = {}
    for node in store.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            if node.targets[0].id in ("_BASE", "_NORMALIZED"):
                declarations[node.targets[0].id] = ast.literal_eval(node.value)
    control = ast.parse(sources["library/catalog/m4_control.py"])
    controls = [node for node in ast.walk(control) if isinstance(node, ast.FunctionDef) and node.name == "_initialize_control"]
    if len(controls) != 1:
        raise ValueError("Authored control schema seam changed")
    loops = [node for node in controls[0].body if isinstance(node, ast.For)
             and isinstance(node.target, ast.Name) and node.target.id == "statement"]
    if len(loops) != 1:
        raise ValueError("Authored literal schema seam changed")
    values = ast.literal_eval(loops[0].iter)
    statements = list(declarations["_BASE"]) + list(declarations["_NORMALIZED"])
    statements += [value for value in values if value.startswith("CREATE TABLE ")]
    if len(statements) != 14 or any(type(value) is not str or not value.startswith("CREATE TABLE ") for value in statements):
        raise ValueError("Exactly fourteen authored tables required")
    for table, field, target, key, high, extra in (
        ('jobs','epoch','job_control','job_id','epoch_high_water',',0'),
        ('document_state','edit_version','document_control','document_id','edit_high_water',''),
    ):
        for event in ('INSERT','UPDATE'):
            name = 'fence_' + table + '_' + event.lower()
            statements.append('CREATE TRIGGER ' + name + ' AFTER ' + event + ' ON ' + table +
                ' BEGIN INSERT INTO ' + target + ' VALUES (NEW.' + key + ',NEW.' + field + extra + ') '
                'ON CONFLICT(' + key + ') DO UPDATE SET ' + high + '=MAX(' + high + ',excluded.' + high + '); END')
    with tempfile.TemporaryDirectory(prefix="authored-storage-schema-") as temporary:
        path = Path(temporary) / "schema.sqlite"
        connection = sqlite3.connect(path)
        try:
            for statement in statements:
                connection.execute(statement)
            connection.commit()
        finally:
            connection.close()
        raw = path.read_bytes()
    profile = {"purpose": "authored-v2-fixture-schema-qualification-only",
        "schema_sha256": observer.sqlite_schema_sha256(raw),
        "statements": statements,
        "reviewed_sources": {name: driver.sha256(sources[name].encode())
                             for name in ("library/catalog/m4_store.py", "library/catalog/m4_control.py")}}
    return raw, profile


def mutant_source(files: dict[str, bytes], defect: str) -> dict[str, bytes]:
    """Controlled candidate changes; only the container executes these bytes."""
    if defect == "orphan":
        addition = '''
class Store(Store):
    def commit_job(self, job_id, epoch, *, fail_before_commit=False):
        from library.common import LibraryError
        import hashlib
        try:
            return super().commit_job(job_id, epoch, fail_before_commit=fail_before_commit)
        except LibraryError as error:
            if error.code == 'injected_failure':
                raw = b'unreferenced-after-rollback'
                self.db.execute('INSERT INTO blobs VALUES (?,?)', ('blob-' + hashlib.sha256(raw).hexdigest(), raw))
            raise
'''
    elif defect == "alias":
        addition = '''
class Store(Store):
    def get_job(self, job_id):
        value = super().get_job(job_id)
        if value['state'] != 'completed':
            return value
        if not hasattr(self, '_qualification_job_alias'):
            self._qualification_job_alias = value
        return self._qualification_job_alias

    def job_manifest(self, job_id):
        if not hasattr(self, '_qualification_manifest_alias'):
            self._qualification_manifest_alias = super().job_manifest(job_id)
        return self._qualification_manifest_alias

    def commit_job(self, job_id, epoch, *, fail_before_commit=False):
        value = super().commit_job(job_id, epoch, fail_before_commit=fail_before_commit)
        if type(value) is not dict or 'documents' not in value:
            return value
        if not hasattr(self, '_qualification_receipt_alias'):
            self._qualification_receipt_alias = value
        return self._qualification_receipt_alias
'''
    else:
        raise ValueError("Unknown declared defect")
    result = dict(files)
    result["library/catalog/store.py"] += addition.encode()
    return result


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateStorageDriverDockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.files = {name: text.encode() for name, text in v2_files().items()} | v2_binary_files()
        # Declared before the first new candidate is physically executed.
        cls.schema_fixture, cls.schema_profile = authored_schema_profile()
        ready, detail = DockerValidator(driver.RUNTIME_IMAGE, {"unused.py": ""}).preflight()
        if not ready:
            raise RuntimeError(detail)

    def qualify(self, case_id, *, defect=None):
        files = self.files if defect is None else mutant_source(self.files, defect)
        source_sha = driver.source_sha256(files)
        label = "candidate-storage-" + case_id + ("-" + defect if defect else "")
        with ArtifactDirectory(label, retain_success=True) as artifacts:
            (artifacts.root / "prospective-schema.sqlite").write_bytes(self.schema_fixture)
            profile = self.schema_profile | {"source_sha256": source_sha,
                       "qualification_only": True, "production_review_authority": False}
            (artifacts.root / "prospective-schema-profile.json").write_bytes(driver.encoded(profile))
            review_sha = driver.sha256(driver.encoded(profile))
            registration = observer.Registration(source_sha, observer.V2_SQLITE_LAYOUT,
                observer.V2_STORAGE_PATHS, review_sha, profile["schema_sha256"])
            (artifacts.root / "prospective-registration.json").write_bytes(driver.encoded(vars(registration)))
            result = driver.run_storage_case(files, case_id, artifacts.root / "execution",
                                              expected_source_sha256=source_sha)
            self.assertTrue(result.completed, result.infrastructure)
            self.assertEqual(result.layout_errors, {})
            self.assertEqual(tuple(result.snapshots), driver.PHASES)
            self.assertTrue(all("catalog.sqlite" in capture for capture in result.snapshots.values()))
            self.assertTrue(result.cleanup_verified)
            terminal = json.loads((Path(result.output_root) / "terminal.json").read_bytes())
            self.assertEqual(terminal["infrastructure"], [])
            self.assertEqual(terminal["responses"]["before"], None)
            if defect is None:
                self.assertEqual(terminal["responses"]["reopened"], None)
            phases = [observer.observe_capture(result.snapshots[phase], registration, source_sha)
                      for phase in driver.PHASES]
            local = observer.evaluate_case(case_id, *phases, result.responses["after"])
            (artifacts.root / "normalized-local-observation.json").write_bytes(driver.encoded(local))
            # Binding and raw storage capture are host-owned; candidate responses
            # are untrusted public return values, never the local verdict.
            self.assertEqual(terminal["source_sha256"], source_sha)
            self.assertEqual(terminal["runtime"]["image"]["Id"], driver.RUNTIME_IMAGE)
            for phase in driver.PHASES:
                raw = (Path(result.output_root) / (phase + "-capture-stdout.bin")).read_bytes()
                record = json.loads((Path(result.output_root) / (phase + "-capture.json")).read_bytes())
                self.assertEqual(driver.sha256(raw), record["stdout"]["sha256"])
                self.assertEqual(driver.parse_capture(raw), result.snapshots[phase])
            return local, phases, result

    def check_positive(self, case_id):
        local, _, _ = self.qualify(case_id)
        self.assertTrue(local["all_local_assertions_passed"], local)
        self.assertIsNone(local["full_requirement_verdict"])

    def test_v2_rollback(self):
        self.check_positive("rollback")

    def test_v2_capacity(self):
        self.check_positive("capacity")

    def test_v2_invalid_admission(self):
        self.check_positive("invalid-admission")

    def test_v2_deferred_semantic(self):
        self.check_positive("deferred-semantic")

    def test_v2_empty_batch(self):
        self.check_positive("empty-batch")

    def test_v2_canonical_replay(self):
        self.check_positive("canonical-replay")

    def test_v2_completed_replay(self):
        self.check_positive("completed-replay")

    def test_v2_value_mutation(self):
        self.check_positive("value-mutation")

    def test_v2_orphan_blob_mutant_has_correct_public_error_but_fails_storage(self):
        local, phases, result = self.qualify("rollback", defect="orphan")
        self.assertEqual(result.responses["after"], {"error": "injected_failure"})
        self.assertEqual(phases[0].data["documents"], phases[1].data["documents"])
        self.assertFalse(local["checks"]["after.persisted-state"])
        self.assertFalse(local["all_local_assertions_passed"])

    def test_v2_cached_alias_mutant_preserves_disk_but_fails_live_reread(self):
        local, phases, _ = self.qualify("value-mutation", defect="alias")
        self.assertEqual(phases[0].data, phases[1].data)
        self.assertTrue(local["checks"]["after.persisted-state"])
        self.assertFalse(local["checks"]["result"])
        self.assertFalse(local["all_local_assertions_passed"])
