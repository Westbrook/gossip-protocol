"""Scoped inert checks. None dispatch Docker or execute generated library code."""
from __future__ import annotations

import ast
import base64
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

from gossip_harness import candidate_product_worker_cases_v1 as cases
from gossip_harness import candidate_product_worker_execution_v1 as execution
from gossip_harness import candidate_product_worker_hooks_v1 as hooks
from gossip_harness import candidate_product_worker_observation_v1 as observer
from gossip_harness import candidate_product_worker_fixture_v1 as fixture
from gossip_harness import candidate_product_process_cases_v1 as prior


class WorkerLifecycleDefinitionV1Tests(unittest.TestCase):
    def test_closed_four_histories_and_all_facet_steps(self):
        rows = cases.cases()
        self.assertEqual(len(rows), 4)
        self.assertEqual([len(row.record["input"]["actions"]) for row in rows], [15, 15, 20, 15])
        for case in rows:
            actions = case.record["input"]["actions"]
            ids = [row["id"] for row in actions]
            self.assertEqual(ids, ["s%03d" % i for i in range(len(ids))])
            self.assertEqual(ids, [row["step_id"] for row in case.record["expected"]])
            for facet in case.record["facets"]:
                self.assertTrue(set(facet["steps"]) <= set(ids))
            self.assertEqual(case, cases.WorkerCase(case.declaration_json))
            changed = case.record
            changed["input"]["actions"].reverse()
            with self.assertRaises(ValueError):
                cases.WorkerCase(cases.encoded(changed))

    def test_only_inputs_and_public_hook_coordinates_enter_candidate_mount(self):
        for case in cases.cases():
            inputs = case.inputs()
            self.assertNotIn("inline/a.txt", inputs)
            self.assertNotIn("inline/b.txt", inputs)
            for name, value in inputs.items():
                if name.startswith("__evaluator__/") and name.endswith(".json"):
                    request = json.loads(value)
                    self.assertEqual(set(request), {"phase", "mode", "directory", "maximum_hold_seconds"})
                    self.assertNotIn("expected", request)
            self.assertNotIn("expected", case.record["input"])

    def test_existing_eight_factories_are_not_redefined(self):
        before = prior.acceptance_cases()
        cases.definitions()
        fixture.instrumented_files()
        self.assertEqual(prior.acceptance_cases(), before)
        self.assertEqual(len(before), 8)

    def test_public_contract_clauses_resolve(self):
        root = Path(prior.__file__).resolve().parents[1]
        raw = (root / "library-cumulative-product-v2.json").read_bytes()
        self.assertEqual(cases.sha(raw), cases.CONTRACT_SHA256)
        contract = json.loads(raw)
        requirements = {row["id"]: row for row in contract["requirements"]}
        for case in cases.cases():
            for facet in case.record["facets"]:
                if "/" in facet["clause"]:
                    identifier, index = facet["clause"].split("/")
                    self.assertIsInstance(requirements[identifier]["clauses"][int(index)], str)

    def test_hook_sources_compile_without_running_candidate(self):
        for name, raw in vars(hooks).items():
            if name.endswith("_SOURCE") and isinstance(raw, str):
                ast.parse(raw, filename=name)
        files = fixture.instrumented_files()
        for name, raw in files.items():
            if name.endswith(".py"):
                ast.parse(raw, filename=name)
        self.assertIn("library/_evaluator_worker_v1.py", files)
        self.assertIn(b"self.db.commit()", files["library/catalog/m4_control.py"])
        self.assertIn(b"before_commit", files["library/catalog/m4_control.py"])
        self.assertIn(b"fcntl.flock", files["library/catalog/v2_maintenance.py"])

    def test_hook_observe_and_expired_pause_are_real_bounded_host_helper_behavior(self):
        # Executes only the evaluator hook in an isolated temporary directory.
        # This is no candidate-process, SQLite-commit or physical crash evidence.
        with tempfile.TemporaryDirectory() as temporary:
            namespace = {"__name__": "inert_hook_test"}
            exec(compile(hooks.HOOK_SOURCE, "evaluator-hook", "exec"), namespace)
            namespace["configure"]({"directory": temporary, "phase": "owner_acquired", "mode": "observe", "maximum_hold_seconds": 0})
            namespace["boundary"]("owner_acquired", lock_fd=3, incarnation="inert", worker_generation=1)
            event = json.loads((Path(temporary) / "event.json").read_bytes())
            self.assertEqual(event["phase"], "owner_acquired")
            self.assertNotIn("expected", event)
        with tempfile.TemporaryDirectory() as temporary:
            namespace = {"__name__": "inert_hook_timeout"}
            exec(compile(hooks.HOOK_SOURCE, "evaluator-hook", "exec"), namespace)
            namespace["configure"]({"directory": temporary, "phase": "before_commit", "mode": "pause", "maximum_hold_seconds": 0})
            with self.assertRaisesRegex(RuntimeError, "hold expired"):
                namespace["boundary"]("before_commit", lock_fd=3, incarnation="inert", worker_generation=1,
                                      job_id="recover", epoch=1)

    def test_policy_rejects_unbounded_or_changed_declared_limits(self):
        policy = execution.WorkerPolicy("sha256:" + "a" * 64)
        for values in ({"hook_timeout_seconds": float("inf")}, {"lifetime_seconds": 300}, {"seed": True},
                       {"stream_limit_bytes": 17 * 1024 * 1024}, {"stop_timeout_seconds": 9}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                replace(policy, **values)


class WorkerLifecycleObservationV1Tests(unittest.TestCase):
    def kernel_fixture(self):
        # An explicitly inert decoder fixture; never accepted by read_original.
        event = {"protocol": hooks.PROTOCOL, "phase": "before_commit", "pid": 1, "lock_fd": 3,
                 "incarnation": "inert", "worker_generation": 1, "job_id": "recover", "epoch": 1}
        return {"state": "complete", "event_b64": base64.b64encode(cases.encoded(event)).decode(),
            "pid": 1, "fd": 3, "fd_target": "/tmp/library.sqlite.maintenance/worker.lock",
            "fd_identity": {"device": 51, "inode": 999, "major": 0, "minor": 51},
            "database_identity": {"device": 51, "inode": 998, "major": 0, "minor": 51},
            "proc_stat": "1 (python) S 0 0 0", "fdinfo": "pos:\t0\nlock:\t1: FLOCK ADVISORY WRITE 1 00:33:999 0 EOF\n",
            "locks": "1: FLOCK ADVISORY WRITE 1 00:33:999 0 EOF\n2: POSIX ADVISORY WRITE 1 00:33:998 1073741825 1073741825\n"}

    def test_marker_alone_never_proves_lock_claim_or_provisional_writes(self):
        fact = observer.kernel_lock_evidence({"state": "unavailable", "event_b64": "e30="})
        self.assertFalse(fact["exclusive_owner_lock"])
        self.assertFalse(fact["matching_application_claim"])
        self.assertFalse(fact["provisional_writes"])

    def test_kernel_owner_and_reserved_lock_are_distinct_from_application_claim(self):
        fact = observer.kernel_lock_evidence(self.kernel_fixture())
        self.assertTrue(fact["exclusive_owner_lock"])
        self.assertTrue(fact["sqlite_reserved_write_lock"])
        self.assertFalse(fact["matching_application_claim"])
        self.assertFalse(fact["provisional_writes"])

    def test_wrong_inode_dead_pid_missing_fdinfo_and_unattributable_lock_stay_unknown(self):
        changes = [{"locks": ""}, {"fdinfo": ""}, {"proc_stat": "1 (python) Z 0"},
                   {"fd_target": "/tmp/unrelated"}, {"locks": "1: OFDLCK ADVISORY WRITE -1 00:33:999 0 EOF\n"}]
        for changed in changes:
            value = deepcopy(self.kernel_fixture())
            value.update(changed)
            self.assertFalse(observer.kernel_lock_evidence(value)["exclusive_owner_lock"])

    def test_strict_json_keeps_duplicate_and_truncated_data_unavailable(self):
        for raw in (b'{"epoch":1,"epoch":2}', b'{"epoch":', b'{"value":NaN}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                observer.strict_value(raw)
        self.assertFalse(observer.exact({"epoch": True}, {"epoch": 1}))
        self.assertFalse(observer.subset({"epoch": 1.0}, {"epoch": 1}))

    def test_caller_supplied_prepared_artifact_cannot_claim_physical_history(self):
        for owner in (None, {"physical": True}, object()):
            with self.subTest(owner=owner), self.assertRaises(ValueError):
                observer.read_original(owner, None)


    def test_complete_missing_or_wrong_job_is_a_discrepancy_not_missing_capture(self):
        columns = ["job_id", "epoch", "state", "total", "completed", "error", "manifest", "content_hashes", "receipt"]
        for rows in ([], [["other", 1, "queued", 0, 0, None, "[]", "[]", None]]):
            snapshot = {"state": "complete", "tables": {"jobs": {"columns": columns, "rows": rows}}}
            with self.assertRaises(observer.DurableDiscrepancy):
                observer.selected_job(snapshot)
        with self.assertRaises(ValueError) as unavailable:
            observer.selected_job({"state": "unavailable"})
        self.assertNotIsInstance(unavailable.exception, observer.DurableDiscrepancy)

    def test_replacement_owner_requires_expected_durable_generation_two(self):
        case = cases.cases()[0].record
        expected = {row["step_id"]: row for row in case["expected"]}
        self.assertEqual(expected["s001"]["worker_generation"], 0)
        self.assertEqual(expected["s006"]["worker_generation"], 1)
        self.assertEqual(expected["s012"]["worker_generation"], 2)

    def test_complete_invalid_json_is_discrepancy_but_ambiguity_remains_unknown(self):
        with self.assertRaises(observer.DurableDiscrepancy):
            observer.product_json(b'{')
        with self.assertRaises(observer.original_observer.AmbiguousJSON):
            observer.product_json(b'{"error":1,"error":2}')

    def test_complete_json_over_observer_allocation_stays_unavailable(self):
        # This complete response fits a supported process capture allocation.
        # It exceeds this observer's independent parsing allocation; neither a
        # syntax failure nor a physical product observation is fabricated here.
        policy = execution.WorkerPolicy("sha256:" + "a" * 64, stream_limit_bytes=16 * 1024 * 1024)
        raw = b'{"payload":"' + b'a' * (4 * 1024 * 1024) + b'"}'
        self.assertLess(len(raw), policy.stream_limit_bytes)
        self.assertEqual(len(json.loads(raw)["payload"]), 4 * 1024 * 1024)
        for parse in (observer.strict_value, observer.product_json):
            with self.subTest(parser=parse.__name__), self.assertRaises(observer.original_observer.ObservationLimit):
                parse(raw)
        job = {**cases.job(state="completed"), "receipt": raw.decode()}
        snapshot = {"state": "complete", "tables": {"jobs": {"columns": list(job), "rows": [list(job.values())]}}}
        findings = observer.completed_graph_findings({"s006": snapshot}, ("s006",), 1)
        self.assertEqual([(step, state) for step, state, _reason in findings], [("s006", "unavailable")])
        with self.assertRaises(ValueError) as invalid_type:
            observer.strict_value(raw.decode())
        self.assertNotIsInstance(invalid_type.exception, observer.original_observer.ObservationLimit)

    def test_complete_duplicate_control_keys_cannot_hide_generation(self):
        snapshot = {"state": "complete", "tables": {"control": {"columns": ["key", "value"],
            "rows": [["worker_generation", "1"], ["worker_generation", "2"]]}}}
        with self.assertRaises(observer.DurableDiscrepancy):
            observer.controls(snapshot)

    def test_missing_earlier_snapshot_does_not_hide_later_complete_wrong_graph(self):
        snapshot = {"state": "complete", "tables": {"jobs": {"columns": ["job_id"], "rows": [["other"]]}}}
        findings = observer.completed_graph_findings({"s009": snapshot, "s011": snapshot}, ("s006", "s009", "s011"), 1)
        self.assertEqual([(step, state) for step, state, _reason in findings],
                         [("s006", "unavailable"), ("s009", "failed"), ("s011", "failed")])

    def test_complete_malformed_receipt_and_mixed_source_types_are_product_mismatches(self):
        job = {**cases.job(state="completed"), "receipt": "{"}
        snapshot = {"state": "complete", "tables": {"jobs": {"columns": list(job), "rows": [list(job.values())]}}}
        with self.assertRaises(observer.DurableDiscrepancy):
            observer.completed_graph(snapshot, 1)
        snapshot["tables"]["jobs"]["rows"][0][-1] = cases.encoded(cases.receipt()).decode()
        snapshot["tables"]["documents"] = {"columns": ["source"], "rows": [["text"], [{"blob_bytes": 1, "blob_sha256": "a" * 64}]]}
        for table in ("document_revisions", "document_state", "blobs"):
            snapshot["tables"][table] = {"columns": [], "rows": []}
        self.assertFalse(observer.completed_graph(snapshot, 1))

    def test_original_raw_worker_exit_survives_capture_loss_and_rejects_wrong_source(self):
        # Inert raw Engine bytes exercise the actual decoder/role comparison.
        # No fake owner can enter read_original or acquire physical authority.
        from types import SimpleNamespace
        from tests.test_candidate_http_execution_v1 import inspection
        from tests.test_candidate_client_process_v4 import runtime_fixture
        cid, prefix = "b" * 64, "s004-worker"
        spec = execution.RoleSpec("cli", "inert-worker", ("python", "-m", "library", "worker", "--once"),
            (("gossip.source", "c" * 64), ("gossip.fixture", "d" * 64)),
            (("/workspace", "/inert-source"), ("/inputs", "/inert-inputs")), "inert-volume")
        running = inspection(spec, container_id=cid, state="running")
        running["Config"].update(WorkingDir="/workspace", Hostname=cid[:12], Domainname="")
        running["Mounts"][-1]["RW"] = True
        final = deepcopy(running)
        final["State"].update(Status="exited", Running=False, Pid=0, ExitCode=7, FinishedAt="2026-10-03T00:00:02.003Z")
        runtime = runtime_fixture()
        wait = {"StatusCode": 7, "Error": None}
        comparison = execution.role_identity_comparison(running, final, spec, runtime, "running-to-exited")
        completion = execution.engine.completion_evidence(wait, final, started=True, killed=False, identity_ok=True)
        row = {"protocol": execution.PROTOCOL, "container_id": cid, "completion": completion,
               "abrupt_death": False, "exit_code": 7}
        evidence = {prefix + "-exit.json": cases.encoded(row), prefix + "-final.json": cases.encoded(comparison)}
        def control(label, operation, method, value, status=200):
            raw = b"" if status == 204 else cases.encoded(value)
            evidence[label + "-request.bin"] = execution.engine._request(method, "/containers/" + cid + "/" + operation)
            evidence[label + "-response.bin"] = ("HTTP/1.1 %d OK\r\nContent-Length: %d\r\n\r\n" % (status, len(raw))).encode() + raw
        control(prefix + "-start", "start", "POST", None, 204)
        control(prefix + "-wait", "wait?condition=not-running", "POST", wait)
        control(prefix + "-final", "json", "GET", final)
        owner = SimpleNamespace(read_authenticated=evidence.__getitem__, runtime=runtime,
                                policy=execution.WorkerPolicy(runtime["image_id"]))
        from unittest.mock import patch
        retained = {}
        local_owner = SimpleNamespace(policy=owner.policy, endpoint=None, _deadline_after=lambda seconds: seconds,
            _remaining=lambda seconds: seconds, _inspect=lambda _label, _cid: final,
            _compare=lambda _label, old, new, role, phase: execution.role_identity_comparison(old, new, role, runtime, phase),
            _retain=lambda name, raw: retained.setdefault(name, raw))
        live = execution._LiveProcess(local_owner, prefix, spec, running)
        live.started, live.running = True, running
        live.reader = SimpleNamespace(join=lambda timeout: None, is_alive=lambda: False)
        live.errors.append("deliberate original attach loss")
        partial = {}
        with patch.object(execution.engine, "_json_control", return_value=wait):
            with self.assertRaisesRegex(ValueError, "attach EOF"):
                live.finish(kill=False, row=partial)
        self.assertEqual(partial, row)
        self.assertEqual(json.loads(retained[prefix + "-exit.json"]), row)
        self.assertNotIn(prefix + "-completion.json", retained)
        reader = observer._Reader(owner)
        reader.workers["w1"] = (spec, running, running)
        start = {"start_label": prefix, "container_id": cid, "worker": "w1"}
        self.assertEqual(reader.worker_exit(row, start, killed=False), 7)
        with self.assertRaises(KeyError):
            reader.worker_finished(row, start, killed=False)
        wrong = deepcopy(final)
        wrong["Config"]["Labels"]["gossip.source"] = "e" * 64
        control(prefix + "-final", "json", "GET", wrong)
        with self.assertRaises(ValueError):
            reader.worker_exit(row, start, killed=False)

    def test_expired_or_missing_original_host_guard_is_unavailable(self):
        from types import SimpleNamespace
        case = cases.cases()[1]
        action = case.record["input"]["actions"][6]
        start = {"start_label": "s004-worker", "container_id": "b" * 64, "worker": "w1"}
        anchor = {"protocol": "worker-hook-host-bound-v1", "clock": "host-monotonic-ns", "container_id": "b" * 64,
                  "start_before_ns": 1000, "maximum_hold_seconds": 90, "hook_mode": "pause"}
        bound = {"worker": "w1", "container_id": "b" * 64, "start_label": "s004-worker", "start_before_ns": 1000,
                 "maximum_hold_seconds": 90, "release_acknowledgement_label": None, "within_bound": True}
        before = {"protocol": anchor["protocol"], "clock": anchor["clock"], "checked_ns": 2000, "bounds": [bound]}
        after = deepcopy(before)
        after["checked_ns"] = 3000
        evidence = {"s004-observation.json": cases.encoded(start), "s004-worker-instrumentation-start.json": cases.encoded(anchor),
                    "s006-before-instrumentation.json": cases.encoded(before), "s006-after-instrumentation.json": cases.encoded(after)}
        positions = {"s004-worker-instrumentation-start.json": 1, "s004-worker-start-request.bin": 2,
                     "s006-step-intent.json": 3, "s006-before-instrumentation.json": 4, "s006-after-instrumentation.json": 5}
        owner = SimpleNamespace(profile=execution.WorkerProfile(case), read_authenticated=evidence.__getitem__,
                                journal=SimpleNamespace(_chain=SimpleNamespace(position=positions.__getitem__)))
        def reader():
            result = observer._Reader(owner)
            result.workers["w1"] = (None, None, None)  # This test exercises only bounded instrumentation chronology.
            return result
        reader().action_guards(action)
        after["checked_ns"] = 90 * 1_000_000_000 + 1000
        after["bounds"][0]["within_bound"] = False
        evidence["s006-after-instrumentation.json"] = cases.encoded(after)
        with self.assertRaisesRegex(ValueError, "hold expired"):
            reader().action_guards(action)
        after["bounds"][0]["within_bound"] = True
        evidence["s006-after-instrumentation.json"] = cases.encoded(after)
        with self.assertRaises(ValueError):
            reader().action_guards(action)
        del evidence["s006-after-instrumentation.json"]
        with self.assertRaises(KeyError):
            reader().action_guards(action)


class WorkerHookPublicationV1Tests(unittest.TestCase):
    @staticmethod
    def _run(source, *, os_proxy=None, argv=None, output=None):
        # Execute only literal evaluator helper code. Module-local imports keep
        # scheduling/fault controls out of global os/sys and other test workers.
        import builtins
        from types import SimpleNamespace
        original_import = builtins.__import__
        def local_import(name, *args, **kwargs):
            if name == "os" and os_proxy is not None:
                return os_proxy
            if name == "sys" and argv is not None:
                return SimpleNamespace(argv=argv)
            return original_import(name, *args, **kwargs)
        namespace = {"__builtins__": {**vars(builtins), "__import__": local_import}}
        if output is not None:
            namespace["print"] = output.append
        exec(compile(source, "local-evaluator-helper", "exec"), namespace)
        return namespace

    @staticmethod
    def _os(**overrides):
        import os
        class LocalOS:
            def __getattr__(self, name):
                return overrides[name] if name in overrides else getattr(os, name)
        return LocalOS()

    def _thread(self, action):
        import threading
        errors = []
        def invoke():
            try:
                action()
            except BaseException as error:
                errors.append(error)
        thread = threading.Thread(target=invoke, daemon=True)
        thread.start()
        return thread, errors

    def test_actual_event_reader_cannot_observe_private_partial_write(self):
        import os
        import threading
        writing, proceed, reader_attempt = threading.Event(), threading.Event(), threading.Event()
        def short_write(fd, raw):
            count = os.write(fd, raw[:7])
            writing.set()
            if not proceed.wait(3):
                raise TimeoutError("local write gate")
            return count
        def observed_open(path, *args, **kwargs):
            if str(path).endswith("/event.json"):
                reader_attempt.set()
            return os.open(path, *args, **kwargs)
        with tempfile.TemporaryDirectory() as temporary:
            hook = self._run(hooks.HOOK_SOURCE, os_proxy=self._os(write=short_write))
            hook["configure"]({"directory": temporary, "phase": "owner_acquired", "mode": "observe", "maximum_hold_seconds": 0})
            writer, errors = self._thread(lambda: hook["boundary"]("owner_acquired", lock_fd=3,
                incarnation="local-publication", worker_generation=1))
            reader, reader_errors, output = None, [], []
            try:
                self.assertTrue(writing.wait(2))
                self.assertFalse((Path(temporary) / "event.json").exists())
                reader, reader_errors = self._thread(lambda: self._run(hooks.EVENT_SOURCE,
                    os_proxy=self._os(open=observed_open), argv=["event", temporary, "2"], output=output))
                self.assertTrue(reader_attempt.wait(2))
                self.assertFalse(output)
            finally:
                proceed.set()
                writer.join(3)
                if reader is not None:
                    reader.join(3)
            self.assertFalse(writer.is_alive())
            self.assertIsNotNone(reader)
            self.assertFalse(reader.is_alive())
            self.assertEqual(errors + reader_errors, [])
            result = json.loads(output[0])
            self.assertEqual(result["state"], "observed")
            event_raw = base64.b64decode(result["event_b64"], validate=True)
            self.assertEqual(event_raw, (Path(temporary) / "event.json").read_bytes())
            self.assertEqual(json.loads(event_raw)["incarnation"], "local-publication")
            self.assertEqual([path.name for path in Path(temporary).iterdir()], ["event.json"])

    def test_actual_paused_hook_cannot_observe_private_release_before_write(self):
        import os
        import threading
        waiting, writing, proceed = threading.Event(), threading.Event(), threading.Event()
        def observed_open(path, *args, **kwargs):
            if str(path).endswith("/release"):
                waiting.set()
            return os.open(path, *args, **kwargs)
        def held_write(fd, raw):
            writing.set()
            if not proceed.wait(3):
                raise TimeoutError("local release gate")
            return os.write(fd, raw)
        with tempfile.TemporaryDirectory() as temporary:
            hook = self._run(hooks.HOOK_SOURCE, os_proxy=self._os(open=observed_open))
            hook["configure"]({"directory": temporary, "phase": "after_claim", "mode": "pause", "maximum_hold_seconds": 4})
            paused, errors = self._thread(lambda: hook["boundary"]("after_claim", lock_fd=3,
                incarnation="local-publication", worker_generation=1, job_id="recover", epoch=1))
            release, release_errors, output = None, [], []
            try:
                self.assertTrue(waiting.wait(2))
                release, release_errors = self._thread(lambda: self._run(hooks.RELEASE_SOURCE,
                    os_proxy=self._os(write=held_write), argv=["release", temporary], output=output))
                self.assertTrue(writing.wait(2))
                self.assertFalse((Path(temporary) / "release").exists())
                self.assertTrue(paused.is_alive())
            finally:
                proceed.set()
                if release is not None:
                    release.join(3)
                paused.join(5)
            self.assertFalse(paused.is_alive())
            self.assertIsNotNone(release)
            self.assertFalse(release.is_alive())
            self.assertEqual(errors + release_errors, [])
            self.assertEqual(json.loads(output[0]), {"released": True, "acknowledgement": "released"})
            self.assertEqual((Path(temporary) / "release").read_bytes(), b"1")
            self.assertEqual(sorted(path.name for path in Path(temporary).iterdir()), ["decision.json", "event.json", "release"])

    def test_actual_expired_hook_release_reports_instrumentation_expiry(self):
        with tempfile.TemporaryDirectory() as temporary:
            hook = self._run(hooks.HOOK_SOURCE)
            hook["configure"]({"directory": temporary, "phase": "after_claim", "mode": "pause", "maximum_hold_seconds": 0})
            with self.assertRaisesRegex(RuntimeError, "hold expired"):
                hook["boundary"]("after_claim", lock_fd=3, incarnation="local-expiry", worker_generation=1,
                                 job_id="recover", epoch=1)
            self.assertEqual((Path(temporary) / "decision.json").read_bytes(), b'{"decision":"expired"}')
            output = []
            self._run(hooks.RELEASE_SOURCE, argv=["release", temporary], output=output)
            self.assertEqual(json.loads(output[0]), {"released": True, "acknowledgement": "expired"})

    def test_publication_faults_never_expose_partial_final_or_overwrite_existing(self):
        import os
        for failure in ("zero_write", "file_fsync", "link", "directory_fsync"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                calls = []
                def fsync(fd):
                    calls.append(fd)
                    if failure == "file_fsync" or (failure == "directory_fsync" and len(calls) == 2):
                        raise OSError("deliberate fsync failure")
                    return os.fsync(fd)
                def link(*args, **kwargs):
                    if failure == "link":
                        raise OSError("deliberate publication failure")
                    return os.link(*args, **kwargs)
                publisher = self._run(hooks.ATOMIC_PUBLICATION_SOURCE,
                    os_proxy=self._os(write=(lambda _fd, _raw: 0) if failure == "zero_write" else os.write,
                                      fsync=fsync, link=link))["_publish_complete"]
                with self.assertRaises(OSError):
                    publisher(temporary, "event.json", b'{"complete":true}')
                paths = sorted(path.name for path in Path(temporary).iterdir())
                self.assertEqual(paths, ["event.json"] if failure == "directory_fsync" else [])
                if paths:
                    self.assertEqual((Path(temporary) / "event.json").read_bytes(), b'{"complete":true}')
        with tempfile.TemporaryDirectory() as temporary:
            publisher = self._run(hooks.ATOMIC_PUBLICATION_SOURCE)["_publish_complete"]
            publisher(temporary, "release", b"1")
            with self.assertRaises(FileExistsError):
                publisher(temporary, "release", b"replacement")
            self.assertEqual((Path(temporary) / "release").read_bytes(), b"1")
            self.assertEqual([path.name for path in Path(temporary).iterdir()], ["release"])


class WorkerLifecycleLocalOwnershipV1Tests(unittest.TestCase):
    def _exercise(self, *, exceptional):
        from contextlib import closing
        from gossip_harness import candidate_checkpoint_head_v1 as head
        from gossip_harness import candidate_execution_journal_v1 as journal
        class DeliberateExit(Exception):
            pass
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            raw, delta, external = root / "raw", root / "delta", root / "head"
            try:
                with closing(head.ExternalHead.create(external, journal_roots=(raw, delta))) as anchor, \
                     closing(journal.OwnerJournal(raw, delta, context={"purpose": "local-resource-lifetime-test"}, authority=anchor)) as owner:
                    owner.retain("original.json", b'{"retained":true}')
                    checkpoint = owner.checkpoint()
                    if exceptional:
                        raise DeliberateExit()
            except DeliberateExit:
                self.assertTrue(exceptional)
            # Real exclusive resources can be reacquired only after both owners
            # closed. This is actual local I/O, not Docker/process qualification.
            with closing(head.ExternalHead.reopen(external, journal_roots=(raw, delta), expected=checkpoint)) as anchor, \
                 closing(journal.OwnerJournal(raw, delta, context={"purpose": "local-resource-lifetime-test"},
                    authority=anchor, expected=checkpoint)) as owner:
                self.assertEqual(owner.read("original.json"), b'{"retained":true}')
                self.assertEqual(anchor.read(), checkpoint)

    def test_actual_anchor_and_journal_close_on_normal_scope(self):
        self._exercise(exceptional=False)

    def test_actual_anchor_and_journal_close_on_exceptional_scope(self):
        self._exercise(exceptional=True)

    def test_unstarted_attach_thread_is_not_joined_and_socket_close_failure_is_retained(self):
        from types import SimpleNamespace
        import threading
        class BrokenWire:
            socket = SimpleNamespace(shutdown=lambda _how: None)
            def close(self):
                raise OSError("deliberate close failure")
        local = execution._LiveProcess(SimpleNamespace(policy=execution.WorkerPolicy("sha256:" + "a" * 64)),
            "inert", None, {})
        local.wire = BrokenWire()
        local.reader = threading.Thread(target=lambda: None)
        with self.assertRaisesRegex(OSError, "deliberate close failure"):
            local.close()
        self.assertTrue(local.closed)
        self.assertIn("deliberate close failure", local.errors[0])

    def test_all_local_owner_resources_close_after_first_teardown_failure(self):
        from types import SimpleNamespace
        calls = []
        def bad():
            calls.append("bad")
            raise OSError("first teardown")
        owner = object.__new__(execution.WorkerExecution)
        owner.closed = False
        owner._live = {"first": SimpleNamespace(close=bad), "second": SimpleNamespace(close=lambda: calls.append("second"))}
        owner.emergency_cleanup = SimpleNamespace(close=lambda: calls.append("emergency"))
        owner.journal = SimpleNamespace(close=lambda: calls.append("journal"))
        with self.assertRaisesRegex(OSError, "first teardown"):
            owner.close()
        self.assertEqual(calls, ["bad", "second", "emergency", "journal"])
        self.assertTrue(owner.closed)

    def test_partial_command_drain_start_reaps_only_its_actual_local_child(self):
        # The closed Docker dispatch is replaced before Popen by one owned local
        # Python child; this is a host teardown check, no Engine/process evidence.
        from types import SimpleNamespace
        from unittest.mock import patch
        import subprocess
        import sys
        import threading
        import time
        original_popen, original_start = subprocess.Popen, threading.Thread.start
        children, starts = [], []
        def local_child(*_args, **kwargs):
            child = original_popen([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)
            children.append(child)
            return child
        def start_one_then_fail(thread):
            starts.append(thread)
            if len(starts) == 2:
                raise RuntimeError("deliberate second drain start failure")
            return original_start(thread)
        retained = {}
        fake = SimpleNamespace(mode="physical", endpoint=SimpleNamespace(socket_path="/not-an-engine", validate=lambda: None),
            policy=execution.WorkerPolicy("sha256:" + "a" * 64), runtime={"fixture": "inert"},
            _history_deadline=time.monotonic()+10, checkpoint=lambda: None,
            _remaining=lambda seconds: seconds, _deadline_after=lambda seconds: time.monotonic()+seconds,
            _retain=lambda name, raw: retained.setdefault(name, raw))
        with patch.object(execution.subprocess, "Popen", side_effect=local_child), \
             patch.object(execution.threading.Thread, "start", new=start_one_then_fail):
            with self.assertRaisesRegex(RuntimeError, "second drain start"):
                execution.WorkerExecution._command(fake, "inert-command", ["docker", "version"])
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].poll())
        self.assertFalse(starts[0].is_alive())
        record = json.loads(retained["inert-command.json"])
        self.assertFalse(record["capture_complete"])
        self.assertTrue(record["control_errors"])
