"""Offline fault controls: real compact journal plus a fixed fake Engine wire.

No Docker daemon, candidate program or provider is contacted. The fake exercises
only the cleanup module's actual closed request dispatcher; ownership, raw
journal authentication, exclusive roots and diagnostic writes are real.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from typing import Any

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness import candidate_client_process_v4 as engine
from gossip_harness import candidate_emergency_cleanup_v1 as cleanup


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class MemoryHead:
    def __init__(self) -> None:
        self.value: chain.PrefixCommitment | None = None
        self.lost = False

    def read(self) -> chain.PrefixCommitment | None:
        if self.lost:
            raise OSError("anchor unavailable")
        return self.value

    def compare_and_set(self, old: chain.PrefixCommitment | None, new: chain.PrefixCommitment) -> bool:
        if self.lost:
            raise OSError("lost durable acknowledgement")
        if self.value != old:
            return False
        self.value = new
        return True


class FakeEngine:
    def __init__(self, runtime: dict[str, Any]) -> None:
        self.runtime = runtime
        self.calls: list[tuple[str, str]] = []
        self.containers: dict[str, dict[str, Any]] = {}
        self.volumes: dict[str, dict[str, Any]] = {}
        self.lose_delete_response = False
        self.return_delete_error = False

    def identity(self, endpoint: Any, image_id: str, *, retain: Any, label: str, timeout_seconds: float) -> dict[str, Any]:
        retain(label + ".json", encoded(self.runtime))
        return dict(self.runtime)

    def control(self, endpoint: Any, method: str, path: str, *, deadline: float, retain: Any, label: str) -> tuple[int, bytes]:
        retain(label + "-request.bin", (method + " " + path).encode())
        self.calls.append((method, path))
        status, body = 404, b'{"message":"No such resource"}'
        if path.startswith("/containers/"):
            target = path.removeprefix("/containers/").split("/")[0].split("?")[0]
            value = next((v for v in self.containers.values() if v["Id"] == target or v["Name"] == "/" + target), None)
            if value is not None:
                if method == "GET":
                    status, body = 200, encoded(value)
                elif method == "DELETE":
                    self.containers.pop(value["Name"].removeprefix("/"))
                    status, body = 204, b""
        elif path.startswith("/volumes/"):
            target = path.removeprefix("/volumes/")
            value = self.volumes.get(target)
            if value is not None:
                if method == "GET":
                    status, body = 200, encoded(value)
                elif method == "DELETE":
                    self.volumes.pop(target)
                    status, body = 204, b""
        if method == "DELETE" and self.return_delete_error:
            status, body = 500, b'{"message":"uncertain remove"}'
        if method == "DELETE" and self.lose_delete_response:
            retain(label + "-response.bin", b"HTTP/1.1 204")
            raise OSError("response framing lost after effect")
        retain(label + "-response.bin", encoded({"status": status, "body": body.decode()}))
        return status, body


class EmergencyCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="gossip-emergency-cleanup-")
        self.base = Path(self.temp.name).resolve()
        self.head = MemoryHead()
        self.journal = chain.CheckpointChain.create(self.base / "raw", self.base / "delta",
            context={"test": "emergency-v1", "source": "a" * 64}, authority=self.head)
        self.endpoint = engine.EngineEndpoint("/private/tmp/fixture-engine.sock", 123, 456)
        self.image = "sha256:" + "b" * 64
        self.runtime = {"image_id": self.image, "daemon_id": "fixture-daemon", "api_version": "1.47"}
        self.mount = self.base / "workspace"
        self.mount.mkdir()
        self.fake = FakeEngine(self.runtime)
        self.channels: list[cleanup.CleanupChannel] = []
        self.counter = 0
        self.patches: list[Any] = [patch.object(engine, "runtime_identity", self.fake.identity),
                        patch.object(engine, "_control", self.fake.control)]
        for context in self.patches:
            context.start()

    def tearDown(self) -> None:
        for context in reversed(self.patches):
            context.stop()
        for channel in self.channels:
            channel.close()
        self.journal.close()
        self.temp.cleanup()

    def channel(self, **overrides: Any) -> cleanup.CleanupChannel:
        args: dict[str, Any] = {"journal": self.journal, "endpoint": self.endpoint,
            "runtime": self.runtime, "source_sha256": "a" * 64, "fixture_sha256": "c" * 64,
            "execution_id": "test-execution", "image_id": self.image,
            "candidate_mount_roots": (self.mount,), "journal_roots": (self.journal.raw_root, self.journal.delta_root)}
        args.update(overrides)
        channel = cleanup.CleanupChannel.create(self.base / "emergency", **args)
        self.channels.append(channel)
        return channel

    def command(self, argv: list[str], stdout: bytes, *, clean: bool = True) -> str:
        label = "control-" + str(self.counter).zfill(3)
        self.counter += 1
        record: dict[str, Any] = {"argv": ["docker", "--host", "unix://" + self.endpoint.socket_path, *argv],
            "exit_code": 0 if clean else 1, "timed_out": False, "capture_complete": True}
        for kind, raw in (("stdout", stdout), ("stderr", b"")):
            name = label + "-" + kind + ".bin"
            self.journal.retain(name, raw)
            record[kind] = {"path": name, "bytes": len(raw), "observed_bytes": len(raw),
                            "sha256": hashlib.sha256(raw).hexdigest(), "truncated": False}
        self.journal.retain(label + ".json", encoded(record))
        return label + ".json"

    def container(self, channel: cleanup.CleanupChannel, name: str = "candidate-one", *, confirmed: bool = True,
                  keeper: bool = False) -> tuple[str, dict[str, Any]]:
        labels = {"gossip.execution": "test-execution", "gossip.source": "a" * 64,
                  "gossip.fixture": "c" * 64, "gossip.step": name}
        if keeper:
            labels = {"gossip.execution": "test-execution", "gossip.role": "volume-keeper"}
        absent = self.command(["container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"], b"")
        claim = channel.claim_container(name=name, labels=labels, argv=("python", "app.py"), preabsence_record=absent)
        resource_id = hashlib.sha256(name.encode()).hexdigest()
        value = {"Id": resource_id, "Name": "/" + name, "Image": self.image,
                 "Path": "python", "Args": ["app.py"], "Config": {"Labels": labels}}
        self.fake.containers[name] = value
        if confirmed:
            created = self.command(["create", "--name", name, self.image, "python", "app.py"], resource_id.encode() + b"\n")
            channel.confirm_container(claim, create_record=created)
        return claim, value

    def volume(self, channel: cleanup.CleanupChannel, *, confirmed: bool = True) -> tuple[str, dict[str, Any]]:
        name = "owned-volume"
        labels = {"gossip.execution": "test-execution", "gossip.snapshot": "test-volume-v1"}
        options = {"type": "tmpfs", "device": "tmpfs", "o": "size=64m"}
        absent = self.command(["volume", "ls", "--quiet", "--filter", "name=^" + name + "$"], b"")
        claim = channel.claim_volume(name=name, labels=labels, options=options, preabsence_record=absent)
        value = {"Name": name, "Driver": "local", "Labels": labels, "Options": options,
                 "CreatedAt": "2026-10-04T12:00:00Z", "Mountpoint": "/var/lib/docker/volumes/owned-volume/_data", "Scope": "local"}
        self.fake.volumes[name] = value
        if confirmed:
            self.journal.retain("volume-baseline.json", encoded(value))
            channel.confirm_volume(claim, baseline_record="volume-baseline.json")
        return claim, value

    def poison(self) -> chain.PrefixCommitment:
        old = self.journal.commitment
        self.head.lost = True
        with self.assertRaises(chain.ChainUnknown):
            self.journal.retain("uncertain-suffix.json", b"unknown")
        self.assertTrue(self.journal.uncertain)
        return old

    def test_prior_known_id_removes_with_lost_anchor_without_healing(self) -> None:
        channel = self.channel()
        _, resource = self.container(channel)
        prefix = self.poison()
        result = channel.run(reason="anchor lost")
        self.assertTrue(result.all_resources_absent)
        self.assertFalse(result.acceptance_authority)
        self.assertFalse(result.main_journal_healed)
        self.assertEqual(self.journal.commitment, prefix)
        self.assertTrue(self.journal.uncertain)
        self.assertEqual(result.dispositions[0].status, "removed")
        self.assertEqual(self.fake.calls[1], ("DELETE", "/containers/" + resource["Id"] + "?force=1&v=0"))
        with self.assertRaises(chain.ChainError):
            self.journal.read_prior("uncertain-suffix.json")

    def test_acknowledged_claim_recovers_unknown_create_full_id(self) -> None:
        channel = self.channel()
        _, resource = self.container(channel, confirmed=False)
        self.poison()
        result = channel.run(reason="create acknowledgement lost")
        self.assertEqual(result.dispositions[0].resource_id, resource["Id"])
        self.assertTrue(result.all_resources_absent)
        self.assertNotIn(("DELETE", "/containers/candidate-one?force=1&v=0"), self.fake.calls)

    def test_unknown_create_absence_is_not_clean(self) -> None:
        channel = self.channel()
        self.container(channel, confirmed=False)
        self.fake.containers.clear()
        self.poison()
        result = channel.run(reason="unknown create")
        self.assertFalse(result.all_resources_absent)
        self.assertEqual(result.dispositions[0].status, "unknown-create-absent")
        self.assertFalse(any(method == "DELETE" for method, _ in self.fake.calls))

    def test_fresh_lookalike_without_claim_never_enters_roster(self) -> None:
        channel = self.channel()
        self.fake.containers["rogue"] = {"Name": "/rogue", "Id": "d" * 64}
        self.poison()
        result = channel.run(reason="lost before any create")
        self.assertEqual(result.dispositions, ())
        self.assertEqual(self.fake.calls, [])
        self.assertIn("rogue", self.fake.containers)

    def test_fresh_lookalike_wrong_labels_denied(self) -> None:
        channel = self.channel()
        _, resource = self.container(channel, confirmed=False)
        resource["Config"]["Labels"]["gossip.execution"] = "other-execution"
        self.poison()
        result = channel.run(reason="create lost")
        self.assertFalse(result.all_resources_absent)
        self.assertFalse(any(method == "DELETE" for method, _ in self.fake.calls))

    def _deny_field(self, field: str, replacement: Any) -> None:
        channel = self.channel()
        _, resource = self.container(channel)
        resource[field] = replacement
        self.poison()
        result = channel.run(reason="identity mismatch")
        self.assertFalse(result.all_resources_absent)
        self.assertFalse(any(method == "DELETE" for method, _ in self.fake.calls))

    def test_known_image_mismatch_denied(self) -> None:
        self._deny_field("Image", "sha256:" + "e" * 64)

    def test_known_name_mismatch_denied(self) -> None:
        self._deny_field("Name", "/different")

    def test_known_argv_mismatch_denied(self) -> None:
        self._deny_field("Args", ["other.py"])

    def test_known_id_lookalike_id_cannot_replace_original(self) -> None:
        channel = self.channel()
        _, resource = self.container(channel)
        resource["Id"] = "e" * 64
        self.poison()
        result = channel.run(reason="resource replaced")
        self.assertEqual(result.dispositions[0].status, "already-absent")
        self.assertIn("candidate-one", self.fake.containers)
        self.assertFalse(any(method == "DELETE" for method, _ in self.fake.calls))

    def test_tampered_prior_claim_blocks_effect_and_preserves_failure(self) -> None:
        channel = self.channel()
        claim, _ = self.container(channel)
        self.poison()
        (self.journal.raw_root / claim).write_bytes(b"{}")
        result = channel.run(reason="anchor loss is original failure")
        self.assertEqual(result.original_failure, "anchor loss is original failure")
        self.assertFalse(result.all_resources_absent)
        self.assertTrue(result.uncertainties)
        self.assertEqual(self.fake.calls, [])

    def test_failed_preabsence_never_creates_claim(self) -> None:
        channel = self.channel()
        record = self.command(["container", "ls", "--all", "--quiet", "--filter", "name=^/bad$"], b"existing\n")
        with self.assertRaises(cleanup.CleanupError):
            channel.claim_container(name="bad", labels={"gossip.execution": "test-execution", "gossip.role": "keeper"},
                                    argv=("python",), preabsence_record=record)
        self.assertFalse(self.fake.calls)

    def test_failed_prior_command_not_reinterpreted_as_absence(self) -> None:
        channel = self.channel()
        record = self.command(["volume", "ls", "--quiet", "--filter", "name=^bad$"], b"", clean=False)
        with self.assertRaises(cleanup.CleanupError):
            channel.claim_volume(name="bad", labels={"gossip.execution": "test-execution"},
                                 options={"type": "tmpfs"}, preabsence_record=record)

    def test_volume_after_containers_and_keeper_with_complete_identity(self) -> None:
        channel = self.channel()
        _, volume = self.volume(channel)
        self.container(channel, "keeper-one", keeper=True)
        self.container(channel, "finite-one")
        self.poison()
        result = channel.run(reason="lost anchor mid invocation")
        self.assertTrue(result.all_resources_absent)
        self.assertEqual([d.name for d in result.dispositions], ["finite-one", "keeper-one", volume["Name"]])
        self.assertEqual(self.fake.calls[-2], ("DELETE", "/volumes/" + volume["Name"]))

    def test_unconfirmed_volume_identity_cannot_authorize_deletion(self) -> None:
        channel = self.channel()
        self.volume(channel, confirmed=False)
        self.poison()
        result = channel.run(reason="volume baseline lost")
        self.assertFalse(result.all_resources_absent)
        self.assertFalse(any(method == "DELETE" for method, _ in self.fake.calls))

    def test_volume_recreation_identity_mismatch_denied(self) -> None:
        channel = self.channel()
        _, resource = self.volume(channel)
        resource["CreatedAt"] = "2026-10-05T12:00:00Z"
        self.poison()
        result = channel.run(reason="volume changed")
        self.assertFalse(result.all_resources_absent)
        self.assertFalse(any(method == "DELETE" for method, _ in self.fake.calls))

    def test_uncertain_container_cleanup_retains_volume(self) -> None:
        channel = self.channel()
        self.volume(channel)
        self.container(channel)
        self.fake.lose_delete_response = True
        self.poison()
        result = channel.run(reason="probe result lost")
        self.assertFalse(result.all_resources_absent)
        self.assertEqual(result.dispositions[0].status, "removal-unresolved")
        self.assertTrue(result.dispositions[0].removal_attempted)
        self.assertIn("owned-volume", self.fake.volumes)
        self.assertEqual(sum(method == "DELETE" for method, _ in self.fake.calls), 1)
        self.assertFalse(any(path.startswith("/volumes/") for _, path in self.fake.calls))

    def test_normal_attempt_never_retried_or_upgraded_after_absence(self) -> None:
        channel = self.channel()
        claim, _ = self.container(channel)
        channel.note_normal_removal(claim)
        self.fake.containers.clear()
        self.poison()
        result = channel.run(reason="normal remove acknowledgement lost")
        self.assertEqual(result.dispositions[0].status, "prior-removal-unresolved")
        self.assertFalse(result.all_resources_absent)
        self.assertFalse(any(method == "DELETE" for method, _ in self.fake.calls))

    def test_fresh_runtime_mismatch_blocks_all_effects(self) -> None:
        channel = self.channel()
        self.container(channel)
        self.fake.runtime = dict(self.runtime, daemon_id="replacement")
        self.poison()
        result = channel.run(reason="runtime unavailable")
        self.assertFalse(result.all_resources_absent)
        self.assertEqual(self.fake.calls, [])

    def test_cleanup_root_overlap_actual_origin_and_existing_root_rejected(self) -> None:
        with self.assertRaises(cleanup.CleanupError):
            self.channel(candidate_mount_roots=(self.base,))
        with self.assertRaises(cleanup.CleanupError):
            self.channel(journal_roots=(self.base / "not-raw", self.journal.delta_root))
        self.channel()
        with self.assertRaises(FileExistsError):
            self.channel()

    def test_missing_prior_channel_binding_blocks_effects(self) -> None:
        channel = self.channel()
        self.container(channel)
        self.poison()
        (self.journal.raw_root / "emergency-cleanup-channel.json").unlink()
        result = channel.run(reason="main origin gone")
        self.assertFalse(result.all_resources_absent)
        self.assertEqual(self.fake.calls, [])

    def test_storage_bound_fails_closed_before_effect(self) -> None:
        channel = self.channel(limits=cleanup.CleanupLimits(max_files=2))
        self.container(channel)
        self.poison()
        result = channel.run(reason="diagnostic quota")
        self.assertFalse(result.all_resources_absent)
        self.assertTrue(result.uncertainties)
        self.assertEqual(self.fake.calls, [])

    def test_resource_limit_admits_no_extra_claim(self) -> None:
        channel = self.channel(limits=cleanup.CleanupLimits(max_resources=1))
        self.container(channel)
        with self.assertRaises(cleanup.CleanupError):
            self.container(channel, "second")

    def test_diagnostic_unknown_suffix_fails_closed(self) -> None:
        channel = self.channel()
        self.container(channel)
        (channel.root / "unowned.bin").write_bytes(b"foreign")
        self.poison()
        result = channel.run(reason="diagnostic origin changed")
        self.assertFalse(result.all_resources_absent)
        self.assertEqual(self.fake.calls, [])

    def test_one_shot_run_and_claims_after_uncertainty_rejected(self) -> None:
        channel = self.channel()
        self.container(channel)
        self.poison()
        result = channel.run(reason="first and only cleanup")
        self.assertTrue(result.all_resources_absent)
        before = list(self.fake.calls)
        with self.assertRaises(cleanup.CleanupError):
            channel.run(reason="retry is prohibited")
        self.assertEqual(self.fake.calls, before)
        with self.assertRaises(cleanup.CleanupError):
            channel.note_normal_removal("emergency-claim-0000.json")

    def test_retired_containers_then_later_fault_allow_volume_cleanup(self) -> None:
        channel = self.channel()
        self.volume(channel)
        retired_ids = []
        for name in ("first", "second"):
            claim, value = self.container(channel, name)
            retired_ids.append(value["Id"])
            channel.note_normal_removal(claim)
            removed = self.command(["rm", "--force", value["Id"]], value["Id"].encode() + b"\n")
            absent = self.command(["container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"], b"")
            self.fake.containers.pop(name)
            channel.confirm_normal_removal(claim, remove_record=removed, absence_record=absent)
        _, remaining = self.container(channel, "later")
        self.poison()
        result = channel.run(reason="later probe acknowledgement lost")
        self.assertTrue(result.all_resources_absent)
        self.assertEqual([row.status for row in result.dispositions], ["removed", "already-absent", "already-absent", "removed"])
        deletes = [path for method, path in self.fake.calls if method == "DELETE"]
        self.assertEqual(deletes, ["/containers/" + remaining["Id"] + "?force=1&v=0", "/volumes/owned-volume"])
        self.assertFalse(any(resource_id in path for resource_id in retired_ids for path in deletes))

    def test_failed_normal_remove_cannot_be_confirmed_by_later_absence(self) -> None:
        channel = self.channel()
        claim, value = self.container(channel)
        channel.note_normal_removal(claim)
        removed = self.command(["rm", "--force", value["Id"]], b"", clean=False)
        absent = self.command(["container", "ls", "--all", "--quiet", "--filter", "name=^/candidate-one$"], b"")
        with self.assertRaises(cleanup.CleanupError):
            channel.confirm_normal_removal(claim, remove_record=removed, absence_record=absent)
        self.fake.containers.clear()
        self.poison()
        result = channel.run(reason="failed normal remove")
        self.assertFalse(result.all_resources_absent)
        self.assertEqual(result.dispositions[0].status, "prior-removal-unresolved")

    def test_fresh_failed_delete_response_preserved_despite_actual_absence(self) -> None:
        channel = self.channel()
        self.container(channel)
        self.fake.return_delete_error = True
        self.poison()
        result = channel.run(reason="main observation lost")
        self.assertFalse(result.all_resources_absent)
        self.assertEqual(result.dispositions[0].status, "removal-unresolved")
        self.assertEqual(len(self.fake.containers), 0)
        self.assertEqual(sum(method == "DELETE" for method, _ in self.fake.calls), 1)

    def test_total_deadline_exhaustion_blocks_resource_request(self) -> None:
        channel = self.channel(limits=cleanup.CleanupLimits(total_seconds=0.01))
        self.container(channel)
        self.poison()
        with patch.object(cleanup.time, "monotonic", side_effect=[0.0, 0.001, 1.0]):
            result = channel.run(reason="cleanup deadline")
        self.assertFalse(result.all_resources_absent)
        self.assertEqual(self.fake.calls, [])

    def test_diagnostic_fsync_failure_blocks_resource_request(self) -> None:
        channel = self.channel()
        self.container(channel)
        self.poison()
        with patch.object(cleanup.os, "fsync", side_effect=OSError("cleanup fsync lost")):
            result = channel.run(reason="diagnostic acknowledgement lost")
        self.assertFalse(result.all_resources_absent)
        self.assertTrue(any("fsync" in value for value in result.uncertainties))
        self.assertEqual(self.fake.calls, [])

    def test_declared_cli_argv_65536_byte_boundary_remains_admissible(self) -> None:
        channel = self.channel()
        absent = self.command(["container", "ls", "--all", "--quiet", "--filter", "name=^/boundary$"], b"")
        claim = channel.claim_container(name="boundary", labels={"gossip.execution": "test-execution", "gossip.role": "keeper"},
                                        argv=("p", "x" * 65535), preabsence_record=absent)
        self.assertEqual(len(json.loads(self.journal.read(claim))["resource"]["argv"][1]), 65535)

    def test_retirement_metadata_spends_protected_healthy_cleanup_reserve(self) -> None:
        self.journal.close()
        self.head = MemoryHead()
        self.journal = chain.CheckpointChain.create(self.base / "reserve-raw", self.base / "reserve-delta",
            context={"test": "cleanup-reserve"}, authority=self.head,
            limits=chain.Limits(max_files=24, cleanup_files=4))
        channel = self.channel()
        claim, value = self.container(channel)
        channel.note_normal_removal(claim)
        removed = self.command(["rm", "--force", value["Id"]], value["Id"].encode())
        absent = self.command(["container", "ls", "--all", "--quiet", "--filter", "name=^/candidate-one$"], b"")
        while self.journal.commitment.raw_file_count < 20:
            self.journal.retain("padding-" + str(self.journal.commitment.sequence) + ".bin", b"x")
        with self.assertRaises(chain.BoundsExceeded):
            self.journal.retain("ordinary-denied.bin", b"x")
        channel.confirm_normal_removal(claim, remove_record=removed, absence_record=absent)
        self.assertEqual(self.journal.commitment.raw_file_count, 21)
        self.assertFalse(self.journal.uncertain)
        self.assertEqual(json.loads(self.journal.read(claim.removesuffix(".json") + "-retired.json"))["removal_acknowledged"], True)

    def test_keeper_order_independent_of_claim_order(self) -> None:
        channel = self.channel()
        self.container(channel, "finite-first")
        self.volume(channel)
        self.container(channel, "keeper-last", keeper=True)
        self.poison()
        result = channel.run(reason="late anchor loss")
        self.assertTrue(result.all_resources_absent)
        self.assertEqual([row.name for row in result.dispositions], ["finite-first", "keeper-last", "owned-volume"])

    def test_unresolved_finite_cleanup_preserves_keeper_and_volume(self) -> None:
        channel = self.channel()
        self.volume(channel)
        self.container(channel, "keeper", keeper=True)
        self.container(channel, "finite")
        self.fake.lose_delete_response = True
        self.poison()
        result = channel.run(reason="finite removal response lost")
        self.assertFalse(result.all_resources_absent)
        self.assertIn("keeper", self.fake.containers)
        self.assertIn("owned-volume", self.fake.volumes)
        self.assertEqual(sum(method == "DELETE" for method, _ in self.fake.calls), 1)

    def test_result_has_no_acceptance_constructor_override(self) -> None:
        result = cleanup.CleanupResult("failure", (), (), False, str(self.base))
        self.assertFalse(asdict(result)["acceptance_authority"])
        with self.assertRaises(TypeError):
            cleanup.CleanupResult("failure", (), (), False, str(self.base), acceptance_authority=True)  # type: ignore[call-arg]


if __name__ == "__main__":
    unittest.main()
