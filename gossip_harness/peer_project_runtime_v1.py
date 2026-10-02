"""Autonomous offline coding proposals and receiver-local public promotion.

Only trusted bootstrap configuration names repositories or evaluation inputs.
The wire carries immutable payload descriptors and proposal evidence; it never
conveys a sender path or a synchronous execute/promotion command. Public Git
promotion is not independent final acceptance or project completion.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import threading
from typing import Any

from .gitstore import GitStore
from .peer_authority_v1 import _Server as AuthorityServer
from .peer_candidate_v1 import CandidatePublisher
from .peer_coding_runtime_v1 import CodingAuthority, CodingPeer, MAX_CONFIG_BYTES, _digest, fixture_workers
from .peer_payload_runtime_v1 import PayloadPeer, PayloadServer
from .peer_promotion_v1 import ReceivedPromotion, SimulatedCrash
from .peer_runtime_v1 import TransportError, _save
from .peer_store_v1 import canonical_bytes, strict_loads

PROTOCOL = "peer-project-runtime-v1"
MAX_OBSERVED_ATTEMPTS = 8
MAX_OBSERVER_BYTES = 12_000


def _candidate_summary(record: dict | None) -> dict | None:
    if record is None:
        return None
    return {"phase": record["phase"], "action_id": record["intent"]["action_id"],
            "proposal_id": record["intent"]["proposal_id"], "offered_sha": record["offered_sha"],
            "bundle_sha256": (record.get("bundle_manifest") or {}).get("bundle_sha256"),
            "bundle_notice_event_id": record["bundle_notice_event_id"], "offer_event_id": record["offer_event_id"]}


def _promotion_summary(state: dict) -> dict:
    """Bound observer output independently of source, suite and receipt sizes."""
    attempts = state["attempts"]
    summaries = []
    for record in attempts[-MAX_OBSERVED_ATTEMPTS:]:
        offer = record["offer"]
        candidate = record.get("candidate") or {}
        validation = record.get("validation") or {}
        receipt = validation.get("receipt") or {}
        summaries.append({"attempt_id": record["attempt_id"], "status": record["status"],
                          "offer_event_id": record["offer_event_id"], "action_id": offer["action_id"],
                          "offered_sha": offer["bundle_manifest"]["offered_sha"],
                          "candidate_sha": candidate.get("candidate_sha"), "head": record.get("head"),
                          "validation": {"receipt_sha256": validation.get("receipt_sha256"),
                                         "source_sha256": validation.get("source_sha256"),
                                         "container_name": receipt.get("container_name"),
                                         "image_id": receipt.get("image_id"), "passed": receipt.get("passed"),
                                         "cleanup_verified": receipt.get("cleanup_verified"),
                                         "status": receipt.get("status"),
                                         "physically_executed": validation.get("physically_executed")}})
    result = {"protocol": state["protocol"], "config_sha256": state["config_sha256"],
              "attempt_count": len(attempts), "omitted_attempts": max(0, len(attempts) - MAX_OBSERVED_ATTEMPTS),
              "attempts": summaries}
    canonical_bytes(result, max_bytes=MAX_OBSERVER_BYTES)
    return result


def _bind_configuration(root: Path, config: dict) -> str:
    """Persist exact local policy while allowing ephemeral ports to be restored."""
    stable = strict_loads(canonical_bytes(config, max_bytes=MAX_CONFIG_BYTES), max_bytes=MAX_CONFIG_BYTES)
    stable["root"] = str(root.resolve())
    stable.pop("port")
    stable.pop("authority_port", None)
    if stable["role"] == "candidate-worker":
        stable["authority"].pop("port")
    identity = {"protocol": PROTOCOL, "configuration_sha256": hashlib.sha256(
        canonical_bytes(stable, max_bytes=MAX_CONFIG_BYTES)).hexdigest(),
        "runtime_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "repository": str((root / "repository.git").resolve()) if stable["role"] != "peer" else None}
    path = root / "project-identity.json"
    if path.exists():
        if canonical_bytes(strict_loads(path.read_bytes(), max_bytes=4096)) != canonical_bytes(identity):
            raise ValueError("Project process identity changed across restart")
    else:
        _save(path, identity)
    return _digest(identity)


def _repository(root: Path, baseline_sha: str) -> GitStore:
    store = GitStore(root / "repository.git")
    if (type(baseline_sha) is not str or len(baseline_sha) != 40
            or any(c not in "0123456789abcdef" for c in baseline_sha)
            or store._commit(baseline_sha) != baseline_sha):
        raise ValueError("Project bootstrap baseline is not an exact commit")
    store._git("merge-base", "--is-ancestor", baseline_sha, store.head())
    return store


class CandidatePeer(CodingPeer):
    """The existing coding timer continues into one durable proposal outbox."""

    def __init__(self, root: Path, node_id: str, mode: str, token: str, *,
                 coding: dict, authority: dict, candidate: dict, interval: float = 0.1,
                 fanout: int = 2):
        if (type(candidate) is not dict or set(candidate) != {"baseline_sha", "allowed_paths", "generation"}
                or type(candidate["allowed_paths"]) is not list or type(candidate["generation"]) is not int
                or candidate["generation"] != 0):
            raise ValueError("Invalid candidate policy")
        super().__init__(root, node_id, mode, token, coding=coding, authority=authority,
                         interval=interval, fanout=fanout)
        try:
            store = _repository(root, candidate["baseline_sha"])
            self.candidates = CandidatePublisher(root / "candidate-journal", self, store,
                                                baseline_sha=candidate["baseline_sha"],
                                                allowed_paths=tuple(candidate["allowed_paths"]), generation=0)
            self.candidate_snapshot = _candidate_summary(self.candidates.state())
            self.project_error: str | None = None
        except BaseException:
            self.owner.close()
            raise

    def dispatch(self, body: dict) -> Any:
        result = super().dispatch(body)
        if body.get("sender") == "driver" and body.get("operation") == "state":
            with self.lock:
                result["candidate"] = strict_loads(canonical_bytes(self.candidate_snapshot))
                result["project_error"] = self.project_error
        return result

    def run_ticks(self) -> None:
        while not self.stop.wait(self.interval):
            with self.lock:
                enabled = self.settings["enabled"]
            if enabled:
                try:
                    self.turn()
                    self.payload_turn()
                    self.coding_tick()
                    snapshot = _candidate_summary(self.candidates.tick())
                    with self.lock:
                        self.candidate_snapshot = snapshot
                except Exception as error:
                    self.project_error = type(error).__name__
                    self.fatal_error = type(error).__name__
                    self.stop.set()


class PromotionPeer(PayloadPeer):
    """Transport and the serial public-promotion timer have separate lifetimes."""

    def __init__(self, root: Path, node_id: str, mode: str, token: str, *,
                 initially_enabled: bool, interval: float = 0.1, fanout: int = 2):
        if type(initially_enabled) is not bool:
            raise ValueError("Promotion gate must be explicitly declared")
        super().__init__(root, node_id, mode, token, interval=interval, fanout=fanout)
        try:
            self.promotion: ReceivedPromotion | None = None
            self.promotion_error: str | None = None
            self.promotion_snapshot: dict | None = None
            self.promotion_busy = False
            self.gate_path = root / "promotion-gate.json"
            if self.gate_path.exists():
                value = strict_loads(self.gate_path.read_bytes(), max_bytes=1024)
                if type(value) is not dict or set(value) != {"enabled"} or type(value["enabled"]) is not bool:
                    raise ValueError("Corrupt durable promotion gate")
                self.promotion_enabled = value["enabled"]
            else:
                _save(self.gate_path, {"enabled": initially_enabled})
                self.promotion_enabled = initially_enabled
        except BaseException:
            self.owner.close()
            raise

    def dispatch(self, body: dict) -> Any:
        if body.get("operation") == "promotion_gate":
            # Reuse all base schema/principal checks through its read-only path.
            # A gate changes eligibility for future local ticks, never executes
            # work and never resets an adapter's failed/terminal journal.
            if body.get("sender") != "driver":
                raise TransportError("Only the fixture observer can configure the declared gate")
            super().dispatch({**body, "operation": "state", "payload": {}})
            value = body.get("payload")
            if type(value) is not dict or set(value) != {"enabled"} or type(value["enabled"]) is not bool:
                raise TransportError("Invalid promotion gate setting")
            with self.lock:
                _save(self.gate_path, value)
                self.promotion_enabled = value["enabled"]
            return {"enabled": value["enabled"]}
        result = super().dispatch(body)
        if body.get("sender") == "driver" and body.get("operation") == "state":
            with self.lock:
                result["promotion"] = strict_loads(canonical_bytes(self.promotion_snapshot))
                result["promotion_error"] = self.promotion_error
                result["promotion_busy"] = self.promotion_busy
                result["promotion_enabled"] = self.promotion_enabled
        return result

    def run_promotions(self) -> None:
        while not self.stop.wait(self.interval):
            with self.lock:
                enabled = self.settings["enabled"] and self.promotion_enabled
            if enabled and self.promotion is not None:
                with self.lock:
                    self.promotion_busy = True
                try:
                    self.promotion.tick()
                except SimulatedCrash:
                    # Explicit process qualification hook, never candidate data.
                    os._exit(85)
                except Exception as error:
                    with self.lock:
                        self.promotion_error = type(error).__name__
                    # Keep mesh and observer availability. The adapter retains
                    # its failure; do not automatically retry until green.
                    return
                finally:
                    try:
                        snapshot = _promotion_summary(self.promotion.state())
                        with self.lock:
                            self.promotion_snapshot = snapshot
                    except Exception as error:
                        with self.lock:
                            if self.promotion_error is None:
                                self.promotion_error = type(error).__name__
                    finally:
                        with self.lock:
                            self.promotion_busy = False
                if self.promotion_error is not None:
                    return


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--promotion-crash-point", choices=["after_admission", "after_validation", "after_intent", "after_git", "after_promotion"])
    args = parser.parse_args()
    config = strict_loads(args.config.read_bytes(), max_bytes=MAX_CONFIG_BYTES)
    common = {"root", "node_id", "mode", "token", "port", "interval", "fanout", "role"}
    extras = {"peer": {"seed"}, "candidate-worker": {"coding", "authority", "candidate"},
              "promotion-service": {"authority", "authority_port", "task_specs", "profiles", "promotion"}}
    if (type(config) is not dict or type(config.get("role")) is not str or config["role"] not in extras
            or set(config) != common | extras[config["role"]]
            or type(config["port"]) is not int or not 0 <= config["port"] <= 65535):
        raise ValueError("Invalid project process configuration")
    role, root = config["role"], Path(config["root"])
    if args.promotion_crash_point is not None and role != "promotion-service":
        raise ValueError("Promotion fault requires the promotion service")
    kwargs = {key: config[key] for key in ("interval", "fanout")}
    if role == "candidate-worker":
        peer: PayloadPeer = CandidatePeer(root, config["node_id"], config["mode"], config["token"],
                                           coding=config["coding"], authority=config["authority"],
                                           candidate=config["candidate"], **kwargs)
    elif role == "promotion-service":
        promotion = config["promotion"]
        if (type(promotion) is not dict or set(promotion) != {"baseline_sha", "cases", "image", "timeout_seconds",
                                                            "case_timeout_seconds", "initially_enabled"}
                or type(config["authority_port"]) is not int or not 0 <= config["authority_port"] <= 65535):
            raise ValueError("Invalid promotion configuration")
        peer = PromotionPeer(root, config["node_id"], config["mode"], config["token"],
                             initially_enabled=promotion["initially_enabled"], **kwargs)
    else:
        peer = PayloadPeer(root, config["node_id"], config["mode"], config["token"], **kwargs)
    authority: CodingAuthority | None = None
    authority_server: AuthorityServer | None = None
    authority_thread: threading.Thread | None = None
    promotion_thread: threading.Thread | None = None
    tick_thread: threading.Thread | None = None
    try:
        identity = _bind_configuration(root, config)
        if role == "peer" and config["seed"] is not None:
            raw = canonical_bytes(config["seed"], max_bytes=600_000)
            peer.publish_payload(raw, "application/json", _digest([PROTOCOL, "provided-seed", hashlib.sha256(raw).hexdigest()]))
        if isinstance(peer, PromotionPeer):
            target = _repository(root, config["promotion"]["baseline_sha"])
            workers, transport_identity = fixture_workers(config["profiles"], root / "offline-transport.jsonl")
            authority = CodingAuthority(root / "authority", config["authority"], peer=peer,
                                        workers=workers, task_specs=config["task_specs"], transport_identity=transport_identity)
            peer.promotion = ReceivedPromotion(root / "promotion-journal", peer=peer, coding=authority.coding,
                                               target=target, cases=config["promotion"]["cases"], image=config["promotion"]["image"],
                                               timeout_seconds=config["promotion"]["timeout_seconds"],
                                               case_timeout_seconds=config["promotion"]["case_timeout_seconds"],
                                               crash_at=args.promotion_crash_point)
            peer.promotion_snapshot = _promotion_summary(peer.promotion.state())
            authority_server = AuthorityServer(config["authority_port"], authority)
            authority_thread = threading.Thread(target=authority_server.serve_forever,
                                                kwargs={"poll_interval": 0.1}, name="coding-authority", daemon=True)
            authority_thread.start()
        with PayloadServer(config["port"], peer) as server:
            tick_thread = threading.Thread(target=peer.run_ticks, name="project-peer-timer", daemon=True)
            tick_thread.start()
            if isinstance(peer, PromotionPeer):
                promotion_thread = threading.Thread(target=peer.run_promotions, name="serial-public-promotion", daemon=True)
                promotion_thread.start()
            ready = {"protocol": PROTOCOL, "node_id": peer.node_id, "role": role, "pid": os.getpid(),
                     "port": server.server_address[1], "configuration_sha256": identity,
                     "provider_mode": "offline-response-fixture"}
            if authority is not None and authority_server is not None:
                ready.update(authority_port=authority_server.server_address[1], config_sha256=authority.config_sha256,
                             dispatch_config_sha256=authority.coding.config_sha256)
            print(json.dumps(ready), flush=True)
            try:
                server.serve_forever(poll_interval=0.1)
            finally:
                peer.stop.set()
                tick_thread.join()
                if promotion_thread is not None:
                    promotion_thread.join()
    finally:
        peer.stop.set()
        for thread in (tick_thread, promotion_thread):
            if thread is not None and thread.is_alive():
                thread.join()
        if authority_server is not None:
            authority_server.shutdown()
            if authority_thread is not None:
                authority_thread.join()
            authority_server.server_close()
        if isinstance(peer, PromotionPeer) and peer.promotion is not None:
            peer.promotion.close()
        if authority is not None:
            authority.close()
        peer.owner.close()


if __name__ == "__main__":
    main()
