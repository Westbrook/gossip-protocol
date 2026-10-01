"""A real-Git, scripted coordination lab, not an autonomous-agent benchmark.

The patch DAG, proposal order, acceptance invariant, and repair policy are fixed.
Only the integration topology and announcement transport vary. Announcements
must reach a receiver before it can fetch a proposal. Every integration level's
prepare and verification calls are counted, including the release promotion.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import time
from typing import Callable

from .gitstore import GitStore
from .transport import Event, Mesh


SCENARIOS = ("disjoint", "text_conflict", "semantic_conflict", "inherited_scope")
TOPOLOGIES = ("hub", "maintainers")
TRANSPORTS = ("bus", "gossip")
PEERS = (
    "worker-alpha", "worker-beta", "worker-alpha-2", "maintainer-alpha",
    "maintainer-beta", "integrator", "release", "observer",
)
EXPECTED = {
    "disjoint": {"alpha": 1, "beta": 2},
    "text_conflict": {"alpha": 1, "beta": 0},
    "semantic_conflict": {"alpha": 6, "beta": 0},
    "inherited_scope": {"alpha": 0, "beta": 0},
}


def _allocation(value: int) -> str:
    return json.dumps({"allocation": value}, sort_keys=True) + "\n"


def _git(path: Path, *args: str) -> str:
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update({"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_NO_REPLACE_OBJECTS": "1"})
    result = subprocess.run(
        ["git", "-C", str(path), *args], check=True,
        capture_output=True, text=True, env=env, timeout=60,
    )
    return result.stdout.strip()


def _state(path: Path) -> dict[str, int]:
    return {
        name: json.loads((path / f"{name}.json").read_text())["allocation"]
        for name in ("alpha", "beta")
    }


def _validate(path: Path, subsystem: str | None = None) -> tuple[bool, str]:
    """Subsystem gates enforce local bounds; release also enforces shared sum."""
    try:
        state = _state(path)
    except (OSError, ValueError, KeyError, TypeError) as error:
        return False, f"Unreadable allocation fixture: {error}"
    return _validate_state(state, subsystem)


def _validate_state(state: dict[str, int], subsystem: str | None = None) -> tuple[bool, str]:
    checked = (subsystem,) if subsystem is not None else ("alpha", "beta")
    if any(type(state[name]) is not int or not 0 <= state[name] <= 10
           for name in checked):
        return False, f"Local allocation must be an integer in [0, 10]: {state}"
    if subsystem is None and sum(state.values()) > 10:
        return False, f"Shared invariant failed: alpha + beta = {sum(state.values())} > 10"
    return True, f"{'Local ' + subsystem if subsystem else 'Global'} allocation invariant passed: {state}"


@dataclass(frozen=True)
class Proposal:
    task_id: str
    producer: str
    subsystem: str
    source: GitStore
    base_sha: str
    tip_sha: str
    dependencies: tuple[str, ...] = ()

    def manifest(self) -> dict:
        return {
            "task_id": self.task_id, "base_sha": self.base_sha,
            "tip_sha": self.tip_sha, "subsystem": self.subsystem,
            "allowed_paths": [f"{self.subsystem}.json"],
            "dependencies": list(self.dependencies),
        }


@dataclass(frozen=True)
class Fixture:
    initial: GitStore
    proposals: tuple[Proposal, ...]
    unpublished_prerequisite: str | None = None


def _fixture(root: Path, scenario: str) -> Fixture:
    initial = GitStore.create(root / "initial", {
        "alpha.json": _allocation(0), "beta.json": _allocation(0),
    })
    base = initial.head()

    def proposal(task_id: str, subsystem: str, value: int,
                 producer: str | None = None) -> Proposal:
        source = GitStore.fork(initial, root / task_id)
        tip = source.propose(
            {f"{subsystem}.json": _allocation(value)},
            base_sha=base, message=f"{task_id}: allocate {value}",
        )
        return Proposal(task_id, producer or f"worker-{subsystem}", subsystem,
                        source, base, tip)

    if scenario == "disjoint":
        return Fixture(initial, (proposal("alpha-one", "alpha", 1),
                                 proposal("beta-two", "beta", 2)))
    if scenario == "text_conflict":
        return Fixture(initial, (proposal("alpha-one", "alpha", 1),
                                 proposal("alpha-two", "alpha", 2,
                                          "worker-alpha-2")))
    if scenario == "semantic_conflict":
        return Fixture(initial, (proposal("alpha-six", "alpha", 6),
                                 proposal("beta-six", "beta", 6)))
    if scenario == "inherited_scope":
        source = GitStore.fork(initial, root / "beta-child")
        parent = source.propose(
            {"alpha.json": _allocation(2)}, base_sha=base,
            message="unpublished-alpha: allocate 2",
        )
        tip = source.propose(
            {"beta.json": _allocation(3)}, base_sha=parent,
            message="beta-child: allocate 3",
        )
        return Fixture(initial, (Proposal(
            "beta-child", "worker-beta", "beta", source, parent, tip, (parent,),
        ),), parent)
    raise ValueError(f"Unknown scenario: {scenario}")


class _Case:
    def __init__(self, root: Path, fixture: Fixture, scenario: str,
                 topology: str, transport: str, seed: int, trace: list[dict]) -> None:
        self.root = root
        self.fixture = fixture
        self.scenario = scenario
        self.topology = topology
        self.transport = transport
        self.seed = seed
        self.trace = trace
        self.mesh = Mesh(PEERS, mode=transport, seed=seed, fanout=3, broker="release")
        self.sequence: dict[str, int] = {}
        self.operations: list[dict] = []
        self.verifications: list[dict] = []
        self.release = GitStore.fork(fixture.initial, root / "release")

    def record(self, kind: str, **details: object) -> None:
        self.trace.append({
            "index": len(self.trace), "scenario": self.scenario,
            "topology": self.topology, "transport": self.transport,
            "seed": self.seed, "round": self.mesh.stats["rounds"],
            "kind": kind, **details,
        })

    def publish(self, producer: str, kind: str, payload: dict) -> Event:
        sequence = self.sequence.get(producer, 0)
        self.sequence[producer] = sequence + 1
        event = Event.create(producer, sequence, kind, payload, topic="allocation")
        self.mesh.publish(producer, event)
        self.record("notice_published", event=event.to_dict())
        return event

    def receive(self, recipient: str, event: Event) -> None:
        for _ in range(100):
            if self.mesh.has(recipient, event.event_id):
                self.record("notice_received", recipient=recipient,
                            event_id=event.event_id,
                            detail="Recipient may now fetch the immutable offered commit")
                return
            self.mesh.step()
        raise RuntimeError(f"Notice did not reach {recipient} after 100 rounds")

    def integrate(self, store: GitStore, source: GitStore, offered_sha: str,
                  stage: str, label: str, allowed_paths: tuple[str, ...] | None,
                  subsystem: str | None = None) -> str:
        expected = store.head()

        def validator(path: Path) -> tuple[bool, str]:
            okay, detail = _validate(path, subsystem)
            result = {"stage": stage, "label": label, "passed": okay,
                      "detail": detail, "candidate_sha": _git(path, "rev-parse", "HEAD")}
            self.verifications.append(result)
            self.record("verification", **result)
            return okay, detail

        candidate = store.prepare(
            source, offered_sha, expected, validator=validator,
            allowed_paths=allowed_paths,
        )
        operation = {
            "stage": stage, "label": label, "offered_sha": offered_sha,
            "old_head": expected, "candidate_sha": candidate.candidate_sha,
            "prepare_status": candidate.status, "status": candidate.status,
            "changed_paths": list(candidate.changed_paths), "detail": candidate.detail,
        }
        self.record("prepare", **operation)
        if candidate.status == "prepared":
            result = store.accept(candidate)
            operation["status"] = result.status
            operation["new_head"] = result.new_head
            self.record("promotion", stage=stage, label=label, status=result.status,
                        tested_sha=candidate.candidate_sha, new_head=result.new_head,
                        old_head=expected)
            if result.status != "accepted":
                raise RuntimeError(f"Unexpected promotion failure without concurrency: {result.status}")
            if result.new_head != candidate.candidate_sha:
                raise AssertionError("Promoted commit differs from the tested candidate")
        else:
            operation["new_head"] = store.head()
            if store.head() != expected:
                raise AssertionError("A rejected proposal advanced the accepted ref")
        self.operations.append(operation)
        return operation["status"]

    def run(self) -> dict:
        started = time.perf_counter()
        self.record("case_started", base_sha=self.fixture.initial.head(),
                    proposal_order=[p.task_id for p in self.fixture.proposals])
        # All leaves become available together; processing order is fixed even
        # when transport delivery order differs. This is not a latency race.
        notices = {p.task_id: self.publish(p.producer, "proposal", p.manifest())
                   for p in self.fixture.proposals}
        maintainer_heads: dict[str, str] = {}
        integration_head: str | None = None
        if self.topology == "hub":
            for proposal in self.fixture.proposals:
                self.receive("release", notices[proposal.task_id])
                self.integrate(self.release, proposal.source, proposal.tip_sha,
                               "release", proposal.task_id,
                               (f"{proposal.subsystem}.json",))
        elif self.topology == "maintainers":
            maintainers = {name: GitStore.fork(self.fixture.initial,
                                             self.root / f"maintainer-{name}")
                           for name in ("alpha", "beta")}
            integration = GitStore.fork(self.fixture.initial, self.root / "integration")
            for proposal in self.fixture.proposals:
                self.receive(f"maintainer-{proposal.subsystem}", notices[proposal.task_id])
                self.integrate(maintainers[proposal.subsystem], proposal.source,
                               proposal.tip_sha, "subsystem", proposal.task_id,
                               (f"{proposal.subsystem}.json",), proposal.subsystem)
            maintainer_heads = {name: repo.head() for name, repo in maintainers.items()}
            # Announce final subsystem tips only after local processing. All
            # local work is still counted; batched tips are not free progress.
            tips = {name: self.publish(f"maintainer-{name}", "subsystem_tip", {
                "subsystem": name, "tip_sha": repo.head(),
            }) for name, repo in maintainers.items()
                    if repo.head() != self.fixture.initial.head()}
            for name, notice in tips.items():
                self.receive("integrator", notice)
                self.integrate(integration, maintainers[name], maintainers[name].head(),
                               "integration", f"subsystem-{name}", (f"{name}.json",))
            integration_head = integration.head()
            if integration_head != self.fixture.initial.head():
                notice = self.publish("integrator", "integration_tip", {
                    "tip_sha": integration_head,
                })
                self.receive("release", notice)
                self.integrate(self.release, integration, integration_head,
                               "release", "tested-integration", None)
                if self.release.head() != integration_head:
                    raise AssertionError("Release must promote the exact integrated tip")
        else:
            raise ValueError(f"Unknown topology: {self.topology}")

        release_files = self.release.read_files()
        state = {name: json.loads(release_files[f"{name}.json"])["allocation"]
                 for name in ("alpha", "beta")}
        accepted_history = set(_git(self.release.path, "rev-list", self.release.head()).splitlines())
        accepted = [proposal.task_id for proposal in self.fixture.proposals
                    if proposal.tip_sha in accepted_history]
        if state != EXPECTED[self.scenario]:
            raise AssertionError(f"Wrong release state for {self.scenario}: {state}")
        global_okay, _ = _validate_state(state)
        if not global_okay:
            raise AssertionError("Global release invariant was violated")
        result = {
            "scenario": self.scenario, "topology": self.topology,
            "transport": self.transport, "seed": self.seed,
            "base_sha": self.fixture.initial.head(),
            "proposals": [proposal.manifest() for proposal in self.fixture.proposals],
            "unpublished_prerequisite": self.fixture.unpublished_prerequisite,
            "release_head": self.release.head(),
            "release_tree": _git(self.release.path, "rev-parse", "HEAD^{tree}"),
            "release_state": state, "accepted_task_ids": accepted,
            "maintainer_heads": maintainer_heads, "integration_head": integration_head,
            "operations": self.operations, "verifications": self.verifications,
            "metrics": {
                "prepare_calls": len(self.operations),
                "verification_calls": len(self.verifications),
                "verification_passes": sum(v["passed"] for v in self.verifications),
                "accepted_promotions": sum(o["status"] == "accepted" for o in self.operations),
                "transport": self.mesh.stats,
                "elapsed_seconds_diagnostic_only": time.perf_counter() - started,
            },
        }
        self.record("case_finished", release_head=result["release_head"],
                    release_state=state, accepted_task_ids=accepted,
                    metrics=result["metrics"])
        return result


def run_matrix(output_dir: Path, seeds: tuple[int, ...] = (0, 1, 2), *,
               progress: Callable[[str], None] | None = None) -> dict:
    """Write a reproducible matrix into a fresh output directory.

    Existing nonempty output directories are refused rather than overwritten.
    Results retain immutable proposal hashes and exact release trees. Wall times
    are diagnostic host-dependent observations, not a topology speed comparison.
    """
    output_dir = Path(output_dir)
    if not seeds or any(type(seed) is not int for seed in seeds) or len(set(seeds)) != len(seeds):
        raise ValueError("seeds must be distinct integers and cannot be empty")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"Refusing to overwrite a nonempty run directory: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    trace: list[dict] = []
    results: list[dict] = []
    # Repositories are disjoint and fixture sources become immutable before
    # case execution. Bounded concurrency amortizes Git process startup while
    # ordered map/concatenation keeps result and trace order reproducible.
    def build_fixture(scenario: str) -> Fixture:
        fixture = _fixture(output_dir / "fixtures" / scenario, scenario)
        if progress is not None:
            progress(f"Fixture ready: {scenario}")
        return fixture

    with ThreadPoolExecutor(max_workers=4) as pool:
        fixture_values = pool.map(
            build_fixture, SCENARIOS,
        )
        fixtures = dict(zip(SCENARIOS, fixture_values))
    jobs = [(seed, scenario, topology, transport)
            for seed in seeds for scenario in SCENARIOS
            for topology in TOPOLOGIES for transport in TRANSPORTS]

    def run_case(job: tuple[int, str, str, str]) -> tuple[dict, list[dict]]:
        seed, scenario, topology, transport = job
        local_trace: list[dict] = []
        root = output_dir / "cases" / f"seed-{seed}" / scenario / f"{topology}-{transport}"
        result = _Case(root, fixtures[scenario], scenario, topology,
                       transport, seed, local_trace).run()
        if progress is not None:
            progress(f"Case verified: seed={seed} {scenario} {topology}/{transport}; "
                     f"release={result['release_state']}; "
                     f"prepares={result['metrics']['prepare_calls']}, "
                     f"verifications={result['metrics']['verification_calls']}")
        return result, local_trace

    with ThreadPoolExecutor(max_workers=4) as pool:
        for result, local_trace in pool.map(run_case, jobs):
            results.append(result)
            for row in local_trace:
                row["index"] = len(trace)
                trace.append(row)
    report = {
        "schema_version": 1,
        "experiment": "scripted-real-git-integration-and-transport",
        "seeds": list(seeds), "cases": results,
        "limitations": [
            "Scripted edits and deterministic gates; no live LLM agents, autonomy, token costs, or project-economics conclusions.",
            "Independent local repositories model delegated integration authority, not independent hosts or Linux development at scale.",
            "No repair policy: rejected proposals remain rejected, making release-state comparisons explicit.",
            "Fixed proposal processing order waits for announcements; transport rounds are not completion-latency evidence.",
            "All prepare and verification calls at subsystem, integration, and release levels are counted; lower root pull count is not lower total cost.",
            "Transport contacts are model operations, not bytes, actual sockets, model tokens, or production event-bus cost.",
            "The bus uses a release-broker star with synchronous anti-entropy snapshots; gossip uses seeded bounded-fanout push-pull exchanges.",
            "No leases or crash-recovery integration in this lab; those correctness mechanisms are tested separately.",
            "Elapsed seconds depend on the host, filesystem, caches, and execution order; they are diagnostic only.",
        ],
    }
    (output_dir / "results.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    (output_dir / "trace.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in trace)
    )
    return report
