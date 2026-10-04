"""Original browser/Engine reader and host predicates with false/unknown separation.

Pure comparison helpers have no physical authority. read_original requires the
actual owner and current external checkpoint and reconstructs original process,
request, role and donor evidence. A browser timeout is no product latency test.
"""
from __future__ import annotations
from dataclasses import asdict, dataclass
import base64
import re
import json
from pathlib import Path
from typing import Any
from . import candidate_product_browser_cases_v1 as cases
from . import candidate_product_browser_transport_v1 as transport
from . import candidate_product_process_execution_v1 as base
from . import candidate_product_process_reader_v1 as reader_base
from . import candidate_product_process_observation_v1 as semantic
from . import candidate_http_transport_v1 as wire
from . import candidate_client_process_v4 as engine
from . import candidate_checkpoint_chain_v1 as chain

PROTOCOL = "candidate-product-browser-observation-v1"


@dataclass(frozen=True)
class Facet:
    selector: str
    state: str
    discrepancies: tuple[str, ...]
    limitations: tuple[str, ...]


@dataclass(frozen=True)
class BrowserObservation:
    protocol: str
    execution_id: str
    binding_sha256: str
    checkpoint_sha256: str
    facets: tuple[Facet, ...]
    known_discrepancies: tuple[str, ...]
    unavailable: tuple[str, ...]
    cleanup_verified: bool
    physical: bool = True
    acceptance_authority: bool = False
    whole_product_acceptance: bool = False
    independent_purpose_credit: bool = False


class ProductDiscrepancy(ValueError):
    """A complete product message is syntactically or semantically wrong."""


def require(value: bool, message: str) -> None:
    if not value:
        raise ValueError(message)


def exact(left: Any, right: Any) -> bool:
    return semantic._exact(left, right)


def product_json(raw: bytes) -> Any:
    require(type(raw) is bytes, "Original complete bytes required")
    try:
        return semantic._strict_json(raw)
    except (semantic.AmbiguousJSON, semantic.ObservationLimit, RecursionError):
        raise
    except (ValueError, UnicodeError) as error:
        raise ProductDiscrepancy("Complete product JSON is malformed") from error


def expected_mutation(action: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    operation = action["op"]
    if operation in ("submit", "submit_error"):
        body = {"job_id": action["job"]["job_id"] if operation == "submit" else action["identifier"], action["kind"]: action["path"]}
        if action["namespace"] is not None:
            body["namespace"] = action["namespace"]
        return "/api/jobs", body
    if operation == "action":
        return "/api/jobs/" + action["job"]["job_id"] + "/" + action["action"], (
            {"epoch": action["job"]["epoch"]} if action["action"] == "commit" else {})
    if operation == "import":
        return "/api/import", {"source": action["path"]}
    return None


def mutation_findings(action: dict[str, Any], messages: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    expected = expected_mutation(action)
    if expected is None:
        return [], []
    target, body = expected
    found = [message for message in messages if message.get("kind") == "request" and message.get("source") == "browser"
             and message.get("action_id") == action["id"] and message["request"].get("method") == "POST"
             and message["request"].get("target") == target]
    if not found:
        return [], ["No original browser-generated expected mutation was observed"]
    wrong, missing = [], []
    for message in found:
        raw = base64.b64decode(message["request"]["body_b64"], validate=True)
        require(cases.sha(raw) == message["request"]["body_sha256"], "Original browser body descriptor differs")
        try:
            actual = product_json(raw)
            if not exact(actual, body):
                wrong.append("Browser-generated mutation differs from exact public body: " + target)
        except ProductDiscrepancy as error:
            wrong.append(str(error))
        except (semantic.AmbiguousJSON, semantic.ObservationLimit, RecursionError) as error:
            missing.append(type(error).__name__ + ":" + str(error))
    return wrong, missing


def api_findings(action: dict[str, Any], responses: dict[str, tuple[int, bytes] | str]) -> tuple[list[str], list[str]]:
    wrong, missing = [], []
    for target in ("/api/jobs", "/api/documents"):
        value = responses.get(target, "Original control response unavailable")
        if type(value) is str:
            missing.append(target + ":" + value)
            continue
        assert isinstance(value, tuple)
        status, raw = value
        if status != 200:
            wrong.append(target + " complete status differs from 200")
            continue
        try:
            parsed = product_json(raw)
            if target == "/api/jobs":
                if type(parsed) is not dict or type(parsed.get("jobs")) is not list:
                    raise ProductDiscrepancy("Complete jobs response has wrong shape")
                actual_jobs = parsed["jobs"]
                if "jobs" in action:
                    if not exact(actual_jobs, action["jobs"]):
                        wrong.append("Persisted API jobs differ from declared state")
                elif "job" in action:
                    selected = [row for row in actual_jobs if type(row) is dict and row.get("job_id") == action["job"]["job_id"]]
                    if not exact(selected, [action["job"]]):
                        wrong.append("Persisted selected API job differs from declared state")
            else:
                if type(parsed) is not dict or type(parsed.get("documents")) is not list or type(parsed.get("total")) is not int:
                    raise ProductDiscrepancy("Complete document response has wrong shape")
                document_rows: list[list[str]] = []
                for row in parsed["documents"]:
                    if type(row) is not dict or type(row.get("source")) is not str or type(row.get("text")) is not str:
                        raise ProductDiscrepancy("Complete document row has wrong type")
                    document_rows.append([row["source"], row["text"]])
                if not exact(document_rows, action["documents"]) or parsed["total"] != len(action["documents"]):
                    wrong.append("Committed document graph differs from declared source/text rows")
        except ProductDiscrepancy as error:
            wrong.append(target + ":" + str(error))
        except (semantic.AmbiguousJSON, semantic.ObservationLimit, RecursionError) as error:
            missing.append(target + ":" + type(error).__name__ + ":" + str(error))
    return wrong, missing


def job_census_findings(fact: dict[str, Any], response: tuple[int, bytes] | str | None
                       ) -> tuple[list[str], list[str], dict[str, dict[str, Any]]]:
    """Join the current complete API inventory to visible rows, allowing flexible text.

    Matching is one-to-one, so a row mentioning another ID incidentally does not
    impose a new punctuation/layout rule. An absent expected row stays unknown.
    """
    if not isinstance(response, tuple) or response[0] != 200:
        return [], ["Complete API job inventory unavailable for DOM census"], {}
    try:
        value = product_json(response[1])
        require(type(value) is dict and type(value.get("jobs")) is list, "API job inventory shape unavailable")
        identifiers = [row["job_id"] for row in value["jobs"]]
        require(all(type(identifier) is str and identifier for identifier in identifiers)
                and len(set(identifiers)) == len(identifiers), "API job identities unavailable or duplicate")
    except (ValueError, KeyError, TypeError, RecursionError) as error:
        return [], ["Complete API job inventory unavailable for DOM census:" + str(error)], {}
    require(type(fact) is dict and type(fact.get("entries")) is list and fact.get("count") == len(fact["entries"]),
            "Malformed trusted job-row census")
    rows = [row for row in fact["entries"] if row["visible"] is True]
    if len(rows) > len(identifiers):
        return ["Visible job rows exceed the complete persisted job inventory"], [], {}
    if len(rows) < len(identifiers):
        return [], ["All persisted job rows were not visible within the observation window"], {}
    # Same identifier alphabet as the public row locator; no CSS/DOM nesting rule.
    matches = [[index for index, identifier in enumerate(identifiers)
                if re.search(r"(?<![A-Za-z0-9_-])" + re.escape(identifier) + r"(?![A-Za-z0-9_-])", row["text"])]
               for row in rows]
    def matching() -> dict[int, int] | None:
        assigned: dict[int, int] = {}
        def assign(row_index: int, seen: set[int]) -> bool:
            for identifier in matches[row_index]:
                if identifier not in seen:
                    seen.add(identifier)
                    if identifier not in assigned or assign(assigned[identifier], seen):
                        assigned[identifier] = row_index
                        return True
            return False
        return assigned if all(assign(index, set()) for index in range(len(rows))) else None
    assignment = matching()
    if assignment is None:
        return ["Visible job rows duplicate or contradict the complete persisted job identities"], [], {}
    # A second complete association cannot authorize an arbitrary row/state
    # attribution. Retain only edges present in every full identity matching.
    alternatives = {index: set(matches[row_index]) - {index} for index, row_index in assignment.items()}
    def ambiguous(index: int) -> bool:
        pending, seen = list(alternatives[index]), set()
        while pending:
            current = pending.pop()
            if current == index:
                return True
            if current not in seen:
                seen.add(current)
                pending.extend(alternatives[current] - seen)
        return False
    associated = {identifiers[index]: rows[row_index] for index, row_index in assignment.items() if not ambiguous(index)}
    missing = [] if len(associated) == len(identifiers) else ["Job row identity association is ambiguous"]
    return [], missing, associated


def dom_findings(action: dict[str, Any], observation: dict[str, Any] | None,
                 jobs_response: tuple[int, bytes] | str | None = None) -> tuple[list[str], list[str]]:
    if observation is None or type(observation.get("dom")) is not dict:
        return [], ["Original DOM facts unavailable"]
    dom = observation["dom"]
    wrong: list[str] = []
    missing: list[str] = []
    def visible(fact: dict[str, Any]) -> list[dict[str, Any]]:
        require(type(fact) is dict and type(fact.get("entries")) is list and fact.get("count") == len(fact["entries"]),
                "Malformed trusted DOM fact")
        return [row for row in fact["entries"] if row["visible"] is True]
    def required(fact: dict[str, Any], label: str, *, unique: bool = True) -> None:
        entries = visible(fact)
        if not entries:
            missing.append(label + " was not visible within the observation window")
        if unique and len(entries) > 1:
            wrong.append(label + " is not unique")
    required(dom["heading"], "heading", unique=False)
    required(dom["status"], "status", unique=False)
    required(dom["job_list"], "Ingestion jobs")
    required(dom["document_list"], "Documents")
    census_wrong, census_missing, associated_jobs = job_census_findings(dom["job_rows"], jobs_response)
    wrong.extend(census_wrong)
    missing.extend(census_missing)
    options = dom["intake_options"]
    selected = options[0].get("selected") if len(options) == 1 else None
    for name, fact in dom["controls"].items():
        # The public select has no prescribed order or default selection.
        if name == "Namespace" and selected not in ("directory", "zip"):
            continue
        required(fact, name, unique=name in ("Job ID", "Local path", "Namespace"))
    required(dom["intake"], "Intake type")
    if len(options) == 1:
        if options[0]["tag"] != "SELECT" or sorted(options[0]["values"]) != ["directory", "json", "zip"]:
            wrong.append("Intake type is not the public native select/options")
        if selected not in ("directory", "json", "zip"):
            missing.append("Current native Intake type selection unavailable")
    for expected in action.get("jobs", []) + ([action["job"]] if "job" in action else []):
        found = [value for value in dom["jobs"] if value["job_id"] == expected["job_id"]]
        if len(found) != 1:
            missing.append("Expected job DOM facts unavailable")
            continue
        value = found[0]
        # The broad per-ID locator is diagnostic: another valid row may mention
        # this ID incidentally. Use the uniquely supported whole-list association.
        associated = associated_jobs.get(expected["job_id"])
        if associated is None:
            missing.append("Expected job row identity unavailable: " + expected["job_id"])
            continue
        text = associated["text"]
        state_matches = re.search(r"\b" + expected["state"] + r"\b", text) is not None
        epochs = re.findall(r"\bepoch\s+([0-9]+)(?![0-9])", text)
        if state_matches and len(epochs) == 1 and epochs[0] != str(expected["epoch"]):
            wrong.append("Displayed current epoch is not the exact public integer")
        elif not state_matches or str(expected["epoch"]) not in epochs:
            missing.append("Expected current state/epoch was not observed")
        progress = re.findall(r"(?<![0-9])([0-9]+)\s*/\s*([0-9]+)(?![0-9])", text)
        if state_matches and len(progress) == 1 and progress[0] != (str(expected["completed"]), str(expected["total"])):
            wrong.append("Displayed job progress contradicts atomic committed progress")
        elif (str(expected["completed"]), str(expected["total"])) not in progress:
            missing.append("Expected current progress unavailable")
        if state_matches:
            enabled = {"queued": {"prepare", "cancel"}, "running": {"commit", "cancel"},
                       "failed": {"retry"}, "cancelled": {"retry"}, "completed": set()}[expected["state"]]
            for verb, fact in value["actions"].items():
                active = [row for row in visible(fact) if row["enabled"] is True]
                if verb not in enabled and active:
                    wrong.append("Action enabled for an invalid observed state: " + verb)
                if verb in enabled and not active:
                    missing.append("Valid visible enabled action unavailable: " + verb)
    for source, fact in dom["documents"].items():
        required(fact, "document " + source)
    # Generic buttons remain diagnostic: auxiliary Export/Edit controls are legal.
    if action.get("error"):
        matches = [row for row in visible(dom["status"]) if action["error"] in row["text"]]
        if len(matches) > 1:
            wrong.append("Current literal error appears in multiple visible status elements")
        elif not matches:
            missing.append("Current literal error status unavailable")
    if action.get("replaces_error") and any(action["replaces_error"] in row["text"] for row in visible(dom["status"])):
        missing.append("Success replacing stale error was not observed")
    if action["op"] == "search_open":
        transition = dom.get("literal_transition")
        if type(transition) is not dict or transition.get("action_id") != action["id"] or transition.get("source") != action["source"]:
            missing.append("Selected literal document image provenance unavailable")
        else:
            require(all(type(transition.get(key)) is int and transition[key] >= 0
                        for key in ("before_count", "after_matching_count", "new_matching_count"))
                    and transition["new_matching_count"] <= transition["after_matching_count"],
                    "Malformed trusted literal image transition")
            if transition["new_matching_count"] > 0:
                wrong.append("Selected literal document created its authored HTML image in Document details")
        required(dom["details"], "Document details", unique=False)
        if not any(action["literal"] in row["text"] for row in visible(dom["details"])):
            missing.append("Complete literal text display unavailable")
    if observation.get("observation_window_satisfied") is not True:
        missing.append("Declared action observation window did not establish readiness; no product latency judgment")
    return wrong, missing


class _Reader(reader_base._Reader):
    owner: Any

    def __init__(self, owner: Any, checkpoint: chain.PrefixCommitment):
        self.owner, self.checkpoint = owner, checkpoint
        self.validate_checkpoint()

    def compare(self, label: str, before: dict[str, Any], after: dict[str, Any], spec: base.RoleSpec,
                phase: str, donor: Any = None) -> dict[str, Any]:
        value = transport.role_identity_comparison(before, after, spec, self.owner.runtime, phase, donor=donor)
        require(value["matches"] is True and self.json(label + ".json") == value, "Original role comparison differs")
        return value

    def command(self, label: str, argv: list[str]) -> bytes:
        result = super().command(label, argv)
        intent = self.json(label + "-command-intent.json")
        require(intent["argv"] == self.json(label + ".json")["argv"]
                and intent["runtime_sha256"] == base.digest(self.owner.runtime), "Original command intent differs")
        return result

    def probe(self, row: dict[str, Any], request: dict[str, Any], server: dict[str, Any], server_spec: base.RoleSpec) -> wire.WireObservation:
        from . import candidate_product_browser_execution_v1 as execution
        owner, label = self.owner, row["label"]
        staging = self.json(label + "-staging.json")
        recipe = execution.bridge_request(request["request"])
        input_raw = wire.build_probe_input(recipe, cases.PORT, owner.policy.probe.wire_limits)
        require(self.raw(label + "-probe-input.bin") == input_raw, "Original helper request differs")
        expected_root = str(Path(self.json("staging.json")["workspace"]).parent / label)
        require(staging == {"root": expected_root, "manifest": execution.source_manifest({"helper.py": wire.helper_source(), "request.json": input_raw})},
                "Exact helper staging differs")
        spec = base.RoleSpec("probe", "gossip-" + owner.execution_id + "-" + label, base.PROBE_ARGV,
            owner._labels("probe", label), (("/probe", expected_root),), server_id=server["Id"])
        current = self.inspect(label + "-server-before", server["Id"])
        self.compare(label + "-server-before-continuity", server, current, server_spec, "running-to-running")
        donor = transport.ProbeDonorEvidence(server_spec, owner.registration, 1,
            base.encoded(server), base.encoded(current), base.encoded(owner.runtime))
        creation = label + "-probe"
        created = reader_base._created(self, creation, spec)
        cid = created["Id"]
        require(row["probe_id"] == cid, "Original probe identity differs")
        process = self.json(creation + "-process.json")
        require(row["process"] == process, "Original process result copy differs")
        policy = owner.policy.probe
        envelope = wire.max_probe_output_bytes(policy.wire_limits)
        require(cases.encoded(self.json(creation + "-intent.json")) == cases.encoded({"protocol": transport.PROTOCOL, "role": asdict(spec),
            "expected": base.digest(created), "runtime": owner.runtime, "policy": asdict(policy),
            "role_policy": transport.role_policy_identity(), "donor": donor.record(), "helper_stdout_envelope": envelope,
            "helper_sha256": wire.helper_sha256()}), "Original probe intent differs")
        before = self.inspect(creation + "-prestart", cid)
        base.validate_state(before, "created")
        self.compare(creation + "-prestart-comparison", created, before, spec, "created-to-prestart")
        started = self.control(creation + "-start", cid, "start", "POST", 204)
        start = process["start_response"]
        require(start["status"] == 204 and start["body_bytes"] == len(started) and start["body_sha256"] == base.sha256(started)
                and start["framing_complete"] is True and start["eof_observed"] is True,
                "Original probe start unavailable")
        require(self.descriptor(start["request"]) == self.raw(creation + "-start-request.bin")
                and self.descriptor(start["response"]) == self.raw(creation + "-start-response.bin"), "Original start bytes differ")
        waited = engine.strict_json_loads(self.control(creation + "-wait", cid, "wait?condition=not-running", "POST"))
        final = self.inspect(creation + "-final", cid)
        comparison = self.compare(creation + "-identity-comparison", created, final, spec, "created-to-exited", donor)
        completion = engine.completion_evidence(waited, final, started=True, killed=False, identity_ok=True)
        require(process["protocol"] == transport.PROTOCOL and process["completion"] == completion
                and process["identity_comparison"] == comparison and cases.encoded(process["donor"]) == cases.encoded(donor.record())
                and completion["natural"] and completion["inspect_exit_code"] == 0 and process["status"] == "completed",
                "Original helper completion differs")
        process_policy = engine.ProcessPolicy(policy.image_id, timeout_seconds=policy.probe_timeout_seconds,
            stream_limit_bytes=envelope, frame_limit_bytes=envelope, transport_timeout_seconds=policy.transport_timeout_seconds)
        stdout, stderr = reader_base._streams(self, creation, cid, process, process_policy)
        require(not stderr, "Trusted helper stderr unavailable")
        self.boundary(label + "-server-after", server, server_spec)
        for phase in ("before", "after"):
            self.boundary(label + "-" + phase + "-keeper", owner.keeper, owner.keeper_spec)
            volume = self.command(label + "-" + phase + "-volume", ["docker", "volume", "inspect", "--format", "{{json .}}", owner.volume])
            require(engine.strict_json_loads(volume) == owner.volume_baseline, "Original held volume differs")
        require(row["removed"] is True, "Original probe retirement unconfirmed")
        removal = self.json(label + "-retire-intent.json")
        require(removal["container_id"] == cid and removal["force"] is True
                and cases.encoded(removal["spec"]) == cases.encoded(asdict(spec)), "Original removal identity differs")
        self.command(label + "-retire-remove", ["docker", "rm", "--force", cid])
        absent = self.command(label + "-retire-absence", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"])
        require(not absent.strip(), "Original probe remains present")
        return wire.decode_probe_output(stdout, recipe, cases.PORT, policy.wire_limits)


def _facet(selector: str, wrong: list[str], missing: list[str]) -> Facet:
    return Facet(selector, "failed" if wrong else "unavailable" if missing else "passed", tuple(wrong), tuple(missing))


def read_original(owner: Any, checkpoint: chain.PrefixCommitment) -> BrowserObservation:
    from . import candidate_product_browser_execution_v1 as execution
    require(type(owner) is execution.BrowserExecution and type(checkpoint) is chain.PrefixCommitment,
            "Exact live physical owner and current external checkpoint required")
    owner._unchanged()
    reader = _Reader(owner, checkpoint)
    require(cases.encoded(reader.json("config.json")) == cases.encoded(owner.config), "Original config/source/admission differs")
    execution.validate_capture_config(owner.profile, reader.json("config.json"))
    intent, terminal = reader.json("intent.json"), reader.json("terminal.json")
    require(intent["protocol"] == owner.profile.execution_protocol == owner.binding.protocol and intent["execution_id"] == owner.execution_id
            and intent["binding_sha256"] == execution.digest(asdict(owner.binding))
            and intent["profile_sha256"] == owner.profile.sha256
            and intent["ordered_actions"] == owner.profile.case.record["actions"] and intent["physical"] is True,
            "Original history/purpose differs")
    require(terminal["protocol"] == owner.profile.execution_protocol and terminal["execution_id"] == owner.execution_id
            and terminal["purpose"] == owner.binding.purpose and terminal["product_acceptance"] is False
            and terminal["global_independent_acceptance"] is False, "Original terminal identity/purpose differs")
    facets: list[Facet] = []
    mechanics_missing = list(terminal["infrastructure"])
    server: dict[str, Any] | None = None
    try:
        staging = reader.json("staging.json")
        require(staging["source_manifest"] == execution.source_manifest(owner.files)
                and staging["input_manifest"] == execution.source_manifest(owner.inputs), "Original staging/source differs")
        keeper_spec = base.RoleSpec("keeper", "gossip-" + owner.execution_id + "-keeper",
            ("python", "-I", "-c", "import time;time.sleep(" + str(owner.policy.lifetime_seconds) + ")"),
            owner._labels("keeper", "state-lifetime"), (), intent["volume"])
        server_spec = base.RoleSpec("server", "gossip-" + owner.execution_id + "-server", cases.SERVER_ARGV,
            owner._labels("server", "server"), (("/workspace", staging["workspace"]), ("/inputs", staging["inputs"])), intent["volume"])
        require(owner.keeper_spec == keeper_spec and owner.server_spec == server_spec and owner.volume == intent["volume"],
                "Prospective source role reconstruction differs")
        volume = engine.strict_json_loads(reader.command("volume-created", ["docker", "volume", "inspect", "--format", "{{json .}}", intent["volume"]]))
        require(volume == reader.json("volume-baseline.json") == owner.volume_baseline
                and volume.get("Name") == intent["volume"] and volume.get("Driver") == "local"
                and volume.get("Options") == execution.VOLUME_OPTIONS
                and volume.get("Labels") == {"gossip.execution": owner.execution_id, "gossip.snapshot": execution.SNAPSHOT_PROTOCOL},
                "Original volume policy/identity differs")
        keeper = reader_base._long_role(reader, "keeper", keeper_spec)
        require(keeper == owner.keeper, "Original keeper differs")
        declaration, argv = execution.setup_argv(owner.profile.case, keeper["Id"])
        require(reader.json("initial-state-intent.json") == {"setup": declaration, "argv": argv,
            "keeper_id": keeper["Id"], "input_manifest": execution.source_manifest(owner.inputs)}, "Original seed setup differs")
        require(reader.command("initial-state", argv) == execution.encoded({"protocol": base.SETUP_PROTOCOL,
            "setup_sha256": execution.digest(declaration), "ready": True}) + b"\n", "Original seed acknowledgment differs")
        server = reader_base._long_role(reader, "server", server_spec)
        require(server == owner.server, "Original server differs")
    except (OSError, ValueError, KeyError, AttributeError) as error:
        mechanics_missing.append("source/server/setup:" + type(error).__name__ + ":" + str(error))
    messages: list[dict[str, Any]] = []
    for sequence in terminal["driver_messages"]:
        try:
            message = reader.json("browser-message-" + str(sequence).zfill(5) + ".bin")
            require(message["protocol"] == execution.IPC_PROTOCOL and message["seq"] == sequence, "Original driver sequence differs")
            messages.append(message)
        except (OSError, ValueError, KeyError) as error:
            mechanics_missing.append("driver-message:" + str(error))
    require(terminal["driver_messages"] == list(range(1, len(terminal["driver_messages"]) + 1)),
            "Original browser message census is not a contiguous prefix")
    actions_declared = owner.profile.case.record["actions"]
    entered_actions = [row["action_id"] for row in messages if row.get("kind") == "action_start"]
    require(entered_actions == [action["id"] for action in actions_declared[:len(entered_actions)]],
            "Original action order differs from the declared whole history")
    driver_authenticated = False
    try:
        command, start, completion = (reader.json(name) for name in ("browser-command-intent.json", "browser-start.json", "browser-process-completion.json"))
        driver_root = Path(reader.json("staging.json")["workspace"]).parent / "trusted-browser"
        expected_argv = [owner.policy.node_executable, str(driver_root / execution.DRIVER.name), "--input",
                         str(driver_root / "input.json"), "--output", str(driver_root / "output")]
        expected_actions = json.loads(cases.encoded(owner.profile.case.record["actions"]))
        for action in expected_actions:
            for job in action.get("jobs", []) + ([action["job"]] if "job" in action else []):
                job["epoch"] = str(job["epoch"])
        require(command["argv"] == expected_argv and command["declaration"] == {
            "protocol": execution.IPC_PROTOCOL, "runtime": owner.browser_runtime, "actions": expected_actions}
            and command["environment"].get("NODE_PATH") == owner.policy.node_modules_path
            and set(command["environment"]) == {"NODE_PATH", "PATH"} and command["start_new_session"] is True,
            "Original driver argv/actions/environment differ from prospective declaration")
        require(command["driver_sha256"] == execution.sha256(execution.DRIVER.read_bytes())
                and command["protocol_sha256"] == execution.sha256(execution.PROTOCOL_SOURCE.read_bytes())
                and command["declaration"]["runtime"] == owner.browser_runtime and command["candidate_mount"] is False
                and start["argv"] == command["argv"] and start["pid"] == completion["pid"]
                and type(start["pid"]) is int and start["pid"] > 0, "Original trusted driver identity differs")
        for path in (execution.DRIVER, execution.PROTOCOL_SOURCE, execution.BASE_PROJECT / "devtools/browser/lifecycle.cjs"):
            require(reader.raw("browser-staged-" + path.name.replace("_", "-") + ".bin") == path.read_bytes(),
                    "Original staged driver source differs")
            require(reader.raw("browser-final-" + path.name.replace("_", "-") + ".bin") == path.read_bytes(),
                    "Final staged driver source differs")
        require(completion["staged_source_unchanged"] is True, "Original trusted driver bytes changed")
        require(bool(messages) and messages[0]["kind"] == "runtime" and messages[0]["runtime"] == owner.browser_runtime,
                "Actual pinned driver runtime unavailable")
        require(any(row.get("kind") == "launched" and row.get("browser_version") == owner.browser_runtime["browser_version"]
                    for row in messages), "Actual pinned Chromium launch unavailable")
        driver_authenticated = True
        if not completion["pipe_complete"] or completion["returncode"] != 0:
            mechanics_missing.append("Driver ended without complete ordinary terminal")
        if not completion["browser_close_acknowledged"]:
            mechanics_missing.append("Owned Chromium close unconfirmed; OS process absence is not inferred")
        if completion["terminal"] is None or completion["terminal"].get("diagnostics"):
            mechanics_missing.append("Browser has retained diagnostic/unsupported bridge activity")
        artifact_completion = reader.json("browser-completion.json")
        require({key: artifact_completion[key] for key in completion} == completion, "Original process/artifact completion differs")
        for artifact in artifact_completion["artifacts"]:
            raw = b"".join(reader.descriptor(part) for part in artifact["parts"])
            require(len(raw) == artifact["bytes"] and execution.sha256(raw) == artifact["sha256"], "Original browser artifact differs")
    except (OSError, ValueError, KeyError) as error:
        mechanics_missing.append("driver:" + type(error).__name__ + ":" + str(error))
    responses: dict[int, tuple[int, bytes] | str] = {}
    request_messages = {row["request_id"]: row for row in messages if row.get("kind") == "request"}
    for row in terminal["request_rows"]:
        identifier = row["request_id"]
        try:
            require(driver_authenticated and server is not None, "Original browser/server provenance unavailable")
            assert server is not None
            request = request_messages[identifier]
            require(reader.json(row["label"] + "-request.json") == request
                    and reader.json(row["label"] + "-observation.json") == row, "Original route record differs")
            observed = reader.probe(row, request, server, owner.server_spec)
            faithful = execution.bridge_response(observed)
            reply = reader.json("browser-reply-" + str(identifier).zfill(4) + ".bin")
            require(reply == {"protocol": execution.IPC_PROTOCOL, "kind": "response", "request_id": identifier, **faithful},
                    "Browser fulfillment differs from original complete response")
            require(type(observed.response.status_code) is int, "Complete status unavailable")
            assert observed.response.status_code is not None
            responses[identifier] = (observed.response.status_code, observed.response.body)
        except (OSError, ValueError, KeyError, AttributeError) as error:
            responses[identifier] = type(error).__name__ + ":" + str(error)
    for action in owner.profile.case.record["actions"]:
        selector = owner.profile.case.identifier + ":" + action["id"] + ":"
        if not driver_authenticated or server is None:
            for kind in ("dom", "api", "mutation"):
                facets.append(_facet(selector + kind, [], ["Authenticated Chromium driver unavailable"]))
            continue
        dom_records = [row for row in messages if row.get("kind") == "observation" and row.get("action_id") == action["id"]]
        observation = dom_records[0] if len(dom_records) == 1 else None
        controls: dict[str, tuple[int, bytes] | str] = {}
        for request in request_messages.values():
            if request.get("source") == "control" and request.get("action_id") == action["id"]:
                controls[request["request"]["target"]] = responses.get(request["request_id"], "Original probe unavailable")
        for kind, evaluate in (("dom", lambda: dom_findings(action, observation, controls.get("/api/jobs"))),
                               ("mutation", lambda: mutation_findings(action, messages))):
            try:
                wrong, missing = evaluate()
            except (OSError, ValueError, KeyError, TypeError) as error:
                wrong, missing = [], ["Trusted observation malformed/unavailable:" + type(error).__name__ + ":" + str(error)]
            facets.append(_facet(selector + kind, wrong, missing))
        wrong, missing = api_findings(action, controls)
        facets.append(_facet(selector + "api", wrong, missing))
    cleanup_verified = False
    try:
        cleanup = reader.json("cleanup-result.json")
        cleanup_verified = (cleanup.get("all_resources_absent") is True and not cleanup.get("uncertainties")
            and cleanup.get("main_journal_healed") is False and cleanup.get("acceptance_authority") is False
            and set(row["name"] for row in cleanup["dispositions"]) == {*owner.owned, owner.volume}
            and all(row["status"] in ("removed", "already-absent") for row in cleanup["dispositions"]))
        require(cleanup_verified == (terminal["cleanup_verified"] is True), "Original cleanup census differs")
    except (OSError, ValueError, KeyError) as error:
        mechanics_missing.append("cleanup:" + str(error))
    if not cleanup_verified:
        mechanics_missing.append("Owned Engine resources not confirmed absent")
    facets.append(_facet(owner.profile.case.identifier + ":mechanics", [], mechanics_missing))
    reader.validate_checkpoint()
    return BrowserObservation(PROTOCOL, owner.execution_id, execution.digest(asdict(owner.binding)),
        execution.digest(asdict(checkpoint)), tuple(facets),
        tuple(f.selector for f in facets if f.discrepancies), tuple(f.selector for f in facets if f.limitations),
        cleanup_verified)
