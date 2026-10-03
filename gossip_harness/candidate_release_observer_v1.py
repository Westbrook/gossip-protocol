"""Fixed host-owned packaged-release observations; never candidate-side scoring.

This narrow gate checks packaging byte completeness and four public CLI commands
on the delivered package. It requires a prospectively registered deliverable-only
Git tree: ancillary development/private files must be excluded by its source
authority. It is not a universal full-repository release inclusion rule. It does not claim M4 acceptance, HTTP/browser coverage,
independent unseen evidence, or authority to dispatch. The controller owns two
separate containers and raw process/capsule capture. Candidate stdout is data,
never a supervisory envelope. No candidate module is imported by this module.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import tarfile
from typing import Any

LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
PROTOCOL = "candidate-release-observer-v1"
DELIVERY_PROFILE = "complete-public-source-capsule-v1"
CASE_IDS = ("release-build", "release-manifest", "released-cli-roundtrip")
RUNTIME_IMAGE = "sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f"
PRODUCT_CONTRACT_SHA256 = "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c"
MAX_CAPSULE_BYTES = 40 * 1024 * 1024
MAX_PACKAGE_BYTES = 16 * 1024 * 1024
MAX_FILE_BYTES = 2 * 1024 * 1024
MAX_FILES = 512
MAX_PROCESS_BYTES = 32768
INPUT_SOURCE = "observer-note.txt"
INPUT_BYTES = "Café research — 雪\nLiteral <b>release</b> content.\n".encode("utf-8")


def _command(module: str, args: tuple[str, ...]) -> tuple[str, ...]:
    # -I avoids candidate-supplied sitecustomize before the explicit bootstrap.
    # Only the candidate subprocess gets /workspace in its import path.
    bootstrap = (
        "import runpy,sys;sys.path.insert(0,'/workspace');"
        f"sys.argv={[module, *args]!r};"
        f"runpy.run_module({module!r},run_name='__main__')"
    )
    return ("python", "-I", "-B", "-c", bootstrap)


BUILD_COMMAND = _command("library.clients.release", ("--output", "/tmp/release"))


class ObservationError(ValueError):
    """Malformed observation or invalid candidate package, never a passing verdict."""


class CapsuleTransportError(ObservationError):
    """Incomplete/malformed tar transport, which requires infrastructure status."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def fixture_files() -> dict[str, bytes]:
    """Public fixed bytes mounted read-only at /inputs in the package container."""
    return {INPUT_SOURCE: INPUT_BYTES}


def _document() -> dict[str, str]:
    raw_source = INPUT_SOURCE.encode("utf-8")
    return {
        "document_id": "doc-" + hashlib.sha256(b"document\0" + raw_source).hexdigest(),
        "source_id": "src-" + hashlib.sha256(b"source\0" + raw_source).hexdigest(),
        "source": INPUT_SOURCE,
        "blob_id": "blob-" + hashlib.sha256(INPUT_BYTES).hexdigest(),
        "title": INPUT_SOURCE,
        "text": INPUT_BYTES.decode("utf-8"),
    }


def cli_steps() -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Four separate processes must reopen the same fresh delivered database."""
    common = ("--db", "/tmp/research.sqlite", "--root", "/inputs")
    actions = (("import", ("import", INPUT_SOURCE)), ("list", ("list",)),
               ("show", ("show", _document()["document_id"])),
               ("export", ("export",)))
    return tuple((name, _command("library", common + args)) for name, args in actions)


def _path(name: Any) -> str:
    if (type(name) is not str or not name or len(name.encode("utf-8")) > 1024
            or "\\" in name or "\0" in name or name.startswith("/")
            or any(part in ("", ".", "..") for part in name.split("/"))
            or str(PurePosixPath(name)) != name):
        raise ObservationError("invalid package path")
    return name


def _check_files(files: dict[str, bytes], *, source: bool = False) -> None:
    if type(files) is not dict or not files or len(files) > MAX_FILES:
        raise ObservationError("invalid file inventory")
    total = 0
    for name, raw in files.items():
        _path(name)
        if type(raw) is not bytes or len(raw) > MAX_FILE_BYTES:
            raise ObservationError("invalid file bytes")
        total += len(raw)
    if (total > MAX_PACKAGE_BYTES or (source and "release-manifest.json" in files)
            or (source and len(files) >= MAX_FILES)):
        raise ObservationError("invalid source/package size or reserved manifest")
    # Files cannot be ancestors of other files, even if a crafted tar says so.
    for name in files:
        if any(str(parent) in files for parent in PurePosixPath(name).parents if str(parent) != "."):
            raise ObservationError("file ancestor collision")


def file_inventory(files: dict[str, bytes]) -> list[dict[str, Any]]:
    _check_files(files)
    return [{"path": name, "sha256": hashlib.sha256(files[name]).hexdigest(),
             "bytes": len(files[name])} for name in sorted(files)]


def parse_capsule(raw: bytes) -> dict[str, bytes]:
    """Read docker-cp's frozen tmp/ tar without filesystem extraction.

    The controller pauses all builder processes before capturing /tmp (bounded by
    a 32 MiB tmpfs). Scratch files remain raw evidence and are never extracted.
    Only the tmp/release subtree is delivered. An absent/empty release returns
    an empty mapping: a complete observed packaging failure. Invalid candidate
    paths/links/special package members raise ObservationError. Incomplete tar
    framing raises CapsuleTransportError and must remain infrastructure failure.
    """
    if type(raw) is not bytes or not raw or len(raw) > MAX_CAPSULE_BYTES or len(raw) % 512:
        raise CapsuleTransportError("invalid capsule length")
    files: dict[str, bytes] = {}
    seen: set[str] = set()
    directories: set[str] = set()
    total = 0
    try:
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
            # Complete transport framing takes precedence over any candidate
            # assertion. A bad package entry followed by a truncated capture
            # must not be normalized as a completed correctness failure.
            members: list[tarfile.TarInfo] = []
            for member in archive:
                if len(members) <= 4096:
                    members.append(member)
            trailer = raw[archive.offset:]
            if len(trailer) < 1024 or any(trailer):
                raise CapsuleTransportError("incomplete or ambiguous capsule trailer")
            if len(members) > 4096:
                raise ObservationError("too many capsule members")
            for member in members:
                name = member.name
                if member.isdir() and name.endswith("/"):
                    name = name[:-1]
                _path(name)
                if name in seen:
                    raise ObservationError("duplicate capsule member")
                seen.add(name)
                if name in ("tmp", "tmp/release"):
                    if not member.isdir() or member.size != 0:
                        raise ObservationError("capsule root is not an empty directory entry")
                    directories.add(name)
                    continue
                if not name.startswith("tmp/"):
                    raise ObservationError("capsule outside tmp root")
                if not name.startswith("tmp/release/"):
                    # Scratch remains bound by the raw archive identity/limit;
                    # never read its bytes or follow its links on the host.
                    continue
                relative = name[len("tmp/release/"):]
                _path(relative)
                if member.isdir():
                    if member.size != 0:
                        raise ObservationError("directory entry has content")
                    directories.add(name)
                    continue
                if not member.isreg() or member.issparse():
                    raise ObservationError("nonregular package member")
                if type(member.size) is not int or not 0 <= member.size <= MAX_FILE_BYTES:
                    raise ObservationError("invalid package member length")
                total += member.size
                if total > MAX_PACKAGE_BYTES or len(files) >= MAX_FILES:
                    raise ObservationError("package size limit")
                reader = archive.extractfile(member)
                if reader is None:
                    raise CapsuleTransportError("missing capsule bytes")
                with reader:
                    content = reader.read(member.size + 1)
                if len(content) != member.size:
                    raise CapsuleTransportError("truncated capsule bytes")
                files[relative] = content
    except (tarfile.TarError, OSError, EOFError, UnicodeError, ValueError) as error:
        if isinstance(error, ObservationError):
            raise
        raise CapsuleTransportError("malformed capsule") from error
    if "tmp" not in directories:
        raise CapsuleTransportError("missing tmp capture root")
    if not files:
        return {}
    if "tmp/release" not in directories:
        raise ObservationError("missing package root directory")
    _check_files(files)
    if any("tmp/release/" + name in directories for name in files):
        raise ObservationError("file/directory package collision")
    return files


def capsule_observation(files: dict[str, bytes]) -> dict[str, Any]:
    """Facts derived by the host from the completed captured package."""
    manifest = files.get("release-manifest.json")
    return {"files": file_inventory(files),
            "content_b64": None if manifest is None else base64.b64encode(manifest).decode("ascii")}


def expected_observations(files: dict[str, bytes]) -> dict[str, Any]:
    """Host-only expectations from exact captured source and public contracts."""
    _check_files(files, source=True)
    records = file_inventory(files)
    source_sha = hashlib.sha256(canonical_bytes(records)).hexdigest()
    manifest = {
        "format": "local-research-library-release-manifest-v1",
        "files": records, "source_sha256": source_sha,
        "runtime": {"image": RUNTIME_IMAGE, "python": "3.12"},
        "product_contract_sha256": PRODUCT_CONTRACT_SHA256,
        "api_versions": ["v0", "lifecycle-v2", "maintenance-v3", "v1"],
        "storage_version": 4,
    }
    package = dict(files, **{"release-manifest.json": canonical_bytes(manifest)})
    doc = _document()
    return {
        "build": {"format": "local-research-library-release-v1", "manifest": "release-manifest.json",
                  "files": len(records), "source_sha256": source_sha},
        "manifest": capsule_observation(package),
        "cli": [{"step_id": "import", "value": {"status": "imported", "document": doc}},
                {"step_id": "list", "value": {"documents": [doc], "total": 1}},
                {"step_id": "show", "value": doc},
                {"step_id": "export", "value": {"format": "local-research-library-v0", "documents": [doc]}}],
    }


def _raw(value: Any, limit: int = MAX_PROCESS_BYTES) -> bytes:
    if type(value) is not str or len(value) > 4 * ((limit + 2) // 3):
        raise ObservationError("invalid encoded bytes")
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error) as error:
        raise ObservationError("invalid encoded bytes") from error
    if len(raw) > limit or base64.b64encode(raw).decode("ascii") != value:
        raise ObservationError("invalid encoded bytes")
    return raw


def _process(value: Any, *, step: bool = False) -> tuple[int, bytes, bytes]:
    keys = {"exit_code", "stdout_b64", "stderr_b64"} | ({"step_id"} if step else set())
    if (type(value) is not dict or set(value) != keys or type(value["exit_code"]) is not int
            or not 0 <= value["exit_code"] <= 255):
        raise ObservationError("invalid process observation")
    return value["exit_code"], _raw(value["stdout_b64"]), _raw(value["stderr_b64"])


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON member")
        result[key] = value
    return result


def _constant(value: str) -> Any:
    raise ValueError("nonfinite JSON number")


def _success(process: tuple[int, bytes, bytes], expected: Any) -> bool:
    status, stdout, stderr = process
    if status != 0 or stderr:
        return False
    try:
        actual = json.loads(stdout.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant)
        return canonical_bytes(actual) == canonical_bytes(expected)
    except (ValueError, UnicodeError, RecursionError):
        return False


def _manifest(value: Any) -> None:
    if type(value) is not dict or set(value) != {"files", "content_b64"}:
        raise ObservationError("invalid package observation")
    records = value["files"]
    if type(records) is not list or not 1 <= len(records) <= MAX_FILES:
        raise ObservationError("invalid package inventory")
    names = []
    total = 0
    for record in records:
        if type(record) is not dict or set(record) != {"path", "sha256", "bytes"}:
            raise ObservationError("invalid package file record")
        names.append(_path(record["path"]))
        digest, size = record["sha256"], record["bytes"]
        if (type(digest) is not str or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest)
                or type(size) is not int or not 0 <= size <= MAX_FILE_BYTES):
            raise ObservationError("invalid package file identity")
        total += size
    if names != sorted(set(names)) or total > MAX_PACKAGE_BYTES:
        raise ObservationError("invalid ordered package inventory")
    if value["content_b64"] is not None:
        raw = _raw(value["content_b64"], MAX_FILE_BYTES)
        manifest = next((record for record in records if record["path"] == "release-manifest.json"), None)
        if manifest != {"path": "release-manifest.json", "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}:
            raise ObservationError("manifest bytes do not match inventory")
    elif any(record["path"] == "release-manifest.json" for record in records):
        raise ObservationError("missing observed manifest bytes")


def score_observations(observed: Any, files: dict[str, bytes]) -> tuple[bool, bool, bool]:
    """Score completed host observations only; malformed framing raises.

    A complete candidate nonzero exit or invalid candidate JSON is a failed
    assertion. Missing direct process observations must never become a pass;
    incomplete CLI observations are accepted only if earlier packaging failed.
    Infrastructure failures (timeouts, cleanup, truncated captures) belong to the
    controller and must not be sent here as completed observations.
    """
    if (type(observed) is not dict or set(observed) != {"protocol", "build", "manifest", "cli"}
            or observed["protocol"] != PROTOCOL):
        raise ObservationError("invalid observation envelope")
    expected = expected_observations(files)
    build = _success(_process(observed["build"]), expected["build"])
    manifest = observed["manifest"]
    if manifest is not None:
        _manifest(manifest)
    package = manifest is not None and canonical_bytes(manifest) == canonical_bytes(expected["manifest"])
    commands = observed["cli"]
    if type(commands) is not list:
        raise ObservationError("invalid CLI observations")
    if commands == [] and not (build and package):
        return build, package, False
    ids = [step["step_id"] for step in expected["cli"]]
    if len(commands) != len(ids):
        raise ObservationError("incomplete CLI observations")
    matches = []
    for command, reference in zip(commands, expected["cli"], strict=True):
        process = _process(command, step=True)
        if command["step_id"] != reference["step_id"]:
            raise ObservationError("changed CLI order")
        matches.append(_success(process, reference["value"]))
    return build, package, all(matches)


def score_case(case_id: str, observed: Any, files: dict[str, bytes]) -> bool:
    """Score one complete case without erasing facts after a later interruption.

    The controller owns capture completion and case availability. It must not
    call this helper for a case with incomplete transport. Other case fields may
    be missing or incomplete; they have no bearing on this case's verdict.
    """
    if (case_id not in CASE_IDS or type(observed) is not dict
            or set(observed) - {"protocol", "build", "manifest", "cli"}
            or observed.get("protocol") != PROTOCOL):
        raise ObservationError("invalid partial observation envelope")
    expected = expected_observations(files)
    key = {"release-build": "build", "release-manifest": "manifest",
           "released-cli-roundtrip": "cli"}[case_id]
    if key not in observed:
        raise ObservationError("missing requested case observation")
    value = observed[key]
    if case_id == "release-build":
        return _success(_process(value), expected["build"])
    if case_id == "release-manifest":
        if value is None:
            return False
        _manifest(value)
        return canonical_bytes(value) == canonical_bytes(expected["manifest"])
    if type(value) is not list or len(value) != len(expected["cli"]):
        raise ObservationError("incomplete CLI observations")
    matches = []
    for command, reference in zip(value, expected["cli"], strict=True):
        process = _process(command, step=True)
        if command["step_id"] != reference["step_id"]:
            raise ObservationError("changed CLI order")
        matches.append(_success(process, reference["value"]))
    return all(matches)
