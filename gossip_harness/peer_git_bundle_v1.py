"""Transfer actual Git bundle bytes into fresh receiver-owned quarantine.

This deliberately narrow text-repository profile grants no validation or Git
publication authority. No candidate code is executed. Git's trusted native pack
parser runs on the host with time/output/file limits; this is not a memory or VM
sandbox for hostile pack-parser exploits.
"""
from __future__ import annotations

import hashlib
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from typing import Any
import uuid

from .gitstore import GitStore
from .peer_store_v1 import canonical_bytes, strict_loads

PROTOCOL = "peer-git-bundle-v1"
PROFILE = "ascii-path-utf8-regular-text-v1"
MAX_BUNDLE_BYTES = 8 * 1024 * 1024
MAX_EXPANDED_BYTES = 32 * 1024 * 1024
MAX_BLOB_BYTES = 1024 * 1024
MAX_OBJECTS = 4096
MAX_COMMITS = 256
MAX_FILES = 256
MAX_OUTPUT_BYTES = 2 * 1024 * 1024
MAX_METADATA_BYTES = 4096
WALL_SECONDS = 60.0
CPU_SECONDS = 20
FILE_BYTES = 16 * 1024 * 1024
_OID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_SEGMENT = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}\Z")
_MANIFEST = {"protocol", "profile", "object_format", "base_sha", "offered_sha", "offered_ref",
             "bundle_sha256", "bundle_bytes", "object_count", "commit_count", "expanded_bytes",
             "objects_sha256", "tree_sha", "files_sha256", "file_count", "source_bytes"}
_LIMIT_SCRIPT = """import os,resource,sys
for key,requested in ((resource.RLIMIT_CPU,int(sys.argv[1])),(resource.RLIMIT_FSIZE,int(sys.argv[2]))):
    old=resource.getrlimit(key)[1]
    limit=requested if old==resource.RLIM_INFINITY else min(old,requested)
    resource.setrlimit(key,(limit,limit))
os.execv(sys.argv[3],sys.argv[3:])
"""


class BundleError(ValueError):
    """A bundle, source profile or retained quarantine could not be qualified."""


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json(value: Any) -> bytes:
    return canonical_bytes(value, max_bytes=4 * 1024 * 1024)


def _save(path: Path, value: Any) -> None:
    with path.open("xb") as stream:
        stream.write(_json(value))
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _oid(value: Any, object_format: str | None = None) -> str:
    if type(value) is not str or _OID.fullmatch(value) is None:
        raise BundleError("A full hexadecimal Git object identity is required")
    if object_format is not None and len(value) != {"sha1": 40, "sha256": 64}[object_format]:
        raise BundleError("Git object identity format differs")
    return value


def _manifest(value: Any) -> dict[str, Any]:
    # Detach caller-owned containers before any subprocess or durable write.
    value = strict_loads(canonical_bytes(value, max_bytes=MAX_METADATA_BYTES), max_bytes=MAX_METADATA_BYTES)
    if (type(value) is not dict or set(value) != _MANIFEST or value["protocol"] != PROTOCOL
            or value["profile"] != PROFILE or type(value["object_format"]) is not str
            or value["object_format"] not in {"sha1", "sha256"}):
        raise BundleError("Unsupported bundle manifest")
    for key in ("base_sha", "offered_sha", "tree_sha"):
        _oid(value[key], value["object_format"])
    if value["offered_ref"] != "refs/harness/proposals/" + value["offered_sha"]:
        raise BundleError("Manifest does not name the exact proposal ref")
    for key in ("bundle_sha256", "objects_sha256", "files_sha256"):
        if type(value[key]) is not str or _SHA.fullmatch(value[key]) is None:
            raise BundleError("Invalid content digest")
    for key, maximum, minimum in (("bundle_bytes", MAX_BUNDLE_BYTES, 1), ("object_count", MAX_OBJECTS, 1),
                                 ("commit_count", MAX_COMMITS, 1), ("expanded_bytes", MAX_EXPANDED_BYTES, 1),
                                 ("file_count", MAX_FILES, 0), ("source_bytes", MAX_EXPANDED_BYTES, 0)):
        if type(value[key]) is not int or not minimum <= value[key] <= maximum:
            raise BundleError("Manifest resource bound exceeded")
    return value


def _header(payload: bytes, manifest: dict[str, Any]) -> None:
    # Only our single-ref, self-contained v3 format is supported. In particular,
    # prerequisites and filtered/promisor bundles cannot hide missing objects.
    end = payload.find(b"\n\n", 0, MAX_METADATA_BYTES)
    if end < 0 or payload[end + 2:end + 6] != b"PACK":
        raise BundleError("Invalid bounded bundle header or pack signature")
    expected = ("# v3 git bundle\n@object-format=" + manifest["object_format"] + "\n"
                + manifest["offered_sha"] + " " + manifest["offered_ref"]).encode("ascii")
    if payload[:end] != expected:
        raise BundleError("Unexpected bundle version, prerequisite, capability or advertised ref")


class _Git:
    """Serial Git commands with retained bounded logs and one shared deadline."""

    def __init__(self, root: Path):
        self.root = root
        self.deadline = time.monotonic() + WALL_SECONDS
        self.commands: list[dict[str, Any]] = []
        executable = shutil.which("git")
        if executable is None:
            raise BundleError("Git executable is unavailable")
        self.executable: str = executable

    def call(self, repository: Path | None, *arguments: str, input_bytes: bytes | None = None,
             maximum: int = MAX_OUTPUT_BYTES) -> bytes:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise BundleError("Bundle operation wall deadline exceeded")
        command_id = len(self.commands)
        prefix = self.root / f"git-{command_id:04d}"
        env = {key: os.environ[key] for key in ("PATH", "TMPDIR", "SYSTEMROOT") if key in os.environ}
        env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                   GIT_TERMINAL_PROMPT="0", GIT_ALLOW_PROTOCOL="file", GIT_NO_REPLACE_OBJECTS="1",
                   LC_ALL="C")
        command = [self.executable, "--literal-pathspecs", "-c", f"core.hooksPath={os.devnull}",
                   "-c", "protocol.allow=never", "-c", "protocol.file.allow=always",
                   "-c", "core.fsmonitor=false", "-c", f"core.attributesFile={os.devnull}",
                   "-c", "core.autocrlf=false", "-c", "gc.auto=0", "-c", "maintenance.auto=false",
                   "-c", "fetch.unpackLimit=0", "-c", "transfer.unpackLimit=0",
                   "-c", "fetch.fsckObjects=true", "-c", "transfer.fsckObjects=true"]
        if repository is not None:
            command += ["-C", str(repository)]
        command += list(arguments)
        launch = [sys.executable, "-I", "-c", _LIMIT_SCRIPT,
                  str(min(CPU_SECONDS, max(1, math.ceil(remaining)))), str(FILE_BYTES), *command]
        stdout, stderr = bytearray(), bytearray()
        overflow = threading.Event()
        process = None
        input_stream = None
        readers: list[threading.Thread] = []
        started = time.monotonic()
        row: dict[str, Any] = {"arguments": list(arguments), "repository": str(repository) if repository else None,
                               "stdout_limit": maximum, "stderr_limit": 65_536,
                               "cpu_seconds": min(CPU_SECONDS, max(1, math.ceil(remaining))),
                               "file_bytes": FILE_BYTES, "timed_out": False}
        try:
            if input_bytes is not None:
                if len(input_bytes) > MAX_OUTPUT_BYTES:
                    raise BundleError("Git input exceeds limit")
                input_path = prefix.with_suffix(".stdin")
                with input_path.open("xb") as input_writer:
                    input_writer.write(input_bytes)
                input_stream = input_path.open("rb")
                row["input_sha256"] = _sha(input_bytes)
            process = subprocess.Popen(launch, env=env, stdin=input_stream or subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)

            def drain(stream: Any, target: bytearray, limit: int) -> None:
                assert process is not None
                try:
                    while chunk := stream.read(65_536):
                        available = max(0, limit - len(target))
                        target.extend(chunk[:available])
                        if len(chunk) > available:
                            overflow.set()
                            try:
                                os.killpg(process.pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                except OSError:
                    overflow.set()
                finally:
                    stream.close()

            for stream, target, limit in ((process.stdout, stdout, maximum), (process.stderr, stderr, 65_536)):
                reader = threading.Thread(target=drain, args=(stream, target, limit), daemon=True)
                reader.start()
                readers.append(reader)
            try:
                row["returncode"] = process.wait(timeout=max(0.001, self.deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                row["timed_out"] = True
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                row["returncode"] = process.wait(timeout=5)
            for reader in readers:
                reader.join(timeout=5)
            if any(reader.is_alive() for reader in readers):
                raise BundleError("Git output readers did not finish")
            if row["timed_out"] or overflow.is_set() or row["returncode"] != 0:
                raise BundleError("Git command failed or exceeded its resource bounds")
            return bytes(stdout)
        except OSError as error:
            raise BundleError("Git subprocess unavailable") from error
        finally:
            # A leader may exit while one of its descendants still holds a
            # pipe. Quiesce the owned group even after the leader has exited.
            if process is not None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=5)
            for reader in readers:
                reader.join(timeout=5)
            if input_stream is not None:
                input_stream.close()
            output, errors = bytes(stdout), bytes(stderr)
            row.update(elapsed_seconds=time.monotonic() - started, output_limited=overflow.is_set(),
                       readers_finished=all(not reader.is_alive() for reader in readers),
                       stdout_sha256=_sha(output), stderr_sha256=_sha(errors))
            prefix.with_suffix(".stdout").write_bytes(output)
            prefix.with_suffix(".stderr").write_bytes(errors)
            _save(prefix.with_suffix(".json"), row)
            self.commands.append(row)
            if not row["readers_finished"]:
                raise BundleError("Owned Git output readers could not be quiesced")


def _path(value: bytes) -> str:
    try:
        name = value.decode("ascii")
    except UnicodeError as error:
        raise BundleError("Repository profile requires ASCII paths") from error
    parts = name.split("/")
    if (len(name) > 240 or len(parts) > 16 or any(_SEGMENT.fullmatch(part) is None for part in parts)):
        raise BundleError("Unsafe or unsupported repository path")
    return name


def _inspect(git: _Git, repository: Path, offered: str, base: str, object_format: str,
             *, quarantine: bool) -> dict[str, Any]:
    for sha in (offered, base):
        resolved = git.call(repository, "rev-parse", "--verify", f"{sha}^{{commit}}").decode().strip()
        if resolved != sha:
            raise BundleError("Offer/base identity is not an exact commit")
    git.call(repository, "merge-base", "--is-ancestor", base, offered)
    reachable = git.call(repository, "rev-list", "--objects", "--no-object-names", offered).decode("ascii").splitlines()
    if not reachable or len(reachable) > MAX_OBJECTS or len(set(reachable)) != len(reachable):
        raise BundleError("Reachable object count exceeds profile")
    for oid in reachable:
        _oid(oid, object_format)
    inventory_raw = git.call(repository, "cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)",
                             input_bytes=("\n".join(sorted(reachable)) + "\n").encode("ascii"))
    inventory: list[dict[str, Any]] = []
    for line in inventory_raw.decode("ascii").splitlines():
        parts = line.split(" ")
        if len(parts) != 3 or parts[1] not in {"blob", "tree", "commit"} or not parts[2].isdigit():
            raise BundleError("Unsupported or missing Git object")
        inventory.append({"oid": _oid(parts[0], object_format), "type": parts[1], "bytes": int(parts[2])})
    if [item["oid"] for item in inventory] != sorted(reachable):
        raise BundleError("Object census does not match reachable history")
    expanded = sum(item["bytes"] for item in inventory)
    commits = [item["oid"] for item in inventory if item["type"] == "commit"]
    if expanded > MAX_EXPANDED_BYTES or not 1 <= len(commits) <= MAX_COMMITS:
        raise BundleError("Expanded object or commit bound exceeded")
    if quarantine:
        all_objects = git.call(repository, "cat-file", "--batch-all-objects", "--batch-check=%(objectname)").decode("ascii").splitlines()
        if sorted(all_objects) != sorted(reachable):
            raise BundleError("Quarantine contains unreachable or extra objects")
    blobs: dict[str, dict[str, Any]] = {}
    for item in inventory:
        if item["type"] != "blob":
            continue
        if item["bytes"] > MAX_BLOB_BYTES:
            raise BundleError("Blob exceeds repository profile")
        data = git.call(repository, "cat-file", "blob", item["oid"], maximum=MAX_BLOB_BYTES)
        try:
            data.decode("utf-8", errors="strict")
        except UnicodeError as error:
            raise BundleError("Repository profile requires UTF-8 text blobs") from error
        if b"\x00" in data or len(data) != item["bytes"]:
            raise BundleError("Binary or inconsistent Git blob")
        blobs[item["oid"]] = {"bytes": len(data), "sha256": _sha(data)}
    trees = {item["oid"] for item in inventory if item["type"] == "tree"}
    offered_files: list[dict[str, Any]] | None = None
    for commit in commits:
        listing = git.call(repository, "ls-tree", "-r", "-t", "-z", "--full-tree", commit)
        files: list[dict[str, Any]] = []
        prefixes: dict[str, str] = {}
        for raw in listing.split(b"\x00"):
            if not raw:
                continue
            fields, separator, raw_path = raw.partition(b"\t")
            tree_fields = fields.split(b" ")
            if (not separator or len(tree_fields) != 3
                    or tree_fields[:2] not in ([b"100644", b"blob"], [b"040000", b"tree"])):
                raise BundleError("Only ordinary nonexecutable files are supported")
            name, oid = _path(raw_path), tree_fields[2].decode("ascii")
            if oid not in (blobs if tree_fields[1] == b"blob" else trees):
                raise BundleError("Tree references an unknown object")
            segments = name.split("/")
            for index in range(1, len(segments) + 1):
                prefix = "/".join(segments[:index])
                folded = prefix.casefold()
                if folded in prefixes and prefixes[folded] != prefix:
                    raise BundleError("Case-insensitive path collision")
                prefixes[folded] = prefix
            if tree_fields[1] == b"blob":
                files.append({"path": name, "mode": "100644", "oid": oid, **blobs[oid]})
        if len(files) > MAX_FILES:
            raise BundleError("Repository file count exceeds profile")
        if commit == offered:
            offered_files = sorted(files, key=lambda item: item["path"])
    if offered_files is None:
        raise BundleError("Offered commit missing from inspected history")
    tree = git.call(repository, "rev-parse", "--verify", f"{offered}^{{tree}}").decode().strip()
    _oid(tree, object_format)
    return {"object_count": len(inventory), "commit_count": len(commits), "expanded_bytes": expanded,
            "objects_sha256": _sha(_json(inventory)), "tree_sha": tree,
            "files_sha256": _sha(_json(offered_files)), "file_count": len(offered_files),
            "source_bytes": sum(item["bytes"] for item in offered_files),
            "objects": inventory, "files": offered_files}


def export_bundle(store: GitStore, offered_sha: str, base_sha: str) -> tuple[bytes, dict[str, Any]]:
    """Export one pinned proposal; return actual self-contained bundle bytes.

    Command logs, the bundle and its local inspection are retained in a fresh
    sibling `peer-bundle-exports` directory. No sender path enters the manifest.
    """
    offered, base = _oid(offered_sha), _oid(base_sha)
    root = store.path.parent / "peer-bundle-exports" / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=False)
    git = _Git(root)
    try:
        object_format = git.call(store.path, "rev-parse", "--show-object-format").decode().strip()
        if object_format not in {"sha1", "sha256"}:
            raise BundleError("Unsupported Git object format")
        _oid(offered, object_format)
        _oid(base, object_format)
        ref = "refs/harness/proposals/" + offered
        if git.call(store.path, "rev-parse", "--verify", ref).decode().strip() != offered:
            raise BundleError("Proposal ref does not pin offered commit")
        inspection = _inspect(git, store.path, offered, base, object_format, quarantine=False)
        path = root / "proposal.bundle"
        git.call(store.path, "bundle", "create", "--version=3", str(path), ref)
        if not 1 <= path.stat().st_size <= MAX_BUNDLE_BYTES:
            raise BundleError("Exported bundle byte bound exceeded")
        payload = path.read_bytes()
        manifest = {"protocol": PROTOCOL, "profile": PROFILE, "object_format": object_format,
                    "base_sha": base, "offered_sha": offered, "offered_ref": ref,
                    "bundle_sha256": _sha(payload), "bundle_bytes": len(payload),
                    **{key: value for key, value in inspection.items() if key not in {"objects", "files"}}}
        manifest = _manifest(manifest)
        _header(payload, manifest)
        _save(root / "manifest.json", manifest)
        _save(root / "receipt.json", {"protocol": PROTOCOL, "status": "exported", "manifest": manifest,
                                      "inspection": inspection, "commands": len(git.commands),
                                      "candidate_execution": False, "publication_authorized": False})
        return payload, manifest
    except Exception as error:
        _save(root / "failure.json", {"protocol": PROTOCOL, "status": "rejected", "error_type": type(error).__name__,
                                      "commands": len(git.commands), "publication_authorized": False})
        raise


def import_bundle(payload: bytes, manifest: dict[str, Any], root: Path) -> GitStore:
    """Verify supplied bytes in fresh retained quarantine, never a sender path.

    The returned GitStore has only a proposal ref and no accepted branch. Pass
    it as the source of the common exact-tree prepare gate. Import is neither a
    validation pass nor permission to move an accepted reference.
    """
    if type(payload) is not bytes or not 1 <= len(payload) <= MAX_BUNDLE_BYTES:
        raise BundleError("Bundle must be bounded immutable bytes")
    checked = _manifest(manifest)
    root = Path(root).absolute()
    if root.exists() or root.is_symlink():
        raise FileExistsError("Quarantine root must be fresh")
    root.mkdir(parents=True, exist_ok=False)
    git = _Git(root)
    try:
        path = root / "received.bundle"
        with path.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        _save(root / "manifest.json", checked)
        if checked["bundle_sha256"] != _sha(payload) or checked["bundle_bytes"] != len(payload):
            raise BundleError("Received bytes differ from their descriptor")
        _header(payload, checked)
        repository = root / "quarantine.git"
        git.call(None, "init", "--bare", "--initial-branch=accepted", "--object-format=" + checked["object_format"], str(repository))
        git.call(repository, "bundle", "verify", str(path))
        git.call(repository, "fetch", "--no-tags", "--no-write-fetch-head", str(path),
                 checked["offered_ref"] + ":" + checked["offered_ref"])
        refs = git.call(repository, "for-each-ref", "--format=%(objectname) %(refname)").decode().splitlines()
        if refs != [checked["offered_sha"] + " " + checked["offered_ref"]]:
            raise BundleError("Quarantine refs differ from the advertised proposal")
        git.call(repository, "fsck", "--full", "--strict", "--no-reflogs")
        inspection = _inspect(git, repository, checked["offered_sha"], checked["base_sha"],
                              checked["object_format"], quarantine=True)
        if any(checked[key] != value for key, value in inspection.items() if key not in {"objects", "files"}):
            raise BundleError("Receiver object/source census differs from descriptor")
        # GitStore recognition is granted only after complete receiver checks.
        marker = repository / "gossip-harness-store"
        with marker.open("x") as stream:
            stream.write("Receiver-owned peer bundle quarantine; version 1\n")
            stream.flush()
            os.fsync(stream.fileno())
        store = GitStore(repository)
        _save(root / "receipt.json", {"protocol": PROTOCOL, "status": "quarantined", "manifest": checked,
                                      "inspection": inspection, "commands": len(git.commands),
                                      "candidate_execution": False, "publication_authorized": False})
        return store
    except Exception as error:
        _save(root / "failure.json", {"protocol": PROTOCOL, "status": "rejected", "error_type": type(error).__name__,
                                      "commands": len(git.commands), "publication_authorized": False})
        raise
