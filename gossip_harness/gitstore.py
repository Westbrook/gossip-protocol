"""Local distributed Git stores and an exact-tree integration gate.

Every store owns its objects. Proposals, test checkouts, and accepted histories
are separate: preparing an offer never advances ``refs/heads/accepted``.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import tempfile
from typing import Callable, Iterator


ACCEPTED = "refs/heads/accepted"
_MARKER = "gossip-harness-store"
_SHA = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
Validator = Callable[[Path], tuple[bool, str]]


class GitError(RuntimeError):
    """A local Git operation failed."""


@dataclass(frozen=True)
class Candidate:
    status: str
    old_head: str
    candidate_sha: str | None
    offered_sha: str
    detail: str
    changed_paths: tuple[str, ...]


@dataclass(frozen=True)
class IntegrationResult:
    status: str
    old_head: str
    new_head: str
    offered_sha: str
    detail: str


def _run(path: Path | None, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    # Do not inherit config injection, object directories, signing, hooks, or
    # replacement-object settings from the process hosting the experiment.
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update({
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ALLOW_PROTOCOL": "file",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_AUTHOR_NAME": "Gossip Harness",
        "GIT_AUTHOR_EMAIL": "harness@example.invalid",
        "GIT_COMMITTER_NAME": "Gossip Harness",
        "GIT_COMMITTER_EMAIL": "harness@example.invalid",
        "GIT_AUTHOR_DATE": "2000-01-01T00:00:00+0000",
        "GIT_COMMITTER_DATE": "2000-01-01T00:00:00+0000",
        "LC_ALL": "C",
    })
    command = [
        "git", "--literal-pathspecs", "-c", f"core.hooksPath={os.devnull}",
        "-c", "commit.gpgSign=false", "-c", "tag.gpgSign=false",
        "-c", "core.fsmonitor=false", "-c", "core.autocrlf=false",
        "-c", "protocol.allow=never", "-c", "protocol.file.allow=always",
    ]
    if path is not None:
        command += ["-C", str(path)]
    result = subprocess.run(command + list(args), env=env, capture_output=True, timeout=60)
    if check and result.returncode:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise GitError(f"git {args[0]} failed ({result.returncode}): {detail}")
    return result


def _path(name: str) -> PurePosixPath:
    if not isinstance(name, str) or not name or "\\" in name or "\x00" in name:
        raise ValueError(f"Invalid repository path: {name!r}")
    parts = name.split("/")
    if PurePosixPath(name).is_absolute() or any(
        part in ("", ".", "..") or part.casefold() == ".git" for part in parts
    ):
        raise ValueError(f"Invalid repository path: {name!r}")
    return PurePosixPath(name)


def _write_changes(root: Path, changes: dict[str, str | None]) -> None:
    for name, content in changes.items():
        relative = _path(name)
        target = root.joinpath(*relative.parts)
        if target.is_symlink() or not target.resolve().is_relative_to(root.resolve()):
            raise ValueError(f"Refusing to write through a symlink: {name!r}")
        if content is None:
            target.unlink(missing_ok=True)
        elif isinstance(content, str):
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        else:
            raise TypeError("File contents must be text or None for a deletion")


def _names(raw: bytes) -> tuple[str, ...]:
    return tuple(sorted({name.decode("utf-8") for name in raw.split(b"\x00") if name}))


def _tracked_snapshot(checkout: Path, paths: tuple[str, ...]) -> dict[str, tuple | None]:
    """Read actual checkout bytes without trusting Git index flags or filters."""
    snapshot: dict[str, tuple | None] = {}
    for name in paths:
        target = checkout / name
        # A changed ancestor symlink must not redirect snapshot reads outside
        # the checkout (or silently turn an untracked file into tracked input).
        symlink_parent = next((parent for parent in reversed(target.parents)
                               if parent != checkout and checkout in parent.parents
                               and parent.is_symlink()), None)
        if symlink_parent is not None:
            snapshot[name] = ("symlink_parent", str(symlink_parent), os.readlink(symlink_parent))
            continue
        try:
            mode = target.lstat().st_mode
            if stat.S_ISLNK(mode):
                contents = os.fsencode(os.readlink(target))
            elif stat.S_ISREG(mode):
                contents = target.read_bytes()
            else:
                contents = b""
            snapshot[name] = (stat.S_IFMT(mode), bool(mode & 0o111), hashlib.sha256(contents).digest())
        except OSError:
            snapshot[name] = None
    return snapshot


class GitStore:
    """A harness-owned bare repository with an append-only accepted branch."""

    def __init__(self, path: str | Path):
        self.path = Path(path).resolve()
        if not (self.path / _MARKER).is_file():
            raise ValueError(f"Not a harness-owned Git store: {self.path}")
        if self._git("rev-parse", "--is-bare-repository") != "true":
            raise ValueError("A GitStore must be a bare repository")

    @staticmethod
    def _new_path(path: str | Path) -> Path:
        target = Path(path).resolve()
        if target.exists():
            raise FileExistsError(f"Refusing to replace an existing path: {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    @classmethod
    def create(cls, path: str | Path, files: dict[str, str]) -> "GitStore":
        """Create a fixture repository without touching any existing repo."""
        for name, content in files.items():
            _path(name)
            if not isinstance(content, str):
                raise TypeError("Initial file contents must be text")
        target = cls._new_path(path)
        _run(None, "init", "--bare", "--initial-branch=accepted", str(target))
        (target / _MARKER).write_text("Local experiment store; version 1\n", encoding="utf-8")
        store = cls(target)
        with tempfile.TemporaryDirectory(prefix="gossip-git-seed-") as directory:
            checkout = Path(directory)
            _run(None, "init", "--initial-branch=accepted", str(checkout))
            _write_changes(checkout, files)
            _run(checkout, "add", "--all")
            _run(checkout, "commit", "--allow-empty", "-m", "Initial fixture")
            store._git("fetch", "--no-tags", str(checkout), f"{ACCEPTED}:{ACCEPTED}")
        return store

    @classmethod
    def fork(cls, source: "GitStore", path: str | Path) -> "GitStore":
        """Copy accepted history through Git transport, without shared objects."""
        target = cls._new_path(path)
        _run(None, "clone", "--bare", "--no-local", "--no-hardlinks",
             "--single-branch", "--branch", "accepted", str(source.path), str(target))
        (target / _MARKER).write_text("Local experiment store; version 1\n", encoding="utf-8")
        return cls(target)

    def _git(self, *args: str) -> str:
        return _run(self.path, *args).stdout.decode("utf-8").strip()

    def _commit(self, sha: str) -> str:
        if not isinstance(sha, str) or not _SHA.fullmatch(sha):
            raise ValueError("Commit identifiers must be full hexadecimal object IDs")
        return self._git("rev-parse", "--verify", f"{sha}^{{commit}}")

    def head(self) -> str:
        return self._git("rev-parse", "--verify", ACCEPTED)

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        self._commit(ancestor)
        self._commit(descendant)
        result = _run(self.path, "merge-base", "--is-ancestor", ancestor, descendant, check=False)
        if result.returncode not in (0, 1):
            raise GitError(result.stderr.decode("utf-8", errors="replace"))
        return result.returncode == 0

    @contextmanager
    def _checkout(self, sha: str) -> Iterator[Path]:
        with tempfile.TemporaryDirectory(prefix="gossip-git-work-") as directory:
            checkout = Path(directory)
            _run(None, "init", "--initial-branch=accepted", str(checkout))
            _run(checkout, "fetch", "--no-tags", str(self.path), sha)
            _run(checkout, "checkout", "--detach", sha)
            yield checkout

    def propose(self, changes: dict[str, str | None], base_sha: str | None = None,
                message: str = "Proposed change") -> str:
        """Create and retain an immutable proposal without advancing accepted."""
        base = self._commit(base_sha or self.head())
        with self._checkout(base) as checkout:
            _write_changes(checkout, changes)
            _run(checkout, "add", "--all")
            _run(checkout, "commit", "--allow-empty", "-m", message)
            sha = _run(checkout, "rev-parse", "HEAD").stdout.decode().strip()
            self._git("fetch", "--no-tags", str(checkout), f"{sha}:refs/harness/proposals/{sha}")
        return sha

    def _introduced_paths(self, old: str, offered: str) -> tuple[str, ...]:
        paths: set[str] = set()
        commits = self._git("rev-list", f"{old}..{offered}").splitlines()
        for commit in commits:
            parents = self._git("rev-list", "--parents", "-n", "1", commit).split()[1:]
            changed = _names(_run(
                self.path, "diff-tree", "--root", "-m", "--no-commit-id",
                "--name-only", "--no-renames", "-r", "-z", commit,
            ).stdout)
            if len(parents) > 1:
                # A merge may import dependencies already in the accepted tree.
                # Do not charge those imports against its subsystem. Introduced
                # ordinary commits are still checked even if a later commit
                # hides/reverts them. New merge resolutions remain in scope.
                changed = tuple(name for name in changed if
                                self._tree_entry(commit, name) != self._tree_entry(old, name))
            paths.update(changed)
        return tuple(sorted(paths))

    def _tree_entry(self, commit: str, name: str) -> bytes:
        return _run(self.path, "ls-tree", "-z", commit, "--", name).stdout

    @staticmethod
    def _candidate_ref(old: str, candidate: str) -> str:
        return f"refs/harness/candidates/{old}/{candidate}"

    def prepare(self, source: "GitStore", offered_sha: str, expected_head: str,
                validator: Validator, allowed_paths: tuple[str, ...] | None = None) -> Candidate:
        """Merge and test an exact private tree, preserving the accepted ref.

        ``allowed_paths`` contains exact files or directory roots, not globs.
        Scope includes introduced commit history, including edits later reverted.
        """
        old = self._commit(expected_head)
        offered = source._commit(offered_sha)
        if self.head() != old:
            return Candidate("stale", old, None, offered, "Accepted head changed before preparation", ())
        roots = tuple(str(_path(root.rstrip("/"))) for root in allowed_paths) if allowed_paths is not None else None
        self._git("fetch", "--no-tags", str(source.path), f"{offered}:refs/harness/offers/{offered}")
        if self.is_ancestor(offered, old):
            self._git("update-ref", self._candidate_ref(old, old), old)
            return Candidate("noop", old, old, offered, "Offer is already part of accepted history", ())
        changed = self._introduced_paths(old, offered)
        if roots is not None:
            outside = [name for name in changed if not any(
                name == root or name.startswith(root + "/") for root in roots
            )]
            if outside:
                return Candidate("scope_rejected", old, None, offered,
                                 "Introduced history exceeds owned paths: " + ", ".join(outside), changed)
        fast_forward = self.is_ancestor(old, offered)
        with self._checkout(offered if fast_forward else old) as checkout:
            if not fast_forward:
                _run(checkout, "fetch", "--no-tags", str(self.path), offered)
                merged = _run(checkout, "merge", "--no-ff", "--no-commit", "--no-edit", offered, check=False)
                if merged.returncode:
                    conflicts = _names(_run(checkout, "diff", "--name-only", "--diff-filter=U", "-z").stdout)
                    if conflicts:
                        return Candidate("text_conflict", old, None, offered,
                                         "Unresolved merge paths: " + ", ".join(conflicts), changed)
                    raise GitError(merged.stderr.decode("utf-8", errors="replace").strip())
                _run(checkout, "commit", "--allow-empty", "-m", f"Integrate {offered}")
            candidate = _run(checkout, "rev-parse", "HEAD").stdout.decode().strip()
            tracked_paths = _names(_run(checkout, "ls-tree", "-r", "--name-only", "-z", candidate).stdout)
            before_validation = _tracked_snapshot(checkout, tracked_paths)
            try:
                valid, detail = validator(checkout)
            except Exception as error:
                valid, detail = False, f"Validator raised {type(error).__name__}: {error}"
            modified = _run(checkout, "diff", "--quiet", "HEAD", "--", check=False).returncode
            moved = _run(checkout, "rev-parse", "HEAD").stdout.decode().strip() != candidate
            changed_checkout = _tracked_snapshot(checkout, tracked_paths) != before_validation
            if modified or moved or changed_checkout:
                valid, detail = False, "Validator modified the tracked tree or checked-out commit"
            if not valid:
                return Candidate("validation_failed", old, None, offered, str(detail), changed)
            self._git("fetch", "--no-tags", str(checkout),
                      f"{candidate}:{self._candidate_ref(old, candidate)}")
        return Candidate("prepared", old, candidate, offered, str(detail), changed)

    def accept(self, candidate: Candidate) -> IntegrationResult:
        """CAS the exact tested candidate; stale offers cannot alter accepted."""
        if candidate.status not in ("prepared", "noop"):
            return IntegrationResult(candidate.status, candidate.old_head, self.head(),
                                     candidate.offered_sha, candidate.detail)
        sha = candidate.candidate_sha
        if sha is None:
            raise ValueError("Prepared candidate is missing its tested commit")
        self._commit(candidate.old_head)
        self._commit(sha)
        pinned = _run(self.path, "rev-parse", "--verify",
                      self._candidate_ref(candidate.old_head, sha), check=False)
        if pinned.returncode or pinned.stdout.decode().strip() != sha:
            raise ValueError("Candidate was not prepared and pinned by this GitStore")
        if not self.is_ancestor(candidate.old_head, sha):
            raise ValueError("Candidate would delete accepted history")
        result = _run(self.path, "update-ref", ACCEPTED, sha, candidate.old_head, check=False)
        if result.returncode:
            actual = self.head()
            if actual != candidate.old_head:
                return IntegrationResult("stale", candidate.old_head, actual, candidate.offered_sha,
                                         "Accepted head advanced; prepare and validate again")
            raise GitError(result.stderr.decode("utf-8", errors="replace").strip())
        status = "noop" if candidate.status == "noop" else "accepted"
        return IntegrationResult(status, candidate.old_head, sha, candidate.offered_sha, candidate.detail)

    def read_files(self, commit: str | None = None) -> dict[str, str]:
        """Read text fixture files from an immutable commit."""
        sha = self._commit(commit or self.head())
        paths = _names(_run(self.path, "ls-tree", "-r", "--name-only", "-z", sha).stdout)
        return {name: _run(self.path, "cat-file", "blob", f"{sha}:{name}").stdout.decode("utf-8")
                for name in paths}
