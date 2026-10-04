"""Shared executor adapter for an independently anchored compact journal.

The caller owns the HeadAuthority and execution/source/intent semantics. This
adapter never creates an authority, dispatches work, retries an uncertain write,
or closes the caller's authority. Constructor return means a reopened prefix
was fully authenticated before any caller can parse its config. No per-file
full checkpoint sink is introduced; checkpoint() is the explicit full boundary.
"""
from __future__ import annotations

from dataclasses import asdict, fields, is_dataclass
import json
from pathlib import Path
from typing import Any, Iterable

from . import candidate_checkpoint_chain_v1 as chain
from . import candidate_http_journal_v3 as strict

PROTOCOL = "candidate-execution-journal-v1"
ControllerCheckpoint = chain.PrefixCommitment
HeadAuthority = chain.HeadAuthority
Limits = chain.Limits
PriorFact = chain.PriorFact
ChainError = chain.ChainError
ChainUnknown = chain.ChainUnknown
BoundsExceeded = chain.BoundsExceeded


def _context(value: Any, limits: Limits) -> dict[str, Any]:
    """Normalize inert dataclass/tuple declarations, then apply strict JSON bounds.

    Keys remain strings; unsupported objects, cycles and nonfinite/non-scalar
    JSON never become a context. No object hooks or repr fallback are used.
    """
    active: set[int] = set()
    nodes = 0
    text_bytes = 0

    def normalize(item: Any, depth: int) -> Any:
        nonlocal nodes, text_bytes
        nodes += 1
        if depth > strict.MAX_DEPTH or nodes > min(strict.MAX_NODES, limits.max_context_bytes):
            raise ChainError("Context structure bound exceeded")
        if type(item) is str:
            if len(item) > limits.max_context_bytes:
                raise ChainError("Context string bound exceeded")
            text_bytes += len(item.encode("utf-8", errors="strict"))
            if text_bytes > limits.max_context_bytes:
                raise ChainError("Context text bound exceeded")
            return item
        if item is None or type(item) in (bool, int, float):
            return item
        dataclass_instance = is_dataclass(item) and not isinstance(item, type)
        if not dataclass_instance and type(item) not in (dict, list, tuple):
            raise ChainError("Unsupported execution context value")
        identity = id(item)
        if identity in active:
            raise ChainError("Cyclic execution context")
        active.add(identity)
        try:
            pairs: Iterable[tuple[str, Any]]
            if dataclass_instance:
                pairs = ((field.name, getattr(item, field.name)) for field in fields(item))
            elif type(item) is dict:
                pairs = iter(item.items())
            else:
                return [normalize(child, depth + 1) for child in item]
            result = {}
            for key, child in pairs:
                if type(key) is not str:
                    raise ChainError("Execution context keys must be strings")
                normalize(key, depth + 1)
                result[key] = normalize(child, depth + 1)
            return result
        finally:
            active.remove(identity)

    try:
        normalized = normalize(value, 0)
        if type(normalized) is not dict:
            raise ChainError("Execution context must normalize to an object")
        wrapped = {"owner_journal_protocol": PROTOCOL, "execution_context": normalized}
        raw = json.dumps(wrapped, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode("utf-8")
        result = strict.decode(raw, max_bytes=limits.max_context_bytes)
        if type(result) is not dict:
            raise ChainError("Execution context object required")
        return result
    except (TypeError, ValueError, UnicodeError, RecursionError, OverflowError) as error:
        raise ChainError("Invalid bounded execution context") from error


class OwnerJournal:
    """Executor-owned chain; prefix identity is distinct from boundary validation.

    An uncertain journal permits only prior-only facts and close. External
    process cleanup must use the executor's separately authenticated ownership
    evidence and fallback channel. cleanup=True spends reserve only while the
    underlying chain is healthy. No config parsing or authority inference is
    performed here. A digest of independently recomputed config is valid context;
    exclude the resulting context/head and any recursive self-reference.
    """
    def __init__(self, raw_root: Path, delta_root: Path, *, context: Any,
                 authority: HeadAuthority, expected: ControllerCheckpoint | None = None,
                 limits: Limits = Limits()):
        if type(limits) is not Limits or authority is None:
            raise ChainError("Typed limits and caller-owned authority required")
        limits = Limits(**asdict(limits))
        normalized = _context(context, limits)
        if expected is None:
            self._chain = chain.CheckpointChain.create(raw_root, delta_root, context=normalized,
                                                       authority=authority, limits=limits)
        else:
            self._chain = chain.CheckpointChain.reopen(raw_root, delta_root, context=normalized,
                                                       authority=authority, expected=expected, limits=limits)

    def retain(self, name: str, raw: bytes, *, cleanup: bool = False) -> ControllerCheckpoint:
        """Incremental exclusive append, not a full old-byte validation."""
        return self._chain.retain(name, raw, cleanup=cleanup)

    @property
    def raw_root(self) -> Path:
        return self._chain.raw_root

    @property
    def delta_root(self) -> Path:
        return self._chain.delta_root

    def has(self, name: str) -> bool:
        """Healthy acknowledged-prefix membership, not physical suffix absence.

        A caller deciding absence at an authority boundary must first perform
        checkpoint(); extra unanchored disk files are never adopted by has().
        """
        return self._chain.has(name)

    def read(self, name: str) -> bytes:
        """Authenticate current head and consumed raw bytes through the chain."""
        return self._chain.read(name)

    def json(self, name: str) -> Any:
        """Parse only authenticated immutable bytes in the canonical journal dialect.

        A JSON/schema error is not a forged byte observation: it raises without
        changing the chain's acknowledgement. Consumer schemas remain explicit.
        """
        raw = self.read(name)
        try:
            value = strict.decode(raw)
            canonical = json.dumps(value, sort_keys=True, separators=(",", ":"),
                                   ensure_ascii=False, allow_nan=False).encode("utf-8")
            if canonical != raw:
                raise ChainError("Noncanonical executor journal JSON")
            return value
        except (TypeError, ValueError, UnicodeError, RecursionError, OverflowError) as error:
            raise ChainError("Invalid authenticated executor journal JSON") from error

    def checkpoint(self) -> ControllerCheckpoint:
        """Fully validate chain and exact raw inventory at this explicit boundary."""
        return self._chain.validate_boundary().commitment

    def read_prior(self, name: str) -> PriorFact:
        """Authenticate one old acknowledged fact, never an uncertain suffix."""
        return self._chain.read_prior(name)

    @property
    def uncertain(self) -> bool:
        return self._chain.uncertain

    @property
    def commitment(self) -> ControllerCheckpoint:
        """Last acknowledged prefix only; obtaining it does not validate bytes."""
        return self._chain.commitment

    def close(self) -> None:
        """Release the chain; leave caller-owned head/process cleanup untouched."""
        self._chain.close()

    def __enter__(self) -> OwnerJournal:
        self._chain.require_current()
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()
