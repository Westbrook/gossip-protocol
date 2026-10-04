# HTTP v3 bounded journal reader

`candidate_http_journal_v3` reads controller-authored journal artifacts up to
32 MiB. It is separate from the frozen Engine control decoder, whose 1 MiB
limit remains unchanged. A large journal is not a large Engine response, and
neither successful parsing nor canonical framing authenticates its contents.
The helper starts no process and grants no execution or acceptance authority.

The public API is:

- `read(path, *, max_bytes=MAX_RECORD_BYTES) -> bytes`
- `decode(raw, *, max_bytes=MAX_RECORD_BYTES) -> Any`
- `read_json(path, *, max_bytes=MAX_RECORD_BYTES) -> Any`
- `evaluator_sources() -> dict[str, str]`

`max_bytes` must be a positive exact integer no larger than 33,554,432. Booleans,
floats and attempts to expand the ceiling are rejected. JSON values may have
object, array or scalar roots; callers impose their own expected record schema.
The source map allows the execution owner to bind this helper to its evaluator
identity. Computing that map does not authorize or authenticate a journal.

## Parsing envelope

The fixed prospective limits are 32 MiB of raw bytes, **64 nested containers**
and **1,000,000 nodes**. Each object, array, member name and scalar value counts
once. The root container has depth one; a scalar root has depth zero. A lexical
scan enforces depth and node limits before the JSON decoder allocates containers.
String contents count as one node, including apparent JSON syntax and escaped
quotes. The scan does not replace grammar validation or relax malformed input.
These limits are evaluator observation bounds, not product requirements.

`decode` accepts immutable bytes containing one UTF-8 JSON value with optional
JSON whitespace. It rejects duplicate object names, including names that become
equal after escape decoding; NaN, Infinity and floating-point overflow; invalid
UTF-8, BOMs, UTF-16/32, unpaired surrogate escapes, malformed syntax and trailing
values. Standard-library integer representation bounds also remain in force;
an unrepresentable number is an unavailable journal, not a product judgment.
This trusted-journal dialect is not a candidate-response parser or a new product
JSON contract.

`read_json` additionally preserves the authored framing rule formerly enforced
by `finite._json`: exact UTF-8 bytes from `json.dumps` with sorted keys, compact
separators, `ensure_ascii=False` and `allow_nan=False`. It therefore rejects
otherwise valid JSON with extra whitespace, reordered keys, alternate number
spelling or escaped Unicode where the canonical encoder writes the character.
`decode` remains available where a caller explicitly needs strict syntax without
that canonical framing rule. Neither path silently coerces duplicate keys.

## Stable no-follow reads

`read` opens every directory component through an anchored descriptor with
`O_NOFOLLOW` and `O_DIRECTORY`, then opens the final file with `O_NOFOLLOW` and
`O_NONBLOCK`. Linked ancestors, a linked final path, directories, FIFOs and other
nonregular types are rejected before content is read. The journal owner already
requires a canonical real root; a platform alias such as a symlinked temporary
directory must be resolved by that owner before calling this helper. The helper
itself never resolves or follows such aliases.

The file's device, inode, mode, link count, size and nanosecond modification/change
timestamps must match its directory entry before open, its opened descriptor,
the descriptor after reading, and the final directory entry. The read is bounded
by the observed size plus a single byte to detect growth. Ancestor entries are
rechecked against their opened directory identities afterward. Size changes,
same-size rewrites, replacement, symlink substitution and ancestor replacement
are rejected. Explicit `..` path components are rejected.

These checks detect the tested filesystem changes; they are not an immutable
snapshot against arbitrary concurrent privileged mutation. They do not bind a
record to a source tree, request, runtime, purpose, chronology or checkpoint.
The controller must still independently authenticate all retained byte hashes,
external checkpoints and schemas. Aggregate journal quota, cleanup reserves,
ordered reference reconstruction and dependent-dispatch decisions belong to the
execution controller, outside this helper.

## Verification scope

Disposable temporary-file tests cover canonical reads, the actual 32 MiB byte
ceiling, the million-node and 64-container boundaries, node counting before
materialization, duplicate/nonfinite/encoding rejection, and final/ancestor
symlinks, nonregular files, growth, truncation, same-size rewrite and replacement
races. A valid journal larger than 1 MiB is accepted here and rejected by the
unchanged Engine decoder. That comparison invokes only its pure decoder; no
Engine endpoint, candidate process or provider is contacted. All historical
sources, pending CLI work and retained study evidence remain untouched.
