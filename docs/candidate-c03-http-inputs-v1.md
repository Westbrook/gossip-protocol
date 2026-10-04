# Confined input staging for HTTP histories

`gossip_harness/candidate_http_inputs_v1.py` stages and verifies the data fixtures
used by the versioned HTTP executor. It accepts the exact immutable `Fixture`
values from the published literal catalog. It performs host filesystem work only;
it neither launches a candidate nor establishes product behavior, container
isolation, successful execution or acceptance.

## API and declaration boundary

- `validate(entries)` checks an immutable tuple of exact `Fixture` objects,
  reconstructing each value through its frozen constructor. Runtime limits are
  1,024 entries and 8 MiB of file content in total. Every parent is an explicit
  directory, every path is unique, and symlink targets must resolve through
  declared real entries inside the owned outer input tree.
- `manifest(entries)` returns protocol `candidate-http-inputs-v1`, ordered
  `entries`, `entry_count` and `file_bytes`. Each entry contains `path`, `kind`,
  `bytes`, `sha256` and `target`. File length/hash and link target are present only
  for their corresponding kinds; other fields are null. Payload bytes and host
  paths are omitted. This is a declaration, not an authenticated observation.
- `stage(root, entries)` requires an existing, empty, absolute real directory.
  It creates explicit directories first, regular files next and data links last,
  then calls `verify`. It never overwrites an existing entry. A failure leaves
  existing or partially written state for the owner to inspect; this helper
  does not delete it or retry the operation.
- `verify(root, entries)` requires exact tree shape, types, file bytes and literal
  link targets. Regular files have mode `0444` and exactly one hard link;
  directories have mode `0755`. The root's own mode is not prescribed.

These are evaluator staging constraints. They do not narrow the product's source
key rules. In particular, the declarations can represent the published invalid
257-byte/17-segment product keys. Fixture limits apply to the whole owned staging
tree, not to the selected service root or a candidate's database.

## No-follow filesystem operations

The helper opens `/` and traverses every root path component through directory
file descriptors with `O_DIRECTORY` and `O_NOFOLLOW`. Relative traversal starts
from that owned root descriptor. A symlink in any root or parent component is
rejected; resolving a path before reading it does not substitute for this check.
Callers should supply a canonical path, such as a resolved disposable temporary
parent followed by a newly created `inputs` directory.

Directory enumeration is bounded by the declared child count. Each entry is
inspected without following links. File verification requires the expected
regular-file metadata before opening, opens with `O_NOFOLLOW` and `O_NONBLOCK`,
compares descriptor identity with the inspected entry, and reads at most the
expected length plus one byte. It then compares exact content and pre/post
metadata and confirms that the pathname still names that descriptor. Nonblocking
open prevents a replaced FIFO from hanging this read. File permissions, link
count and size checks are independent of byte comparison.

Link verification uses `lstat` semantics and `readlink`, retaining the exact target
string. It never reads a link's destination. Directory metadata is checked around
each inspection, and the absolute root is reopened without following links to
confirm that its device/inode identity remains the same.

The declaration validator examines raw target components before normalizing
parent traversal. A target such as `p/link/../../outside` cannot conceal traversal
through another symlink merely because lexical normalization produces an owned
path. Absolute/outer escapes, missing targets, chains, cycles, linked ancestors
and children below files or links reject. A link may cross the candidate's chosen
service root into another declared directory inside the outer `/inputs` tree;
that is a deliberate test input, not a host escape.

## Integration and limits

The controller must create the empty owned input root, pass exactly the registered
fixture tuple, bind its manifest and perform verification at the declared
lifecycle boundaries. It must still inspect the container's complete mount
inventory and require a read-only private input bind. Candidate source and trusted
probe helper staging retain their separate, link-free checks. Expected outputs,
state oracles and evaluator code must never be added to candidate mounts.

These checks are bounded snapshots of trusted owned staging. They do not make
arbitrary hostile host mutation atomic, lock the filesystem, prove what bytes a
candidate read, or establish that a container used the declared database. The
fixture tree must stay under the trusted controller's ownership throughout the
history. Physical root/link, mount, same-volume and process-lineage controls are
separate requirements for the new executor. A staging failure is evaluator
uncertainty, not a failed application requirement.

The scoped declaration, staging and verification tests exercise disposable host
files and the six actual catalog link layouts. Their root-owned combined receipt
is authoritative for the final executed count and source identity. No previous
container qualification is upgraded by these host checks, and this module adds
no comparative model result.
