# Versioned JSON intake reference repair

The new `library_v2_json_reference_v2` source generator repairs the diagnosed
reference defect recorded in
[`candidate-b02-diagnostic-v1.md`](candidate-b02-diagnostic-v1.md). The frozen
reference and its failed physical receipts remain intact. Only the generated
`library/ingestion/jobs.py` changes.

`corrected_v2_files()` returns the complete corrected text tree;
`corrected_v2_binary_files()` returns the unchanged compatibility snapshots.
`corrected_v2_source_inputs()` verifies and returns 41 exact source identities:
the new generator checked against its import-time identity, 31 transitive authored
generator modules, three normative inputs and six public compatibility
fixture/constructor inputs. The caller must bind that inventory, generated
candidate bytes and the execution contract in its prospective freeze. The baseline tree digest binds every generated
file, including the outputs of already-loaded generator functions. Every call
checks dependency files before and after generation. These checks detect changed
inputs or output drift; they are not a hostile-host attestation mechanism.

The exact original jobs source and both textual transformation seams are guarded.
The replacement adds `_read_json_path`, using the existing `_path_fd` traversal
and regular-file checks. It reads the opened descriptor until EOF, translates
`OSError` with the existing `_io_error`, and closes the descriptor in `finally`.
Only `submit_json` calls this helper. All ZIP/archive limits, directory handling,
UTF-8 decoding, strict JSON parsing, schema validation, decoded content limits,
submission and transaction logic are unchanged.

M1-I24 accounts for UTF-8 bytes of decoded text. The inherited 32,768-byte member,
524,288-byte total and 64-member limits remain enforced. The archive's
1,048,576-byte bound still applies to ZIP. No encoded JSON-file limit is declared:
control-character escaping and insignificant JSON whitespace can expand legal
serialization. Consequently this repair introduces no replacement JSON wire cap.
The reference materializes the JSON bytes and parsed values in memory. Ordinary
container memory/time limits still constrain execution; exhaustion is an
infrastructure observation, not an invented product `too_large` judgment. A future
streaming parser would need separately qualified semantics and is outside this
small repair.

The new host checks parse generated source without importing or executing it.
They establish narrow change scope, unchanged baseline outputs, transitive input
coverage, dependency/output-drift rejection, and preserved descriptor/error
structure. They do not establish runtime behavior. The verification owner must
freeze and run the applicable candidate histories in fresh containers.

Prospective runtime controls should include the already-frozen
`intake-json-control-total-byte-limit` history (16 entries of 32,768 U+0001 bytes;
524,288 decoded bytes, 3,146,333 serialized bytes), plus separately named controls
with a small valid JSON object surrounded by over 1 MiB of legal whitespace.
The latter prevents a larger arbitrary wire cap from appearing to fix the issue.
Likewise padded malformed JSON and padded invalid UTF-8 should retain their exact
original parser error classifications; decoded one-over-limit input must still
return `too_large`. These additional controls must not rewrite the frozen 238-case
B02 roster or its original observations.

This is development reference qualification. It supplies no held-out acceptance,
completed requirement count, swarm comparison, or statistical superiority result.
