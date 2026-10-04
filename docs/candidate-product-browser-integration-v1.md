# Candidate browser integration checkpoint

The browser evaluator is integrated and passes 97 fresh affected offline checks.
Its first physical qualification attempt failed; six controls did not run.
This is an incomplete engineering checkpoint, with zero new model-quality samples.

The intended stale-commit control reached only the opening browser action. Two
actual GET requests were sent and captured; no control reads or stale mutation
were observed. Node exited with code 2, and the owner encountered a broken pipe.
The retained evidence does not establish the underlying Node exit exception.
Engine resource cleanup was verified, while Chromium close was unconfirmed.
The observer correctly marked the missing evidence unavailable and did not grant
acceptance or a successful negative-control result.

The offline and physical receipts were reconciled against all 686 current source
inputs and their original final runtime identities. No receipt was reused. The
original failed run remains retained; there has been no automatic retry or
deadline expansion. See the [machine-readable checkpoint](../analysis/candidate-product-browser-integration-v1.json)
for exact evidence paths, hashes and counts, and the [evaluator contract](candidate-product-browser-v1.md)
for its limited DOM/UI-over-bridge scope.

The next step is to resolve this observed execution failure and qualify all seven
controls. The larger live comparison, gossip-versus-orchestration comparison,
and independent task-family confirmation remain outstanding.
