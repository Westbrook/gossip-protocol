# Original HTTP mechanics: final physical audit

All **20 original physical tests passed**, and independent review now covers
all 20. The [checkpoint](../analysis/candidate-http-v3-physical-audit-checkpoint.json)
binds the frozen source, four executed classes, retained original logs and four
independent audits. No candidate or test was rerun to assemble this checkpoint.

The final mixed history exercised 87 steps: 54 HTTP exchanges and 29 CLI actions,
with 83 natural finite completions. Review reconciled all 5,137 raw journal files
and 5,137 external checkpoints, both HTTP-to-CLI and CLI-to-HTTP state transitions,
86 containers, one volume, two server epochs and cleanup. The full original batch
took 8,151.46 seconds. These timings belong to the original execution contract.

This is a harness result. It does not accept an agent-generated product or add an
agent-quality comparison. The full 276-history HTTP product catalog remains
separate. Incomplete request-send coverage remains open: the E05 fixture sent
the entire buffered request. Fault fixtures also retain their limits; simulated
storage failure is not physical disk exhaustion, and keeper loss is not a
database-loss recovery experiment.

The newer compact journal changes the execution contract and needs its own
combined qualification. Its storage-only improvement cannot be applied to these
historical timings. Original evidence, including audit-script errors and their
corrections, is retained without replacing the raw execution records.
