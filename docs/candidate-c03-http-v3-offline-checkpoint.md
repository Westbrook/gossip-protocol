# Full HTTP histories: offline implementation checkpoint

The versioned controller now preserves complete HTTP histories, per-epoch roots,
confined input links and finite CLI transitions on one database namespace.
Independent source review and **141 fresh offline checks
in 11 classes passed** after the combined static gate.

- [Execution contract and limits](candidate-c03-http-execution-v3.md)
- [Input staging](candidate-c03-http-inputs-v1.md)
- [Strict journal reader](candidate-c03-http-journal-v3.md)
- [Exact source identities, checks and measurements](../analysis/candidate-c03-http-v3-offline-checkpoint.json)
- [Independent source-review projection](../analysis/candidate-c03-http-v3-source-review.json)

Two complete catalog histories were exercised through the controller with inert
Engine/socket boundaries: the 87-step history containing 29 finite CLI steps,
and a 70-request history. Their real host journal sizes and elapsed test times
are retained in the checkpoint. These are controller capacity observations,
not candidate executions or swarm speed measurements.

The controller retains full step, observation and epoch rows before compact
terminal references. Failed retention preserves the distinction between an
attempted operation with unavailable evidence and an unentered suffix. Cleanup
has protected journal space and a bounded time opportunity; successful resource
removal is still something to observe, never assume.

The final gate had no failures, skips, reuse or source/runtime drift. It covered
443 static sources and the declared type roots, with
117 unchanged historical type
diagnostics. This is not a claim of whole-repository type safety.

The first combined attempt hit its 180-second lifecycle-class timeout after
129 passes; one test was interrupted and 11 were not run. Its logs and partial
journal remain preserved. Only that offline class window changed to 1,800
seconds before the fresh final gate; production limits and assertions stayed
unchanged. The timeout is a verification failure, not a candidate-quality result.

**Fresh physical qualification and candidate acceptance remain unfinished.**
No model, candidate or Engine call was made in this checkpoint. Qualification
receipts cannot be renamed into independent acceptance. The full acceptance
bridge, cumulative project rehearsal and matched live comparison remain
required, with the existing approval boundaries unchanged.
