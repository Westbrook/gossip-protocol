# Browser qualification: second physical attempt

The second browser qualification attempt **failed: one control errored and six were not run**. It ended after 934 seconds. The intended stale-token commit action was never reached, so the run provides no defect-detection qualification or new model-quality sample.

The controller reached its unchanged 900-second work boundary. An independent audit verified 21 complete HTTP exchanges (20 responses with status 200, one with status 409). Request 22 created a probe but lacks ordinary start/request-completion evidence and remains unknown. Four of six browser actions were entered. The original reader then errored while interpreting the incomplete row.

The audit reconstructed all 1,601 journal files and checked 52 evaluator source pins. All 25 claimed Docker resources were removed. The driver exited with signal 9; clean Chromium closure was not acknowledged. Retained-byte consistency and successful resource removal do not repair the failed qualification.

Two separate corrections are being drafted:

1. Preserve fresh full-journal verification while reusing directory descriptors within each checkpoint. Per-file path and identity guards, all data reads/hashes, all 50 full checks per ordinary request and existing deadlines remain required. The benefit is unmeasured.
2. Treat missing, never-acknowledged probe evidence as an unavailable dependency without attempting a read that marks the journal uncertain. Existing authentication for present data, fatal authority errors, initial/final prefix validation and independent cleanup remain mandatory.

No retry has been launched. Both earlier failures and the independently audited 488-test offline pass remain retained at their original source identities. The larger live comparisons remain unfinished.

- [Failure checkpoint](../analysis/candidate-product-browser-physical2-failure-v1.json)
- [Independent audit](../analysis/candidate-product-browser-physical2-audit-v1.json)
- [Preceding offline qualification](cumulative-evaluator-offline-qualification-v1.md)
