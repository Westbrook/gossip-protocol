"""Closed prospective finite mappings; declarations never grant semantic authority.

The 140 individual allocations preserve the original histories and expected
values. The HTTP profile retains MAP-A and therefore requires fresh independent
acceptance plus independently approved PUBLIC/SUPPLEMENT purpose conversions.
The selected B02 subset excludes the separate 42 intake associations. Every
source unit and every other kind/lane remains in the full scope denominator.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Any

HTTP_MAPPING = "cumulative-finite-http-readbacks-v1"
STORAGE_MAPPING = "cumulative-finite-b02-mapping-v1"
M2_MAPPING = "m2-finite-mapping-v2"
HTTP_CAPABILITIES = ("http", "public-contract")
HTTP_PURPOSE_CONVERSIONS = (
    ("M1-GATE-PUBLIC", "public_development", "independent_acceptance"),
    ("M1-GATE-V0-SUPPLEMENT", "public_contract_regression", "independent_acceptance"),
)
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


@dataclass(frozen=True)
class Allocation:
    unit_id: str
    history_id: str
    predicate_id: str
    kind: str
    logical_gate_id: str
    rationale_id: str


ALLOCATIONS = (
    Allocation('m1:V0-ID-01', 'HTTP-DOCUMENT-ROUTES/show-0', 'document-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-document-value'),
    Allocation('m1:V0-DOC-01', 'HTTP-DOCUMENT-ROUTES/show-0', 'document-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-document-value'),
    Allocation('shared:value_types:DOCUMENT0', 'HTTP-DOCUMENT-ROUTES/show-0', 'document-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-document-value'),
    Allocation('m1:V0-ID-01', 'HTTP-DOCUMENT-ROUTES/show-1', 'document-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-document-value'),
    Allocation('m1:V0-DOC-01', 'HTTP-DOCUMENT-ROUTES/show-1', 'document-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-document-value'),
    Allocation('shared:value_types:DOCUMENT0', 'HTTP-DOCUMENT-ROUTES/show-1', 'document-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-document-value'),
    Allocation('m1:V0-ID-01', 'HTTP-DOCUMENT-ROUTES/show-2', 'document-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-document-value'),
    Allocation('m1:V0-DOC-01', 'HTTP-DOCUMENT-ROUTES/show-2', 'document-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-document-value'),
    Allocation('shared:value_types:DOCUMENT0', 'HTTP-DOCUMENT-ROUTES/show-2', 'document-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-document-value'),
    Allocation('m1:V0-ID-01', 'HTTP-DOCUMENT-ROUTES/show-3', 'document-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-document-value'),
    Allocation('m1:V0-DOC-01', 'HTTP-DOCUMENT-ROUTES/show-3', 'document-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-document-value'),
    Allocation('shared:value_types:DOCUMENT0', 'HTTP-DOCUMENT-ROUTES/show-3', 'document-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-document-value'),
    Allocation('m1:V0-ID-02', 'HTTP-DOCUMENT-ROUTES/show-0', 'document-subject', 'history', 'M1-GATE-PUBLIC', 'paired-document-identity'),
    Allocation('m1:V0-ID-02', 'HTTP-DOCUMENT-ROUTES/show-0', 'document-subject', 'history', 'M1-GATE-V0-SUPPLEMENT', 'paired-document-identity'),
    Allocation('m1:V0-ID-02', 'HTTP-DOCUMENT-ROUTES/show-1', 'document-subject', 'history', 'M1-GATE-PUBLIC', 'paired-document-identity'),
    Allocation('m1:V0-ID-02', 'HTTP-DOCUMENT-ROUTES/show-1', 'document-subject', 'history', 'M1-GATE-V0-SUPPLEMENT', 'paired-document-identity'),
    Allocation('m1:V0-ID-02', 'HTTP-PERSIST-LISTENER/root-independence-source-ids', 'first-root-same-document', 'history', 'M1-GATE-PUBLIC', 'same-document-after-root-replacement'),
    Allocation('m1:V0-ID-02', 'HTTP-PERSIST-LISTENER/root-independence-source-ids', 'first-root-same-document', 'history', 'M1-GATE-V0-SUPPLEMENT', 'same-document-after-root-replacement'),
    Allocation('m1:V0-ID-02', 'HTTP-PERSIST-LISTENER/root-independence-source-ids', 'second-root-same-document', 'history', 'M1-GATE-PUBLIC', 'same-document-after-root-replacement'),
    Allocation('m1:V0-ID-02', 'HTTP-PERSIST-LISTENER/root-independence-source-ids', 'second-root-same-document', 'history', 'M1-GATE-V0-SUPPLEMENT', 'same-document-after-root-replacement'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-DOCUMENT-ROUTES/unchanged-import', 'subject-after-documents', 'history', 'M1-GATE-PUBLIC', 'unchanged-public-readback'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-DOCUMENT-ROUTES/unchanged-import', 'subject-after-documents', 'history', 'M1-GATE-V0-SUPPLEMENT', 'unchanged-public-readback'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-ERROR-STATUS/source-changed', 'subject-after-documents', 'history', 'M1-GATE-PUBLIC', 'refused-public-readback'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-ERROR-STATUS/source-changed', 'subject-after-documents', 'history', 'M1-GATE-V0-SUPPLEMENT', 'refused-public-readback'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-DOCUMENT-ROUTES/unchanged-import', 'subject-after-export', 'history', 'M1-GATE-PUBLIC', 'unchanged-public-readback'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-DOCUMENT-ROUTES/unchanged-import', 'subject-after-export', 'history', 'M1-GATE-V0-SUPPLEMENT', 'unchanged-public-readback'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-ERROR-STATUS/source-changed', 'subject-after-export', 'history', 'M1-GATE-PUBLIC', 'refused-public-readback'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-ERROR-STATUS/source-changed', 'subject-after-export', 'history', 'M1-GATE-V0-SUPPLEMENT', 'refused-public-readback'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-ERROR-STATUS/source-changed', 'source-changed-subject', 'negative', 'M1-GATE-PUBLIC', 'classified-source-change'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-ERROR-STATUS/source-changed', 'source-changed-subject', 'negative', 'M1-GATE-V0-SUPPLEMENT', 'classified-source-change'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-PERSIST-LISTENER/root-change-source-changed', 'root-change-refusal', 'negative', 'M1-GATE-PUBLIC', 'classified-source-change'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-PERSIST-LISTENER/root-change-source-changed', 'root-change-refusal', 'negative', 'M1-GATE-V0-SUPPLEMENT', 'classified-source-change'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-PERSIST-LISTENER/root-change-source-changed', 'second-root-preserved-documents', 'history', 'M1-GATE-PUBLIC', 'refused-public-readback'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-PERSIST-LISTENER/root-change-source-changed', 'second-root-preserved-documents', 'history', 'M1-GATE-V0-SUPPLEMENT', 'refused-public-readback'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-PERSIST-LISTENER/root-change-source-changed', 'second-root-preserved-export', 'history', 'M1-GATE-PUBLIC', 'refused-public-readback'),
    Allocation('m1:V0-REPLAY-01', 'HTTP-PERSIST-LISTENER/root-change-source-changed', 'second-root-preserved-export', 'history', 'M1-GATE-V0-SUPPLEMENT', 'refused-public-readback'),
    Allocation('m1:V0-QUERY-01', 'HTTP-DOCUMENT-ROUTES/none', 'documents-subject', 'positive', 'M1-GATE-PUBLIC', 'literal-casefold-search'),
    Allocation('m1:V0-QUERY-01', 'HTTP-DOCUMENT-ROUTES/none', 'documents-subject', 'positive', 'M1-GATE-V0-SUPPLEMENT', 'literal-casefold-search'),
    Allocation('m1:V0-QUERY-01', 'HTTP-DOCUMENT-ROUTES/one', 'documents-subject', 'positive', 'M1-GATE-PUBLIC', 'literal-casefold-search'),
    Allocation('m1:V0-QUERY-01', 'HTTP-DOCUMENT-ROUTES/one', 'documents-subject', 'positive', 'M1-GATE-V0-SUPPLEMENT', 'literal-casefold-search'),
    Allocation('m1:V0-QUERY-01', 'HTTP-DOCUMENT-ROUTES/casefold', 'documents-subject', 'positive', 'M1-GATE-PUBLIC', 'literal-casefold-search'),
    Allocation('m1:V0-QUERY-01', 'HTTP-DOCUMENT-ROUTES/casefold', 'documents-subject', 'positive', 'M1-GATE-V0-SUPPLEMENT', 'literal-casefold-search'),
    Allocation('m1:V0-QUERY-01', 'HTTP-DOCUMENT-ROUTES/literal', 'documents-subject', 'positive', 'M1-GATE-PUBLIC', 'literal-casefold-search'),
    Allocation('m1:V0-QUERY-01', 'HTTP-DOCUMENT-ROUTES/literal', 'documents-subject', 'positive', 'M1-GATE-V0-SUPPLEMENT', 'literal-casefold-search'),
    Allocation('m1:V0-QUERY-01', 'HTTP-DOCUMENT-ROUTES/unicode-casefold', 'documents-subject', 'positive', 'M1-GATE-PUBLIC', 'literal-casefold-search'),
    Allocation('m1:V0-QUERY-01', 'HTTP-DOCUMENT-ROUTES/unicode-casefold', 'documents-subject', 'positive', 'M1-GATE-V0-SUPPLEMENT', 'literal-casefold-search'),
    Allocation('m1:V0-QUERY-02', 'HTTP-DOCUMENT-ROUTES/all', 'documents-subject', 'positive', 'M1-GATE-PUBLIC', 'ordered-prepage-total'),
    Allocation('m1:V0-QUERY-02', 'HTTP-DOCUMENT-ROUTES/all', 'documents-subject', 'positive', 'M1-GATE-V0-SUPPLEMENT', 'ordered-prepage-total'),
    Allocation('m1:V0-QUERY-02', 'HTTP-DOCUMENT-ROUTES/offset-one', 'documents-subject', 'positive', 'M1-GATE-PUBLIC', 'ordered-prepage-total'),
    Allocation('m1:V0-QUERY-02', 'HTTP-DOCUMENT-ROUTES/offset-one', 'documents-subject', 'positive', 'M1-GATE-V0-SUPPLEMENT', 'ordered-prepage-total'),
    Allocation('m1:V0-QUERY-02', 'HTTP-DOCUMENT-ROUTES/limit-one', 'documents-subject', 'boundary', 'M1-GATE-PUBLIC', 'minimum-page'),
    Allocation('m1:V0-QUERY-02', 'HTTP-DOCUMENT-ROUTES/limit-one', 'documents-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'minimum-page'),
    Allocation('m1:V0-QUERY-02', 'HTTP-DOCUMENT-ROUTES/offset-beyond', 'documents-subject', 'boundary', 'M1-GATE-PUBLIC', 'finite-empty-page'),
    Allocation('m1:V0-QUERY-02', 'HTTP-DOCUMENT-ROUTES/offset-beyond', 'documents-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'finite-empty-page'),
    Allocation('m1:V0-QUERY-02', 'HTTP-DOCUMENT-ROUTES/offset-positive-limit', 'documents-subject', 'boundary', 'M1-GATE-PUBLIC', 'minimum-page'),
    Allocation('m1:V0-QUERY-02', 'HTTP-DOCUMENT-ROUTES/offset-positive-limit', 'documents-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'minimum-page'),
    Allocation('m1:V0-QUERY-03', 'HTTP-DOCUMENT-ROUTES/all', 'documents-subject', 'boundary', 'M1-GATE-PUBLIC', 'page-limit-100'),
    Allocation('m1:V0-QUERY-03', 'HTTP-DOCUMENT-ROUTES/all', 'documents-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'page-limit-100'),
    Allocation('m1:V0-QUERY-03', 'HTTP-DOCUMENT-ROUTES/limit-one', 'documents-subject', 'boundary', 'M1-GATE-PUBLIC', 'minimum-page'),
    Allocation('m1:V0-QUERY-03', 'HTTP-DOCUMENT-ROUTES/limit-one', 'documents-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'minimum-page'),
    Allocation('m1:V0-QUERY-03', 'HTTP-DOCUMENT-ROUTES/offset-beyond', 'documents-subject', 'positive', 'M1-GATE-PUBLIC', 'finite-valid-offset'),
    Allocation('m1:V0-QUERY-03', 'HTTP-DOCUMENT-ROUTES/offset-beyond', 'documents-subject', 'positive', 'M1-GATE-V0-SUPPLEMENT', 'finite-valid-offset'),
    Allocation('m1:V0-QUERY-03', 'HTTP-DOCUMENT-ROUTES/query-256', 'documents-subject', 'boundary', 'M1-GATE-PUBLIC', 'query-char-256'),
    Allocation('m1:V0-QUERY-03', 'HTTP-DOCUMENT-ROUTES/query-256', 'documents-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'query-char-256'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/0-offset', 'invalid-query-subject', 'negative', 'M1-GATE-PUBLIC', 'malformed-query'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/0-offset', 'invalid-query-subject', 'negative', 'M1-GATE-V0-SUPPLEMENT', 'malformed-query'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/1-limit', 'invalid-query-subject', 'negative', 'M1-GATE-PUBLIC', 'malformed-query'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/1-limit', 'invalid-query-subject', 'negative', 'M1-GATE-V0-SUPPLEMENT', 'malformed-query'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/2-offset', 'invalid-query-subject', 'boundary', 'M1-GATE-PUBLIC', 'below-offset-zero'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/2-offset', 'invalid-query-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'below-offset-zero'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/3-limit', 'invalid-query-subject', 'boundary', 'M1-GATE-PUBLIC', 'below-limit-one'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/3-limit', 'invalid-query-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'below-limit-one'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/4-limit', 'invalid-query-subject', 'boundary', 'M1-GATE-PUBLIC', 'above-limit-100'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/4-limit', 'invalid-query-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'above-limit-100'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/5-q', 'invalid-query-subject', 'boundary', 'M1-GATE-PUBLIC', 'above-query-256'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/5-q', 'invalid-query-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'above-query-256'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/6-offset', 'invalid-query-subject', 'negative', 'M1-GATE-PUBLIC', 'malformed-query-text-true'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/6-offset', 'invalid-query-subject', 'negative', 'M1-GATE-V0-SUPPLEMENT', 'malformed-query-text-true'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/7-limit', 'invalid-query-subject', 'negative', 'M1-GATE-PUBLIC', 'malformed-query'),
    Allocation('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/7-limit', 'invalid-query-subject', 'negative', 'M1-GATE-V0-SUPPLEMENT', 'malformed-query'),
    Allocation('m1:V0-QUERY-04', 'HTTP-EMPTY-HEALTH/documents', 'documents-subject', 'boundary', 'M1-GATE-PUBLIC', 'empty-listing'),
    Allocation('m1:V0-QUERY-04', 'HTTP-EMPTY-HEALTH/documents', 'documents-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'empty-listing'),
    Allocation('m1:V0-QUERY-04', 'HTTP-DOCUMENT-ROUTES/all', 'documents-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-listing-shape'),
    Allocation('m1:V0-QUERY-04', 'HTTP-DOCUMENT-ROUTES/all', 'documents-subject', 'positive', 'M1-GATE-V0-SUPPLEMENT', 'exact-listing-shape'),
    Allocation('m1:V0-QUERY-04', 'HTTP-DOCUMENT-ROUTES/limit-one', 'documents-subject', 'boundary', 'M1-GATE-PUBLIC', 'minimum-page'),
    Allocation('m1:V0-QUERY-04', 'HTTP-DOCUMENT-ROUTES/limit-one', 'documents-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'minimum-page'),
    Allocation('m1:V0-QUERY-04', 'HTTP-DOCUMENT-ROUTES/offset-beyond', 'documents-subject', 'boundary', 'M1-GATE-PUBLIC', 'finite-empty-page'),
    Allocation('m1:V0-QUERY-04', 'HTTP-DOCUMENT-ROUTES/offset-beyond', 'documents-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'finite-empty-page'),
    Allocation('m1:V0-QUERY-04', 'HTTP-DOCUMENT-ROUTES/show-0', 'document-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-document-value'),
    Allocation('m1:V0-QUERY-04', 'HTTP-DOCUMENT-ROUTES/show-0', 'document-subject', 'positive', 'M1-GATE-V0-SUPPLEMENT', 'exact-document-value'),
    Allocation('m1:V0-QUERY-04', 'HTTP-EMPTY-HEALTH/missing-documents', 'missing-document-subject', 'negative', 'M1-GATE-PUBLIC', 'missing-document'),
    Allocation('m1:V0-QUERY-04', 'HTTP-EMPTY-HEALTH/missing-documents', 'missing-document-subject', 'negative', 'M1-GATE-V0-SUPPLEMENT', 'missing-document'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-EMPTY-HEALTH/export', 'export-subject', 'boundary', 'M1-GATE-PUBLIC', 'empty-export'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-EMPTY-HEALTH/export', 'export-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'empty-export'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-DOCUMENT-ROUTES/export-all', 'export-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-export-value'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-DOCUMENT-ROUTES/export-all', 'export-subject', 'positive', 'M1-GATE-V0-SUPPLEMENT', 'exact-export-value'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-DOCUMENT-ROUTES/export-empty', 'export-subject', 'boundary', 'M1-GATE-PUBLIC', 'empty-selected-export'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-DOCUMENT-ROUTES/export-empty', 'export-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'empty-selected-export'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-DOCUMENT-ROUTES/export-reverse', 'export-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-export-value'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-DOCUMENT-ROUTES/export-reverse', 'export-subject', 'positive', 'M1-GATE-V0-SUPPLEMENT', 'exact-export-value'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-DOCUMENT-ROUTES/export-one', 'export-subject', 'positive', 'M1-GATE-PUBLIC', 'exact-export-value'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-DOCUMENT-ROUTES/export-one', 'export-subject', 'positive', 'M1-GATE-V0-SUPPLEMENT', 'exact-export-value'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-DOCUMENT-ROUTES/export-duplicate', 'unclassified-export-subject', 'negative', 'M1-GATE-PUBLIC', 'unclassified-export-rejection'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-DOCUMENT-ROUTES/export-duplicate', 'unclassified-export-subject', 'negative', 'M1-GATE-V0-SUPPLEMENT', 'unclassified-export-rejection'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-DOCUMENT-ROUTES/export-missing-after-valid', 'unclassified-export-subject', 'negative', 'M1-GATE-PUBLIC', 'unclassified-export-rejection'),
    Allocation('m1:V0-EXPORT-01', 'HTTP-DOCUMENT-ROUTES/export-missing-after-valid', 'unclassified-export-subject', 'negative', 'M1-GATE-V0-SUPPLEMENT', 'unclassified-export-rejection'),
    Allocation('shared:constants:max_query_characters', 'HTTP-DOCUMENT-ROUTES/query-256', 'documents-subject', 'boundary', 'M1-GATE-PUBLIC', 'query-char-256'),
    Allocation('shared:constants:max_query_characters', 'HTTP-DOCUMENT-ROUTES/query-256', 'documents-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'query-char-256'),
    Allocation('shared:constants:max_query_characters', 'HTTP-QUERY-VALUES/5-q', 'invalid-query-subject', 'boundary', 'M1-GATE-PUBLIC', 'above-query-256'),
    Allocation('shared:constants:max_query_characters', 'HTTP-QUERY-VALUES/5-q', 'invalid-query-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'above-query-256'),
    Allocation('shared:constants:max_page_size', 'HTTP-DOCUMENT-ROUTES/all', 'documents-subject', 'boundary', 'M1-GATE-PUBLIC', 'page-limit-100'),
    Allocation('shared:constants:max_page_size', 'HTTP-DOCUMENT-ROUTES/all', 'documents-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'page-limit-100'),
    Allocation('shared:constants:max_page_size', 'HTTP-QUERY-VALUES/4-limit', 'invalid-query-subject', 'boundary', 'M1-GATE-PUBLIC', 'above-limit-100'),
    Allocation('shared:constants:max_page_size', 'HTTP-QUERY-VALUES/4-limit', 'invalid-query-subject', 'boundary', 'M1-GATE-V0-SUPPLEMENT', 'above-limit-100'),
    Allocation('shared:constants:max_post_bytes', 'HTTP-BODY-WIRE/raw-bytes-65536', 'body-bound-65536', 'boundary', 'M1-GATE-HTTP', 'body-bound-65536'),
    Allocation('shared:constants:max_post_bytes', 'HTTP-BODY-WIRE/raw-bytes-65537', 'body-bound-65537', 'boundary', 'M1-GATE-HTTP', 'body-bound-65537'),
    Allocation('m1:M1-J03', 'fresh-value-create_job', 'storage:after.result.0', 'positive', 'M1-GATE-JOBS', 'finite-total-progress'),
    Allocation('m1:M1-J03', 'fresh-value-create_job', 'storage:after.persisted-state', 'positive', 'M1-GATE-JOBS', 'finite-total-progress'),
    Allocation('m1:M1-J03', 'fresh-value-fail_job', 'storage:after.result.0', 'positive', 'M1-GATE-JOBS', 'finite-total-progress'),
    Allocation('m1:M1-J03', 'fresh-value-fail_job', 'storage:after.persisted-state', 'positive', 'M1-GATE-JOBS', 'finite-total-progress'),
    Allocation('m1:M1-J03', 'state-cancel_job-running', 'storage:after.result.0', 'positive', 'M1-GATE-JOBS', 'finite-total-progress'),
    Allocation('m1:M1-J03', 'state-cancel_job-running', 'storage:after.persisted-state', 'positive', 'M1-GATE-JOBS', 'finite-total-progress'),
    Allocation('m1:M1-J03', 'store-commit_job-running-current', 'storage:after.result.0', 'positive', 'M1-GATE-JOBS', 'finite-total-progress'),
    Allocation('m1:M1-J03', 'store-commit_job-running-current', 'storage:after.persisted-state', 'positive', 'M1-GATE-JOBS', 'finite-total-progress'),
    Allocation('m1:M1-J09', 'fresh-value-create_job', 'storage:reopened.result.0', 'history', 'M1-GATE-JOBS', 'finite-store-reopen'),
    Allocation('m1:M1-J09', 'fresh-value-create_job', 'storage:reopened.persisted-state', 'history', 'M1-GATE-JOBS', 'finite-store-reopen'),
    Allocation('m1:M1-J09', 'fresh-value-start_job', 'storage:reopened.result.0', 'history', 'M1-GATE-JOBS', 'finite-store-reopen'),
    Allocation('m1:M1-J09', 'fresh-value-start_job', 'storage:reopened.persisted-state', 'history', 'M1-GATE-JOBS', 'finite-store-reopen'),
    Allocation('m1:M1-J09', 'fresh-value-fail_job', 'storage:reopened.result.0', 'history', 'M1-GATE-JOBS', 'finite-store-reopen'),
    Allocation('m1:M1-J09', 'fresh-value-fail_job', 'storage:reopened.persisted-state', 'history', 'M1-GATE-JOBS', 'finite-store-reopen'),
    Allocation('m1:M1-J09', 'fresh-value-cancel_job', 'storage:reopened.result.0', 'history', 'M1-GATE-JOBS', 'finite-store-reopen'),
    Allocation('m1:M1-J09', 'fresh-value-cancel_job', 'storage:reopened.persisted-state', 'history', 'M1-GATE-JOBS', 'finite-store-reopen'),
    Allocation('m1:M1-J09', 'fresh-value-retry_job', 'storage:reopened.result.0', 'history', 'M1-GATE-JOBS', 'finite-store-reopen'),
    Allocation('m1:M1-J09', 'fresh-value-retry_job', 'storage:reopened.persisted-state', 'history', 'M1-GATE-JOBS', 'finite-store-reopen'),
    Allocation('m1:M1-J09', 'fresh-value-commit_job', 'storage:reopened.result.0', 'history', 'M1-GATE-JOBS', 'finite-store-reopen'),
    Allocation('m1:M1-J09', 'fresh-value-commit_job', 'storage:reopened.persisted-state', 'history', 'M1-GATE-JOBS', 'finite-store-reopen'),
    Allocation('m1:M1-I25', 'input-manifest-snapshot', 'storage:after.result.3', 'history', 'M1-GATE-INTAKE', 'input-mutation-conservation'),
    Allocation('m1:M1-I25', 'input-manifest-snapshot', 'storage:after.persisted-state', 'history', 'M1-GATE-INTAKE', 'input-mutation-conservation'),
    Allocation('m1:M1-I25', 'input-manifest-snapshot', 'storage:reopened.persisted-state', 'history', 'M1-GATE-INTAKE', 'input-mutation-conservation'),
    Allocation('m1:M1-I25', 'input-manifest-snapshot', 'storage:reopened.persisted-strings', 'history', 'M1-GATE-INTAKE', 'input-mutation-conservation'),
)

RATIONALES: dict[str, str] = {'above-limit-100': 'Exact limit101 refusal immediately above limit100.',
 'above-query-256': 'Exact 257 multibyte-character refusal.',
 'below-limit-one': 'Exact limit0 refusal immediately below limit1.',
 'below-offset-zero': 'Exact -1 refusal immediately below the offset0 bound.',
 'body-bound-65536': 'Finite exact upper-bound non-rejection/status with prescribed complete history; '
                     'success body wrapper unspecified.',
 'body-bound-65537': 'Finite one-over body rejection at400; error code unspecified.',
 'classified-source-change': 'Actual source_changed error and prescribed status only.',
 'empty-export': 'Exact empty-catalog all export, fixed format and no extra keys.',
 'empty-listing': 'Empty-state listing exact documents=[] and total0.',
 'empty-selected-export': 'Exact empty selection export, fixed format and no extra keys.',
 'exact-document-value': 'This exact original six-field document value only; no universal identity '
                         'algorithm, all path rules or hidden state proof.',
 'exact-export-value': 'Exact selected/all source-sorted documents and fixed format; no timestamps or extra '
                       'keys.',
 'exact-listing-shape': 'Only exact documents and pre-pagination total plus original ordered values.',
 'finite-empty-page': 'Finite beyond-end offset9 yields empty documents and unchanged pre-slice total; not '
                      'an exact last-valid-offset threshold.',
 'finite-store-reopen': 'Six original transition histories span five state names; selected get_job or paused '
                        'state after same-process Store reopen. No process death.',
 'finite-total-progress': 'Selected response JOB or commit receipt.job, or separately selected paused state, '
                          'supports total2/progress0or2; no atomic-visibility proof.',
 'finite-valid-offset': 'Ordinary accepted offset9; explicitly positive, not a numeric limit boundary.',
 'input-mutation-conservation': 'Only caller input mutation and exact admitted '
                                'manifest/text/hash/stored-string conservation; no after.case.immutable-* '
                                'selector, source-file mutation or unequal-hash-order claim.',
 'literal-casefold-search': 'Exact expected list for this declared literal/casefold query only; no '
                            'regex/browser interpretation or exhaustive Unicode claim.',
 'malformed-query': 'This exact abc or fractional transport text produces invalid_request, without claiming '
                    'all structured types.',
 'malformed-query-text-true': 'Exact text true is malformed offset, not a JSON/direct-Python boolean '
                              'control.',
 'minimum-page': 'The selected declared limit1 case exercises the lower positive page-size bound with exact '
                 'response shape/total.',
 'missing-document': 'Actual missing-ID not_found behavior.',
 'ordered-prepage-total': 'Exact source order, selected slice and pre-slice total on this ordinary valid '
                          'case; not a boundary claim.',
 'page-limit-100': 'Exact declared limit100 acceptance, not 100 returned documents or another interface.',
 'paired-document-identity': 'Selected complete corpus value participates in original whole-history '
                             'shared-blob/distinct-source identity observation; a single edge is not the '
                             'whole conjunction.',
 'query-char-256': 'Exact 256 multibyte-character acceptance; bytes and character count differ.',
 'refused-public-readback': 'Exact prescribed public state after a refused changed-source import; no hidden '
                            'storage guarantee.',
 'same-document-after-root-replacement': 'Same.txt exact original identity/value before or after authored '
                                         'same-DB root/epoch replacement; no crash or source-inspection '
                                         'credit.',
 'unchanged-public-readback': 'Exact public listing/export following same-bytes import; no specified success '
                              'wrapper or physical row/blob conservation.',
 'unclassified-export-rejection': 'Reject exact duplicate/missing selection, without an invented fixed '
                                  'status/error code or streaming partial-output guarantee.'}

def allocations(family: str) -> tuple[Allocation, ...]:
    if family not in ("http", "storage-b02"):
        raise ValueError("Unknown closed finite mapping family")
    return tuple(row for row in ALLOCATIONS
                 if row.history_id.startswith("HTTP-") == (family == "http"))


def sources() -> dict[str, str]:
    current = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if current != LOADED_SOURCE_SHA256:
        raise ValueError("Loaded finite mapping source changed")
    return {"gossip_harness/cumulative_finite_mapping_v1.py": current}


@dataclass(frozen=True)
class HttpAllocation:
    allocation: Allocation
    step_index: int


_HTTP_PREDICATES: dict[str, tuple[str, str, str, str]] = {'body-bound-65536': ('-subject-request', 'POST', 'unspecified', '/api/jobs'),
 'body-bound-65537': ('-subject-request', 'POST', 'error', '/api/jobs'),
 'document-subject': ('-subject', 'GET', 'document', 'exact original /api/documents/<authored document_id>'),
 'documents-subject': ('-subject',
                       'GET',
                       'documents',
                       'exact original /api/documents query target, including declared offset/limit/q'),
 'export-subject': ('-subject', 'original GET or POST fixed by this named history', 'export', '/api/export'),
 'first-root-same-document': ('-first-root-document-1',
                              'GET',
                              'document',
                              '/api/documents/<exact authored same.txt document_id>'),
 'invalid-query-subject': ('-subject',
                           'GET',
                           'error',
                           'exact original query target of the named HTTP-QUERY-VALUES history'),
 'missing-document-subject': ('-subject', 'GET', 'error', '/api/documents/missing'),
 'root-change-refusal': ('-changed-source-rejected', 'POST', 'error', '/api/import'),
 'second-root-preserved-documents': ('-second-root-preserved-state-documents-0',
                                     'GET',
                                     'documents',
                                     '/api/documents?offset=0&limit=100'),
 'second-root-preserved-export': ('-second-root-preserved-state-export', 'GET', 'export', '/api/export'),
 'second-root-same-document': ('-second-root-document-1',
                               'GET',
                               'document',
                               '/api/documents/<exact authored same.txt document_id>'),
 'source-changed-subject': ('-subject', 'POST', 'error', '/api/import'),
 'subject-after-documents': ('-subject-after-documents-0',
                             'GET',
                             'documents',
                             '/api/documents?offset=0&limit=100'),
 'subject-after-export': ('-subject-after-export', 'GET', 'export', '/api/export'),
 'unclassified-export-subject': ('-subject', 'POST', 'error', '/api/export')}

def resolve_http(profile: Any) -> tuple[HttpAllocation, ...]:
    """Resolve exact immutable original indices, not observed candidate values."""
    from . import candidate_http_execution_v4 as execution
    from . import candidate_http_observation_source_v1 as observer
    from . import candidate_http_fixtures_v1 as fixtures
    from . import cumulative_observation_profile_v1 as cumulative

    execution.require(type(profile) is execution.HttpProductProfile
        and profile.mapping_profile == HTTP_MAPPING, "Exact finite HTTP profile required")
    profile.check_current(purpose="independent_acceptance")
    target = profile.cumulative_profile
    execution.require(type(target) is cumulative.CumulativeProfile,
                      "Exact final-M4 cumulative profile required")
    assert target is not None
    execution.require(target.record()["original_definition"] == profile.case.record(),
                      "Finite mapping must preserve the entire original history")
    result = []
    for allocation in allocations("http"):
        if allocation.history_id != profile.case.row_id:
            continue
        suffix, method, shape, target_text = _HTTP_PREDICATES[allocation.predicate_id]
        matches = [(i, step) for i, step in enumerate(profile.case.steps)
                   if step.step_id.endswith(suffix)]
        execution.require(len(matches) == 1, "Finite predicate must select exactly one original step")
        index, step = matches[0]
        request, expectation = step.request, step.expectation
        execution.require(step.kind == "request" and request is not None and expectation is not None
            and not expectation.raw_facts_only and expectation.semantic is not None,
            "Finite mapping requires the actual normative request expectation")
        assert request is not None and expectation is not None and expectation.semantic is not None
        semantic = expectation.semantic
        if method.startswith("original GET or POST"):
            method = "GET" if profile.case.row_id in (
                "HTTP-EMPTY-HEALTH/export", "HTTP-DOCUMENT-ROUTES/export-all") else "POST"
        execution.require(request.method == method and semantic.shape == shape,
                          "Finite original method/semantic shape differs")
        if target_text.startswith("/") and "<" not in target_text:
            execution.require(request.target == target_text, "Finite exact original target differs")
        required = observer._semantic_required(semantic)
        if shape == "unspecified":
            execution.require(allocation.predicate_id == "body-bound-65536"
                and semantic.status == 200 and "status" in required,
                "Unspecified body cannot support data-value mappings")
        else:
            execution.require("body_shape_value" in required,
                              "Finite data mapping needs the original required semantic facet")
        if allocation.predicate_id.startswith("body-bound-"):
            size = int(allocation.predicate_id.removeprefix("body-bound-"))
            execution.require(request.body == fixtures.request_body_boundary(size)
                and len(request.body) == size and dict(request.headers).get("Content-Length") == str(size)
                and dict(request.headers).get("Content-Type") == "application/json"
                and semantic.status == (200 if size == 65536 else 400),
                "Exact original body boundary differs")
        result.append(HttpAllocation(allocation, index))
    return tuple(result)
