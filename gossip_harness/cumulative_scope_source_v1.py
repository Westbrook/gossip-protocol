"""Full source denominator and executable selector declarations, without approval.

The pinned independent ownership review authorizes ownership interpretation only.
Selector declarations are concrete review inputs. They are not a ScopePlan,
qualified evaluator, candidate observation or receipt-authentication shortcut.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from . import project_acceptance_compiler_v1 as compiler
from . import project_acceptance_registry_v1 as registry

PROTOCOL = 'cumulative-scope-source-v1'
OWNERSHIP = 'analysis/cumulative-shared-ownership-v1.json'
OWNERSHIP_SHA256 = 'c4f1f4e3a9636d13c99a314b7d29269b2dc3bfde43e56c41425e63ca24b933ad'
REVIEW = 'analysis/cumulative-shared-ownership-review-v1.json'
REVIEW_SHA256 = '720b558b8df730a3c16103d3945cdd8fd22f37d6e5bc8bac64eee430161a1e99'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


class ScopeSourceError(ValueError):
    """Unrecognized, incomplete or substituted source definition; not a defect."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ScopeSourceError(message)


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                      separators=(',', ':')).encode()


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def pointer(document: Any, path: str) -> Any:
    require(type(path) is str and (path == '' or path.startswith('/')), 'Invalid JSON pointer')
    value = document
    for part in path.split('/')[1:]:
        key = part.replace('~1', '/').replace('~0', '~')
        value = value[int(key)] if type(value) is list else value[key]
    return value


@dataclass(frozen=True, slots=True)
class SourceUnit:
    obligation: compiler.Obligation
    value_json: bytes
    requirement_ids: tuple[str, ...]
    ownership_reason: str
    interaction_requirement_ids: tuple[str, ...]
    interaction_reason: str
    applicability_input: str
    eligible_cells: tuple[compiler.ApplicabilityCell, ...]


@dataclass(frozen=True, slots=True)
class SourceCatalog:
    inventory: compiler.Inventory
    units: tuple[SourceUnit, ...]
    ownership_sha256: str
    ownership_review_sha256: str
    # Retain complete original notes/authorities in inventory. No automatic N/A.
    source_pins: tuple[tuple[str, str], ...]

    @property
    def sha256(self) -> str:
        return sha(encoded({'protocol': PROTOCOL, 'inventory': self.inventory.sha256,
            'units': [{**asdict(item), 'value_json': item.value_json.decode()} for item in self.units], 'ownership': self.ownership_sha256,
            'review': self.ownership_review_sha256, 'source_pins': self.source_pins}))

    def unit(self, identifier: str) -> SourceUnit:
        matches = [row for row in self.units if row.obligation.id == identifier]
        require(len(matches) == 1, 'Unknown obligation')
        return matches[0]


def load_catalog(root: Path) -> SourceCatalog:
    """Resolve every original value/citation; recognized review scope stays narrow."""
    root = Path(root)
    inventory = compiler.load_inventory(root, product_file=compiler.V2_FILE,
                                       expected_product_sha256=compiler.PRODUCT_V2_SHA256)
    require((len(inventory.product_ids), len(inventory.prerequisite_ids), len(inventory.obligations),
             len(inventory.qualification_rules), len(inventory.planning_notes)) == (123, 3, 312, 22, 188),
            'Full frozen denominator differs')
    pins = {compiler.M1_FILE: compiler.M1_SHA256, compiler.V1_FILE: compiler.PRODUCT_V1_SHA256,
            compiler.V2_FILE: compiler.PRODUCT_V2_SHA256, OWNERSHIP: OWNERSHIP_SHA256, REVIEW: REVIEW_SHA256}
    documents = {}
    for name, expected in pins.items():
        raw = (root / name).read_bytes()
        require(sha(raw) == expected, 'Unrecognized source/review bytes: ' + name)
        documents[name] = json.loads(raw)
    owners, review = documents[OWNERSHIP], documents[REVIEW]
    require(review['status'] == 'cleared_for_ownership_design_only'
            and {'path': OWNERSHIP, 'sha256': OWNERSHIP_SHA256} in review['reviewed_files'],
            'Review does not bind this ownership design')
    records = {row['obligation_id']: row for row in owners['records']}
    shared = {row.id for row in inventory.obligations if not row.requirement_ids}
    require(len(records) == len(owners['records']) == 57 and set(records) == shared,
            'Shared ownership census differs')
    products = set(inventory.product_ids)
    by_hash = {pins[name]: documents[name] for name in (compiler.M1_FILE, compiler.V1_FILE, compiler.V2_FILE)}
    units = []
    for obligation in inventory.obligations:
        value = pointer(by_hash[obligation.source_sha256], obligation.source_pointer)
        require(sha(encoded(value)) == obligation.value_sha256, 'Obligation value differs')
        requirement_ids, reason = obligation.requirement_ids, 'Owners explicitly declared by the frozen source.'
        interactions: tuple[str, ...] = ()
        interaction_reason = ''
        applicability_input = 'No assertion class or lane applicability has been approved by source loading.'
        if obligation.id in records:
            record = records[obligation.id]
            selected = [row for row in record['source_instances'] if row['product_version'] == 2]
            require(len(selected) == 1, 'Missing selected-version ownership source')
            item = selected[0]
            require((item['inventory_sha256'], item['source_sha256'], item['json_pointer'], item['value_sha256'])
                    == (inventory.sha256, obligation.source_sha256, obligation.source_pointer, obligation.value_sha256),
                    'Ownership source instance differs')
            requirement_ids = tuple(record['owner_requirement_ids'])
            interactions = tuple(record['interaction_requirement_ids'])
            require(bool(requirement_ids) and len(set(requirement_ids)) == len(requirement_ids)
                    and set(requirement_ids) <= products and set(interactions) <= products,
                    'Invalid reviewed owner/interaction')
            # Resolve the actual cited norms, not just the ledger's self-hashes.
            for key in ('source_instances', 'owner_basis', 'interaction_basis', 'amendment_basis'):
                for citation in record.get(key, []):
                    require(citation['source_file'] in documents
                            and pins[citation['source_file']] == citation['source_sha256']
                            and sha(encoded(pointer(documents[citation['source_file']], citation['json_pointer'])))
                            == citation['value_sha256'], 'Ownership citation differs')
            reason, interaction_reason = record['ownership_reason'], record['interaction_reason']
            require(record['applicability']['assertion_class_or_lane_na_approved'] is False,
                    'Ownership must not grant applicability approval')
            applicability_input = record['applicability']['activation'] + ' ' + record['applicability']['version_rule']
        cells = tuple(compiler.ApplicabilityCell(kind, gate.id)
            for gate in inventory.logical_gates
            if gate.role == obligation.role and set(requirement_ids).intersection(gate.requirement_ids)
            for kind in (compiler.KINDS if obligation.role == 'product' else ('inspection',)))
        units.append(SourceUnit(obligation, encoded(value), requirement_ids, reason, interactions,
                                interaction_reason, applicability_input, cells))
    return SourceCatalog(inventory, tuple(units), OWNERSHIP_SHA256, REVIEW_SHA256, tuple(sorted(pins.items())))


@dataclass(frozen=True, slots=True)
class Selector:
    case_id: str
    observation_pointer: str
    disposition: str
    definition_pointer: str
    evidence_kind: str = 'semantic'


@dataclass(frozen=True, slots=True)
class AssertionDeclaration:
    """Concrete partial clause assertion; full-clause adequacy still needs review."""
    obligation_id: str
    assertion: compiler.Assertion
    case_id: str
    observation_pointer: str
    rationale: str


@dataclass(frozen=True, slots=True)
class ExecutableSlice:
    family: str
    history_id: str
    gate: registry.Gate
    definition_sha256: str
    original_definition_purpose: str
    source_contract_sha256: str
    profile_sha256: str
    evaluator_sources: tuple[tuple[str, str], ...]
    selectors: tuple[Selector, ...]
    assertions: tuple[AssertionDeclaration, ...]
    remaining_coverage: tuple[str, ...]
    factory_input_json: str

    @property
    def sha256(self) -> str:
        return sha(encoded({'protocol': PROTOCOL, 'slice': asdict(self)}))

    def compiler_records(self, *, suite_id: str, physical_slot: str) -> tuple[compiler.SuiteDefinition, compiler.ExecutionGate, tuple[compiler.CoverageEdge, ...]]:
        """Bind existing physical identity; never split one execution to bypass bounds."""
        registry.identifier(suite_id)
        registry.identifier(physical_slot)
        logical_ids = tuple(dict.fromkeys(key for item in self.assertions for key in item.assertion.logical_gate_ids))
        capabilities = ('cli',) if self.family == 'cli' else (('cli', 'http', 'public-contract', 'process-restart', 'migration') if self.family == 'product-process' else ('http',))
        binding = self.gate.binding
        suite = compiler.SuiteDefinition(suite_id, self.gate.ordered_case_ids, binding.ordered_suite_sha256,
            self.definition_sha256, self.original_definition_purpose, binding.purpose,
            self.source_contract_sha256, binding.evaluator_sha256, True, True, capabilities)
        execution = compiler.ExecutionGate(self.gate.gate_id, physical_slot, suite_id, 'product', logical_ids,
            binding.runtime_image_sha256, binding.environment_sha256, binding.limits_sha256,
            binding.seed_sha256, binding.execution_protocol)
        edges = tuple(compiler.CoverageEdge(item.obligation_id, item.assertion.id,
            self.gate.gate_id, item.case_id, item.observation_pointer) for item in self.assertions)
        return suite, execution, edges


def cli_slice(registration: Any) -> ExecutableSlice:
    """Derive actual CLI verifier selectors and narrow prose-grounded assertions.

    Input is the existing executor's complete prospective registration. No output
    or observed candidate value is read. This neither executes nor authenticates it.
    """
    from . import candidate_client_execution_v5 as execution
    from . import cumulative_observation_profile_v1 as profiles
    from . import candidate_observation_admission_v1 as admission
    require(type(registration) is execution.ClientRegistration, 'Exact CLI registration required')
    actual = execution.observation_registration(registration)
    value = execution.profile_for_binding(registration.binding)
    require(type(value) is profiles.CumulativeProfile, 'Final-M4 prospective profile required')
    assert value is not None
    profiles.assert_profile_current(value)
    sources = execution.evaluator_sources()
    admission.verify_loaded_sources(sources)
    require(registration.binding.evaluator_sha256 == execution.digest(sources), 'CLI evaluator differs')
    definition = value.target_definition()['definition']
    steps = {row['step_id']: row for row in definition['recipe']['steps']}
    selectors, assertions = [], []
    for index, cell in enumerate(value.diagnostic_cells):
        path = '/cells/' + str(index) + '/status'
        selectors.append(Selector(cell.case_id, path, cell.applicability,
            '/expectations/' + cell.step_id + '/assertion_ids'))
        if cell.applicability != 'normative':
            continue
        expected = definition['expectations'][cell.step_id]
        command = steps[cell.step_id]['argv'][7:]
        kind = 'positive' if expected['kind'] == 'success' else 'negative'
        targets: list[tuple[str, str, str]] = []
        if cell.assertion_id in ('process.exit', 'stdout.json', 'stderr.error', 'stderr.error_code'):
            targets.append(('V0-CLI-02', kind, 'Exact declared exit/channel/JSON or error facet; no wrapper or empty-other-stream inference.'))
        if expected['kind'] == 'success' and cell.assertion_id == 'process.exit' and command:
            if command[0] in ('import', 'list', 'search', 'show', 'export'):
                targets.append(('V0-CLI-01', 'positive', 'Declared command/flag form reached natural exit0 only; command effect/output and serve remain separate.'))
            if command[0].startswith('job-') or command[0] == 'jobs':
                targets.append(('M1-CLI-01', 'positive', 'Declared jobs command form reached natural exit0 only; public state/output effects require separate selectors.'))
        if value.case_id in ('cli-legacy-persistence', 'cli-db-root-isolation') and cell.assertion_id == 'stdout.value':
            targets.append(('V0-CLI-03', 'history', 'Known supported public value across separate declared processes and persistent DB/root; no listener/browser claim.'))
        for target, assertion_kind, rationale in targets:
            identifier = 'cli-' + sha(encoded([value.case_id, cell.case_id, target, assertion_kind]))[:24]
            assertions.append(AssertionDeclaration('m1:' + target,
                compiler.Assertion(identifier, assertion_kind, ('M1-GATE-CLI',)), cell.case_id, path, rationale))
    require(tuple(row.case_id for row in selectors if row.disposition == 'normative')
            == actual.gate.ordered_case_ids, 'CLI selector census differs')
    return ExecutableSlice('cli', value.case_id, actual.gate, actual.definition_sha256,
        actual.original_definition_purpose, compiler.M1_SHA256, value.sha256,
        tuple(sorted(sources.items())), tuple(selectors), tuple(assertions), tuple(value.record()['remaining_coverage']),
        encoded({'registration': asdict(registration)}).decode())


def http_slice(registration: Any, profile: Any, policy: Any) -> ExecutableSlice:
    """Exact existing HTTP semantic selectors; raw-only/CLI/mechanics get no invented credit."""
    from . import candidate_http_execution_v4 as execution
    from . import cumulative_observation_profile_v1 as profiles
    from . import candidate_observation_admission_v1 as admission
    require(type(registration) is execution.HttpRegistration and type(profile) is execution.HttpProductProfile
            and type(policy) is execution.HttpPolicy, 'Exact HTTP registration/profile/policy required')
    value = profile.cumulative_profile
    require(type(value) is profiles.CumulativeProfile, 'Final-M4 prospective HTTP profile required')
    assert value is not None
    observed = registration.observation
    actual = execution.observation_registration_for(registration.binding, profile, policy,
        subject=observed.gate.binding.subject, gate_id=observed.gate.gate_id,
        commit_oid=registration.commit_oid, tree_oid=registration.tree_oid,
        repetition_id=registration.repetition_id, cohort_trajectory_ids=observed.cohort_trajectory_ids)
    require(actual == observed, 'HTTP original registration differs')
    sources = execution.evaluator_sources()
    admission.verify_loaded_sources(sources)
    require(registration.binding.evaluator_sha256 == execution.digest(sources), 'HTTP evaluator differs')
    selectors, assertions = [], []
    for index, cell in enumerate(value.diagnostic_cells):
        step = profile.case.steps[index]
        path = '/diagnostics/' + str(index) + '/status'
        disposition = cell.applicability
        # Current mixed CLI observer has no qualified public-state wrapper.
        if step.kind == 'cli':
            disposition = 'unqualified'
        selectors.append(Selector(cell.case_id, path, disposition,
                                  '/steps/' + str(index) + '/expectation'))
        if disposition != 'normative' or step.kind != 'request' or step.expectation is None:
            continue
        semantic = step.expectation.semantic
        if semantic is None:
            continue
        # These are exact original obligation labels and finite HTTP subfacets,
        # not whole-table or interaction closure. Review retains missing joins.
        targets = []
        is_error = semantic.shape == 'error'
        if 'V0-HTTP-02' in profile.case.requirement_ids and is_error:
            targets.append(('V0-HTTP-02', 'Declared status/content/JSON/error facets of this actual wire request.'))
        route = '' if step.request is None else step.request.target.split('?', 1)[0]
        if not is_error and route.startswith(('/api/documents', '/api/export', '/api/import')) and 'V0-HTTP-01' in profile.case.requirement_ids:
            targets.append(('V0-HTTP-01', 'Exact successful declared legacy request and finite JSON result; all other route/shape/order cases remain separate.'))
        if route.startswith('/api/jobs'):
            if not is_error and 'M1-HTTP-01' in profile.case.requirement_ids:
                targets.append(('M1-HTTP-01', 'Exact declared jobs route and finite result; no complete table/state-machine proof.'))
            if 'M1-HTTP-02' in profile.case.requirement_ids:
                targets.append(('M1-HTTP-02', 'Declared job success/error status and value only; no unsupported error inference.'))
            if route == '/api/jobs' and step.request is not None and step.request.method == 'POST' and 'M1-I30' in profile.case.requirement_ids:
                targets.append(('M1-I30', 'This exact submitted body shape acceptance/refusal; source value validation and other shapes remain separate.'))
            body_value = None
            if step.request is not None and step.request.method == 'POST' and route == '/api/jobs':
                try:
                    body_value = json.loads(step.request.body)
                except (ValueError, UnicodeError):
                    pass
            path_subject = (route == '/api/jobs/subject/prepare' or
                (route == '/api/jobs' and type(body_value) is dict and body_value.get('job_id') == 'subject'))
            if profile.case.row_id.startswith('HTTP-ROOT-PATH/') and path_subject and (is_error or route == '/api/jobs/subject/prepare') and step.request is not None and step.request.method == 'POST' and 'M1-I31' in profile.case.requirement_ids:
                targets.append(('M1-I31', 'This exact authored public path/namespace acceptance/refusal; inaccessible storage effects are not inferred.'))
        if profile.case.row_id.startswith('HTTP-BODY-WIRE/') and is_error and 'V0-HTTP-03' in profile.case.requirement_ids:
            targets.append(('V0-HTTP-03', 'This declared malformed content/body request and supported error response; raw-only captures remain unqualified.'))
        if semantic.shape == 'health':
            targets.append(('V0-HTTP-04', 'Exact authorized M4 health successor; compiler compatibility must retain m1:V0-HTTP-04 and M4-API-SCHEMA:clause:2.'))
        for target, rationale in targets:
            identifier = 'http-' + sha(encoded([value.case_id, cell.case_id, target]))[:24]
            assertions.append(AssertionDeclaration('m1:' + target,
                compiler.Assertion(identifier, 'negative' if is_error else 'positive', ('M1-GATE-HTTP',)),
                cell.case_id, path, rationale))
    selectors.append(Selector(profile.mechanics_case_id, '/mechanics_guard/status', 'normative',
                              '/mechanics_guard', 'mechanics'))
    require(tuple(row.case_id for row in selectors if row.case_id in actual.gate.ordered_case_ids)
            == actual.gate.ordered_case_ids, 'HTTP ordered selector census differs')
    return ExecutableSlice('http', value.case_id, actual.gate, actual.definition_sha256,
        actual.original_definition_purpose, compiler.M1_SHA256, profile.sha256,
        tuple(sorted(sources.items())), tuple(selectors), tuple(assertions), tuple(value.record()['remaining_coverage']),
        encoded({'registration': asdict(registration), 'policy': asdict(policy)}).decode())


def product_process_slice(registration: Any, profile: Any, policy: Any) -> ExecutableSlice:
    """Concrete finite M3/M4 values, never crash, SQL layout or browser evidence."""
    from . import candidate_product_process_execution_v1 as execution
    from . import candidate_product_process_observation_v1 as observation
    from . import candidate_observation_admission_v1 as admission
    require(type(registration) is execution.HttpRegistration and type(profile) is execution.HttpProductProfile
            and type(policy) is execution.HttpPolicy, 'Exact product-process registration/profile/policy required')
    observed = registration.observation
    actual = execution.observation_registration_for(registration.binding, profile, policy,
        subject=observed.gate.binding.subject, gate_id=observed.gate.gate_id,
        commit_oid=registration.commit_oid, tree_oid=registration.tree_oid,
        repetition_id=registration.repetition_id, cohort_trajectory_ids=observed.cohort_trajectory_ids)
    require(actual == observed, 'Product-process original registration differs')
    catalog = observation.selector_catalog(profile.case.row_id, purpose=registration.binding.purpose)
    admission.verify_loaded_sources(catalog['evaluator_sources'])
    require(registration.binding.evaluator_sha256 == execution.digest(catalog['evaluator_sources']),
            'Product-process evaluator differs')
    selectors = tuple(Selector(row['case_id'], row['observation_pointer'], row['disposition'],
        row['definition_pointer'], 'mechanics' if row['evidence_kind'] == 'physical_mechanics' else 'semantic')
        for row in catalog['selectors'])
    inventory = load_catalog(Path(__file__).resolve().parents[1])
    original = json.loads(profile.case.original_json)
    assertions = []
    for index, (action, step) in enumerate(zip(original['input']['actions'], profile.case.steps, strict=True)):
        if step.kind not in ('request', 'cli'):
            continue
        expected = step.expectation.record()
        error = expected.get('status', 200) >= 400 or expected.get('exit', 0) != 0
        kind = 'negative' if error else 'positive'
        args = action.get('args', [])
        command = args[0] if args else ''
        path = action.get('path', '')
        record = expected.get('json', expected.get('json_subset', {}))
        row_id = profile.case.row_id
        # Each target describes a demonstrated subfacet, not the whole clause.
        claims: list[tuple[str, str, str]] = []
        def claim(identifier: str, why: str, assertion_kind: str = kind) -> None:
            claims.append((identifier, assertion_kind, why))
        if '/api/diagnostics' in path or '/api/maintenance/diagnostics' in path or command == 'diagnostics':
            claim('M3-DIAGNOSTICS:clause:0', 'Only explicitly expected diagnostic keys/counts and closed outer keys; unasserted fields remain unspecified.')
            if isinstance(record, dict) and ('worker_state' in record or 'index_state' in record):
                claim('M3-DIAGNOSTICS:clause:1', 'Declared stopped worker or current index value after finite history; no live-owner liveness inference.')
            if isinstance(record, dict) and any(key in record for key in ('documents', 'blobs', 'revisions')):
                claim('M3-DIAGNOSTICS:clause:2', 'Exact asserted public graph counts after the declared sequence; no orphan-table inspection.')
        if row_id.startswith('process-worker-'):
            if '/enqueue' in path or '/api/jobs' in path or command in ('worker', 'job-retry'):
                claim('M3-WORKER-RECOVERY:clause:0', 'Explicit enrollment/manual/terminal/retry result and exact known subsequent state in this finite sequence.', 'history')
            if command == 'worker':
                claim('M3-WORKER-RECOVERY:clause:1', 'Finite --once processed-ID/order response; no second-owner exclusion, daemon polling or durable claim proof.', 'history')
            if error and isinstance(record, dict) and record.get('error') == 'stale_epoch':
                claim('M3-WORKER-RECOVERY:clause:2', 'Only inherited stale epoch refusal after retry; incarnation and stale-worker fences remain missing.')
        if row_id == 'process-reindex-resume-and-generation-restart':
            if command == 'reindex' or path == '/api/maintenance/reindex':
                claim('M3-REINDEX:clause:0', 'Exact bounded-step cursor/processed/total/generation result in the authored sequence.', 'history')
                prior_reindex = [j for j, earlier in enumerate(original['input']['actions'][:index])
                    if earlier.get('path') == '/api/maintenance/reindex' or earlier.get('args', [None])[0] == 'reindex']
                if prior_reindex:
                    prior_expected = profile.case.steps[prior_reindex[-1]].expectation.record().get('json', {})
                    if isinstance(record, dict) and record.get('target_generation') != prior_expected.get('target_generation'):
                        claim('M3-REINDEX:clause:1', 'Expected generation restart/result after known edits; no atomic pointer-switch/crash proof.', 'history')
            if path.startswith('/api/lifecycle/documents') and action.get('method') == 'GET':
                claim('M3-REINDEX:clause:2', 'Known current query values while derived index is stale or rebuilt across real process epochs.', 'history')
                claim('M2-QUERY:clause:1', 'Exact declared text/deleted filter result, total and generation; other filter combinations remain missing.')
        if row_id == 'process-export-canonical-selection-and-byte-limit':
            if path == '/api/export-bundle' or command == 'export-bundle':
                claim('M3-EXPORT:clause:0', 'Explicit finite selection including empty/deleted refusal and declared CLI/HTTP result.')
                if not error or record == {'error': 'too_large'}:
                    claim('M3-EXPORT:clause:1', 'Declared exact payload/order/Unicode or max_bytes refusal; only canonical_wire_bytes selector proves byte encoding.',
                          'boundary' if error else kind)
                if not error:
                    claim('M3-EXPORT:clause:2', 'Actual declared successful HTTP body or CLI JSON projection only; browser download remains missing.')
            if path == '/api/v1/export' or command == 'export-v1':
                claim('M4-API-SCHEMA:clause:1', 'Exact opt-in v4 export logical revision IDs/projection; no physical layout or SQL preservation.')
        if row_id.startswith('process-backup-'):
            if path.startswith('/api/maintenance/backups') or command in ('backups', 'backup', 'backup-root-adopt'):
                claim('M3-BACKUP-RESTORE:clause:0', 'Only explicit empty-root adoption/refusal or existing-name refusal and logical backup metadata; filesystem safety remains separate.')
            if row_id == 'process-backup-restore-fences-edits-and-removed-id':
                if path == '/api/maintenance/backups' and not error:
                    claim('M3-BACKUP-RESTORE:clause:1', 'Expected logical payload digest and bounded envelope byte metadata; no actual file canonical/fsync/publication proof.')
                if command == 'restore-backup' or (path == '/api/maintenance/restore' and action.get('method') == 'POST'):
                    claim('M3-BACKUP-RESTORE:clause:3', 'Successful restore result/current generation or stale-generation refusal; lock/atomic activation remains missing.', 'history')
                    claim('M3-BACKUP-RESTORE:clause:5', 'Declared restore result and stale repeat refusal; no abrupt pre/post-commit evidence.', 'history')
                if '/api/v1/documents' in path or command in ('import', 'refresh', 'document-v1'):
                    claim('M3-BACKUP-RESTORE:clause:4', 'Known restored/removed document and edit-token state in this finite sequence; job receipt/control preservation remains missing.', 'history')
                    claim('M2-IDENTITY-REVISIONS:clause:2', 'Observed edit token/refusal subfacet after known effective changes; concurrency and transaction atomicity remain missing.', 'history')
        if row_id.startswith('process-public-'):
            if command == 'migrate':
                claim('M4-MIGRATION:clause:1', 'Declared public schema0/2 migration or schema4 no-op result only; corrupt/unknown/schema3 cases remain missing.')
                claim('M4-MIGRATION:clause:3', 'Exact initial and repeated explicit migrate result; no abrupt commit-boundary or incarnation proof.', 'history')
            if path.startswith(('/api/lifecycle/documents', '/api/v1/documents', '/api/jobs')) or command == 'documents-v1':
                claim('M4-MIGRATION:clause:2', 'Exact expected logical snapshot projection across restart; physical SQL and raw stored manifest/receipt bytes remain missing.', 'history')
            if path == '/api/jobs':
                claim('M4-COMPATIBILITY:clause:0', 'Exact retained legacy jobs HTTP shape/state through migration; other inherited interfaces remain separate.', 'history')
        if path.startswith('/api/v1/documents') or command in ('documents-v1', 'document-v1'):
            if 'M4-API-SCHEMA' in profile.case.requirement_ids:
                claim('M4-API-SCHEMA:clause:1', 'Expected current/history revision-ID projections; storage normalization remains separate.')
        if 'M3-INTERFACES' in profile.case.requirement_ids and (
                path.startswith('/api/maintenance/') or command in ('worker', 'export-bundle', 'backups', 'backup-root-adopt')):
            claim('M3-INTERFACES:clause:0', 'This exact declared CLI/HTTP form and selected value; no browser, shared delegation or arbitrary path-security inference.')
        step_selectors = [row for row in catalog['selectors'] if row['case_id'] == profile.diagnostic_case_ids[index]
                          and row['disposition'] == 'normative' and row['evidence_kind'] != 'physical_mechanics']
        for identifier, assertion_kind, rationale in claims:
            unit = inventory.unit(identifier)
            lanes = {'public-contract', 'cli' if step.kind == 'cli' else 'http'}
            # Chronological logical state across retired servers/finite CLI is
            # process-restart evidence. It never supplies durability/crash credit.
            if assertion_kind == 'history':
                lanes.add('process-restart')
            if row_id.startswith('process-public-'):
                lanes.add('migration')
            logical = tuple(gate.id for gate in inventory.inventory.logical_gates
                if gate.role == 'product' and gate.lane in lanes
                and set(unit.requirement_ids) & set(gate.requirement_ids) & set(profile.case.requirement_ids))
            if not logical:
                continue
            for selector in step_selectors:
                # Status/exit alone never proves claimed JSON state or bytes.
                facet = selector['facet']
                if not (facet == 'json_exact' or facet.startswith('json_field:') or facet.startswith('integer_range:') or facet == 'canonical_wire_bytes'):
                    continue
                if identifier == 'M3-DIAGNOSTICS:clause:1' and facet.startswith('json_field:') and facet not in ('json_field:worker_state', 'json_field:index_state'):
                    continue
                if identifier == 'M3-DIAGNOSTICS:clause:2' and facet.startswith('json_field:') and facet not in ('json_field:documents', 'json_field:revisions', 'json_field:blobs'):
                    continue
                selected_rationale = rationale
                if path == '/api/maintenance/backups' and not error and identifier == 'M3-BACKUP-RESTORE:clause:0':
                    if facet != 'json_field:name':
                        continue
                    selected_rationale = 'Only the returned declared backup basename; freshness, root/path checks and full snapshot content are separate.'
                if path == '/api/maintenance/backups' and not error and identifier == 'M3-BACKUP-RESTORE:clause:1':
                    meanings = {'json_field:name': 'Returned declared metadata basename only.',
                        'json_field:generation': 'Returned declared snapshot generation only.',
                        'json_field:payload_sha256': 'Returned digest equals independently authored logical payload digest only; retained file and stored raw text remain unobserved.',
                        'integer_range:/bytes': 'Returned envelope byte metadata within the declared bound only; actual file bytes/publication remain unobserved.'}
                    if facet not in meanings:
                        continue
                    selected_rationale = meanings[facet]
                identifier_hash = sha(encoded([row_id, selector['observation_pointer'], identifier, assertion_kind]))[:24]
                assertions.append(AssertionDeclaration(identifier, compiler.Assertion('process-' + identifier_hash,
                    assertion_kind, logical), selector['case_id'], selector['observation_pointer'],
                    selected_rationale + ' Selected facet: ' + facet + '; no other clause subfacet is inferred.'))
    require(tuple(catalog['ordered_case_ids']) == actual.gate.ordered_case_ids, 'Product-process case roster differs')
    return ExecutableSlice('product-process', profile.case.row_id, actual.gate, actual.definition_sha256,
        actual.original_definition_purpose, compiler.PRODUCT_V2_SHA256, profile.sha256,
        tuple(sorted(catalog['evaluator_sources'].items())), selectors, tuple(assertions),
        tuple(catalog['required_unfinished_coverage']), encoded({'registration': asdict(registration), 'policy': asdict(policy)}).decode())


def scope_review_input(catalog: SourceCatalog, declaration: compiler.Declaration) -> compiler.ScopePlan:
    """Populate actual declared cells; leave every other eligible cell unresolved.

    This is a prospective review input, never an approved applicability plan. No
    N/A exclusions, compatibility overrides or purpose waivers are fabricated.
    """
    plans = {row.obligation_id: row for row in declaration.obligations}
    applicability = []
    for unit in catalog.units:
        plan = plans.get(unit.obligation.id)
        cells = tuple(dict.fromkeys(compiler.ApplicabilityCell(a.kind, gate)
            for a in (() if plan is None else plan.assertions) for gate in a.logical_gate_ids))
        require(set(cells) <= set(unit.eligible_cells), 'Assertion cell outside exact source/owner universe')
        applicability.append(compiler.ObligationApplicability(unit.obligation.id, unit.requirement_ids, cells, (),
            'Concrete proposed cells only; adequacy and every remaining eligible cell need independent semantic review.'))
    dispositions = tuple(compiler.PlanningDisposition(owner, gap_id,
        tuple(unit.obligation.id for unit in catalog.units if owner in unit.requirement_ids),
        'Original note retained as a review duty across these exact owner units; no claim of implementation or waiver.')
        for owner, gap_id, _, _ in catalog.inventory.planning_notes)
    return compiler.ScopePlan(catalog.inventory.sha256, compiler.declaration_fingerprint(declaration),
        tuple(applicability), dispositions, tuple(row.id for row in catalog.inventory.qualification_rules),
        declaration.purposes, declaration.compatibility, ())


def coverage_census(catalog: SourceCatalog, declaration: compiler.Declaration) -> dict[str, Any]:
    """Executable declarations versus full universe; counts never assert adequacy."""
    scope = scope_review_input(catalog, declaration)
    plans = {row.obligation_id: row for row in declaration.obligations}
    cells = {row.obligation_id: set(row.required_cells) for row in scope.applicability}
    units: list[dict[str, Any]] = [{'id': unit.obligation.id, 'role': unit.obligation.role, 'requirement_ids': list(unit.requirement_ids),
        'source_pointer': unit.obligation.source_pointer, 'source_sha256': unit.obligation.source_sha256,
        'declared_assertions': len(plans[unit.obligation.id].assertions),
        'declared_cells': [asdict(row) for row in unit.eligible_cells if row in cells[unit.obligation.id]],
        'unresolved_cells': [asdict(row) for row in unit.eligible_cells if row not in cells[unit.obligation.id]],
        'full_semantic_adequacy': 'not_reviewed'} for unit in catalog.units]
    return {'protocol': PROTOCOL, 'inventory_sha256': catalog.inventory.sha256, 'source_units': len(units),
        'product_requirements': len(catalog.inventory.product_ids), 'prerequisites': len(catalog.inventory.prerequisite_ids),
        'authority_rules': len(catalog.inventory.qualification_rules), 'gap_notes': len(catalog.inventory.planning_notes),
        'physical_slots': len(declaration.gates), 'declared_assertions': sum(row['declared_assertions'] for row in units),
        'units_with_assertions': sum(bool(row['declared_assertions']) for row in units),
        'eligible_cells': sum(len(unit.eligible_cells) for unit in catalog.units),
        'declared_cells': sum(len(row['declared_cells']) for row in units),
        'unresolved_cells': sum(len(row['unresolved_cells']) for row in units), 'units': units,
        'semantic_approval': False, 'product_observations': 0,
        'meaning': 'Eligible cells require applicability decisions, not automatically one independent test each; a declared facet does not prove the whole source unit.'}


def assemble_declaration(catalog: SourceCatalog, cohort: compiler.CohortDesign,
                         slices: tuple[ExecutableSlice, ...], *, review_sha256: str,
                         purposes: tuple[compiler.PurposeAssignment, ...] = (),
                         compatibility: tuple[compiler.CompatibilityOverride, ...] = (),
                         additional_plans: tuple[compiler.ObligationPlan, ...] = (),
                         additional_suites: tuple[compiler.SuiteDefinition, ...] = (),
                         additional_gates: tuple[compiler.ExecutionGate, ...] = (),
                         additional_edges: tuple[compiler.CoverageEdge, ...] = ()) -> compiler.Declaration:
    """Combine concrete reviewed component declarations without shrinking full scope.

    review_sha256 references an earlier independent component review, NOT this
    declaration's future complete scope review (which would create a hash cycle).
    No review authority follows from calling this structural constructor.
    Additional modules must supply actual source-bound selectors and get the
    later full semantic review; missing observations remain empty assertion plans.
    """
    registry.sha256(review_sha256)
    require(type(catalog) is SourceCatalog and type(cohort) is compiler.CohortDesign
            and type(slices) is tuple and all(type(row) is ExecutableSlice for row in slices),
            'Typed source catalog/cohort/physical slices required')
    require(len({row.gate.gate_id for row in slices}) == len(slices), 'Duplicate physical gate')
    groups: dict[str, list[compiler.Assertion]] = {unit.obligation.id: [] for unit in catalog.units}
    suites, gates, edges = list(additional_suites), list(additional_gates), list(additional_edges)
    for component in slices:
        verify_slice(component)
        suite, gate, component_edges = component.compiler_records(
            suite_id='suite-' + sha(component.gate.gate_id.encode())[:24], physical_slot=component.gate.gate_id)
        suites.append(suite); gates.append(gate); edges.extend(component_edges)
        valid_selectors = {(row.case_id, row.observation_pointer): row for row in component.selectors}
        for claim in component.assertions:
            require(claim.obligation_id in groups, 'Assertion targets foreign obligation')
            selected = valid_selectors.get((claim.case_id, claim.observation_pointer))
            require(selected is not None and selected.disposition == 'normative'
                    and selected.evidence_kind == 'semantic' and claim.case_id in component.gate.ordered_case_ids,
                    'Unspecified/unqualified/mechanics selector cannot mint semantic credit')
            if claim.assertion not in groups[claim.obligation_id]:
                groups[claim.obligation_id].append(claim.assertion)
    additions = {row.obligation_id: row for row in additional_plans}
    require(len(additions) == len(additional_plans) and set(additions) <= set(groups), 'Foreign/duplicate additional plan')
    plans = []
    for unit in catalog.units:
        extra = additions.get(unit.obligation.id)
        if extra is not None:
            require(extra.requirement_ids == unit.requirement_ids, 'Additional plan changed source/reviewed owners')
            groups[unit.obligation.id].extend(item for item in extra.assertions if item not in groups[unit.obligation.id])
        plans.append(compiler.ObligationPlan(unit.obligation.id, unit.requirement_ids,
            tuple(groups[unit.obligation.id]), extra.review_sha256 if extra is not None else review_sha256,
            unit.ownership_reason))
    return compiler.Declaration(catalog.inventory.sha256, tuple(plans), tuple(suites), tuple(gates),
                                tuple(edges), purposes, compatibility, cohort)


def _gate(value: dict[str, Any]) -> registry.Gate:
    body = value['binding']
    binding = registry.Binding(**{**body, 'subject': registry.Subject(**body['subject'])})
    return registry.Gate(value['gate_id'], tuple(value['requirement_ids']), tuple(value['ordered_case_ids']), binding)


def verify_slice(value: ExecutableSlice) -> None:
    """Rebuild from closed existing factories; replacing dataclass hashes is insufficient."""
    require(type(value) is ExecutableSlice, 'Exact source-derived slice required')
    record = json.loads(value.factory_input_json)
    registration = record['registration']
    if value.family == 'cli':
        from . import candidate_client_execution_v5 as cli
        registration = cli.ClientRegistration(cli.ClientBinding(**registration['binding']),
            registration['commit_oid'], registration['tree_oid'], registration['repetition_id'],
            _gate(registration['gate']), tuple(registration['cohort_trajectory_ids']))
        actual = cli_slice(registration)
    elif value.family == 'http':
        from . import candidate_http_execution_v4 as http
        from . import candidate_http_cases_v1 as cases
        from . import cumulative_observation_profile_v1 as profiles
        from . import candidate_observation_admission_v1 as admission
        observed = registration['observation']
        observation = admission.ObservationRegistration(**{**observed, 'gate': _gate(observed['gate']),
            'cohort_trajectory_ids': tuple(observed['cohort_trajectory_ids'])})
        registration = http.HttpRegistration(http.HttpBinding(**registration['binding']), registration['commit_oid'],
            registration['tree_oid'], registration['repetition_id'], observation)
        policy = http.HttpPolicy(**{**record['policy'], 'wire_limits': http.wire.WireLimits(**record['policy']['wire_limits'])})
        case = next(row for row in cases.definitions() if row.row_id == value.history_id)
        cumulative = profiles.http_profile(value.history_id, purpose=value.gate.binding.purpose)
        profile = http.HttpProductProfile(case, profiles.ORIGINAL_DEFINITION_PURPOSE, cumulative)
        actual = http_slice(registration, profile, policy)
    elif value.family == 'product-process':
        from . import candidate_product_process_execution_v1 as product
        from . import candidate_product_process_core_v1 as core
        from . import candidate_observation_admission_v1 as product_admission
        observed = registration['observation']
        observation = product_admission.ObservationRegistration(**{**observed, 'gate': _gate(observed['gate']),
            'cohort_trajectory_ids': tuple(observed['cohort_trajectory_ids'])})
        product_registration = product.HttpRegistration(product.HttpBinding(**registration['binding']), registration['commit_oid'],
            registration['tree_oid'], registration['repetition_id'], observation)
        product_policy = product.HttpPolicy(**{**record['policy'], 'wire_limits': product.wire.WireLimits(**record['policy']['wire_limits'])})
        product_profile = product.HttpProductProfile(core.case_definition(value.history_id), core.ORIGINAL_DEFINITION_PURPOSE)
        actual = product_process_slice(product_registration, product_profile, product_policy)
    else:
        raise ScopeSourceError('Unknown executable factory; version the integration before registration')
    require(actual == value, 'Source-derived selectors/assertions/purpose were substituted')
