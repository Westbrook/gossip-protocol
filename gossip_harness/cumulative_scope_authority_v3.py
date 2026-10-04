"""Versioned M2 and storage-aware scope enrollment, sharing the original trust machinery.

v1 remains a closed factory. This extension changes the prospective request and
registration identity, not the protected reviewer delivery/chronology contract.
No production full semantic review or prerequisite producer is supplied here.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from . import cumulative_scope_authority_v2 as previous
from . import cumulative_scope_source_v3 as source
from . import candidate_scope_consumer_v1 as consumer
from . import cumulative_cli_projection_v1 as projection
from . import project_acceptance_compiler_v1 as compiler
from . import project_acceptance_registry_v1 as registry
from .candidate_checkpoint_chain_v1 import PrefixCommitment

PROTOCOL = 'cumulative-scope-authority-v3'
LOADED_SOURCE_SHA256 = source.sha(Path(__file__).read_bytes())
ReviewEnrollment = previous.ReviewEnrollment
RegistrationMissing = previous.RegistrationMissing
RegistrationProvenance = previous.RegistrationProvenance
StagedDesign = previous.StagedDesign
REVIEW_PURPOSE = previous.REVIEW_PURPOSE


def implementation_sources() -> dict[str, str]:
    from . import candidate_observation_admission_v1 as admission
    result = previous.implementation_sources()
    result.update(projection.definition_sources())
    root = Path(__file__).resolve().parent
    for name in ('cumulative_scope_source_v3.py', 'cumulative_scope_authority_v3.py'):
        result['gossip_harness/' + name] = source.sha((root / name).read_bytes())
    admission.verify_loaded_sources(result)
    return result


@dataclass(frozen=True, slots=True)
class ScopeSubmission(previous.ScopeSubmission):
    """Same complete inventory, with an explicit optional public CLI clarification."""
    cli_projection_contract: dict[str, Any] | None = None

    def request(self) -> dict[str, Any]:
        consumer.require(type(self) is ScopeSubmission and type(self.slices) is tuple,
                         'Exact version3 prospective submission required')
        for item in self.slices:
            source.verify_slice(item)
        # Reuse the complete original target builder with no executable catalogs;
        # insert only freshly verified v3 catalogs and rehash the exact suite
        # targets before exposing/retaining this request. No old request is reused.
        base = previous.previous.ScopeSubmission(self.catalog, self.declaration, self.scope,
            self.subject, (), self.qualification_specs).request()
        for target in base['targets']:
            if target['id'].startswith('suite:'):
                suite_id = target['value']['suite']['id']
                target['value']['source_catalogs'] = [asdict(item) for item in self.slices
                    if any(gate.id == item.gate.gate_id and gate.suite_id == suite_id
                           for gate in self.declaration.gates)]
                target['sha256'] = source.sha(source.encoded(target['value']))
        capacity = registry.capacity_manifest(self.declaration.capacity_profile)
        base['targets'] = [row for row in base['targets'] if row['id'] != 'capacity-contract']
        base['targets'].append({'id': 'capacity-contract', 'value': capacity,
            'sha256': source.sha(source.encoded(capacity)),
            'duty': 'Review the explicitly selected full-history aggregation bound and separate resource qualification; no scope truncation or hidden subdivision.'})
        clarified = self.cli_projection_contract is not None
        cli_slices = [item for item in self.slices if item.family == 'cli']
        consumer.require(all((item.gate.binding.execution_protocol == projection.EXECUTION_PROTOCOL) == clarified
            for item in cli_slices), 'CLI slices and mandatory clarification review must agree')
        if clarified:
            consumer.require(self.cli_projection_contract == projection.manifest(), 'Exact paired addendum review identity required')
            value = {'effective_requirements':projection.manifest(),'public_addendum_text':projection.text(),
                'execution_contract_sha256':self.subject.execution_contract_sha256,
                'successful_command_projections':dict(projection.COMMANDS),
                'unchanged_unspecified_commands':['import','jobs'],
                'affected_cli_slices':[asdict(item) for item in cli_slices],
                'unchanged_inventory_sha256':self.catalog.inventory.sha256,
                'full_original_obligation_count':len(self.catalog.units)}
            base['targets'].append({'id':projection.REVIEW_TARGET,'value':value,
                'sha256':source.sha(source.encoded(value)),
                'duty':'Independently review the full prospective text and all six JOB/TOKEN/RECEIPT projections, exact fields, applicable receipt/epoch semantics, every affected CLI selector and source clause. Confirm literal import/jobs remain unspecified and all original312 units,22 authority rules and188 gap duties remain required. A matching digest or attachment presence is not semantic approval.'})
            base['effective_requirements'] = projection.manifest()
        base['protocol'] = projection.SCOPE_PROTOCOL if clarified else PROTOCOL
        base['implementation_sources'] = implementation_sources()
        base['factory_protocol'] = source.PROTOCOL
        base['capacity_contract'] = capacity
        return base


class ScopeRegistrationController(previous.ScopeRegistrationController):
    """Exact v3 requests; original external-head and enrolled-review checks reused."""
    def _check_submission(self, submission: previous.previous.ScopeSubmission) -> dict[str, Any]:
        try:
            consumer.require(type(submission) is ScopeSubmission
                and submission.catalog == source.load_catalog(self.root),
                'Version3 complete source/ownership catalog required')
            known = {unit.obligation.id: unit.requirement_ids for unit in submission.catalog.units}
            plans = submission.declaration.obligations
            consumer.require(len({row.obligation_id for row in plans}) == len(plans)
                and all(row.obligation_id in known and row.requirement_ids == known[row.obligation_id] for row in plans),
                'Declaration changed exact source or reviewed owners')
            return submission.request()
        except OSError as error:
            raise consumer.AuthorityUnavailable(str(error)) from error
        except (ValueError, TypeError, LookupError) as error:
            raise consumer.AuthorityError(str(error)) from error

    def register(self, submission: previous.previous.ScopeSubmission, *, report_name: str) -> consumer.RegisteredAcceptance:
        """Explicit host installation after real full review and complete compilation."""
        staged = self.stage(submission)
        if staged.design.registry is None or staged.design.blockers or staged.missing_executable_edges:
            raise RegistrationMissing('Complete executable decomposition is unavailable: '
                + ','.join(row.code + ':' + row.target for row in staged.design.blockers)
                + ','.join(staged.missing_executable_edges))
        self.authenticate_review(staged, report_name=report_name)
        assert staged.design.registry is not None
        product = staged.design.registry
        # Closed factory's physical gate must equal compiled normalized scope.
        # A semantic review cannot silently change the owner's requirements or roster.
        for item in submission.slices:
            consumer.require(next((row for row in product.gates if row.gate_id == item.gate.gate_id), None) == item.gate,
                             'Compiled gate differs from actual executable original')
        registered = consumer.RegisteredAcceptance(submission.catalog.inventory.sha256, submission.scope.sha256,
            compiler.declaration_fingerprint(submission.declaration), registry.design_fingerprint(product),
            submission.subject.execution_contract_sha256, submission.qualification_specs)
        consumer.qualification_requests(registered, staged.design, submission.declaration)
        material = {'protocol': submission.request()['protocol'], 'request_name': staged.request_name, 'request_sha256': staged.request_sha256,
            'report_name': report_name, 'report_sha256': source.sha(self.chain.read(report_name)),
            'subject': asdict(submission.subject), 'registered': asdict(registered)}
        raw = source.encoded(material)
        name = 'scope-registration-' + registry.fingerprint(submission.subject) + '.json'
        if self.chain.has(name):
            consumer.require(self.chain.read(name) == raw, 'Immutable registration replay differs')
        else:
            self.chain.retain(name, raw)
        self.expected = self.chain.validate_boundary().commitment
        enrollment = next(row for row in self.enrollments if row.report_name == report_name)
        self._registration_order(report_name, enrollment.provenance_name, name)
        self._installed[registry.fingerprint(submission.subject)] = (submission, name, raw, staged)
        return registered

    def open_snapshot(self) -> consumer.AuthoritySnapshot:
        self._current()
        return ScopeSnapshot(self, self.expected)


class ScopeSnapshot(previous.ScopeSnapshot):
    """Shared original registration reader; every recheck returns through v3 owner."""
    def __init__(self, owner: ScopeRegistrationController, expected: PrefixCommitment):
        consumer.require(type(owner) is ScopeRegistrationController, 'Exact version3 scope owner required')
        previous.previous.ScopeSnapshot.__init__(self, owner, expected)
