"""Package actual FinalV3 inputs for a separately pinned cold verifier.

Exporting confers no authority and does not run acceptance. The protected host
supplies original genesis contexts; they are checked against each actual chain.
The returned pin must be approved independently with the prospective mapping
and qualification capsule. Partial exports are preserved and never overwritten.
"""
from __future__ import annotations

from dataclasses import asdict
import os
from pathlib import Path
from typing import Any

from . import candidate_checkpoint_chain_v1 as checkpoint
from .candidate_checkpoint_head_v1 import ExternalHead
from . import cumulative_final_acceptance_v1 as shared
from . import cumulative_final_acceptance_v3 as final
from . import cumulative_final_originals_v1 as originals
from . import cumulative_rehearsal_codec_v1 as codec
from . import project_acceptance_registry_v1 as registry
from .peer_financial_terminal_v1 import require, sha

PROTOCOL = 'cumulative-rehearsal-export-v1'
MAX_EXPORT_BYTES = 512 * 1024 * 1024
MAX_EXPORT_FILES = 2 * originals.MAX_OBSERVATIONS + 128


class InputWriter:
    def __init__(self, root: Path):
        self.root = originals.path(str(root))
        require(not self.root.exists(), 'New protected export directory required; never overwrite originals')
        self.root.mkdir(mode=0o700)
        self.bytes = self.files = 0

    def put(self, value: Any, *, typed: bool = False) -> dict[str,str]:
        raw = codec.encoded(codec.pack(value) if typed else value)
        require(len(raw) <= codec.MAX_BYTES and self.bytes + len(raw) <= MAX_EXPORT_BYTES
            and self.files < MAX_EXPORT_FILES, 'Protected export bound exceeded')
        name = f'input-{self.files:06d}-' + sha(raw) + '.json'
        path = self.root/name
        with path.open('xb') as stream:
            os.fchmod(stream.fileno(),0o600);stream.write(raw);stream.flush();os.fsync(stream.fileno())
        fd = os.open(self.root,os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try: os.fsync(fd)
        finally: os.close(fd)
        self.files += 1; self.bytes += len(raw)
        return {'path':str(path),'sha256':sha(raw)}


def proof_descriptor(chain: checkpoint.CheckpointChain, context: dict) -> dict:
    require(type(chain) is checkpoint.CheckpointChain and type(chain.authority) is ExternalHead,
            'Exact original chain and independently held head required')
    assert isinstance(chain.authority, ExternalHead)
    current = chain.validate_boundary().commitment
    value = {'raw':str(chain.raw_root),'delta':str(chain.delta_root),'head':str(chain.authority.root),
        'context':context,'limits':asdict(chain.limits),'expected':asdict(current)}
    genesis = checkpoint._encoded({'protocol':checkpoint.PROTOCOL,'kind':'genesis','context':context,
        'raw_root':value['raw'],'delta_root':value['delta'],'limits':value['limits']})
    require(sha(checkpoint._GENESIS_DOMAIN+genesis) == current.context_sha256,
            'Supplied original context is not this independently anchored chain')
    chain.validate_boundary(expected=current)
    return value


def export_final_packet(owner: final.FinalAcceptanceV3, specifications: tuple[shared.ObservationSpec,...], *,
                        destination: Path, contexts: dict[str,dict]) -> dict[str,str]:
    require(type(owner) is final.FinalAcceptanceV3 and type(specifications) is tuple
        and all(type(spec) is shared.ObservationSpec for spec in specifications), 'Exact original final owner/specifications required')
    owner._current_originals()
    shared.workflow.validate_plan(owner.plan, owner.repository)
    require(owner.records.read('final.assessment') is not None and owner.freeze is not None
        and owner.prerequisite_owner is not None and owner.promotion_owner is not None,
        'Original assessment and original authority capabilities must exist before packaging')
    assert owner.prerequisite_owner is not None and owner.promotion_owner is not None
    destination = originals.path(str(destination))
    protected = owner._protected_roots()
    for spec in specifications:
        protected.extend((spec.root,spec.delta_root,spec.cleanup_root,spec.checkpoint_authority.root))
        if spec.layout_authority is not None:
            chain = spec.layout_authority.journal
            protected.extend((chain.raw_root,chain.delta_root,chain.authority.root))
    for enrollment in owner.enrollments.values():
        if enrollment.spec is not None and enrollment.spec.kind == 'workflow_inspection':
            from . import candidate_workflow_review_v1 as workflow_review
            delivery = enrollment.owner.product_delivery
            require(type(delivery) is workflow_review.ProductInspectionDelivery, 'Original host delivery required before export')
            protected.extend((delivery.journal.raw_root,delivery.journal.delta_root,delivery.journal.authority.root))
    require(all(not destination.is_relative_to(path) and not path.is_relative_to(destination) for path in protected),
            'Export must be outside all candidate and original proof roots')
    writer = InputWriter(destination)
    proofs: dict[str,dict] = {}
    chains: dict[str,checkpoint.CheckpointChain] = {}
    def proof(chain: checkpoint.CheckpointChain) -> str:
        key = chain.commitment.context_sha256
        require(key in contexts, 'Protected original context missing')
        record = proof_descriptor(chain,contexts[key])
        require(key not in proofs or proofs[key] == record, 'Aliased protected proof identity')
        proofs[key] = record; chains[key] = chain
        return key
    def reviews(authority: Any) -> dict:
        authority._current()
        return {'chain':proof(authority.chain),'enrollments':writer.put(authority.enrollments,typed=True)}
    observations = {}
    for spec in specifications:
        registration = spec.observation_registration()
        key = 'final.observation.' + registry.fingerprint(registration.gate)
        require(key not in observations, 'Duplicate original observation specification')
        row = owner.enrollments.get(key)
        require(row is not None and row.phase == 'complete' and row.owner is not None
            and row.registration == registration and row.post_checkpoint is not None,
            'Unexecuted or unknown slot cannot be packaged as a physical observation')
        assert row is not None and row.post_checkpoint is not None
        require(row.owner.checkpoint() == row.post_checkpoint, 'Original physical prefix changed before export')
        inputs = {name:getattr(spec,name) for name in ('kind','root','delta_root','cleanup_root','registration',
            'policy','recipe','profile','cumulative_profile','endpoint','layout_plan')}
        inputs['store'] = spec.store.path.resolve()
        # Bind the actual endpoint selected by the original owner even if the
        # launcher used None as an environment-default construction shorthand.
        inputs['endpoint'] = None if spec.kind == 'workflow_inspection' else row.owner.endpoint
        layout = None
        if spec.layout_authority is not None:
            spec.layout_authority.authenticate(spec.layout_plan)
            layout = {'chain':proof(spec.layout_authority.journal),
                'enrollment':writer.put(spec.layout_authority.enrollment,typed=True)}
        observations[key] = {'inputs':writer.put(inputs,typed=True),'layout_review':layout,
            'proof':{'raw':str(spec.root),'delta':str(spec.delta_root),'head':str(spec.checkpoint_authority.root),
                'expected':asdict(row.post_checkpoint)}}
        if spec.kind == 'workflow_inspection':
            from . import candidate_workflow_review_v1 as workflow_review
            delivery = row.owner.product_delivery
            require(type(delivery) is workflow_review.ProductInspectionDelivery,
                    'Exact independently delivered product inspection originals required')
            delivery.authenticate(codec.bounded_decode(row.owner.read_authenticated('review-request.json')))
            observations[key]['product_review'] = {'chain':proof(delivery.journal),
                'expected':asdict(delivery.expected),'enrollment':writer.put(delivery.enrollment,typed=True)}
    require(set(observations) == set(owner.enrollments), 'Original physical slot census is incomplete')
    exposure = shared.workflow.contract_fields(owner.plan)
    if exposure:
        exposure = {**shared.projection.contract_fields(owner.plan),**exposure}
    packet = {**exposure,'protocol':originals.PROTOCOL,'repository':str(owner.repository),'ledger_identity':owner.ledger_identity,
        'proofs':proofs,'study':proof(owner.study_chain),'final':proof(owner.chain),
        'scope':{'chain':proof(owner.scope_owner.chain),'enrollments':writer.put(owner.scope_owner.enrollments,typed=True),
            'submissions':[writer.put(owner.submissions[subject.trajectory_id],typed=True) for subject in owner.subjects]},
        'admission_reviews':reviews(owner.prerequisite_owner.reviews),
        'promotion_reviews':reviews(owner.promotion_owner.reviews),
        'controls':[{'chain':proof(control.chain),'root':str(control.root),'reviews':reviews(authority)}
            for control,authority in owner.control_owners],
        'harness_repository':str(owner.prerequisite_owner.store.path.resolve()),'observations':observations}
    owner._current_originals()
    for key,chain in chains.items():
        require(proof_descriptor(chain,contexts[key]) == proofs[key], 'Protected proof changed during export')
    return writer.put(packet)
