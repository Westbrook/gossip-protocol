"""Process-scoped qualification issued before deadlines, claims and reserves.

An immutable external-head-anchored record attests a complete cold audit at
issuance. Entry checks authenticate that record, all original current heads,
input/source bytes, actual child design and its approved wallet. They do not
regrade historical candidate outputs. Recovery cannot deserialize a capability:
a new process must issue a new proof from a complete cold audit.

Revocation advances the grant head. An entry check linearizes at its final head
read; already-entered provider calls settle normally. Protected host writers must
obey the journal protocol and preserve immutable originals. A malicious host
rolling back both independent expectations and originals is outside the model.
Out-of-protocol edits to old raw bytes with an unchanged head are not continuously
regraded; the next full issuance audit detects them. This is explicit issuance
attestation, not a mutable audit cache or live-candidate acceptance reuse.
"""
from __future__ import annotations
from contextlib import ExitStack
from dataclasses import asdict, dataclass
import os
from pathlib import Path
import stat
import threading
from typing import Any
from . import candidate_checkpoint_chain_v1 as checkpoint
from . import candidate_checkpoint_head_v1 as head_module
from .candidate_checkpoint_head_v1 import ExternalHead
from . import candidate_http_journal_v3 as stable
from . import candidate_observation_admission_v1 as admission
from . import cumulative_rehearsal_capsule_v3 as capsule
from . import cumulative_rehearsal_codec_v1 as codec
from . import cumulative_final_originals_v1 as originals
from . import peer_financial_qualification_v5_final_v3 as previous
from .cumulative_study_controller_v2 import StudyPlan, digest
from .peer_financial_terminal_v1 import require, sha

PROTOCOL='cumulative-issued-qualification-v1'
GRANT_NAME='issued-qualification.json'
SOURCES=('gossip_harness/cumulative_issued_qualification_v1.py',
    'gossip_harness/cumulative_rehearsal_capsule_v3.py',
    'gossip_harness/cumulative_qualified_study_v1.py')
POLICY={'protocol':PROTOCOL,'candidate_acceptance_transfer':False,
    'attestation':'complete matching fixture originals at issuance',
    'fresh_entry':'grant and all original heads, inputs, sources, runtime/profile, permit, SQL/reservation/lease',
    'recovery':'new complete cold audit before any new lease',
    'revocation':'blocks entries after final current-head check; in-flight provider calls settle',
    'wallet_policy':capsule.WALLET_POLICY}
MAX_WITNESSES=3*originals.MAX_OBSERVATIONS+1024
_TOKEN=object()


def implementation_sources() -> dict[str,str]:
    root=Path(__file__).resolve().parents[1]
    result=previous.implementation_sources()
    result.update({name:sha((root/name).read_bytes()) for name in SOURCES})
    return result


@dataclass(frozen=True)
class FileWitness:
    path: str
    sha256: str
    size: int
    device: int
    inode: int
    head_lock: tuple[int,int,int,int] | None = None

    @classmethod
    def capture(cls, path: Path, expected: bytes | None = None, *, head: bool = False) -> FileWitness:
        path=originals.path(str(path))
        lock=None
        if head:
            require(path.name == 'head.json' and set(os.listdir(path.parent)) == {'head.json','head.lock'},
                    'Original head contains an uncertain or foreign publication')
            item=(path.parent/'head.lock').stat(follow_symlinks=False)
            require(stat.S_ISREG(item.st_mode) and item.st_nlink == 1 and item.st_size == 0
                and item.st_uid == os.geteuid() and stat.S_IMODE(item.st_mode) == 0o600,
                'Original head ownership lock changed')
            lock=(item.st_dev,item.st_ino,item.st_mode,item.st_uid)
        before=path.stat(follow_symlinks=False)
        raw=stable.read(path)
        after=path.stat(follow_symlinks=False)
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
            and (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)
                == (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)
            and (expected is None or raw == expected), 'Protected original changed while witnessing')
        if head:
            item=(path.parent/'head.lock').stat(follow_symlinks=False)
            require(set(os.listdir(path.parent)) == {'head.json','head.lock'}
                and lock == (item.st_dev,item.st_ino,item.st_mode,item.st_uid),
                'Original head ownership changed during read')
        return cls(str(path),sha(raw),len(raw),before.st_dev,before.st_ino,lock)

    def current(self) -> None:
        value=type(self).capture(Path(self.path),head=self.head_lock is not None)
        require(value == self, 'Original input, head or issued proof changed/revoked')


def _head_witness(proof: dict) -> FileWitness:
    root=originals.path(proof['head'])
    expected=checkpoint.PrefixCommitment(**proof['expected'])
    wire=head_module._encoded({'protocol':head_module.PROTOCOL,
        'journal_roots':[proof['raw'],proof['delta']],'commitment':asdict(expected)})
    return FileWitness.capture(root/'head.json',wire,head=True)


def _inputs_and_heads(reference: dict) -> tuple[FileWitness,...]:
    value=capsule.bound(reference)
    packet=capsule.bound(value['final_packet'])
    witnesses:dict[str,FileWitness]={}
    def put(row:FileWitness) -> None:
        require(row.path not in witnesses or witnesses[row.path] == row, 'Aliased original witness')
        witnesses[row.path]=row
        require(len(witnesses) <= MAX_WITNESSES,'Original witness count exceeded')
    def refs(obj:Any) -> None:
        if type(obj) is dict:
            if set(obj) == {'path','sha256'}:
                record=FileWitness.capture(originals.path(obj['path']))
                require(record.sha256 == obj['sha256'],'Pinned original input changed')
                put(record)
            else:
                for child in obj.values(): refs(child)
        elif type(obj) in (tuple,list):
            for child in obj: refs(child)
    refs(reference); refs(value); refs(packet)
    for proof in packet['proofs'].values(): put(_head_witness(proof))
    for row in packet['observations'].values(): put(_head_witness(row['proof']))
    put(_head_witness(value['study_proof']))
    return tuple(witnesses[key] for key in sorted(witnesses))


class IssuedQualification:
    """Closed process capability; caller dictionaries cannot mint it."""
    def __init__(self, token: object, *, record: bytes, witness: tuple[FileWitness,...],
                 journal: dict, repository: Path, checkpoint: checkpoint.PrefixCommitment):
        require(token is _TOKEN, 'Use complete cold issuance; no grant deserialization')
        self._record=record; self._record_sha256=sha(record)
        self._witness=witness; self._journal=journal; self._repository=repository
        self._expected=checkpoint; self._pid=os.getpid(); self._thread=threading.get_ident()
        self._lock=threading.RLock(); self._revoked=False; self._retired=False
        self._owners:set[str]=set()
        self._integrity=digest({'record':self._record_sha256,'witness':[asdict(row) for row in witness],
            'journal':journal,'repository':str(repository),'expected':asdict(checkpoint),'pid':self._pid})

    @classmethod
    def issue(cls, reference: dict, *, root: Path, delta_root: Path, authority: ExternalHead,
              live_plan: StudyPlan, repository: Path) -> IssuedQualification:
        require(type(live_plan) is StudyPlan and type(authority) is ExternalHead,
                'Exact prospective live study and independent grant authority required')
        require(live_plan.runtime.get('live_qualification_protocol') == PROTOCOL
            and live_plan.runtime.get('qualified_study_protocol') == 'cumulative-qualified-study-v1',
            'Issued qualification lifecycle was not prospectively pinned')
        repository=originals.path(str(repository)); root=originals.path(str(root)); delta_root=originals.path(str(delta_root))
        current=implementation_sources()
        require(all(live_plan.source_pins.get(name) == pin for name,pin in current.items()),
                'Complete issued qualification source closure missing')
        live_plan.verify_sources(repository); admission.verify_loaded_sources(current)
        require(authority.journal_roots == (root,delta_root) and authority.read() is None,
                'Fresh separately anchored grant journal required')
        original_witnesses=_inputs_and_heads(reference)
        protected=tuple(Path(row.path) for row in original_witnesses)
        require(all(not candidate.is_relative_to(old.parent) and not old.is_relative_to(candidate)
            for candidate in (root,delta_root,authority.root) for old in protected),
            'Grant roots overlap original proof/input roots')
        context={'protocol':PROTOCOL,'policy':POLICY,'capsule':reference,
            'live_study_sha256':live_plan.sha256,'sources_sha256':digest(live_plan.source_pins)}
        with ExitStack() as stack:
            chain=checkpoint.CheckpointChain.create(root,delta_root,context=context,authority=authority)
            stack.callback(chain.close)
            # This intent is before the complete audit and any issue. A lost
            # acknowledgement leaves no capability and is never healed/reused.
            chain.retain('issuance-intent.json',codec.encoded(context))
            facts=capsule.audit_closed_capsule(reference,live_plan=live_plan,repository=repository)
            require(facts['live_study_sha256'] == live_plan.sha256
                and facts['sources'] == live_plan.source_pins
                and facts['children'] == [{'cohort':row.cohort,'trajectory':row.trajectory} for row in live_plan.roster.children]
                and len(facts['execution_designs']) == 6,
                'Cold proof does not cover the exact complete live study')
            for row in original_witnesses: row.current()
            require(implementation_sources() == current,'Issuer source changed during cold audit')
            live_plan.verify_sources(repository); admission.verify_loaded_sources(current)
            raw=codec.encoded({'protocol':PROTOCOL,'policy':POLICY,'context':context,
                'originals':facts,'witnesses':[asdict(row) for row in original_witnesses],
                'issuer_pid':os.getpid(),'candidate_acceptance_authority':False})
            chain.retain(GRANT_NAME,raw)
            expected=chain.validate_boundary().commitment
            for row in original_witnesses: row.current()
            journal={'raw':str(root),'delta':str(delta_root),'head':str(authority.root),'context':context,
                'limits':asdict(chain.limits),'expected':asdict(expected)}
            grant=FileWitness.capture(root/GRANT_NAME,raw)
            anchor=_head_witness(journal)
            chain.validate_boundary(expected=expected)
        authority.close()
        return cls(_TOKEN,record=raw,witness=(*original_witnesses,grant,anchor),
            journal=journal,repository=repository,checkpoint=expected)

    @property
    def proof_reference(self) -> dict:
        return {'protocol':PROTOCOL,'path':str(Path(self._journal['raw'])/GRANT_NAME),
            'sha256':self._record_sha256,'context_sha256':self._expected.context_sha256,
            'head_sha256':self._expected.head_sha256}

    def reference_for(self, child: dict) -> dict:
        with self._lock:
            facts=IssuedQualification._current(self)
            require(child in facts['children'],'Grant does not cover that prospective child')
            return {'protocol':PROTOCOL,'capsule':facts['capsule'],'approved_child':dict(child)}

    def _current(self) -> dict:
        try:
            return IssuedQualification._current_checked(self)
        except BaseException:
            self._revoked=True
            raise

    def _current_checked(self) -> dict:
        require(os.getpid() == self._pid and not self._revoked, 'Grant revoked, closed or inherited by another process')
        require(sha(self._record) == self._record_sha256 and self._integrity == digest({
            'record':self._record_sha256,'witness':[asdict(row) for row in self._witness],
            'journal':self._journal,'repository':str(self._repository),
            'expected':asdict(self._expected),'pid':self._pid}), 'Issued capability state changed')
        for row in self._witness: row.current()
        record=codec.bounded_decode(self._record)
        require(record['protocol'] == PROTOCOL and record['policy'] == POLICY,'Issued policy changed')
        sources=record['originals']['sources']
        require(all(sources.get(name) == pin for name,pin in implementation_sources().items()),'Current issuer/runtime source changed')
        for name,pin in sources.items():
            require(sha(stable.read(self._repository/name)) == pin,'Prospective study source changed')
        admission.verify_loaded_sources(sources)
        return record['originals']

    def check_study(self, plan: StudyPlan, *, repository: Path, ledger_identity: dict) -> dict:
        """Non-consuming fresh check before controller/child initialization."""
        require(type(self) is IssuedQualification and all(hasattr(self,name) for name in
            ('_lock','_record','_integrity','_witness','_journal','_expected','_pid','_revoked')),
            'Actual completed issued capability required before study initialization')
        require(type(plan) is StudyPlan, 'Exact prospective study required')
        plan.__post_init__()
        with self._lock:
            facts=IssuedQualification._current(self)
            require(repository == self._repository and plan.sha256 == facts['live_study_sha256']
                and plan.source_pins == facts['sources'] and ledger_identity == facts['wallet_authorization']['ledger_identity'],
                'Issued qualification belongs to a different study, source, repository or wallet')
            from .peer_financial_authority_v2 import ledger_identity as actual_ledger_identity
            require(actual_ledger_identity(Path(ledger_identity['path'])) == ledger_identity,
                    'Original live wallet changed before study initialization')
            self._witness[-1].current()
            return {'protocol':PROTOCOL,'issued_proof':self.proof_reference,
                    'live_study_sha256':facts['live_study_sha256'],'candidate_acceptance_authority':False}

    def check(self, reference: dict, execution_design: dict, sources: dict, *, live_limits: dict,
              ledger_identity: dict) -> dict:
        # Short checks can serialize; the lock is never held across provider I/O.
        with self._lock:
            facts=IssuedQualification._current(self)
            originals.closed(reference,'protocol capsule approved_child')
            require(reference['protocol'] == PROTOCOL and reference['capsule'] == facts['capsule'],
                    'Permit does not name this independently audited capsule policy')
            children=facts['children']
            require(reference['approved_child'] in children,'Permit child not prospectively covered')
            index=children.index(reference['approved_child'])
            require(digest(execution_design) == digest(facts['execution_designs'][index])
                and sources == facts['sources'], 'Child source/design/suite/runtime/limits/purpose changed')
            wallet=facts['wallet_authorization']; limits=wallet['children'][index]
            require(ledger_identity == wallet['ledger_identity'],'Original live wallet identity changed')
            originals.closed(live_limits,'expected_opening_usage expected_global_cap incremental_cap_micro_usd max_workers')
            require(all(type(value) is int for value in live_limits.values())
                and live_limits['expected_global_cap'] == wallet['expected_global_cap']
                and wallet['initial_opening_usage'] <= live_limits['expected_opening_usage']
                and live_limits['incremental_cap_micro_usd'] == limits['incremental_cap_micro_usd']
                and live_limits['max_workers'] == limits['max_workers']
                and 0 < live_limits['incremental_cap_micro_usd'] <= live_limits['expected_global_cap']-live_limits['expected_opening_usage'],
                'Actual approved wallet is outside prospective closed substitution')
            self._witness[-1].current()
            return {'protocol':PROTOCOL,'reference':reference,'issued_proof':self.proof_reference,'audited_at_issuance':True,
                'raw_regraded_at_invocation':False,'candidate_acceptance_authority':False}

    def bind_owner(self, cohort: str, *, journal: checkpoint.CheckpointChain,
                   expected: checkpoint.PrefixCommitment, permit_sha256: str) -> FileWitness:
        """Retain the fresh issuance/owner join before financial open or recovery.

        Each process capability consumes a child at most once. A failed open is
        still consumed; a recovery owner needs a newly audited issuance.
        """
        require(threading.get_ident() == self._thread, 'Owner binding belongs to issuer thread')
        with self._lock:
            facts=IssuedQualification._current(self)
            require(cohort in {row['cohort'] for row in facts['children']} and cohort not in self._owners,
                    'Child already used this issuance; recovery requires a new complete audit')
            require(type(journal) is checkpoint.CheckpointChain, 'Actual independent study journal required')
            journal.validate_boundary(expected=expected)
            self._owners.add(cohort)
            raw=codec.encoded({'protocol':PROTOCOL,'kind':'financial-owner-admission','cohort':cohort,
                'permit_sha256':permit_sha256,'issued_proof':self.proof_reference,
                'live_study_sha256':facts['live_study_sha256']})
            name='qualification-owner-'+sha(raw)+'.json'
            journal.retain(name,raw)
            journal.validate_boundary()
            return FileWitness.capture(journal.raw_root/name,raw)

    def revoke(self, reason: str) -> None:
        require(os.getpid() == self._pid and threading.get_ident() == self._thread,
                'Only issuer owner may revoke')
        require(type(reason) is str and 1 <= len(reason) <= 1024,'Bounded revocation reason required')
        with self._lock:
            if self._retired: return
            self._revoked=True
            self._retired=True
            with ExitStack() as stack:
                root=Path(self._journal['raw']); delta=Path(self._journal['delta'])
                authority=ExternalHead.reopen(Path(self._journal['head']),journal_roots=(root,delta),expected=self._expected)
                stack.callback(authority.close)
                chain=checkpoint.CheckpointChain.reopen(root,delta,context=self._journal['context'],
                    authority=authority,expected=self._expected,limits=checkpoint.Limits(**self._journal['limits']))
                stack.callback(chain.close)
                chain.retain('qualification-revoked.json',codec.encoded({'protocol':PROTOCOL,
                    'issued_reference':self.proof_reference,'reason':reason}),cleanup=True)
                chain.validate_boundary()
