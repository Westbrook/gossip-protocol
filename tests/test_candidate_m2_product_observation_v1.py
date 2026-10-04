"""Actual owner/journal reader controls with explicitly synthetic raw origins.

No candidate or Engine is executed. These fixtures cannot publish acceptance.
"""
from copy import deepcopy
import io
import tarfile
import unittest

from gossip_harness import candidate_m2_product_execution_v1 as execution
from gossip_harness import candidate_m2_product_profile_v1 as profile
from gossip_harness import candidate_m2_product_observation_v1 as observer
from gossip_harness import candidate_observation_admission_v1 as admission
from tests import test_candidate_m2_product_execution_v1 as fixtures


class CandidateM2ProductObservationV1Tests(unittest.TestCase):
    setUpClass = classmethod(fixtures.CandidateM2ProductExecutionV1Tests.setUpClass.__func__)
    setUp = fixtures.CandidateM2ProductExecutionV1Tests.setUp
    make = fixtures.CandidateM2ProductExecutionV1Tests.make
    owner = fixtures.CandidateM2ProductExecutionV1Tests.owner
    intent = fixtures.CandidateM2ProductExecutionV1Tests.intent

    def command(self, owner, label, argv, raw):
        argv = owner.docker + argv[1:]
        owner._retain(label+'-dispatch.json',profile.encoded({'argv':argv,'limit':execution.b02.MAX_CAPTURE_BYTES}))
        owner._retain_blob(label+'-stdout.bin',raw)
        owner._retain(label+'-stderr.bin',b'')
        record={'argv':argv,'arguments':argv,'exit_code':0,'timed_out':False,'capture_complete':True}
        for kind,value in (('stdout',raw),('stderr',b'')):
            record[kind]={'path':label+'-'+kind+'.bin','sha256':execution.sha(value),
                'bytes':len(value),'observed_bytes':len(value),'truncated':False}
        owner._retain(label+'.json',profile.encoded(record))

    def prefix(self):
        owner=self.owner();self.intent(owner)
        container='c'*64
        self.command(owner,'container-create',['docker','create'],container.encode()+b'\n')
        inspection={'Id':container,'Name':'/fixture-container','Image':self.policy.image_id,
            'State':{'Running':True,'Paused':True,'Pid':123,'StartedAt':'fixture-start'},
            'Config':{'Labels':{'gossip.execution':'fixture-execution','gossip.source':self.binding.source_sha256,
                'gossip.fixture':self.binding.fixture_sha256}},
            'HostConfig':{'NetworkMode':'none','ReadonlyRootfs':True},
            'Mounts':[{'Destination':'/tmp','Type':'volume','Name':'fixture-volume','Driver':'local','RW':True}]}
        return owner,container,inspection

    def action(self, owner, container, inspection, index, result, *, capture=True, raw_response=None):
        phase=self.value.phases[index]
        proof={'source_manifest':admission.source_manifest(owner.files),
            'helper_manifest':admission.source_manifest(profile.adapter_files(self.value.case_id)),
            'fixtures_sha256':profile.digest(profile.input_fixtures(self.value.case_id))}
        for boundary in ('before','after'):
            owner._retain(phase+'-staging-'+boundary+'.json',profile.encoded(proof))
            owner._retain(phase+'-runtime-'+boundary+'-verified.json',profile.encoded(
                {'runtime':owner.runtime,'runtime_sha256':profile.digest(owner.runtime)}))
        owner._retain(phase+'-request.json',profile.encoded({'phase':phase,'request':phase+'\n'}))
        owner._retain(phase+'-response.json',raw_response if raw_response is not None else
            profile.encoded({'phase':phase,'value':{'action_index':index,'result':result}})+b'\n')
        if not capture: return
        self.command(owner,phase+'-pause',['docker','pause',container],container.encode())
        self.command(owner,phase+'-state',['docker','inspect','--format','{{json .}}',container],profile.encoded(inspection))
        output=io.BytesIO()
        with tarfile.open(fileobj=output,mode='w') as archive:
            for name in ('tmp','tmp/m2'):
                entry=tarfile.TarInfo(name);entry.type=tarfile.DIRTYPE;archive.addfile(entry)
            raw=profile.fixture_files(self.value.case_id)['seed.sqlite']
            entry=tarfile.TarInfo('tmp/m2/library.sqlite');entry.size=len(raw);archive.addfile(entry,io.BytesIO(raw))
        self.command(owner,phase+'-capture',['docker','cp',container+':/tmp','-'],output.getvalue())
        self.command(owner,phase+'-unpause',['docker','unpause',container],container.encode())

    def test_original_prefix_keeps_known_failed_action_and_complete_missing_tail(self):
        owner,container,inspection=self.prefix()
        self.action(owner,container,inspection,0,{'wrong':'known product result'})
        result=observer.reconstruct(owner)
        self.assertEqual(result['projection']['observations'][0]['disposition'],'fail')
        self.assertEqual(result['projection']['observations'][1]['disposition'],'unavailable')
        self.assertEqual(len(result['projection']['diagnostics']),18)
        self.assertTrue(result['phase_facts'][0]['capture_authenticated'])
        self.assertIsNone(result['original_terminal_sha256'])
        self.assertEqual(result['mechanics']['status'],'infrastructure_error')

    def test_later_wrong_fixture_censors_only_affected_action(self):
        owner,container,inspection=self.prefix()
        self.action(owner,container,inspection,0,{'wrong':'earlier'})
        later=deepcopy(inspection);later['Config']['Labels']['gossip.fixture']='d'*64
        self.action(owner,container,later,1,{'wrong':'not attributable'})
        result=observer.reconstruct(owner)
        self.assertEqual(result['projection']['observations'][0]['disposition'],'fail')
        self.assertEqual(result['projection']['observations'][1]['disposition'],'unavailable')
        self.assertIn('lineage',result['phase_facts'][1]['reason'])

    def test_later_pid_drift_preserves_original_failure_and_blocks_current_phase(self):
        owner,container,inspection=self.prefix()
        self.action(owner,container,inspection,0,{'wrong':'earlier'})
        later=deepcopy(inspection);later['State']['Pid']+=1
        self.action(owner,container,later,1,{'wrong':'not attributable'})
        result=observer.reconstruct(owner)
        self.assertEqual(result['projection']['observations'][0]['disposition'],'fail')
        self.assertEqual(result['projection']['observations'][1]['disposition'],'unavailable')
        self.assertEqual(result['mechanics']['status'],'infrastructure_error')

    def test_missing_capture_does_not_erase_independently_retained_api_failure(self):
        owner,container,inspection=self.prefix()
        self.action(owner,container,inspection,0,{'wrong':'known'},capture=False)
        result=observer.reconstruct(owner)
        self.assertEqual(result['projection']['observations'][0]['disposition'],'fail')
        self.assertFalse(result['phase_facts'][0]['capture_authenticated'])
        self.assertEqual(result['mechanics']['status'],'infrastructure_error')

    def test_fixture_and_uploaded_dict_cannot_publish_physical_original(self):
        owner=self.owner();self.intent(owner)
        with self.assertRaises(observer.AuthorityError): observer.publish_verifier(owner)
        with self.assertRaises(observer.AuthorityError): observer.M2ObservationSource(owner,owner.checkpoint())
        with self.assertRaises(observer.AuthorityError): observer.reconstruct({'passed':True})
        self.assertFalse(owner.has_retained(observer.VERIFIER_FILE))

    def test_stale_review_revokes_reader_and_retained_observation(self):
        owner,container,inspection=self.prefix()
        self.action(owner,container,inspection,0,{'wrong':'known'})
        self.review_journal.retain('late.json',b'{}')
        with self.assertRaises(admission.AdmissionError): observer.reconstruct(owner)

    def test_malformed_later_candidate_line_does_not_erase_earlier_known_failure(self):
        owner,container,inspection=self.prefix()
        self.action(owner,container,inspection,0,{'wrong':'earlier'})
        self.action(owner,container,inspection,1,None,capture=False,raw_response=b'{broken\n')
        result=observer.reconstruct(owner)
        self.assertEqual(result['projection']['observations'][0]['disposition'],'fail')
        self.assertEqual(result['projection']['observations'][1]['disposition'],'unavailable')
        self.assertIn('JSON',result['phase_facts'][1]['reason'])

    def test_malformed_reply_cannot_hide_contradictory_original_session_stream(self):
        owner,container,inspection=self.prefix()
        self.action(owner,container,inspection,0,None,capture=False,raw_response=b'{broken\n')
        raw=b'different-original-line\n'
        owner._retain('session-stdout.bin',raw)
        owner._retain('session.json',profile.encoded({'stdout':{'sha256':execution.sha(raw),'bytes':len(raw)}}))
        with self.assertRaisesRegex(observer.AuthorityError,'session stdout'):
            observer.reconstruct(owner)
