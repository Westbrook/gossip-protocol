"""Prospective real Chromium + owned Engine qualification controls.

These classes execute actual candidate sources. Positive reference fixtures are
not study samples. The negative fixtures perform their defect on the actual
browser/wire path; no prepared artifact is substituted for a failed execution.
"""
from __future__ import annotations
from contextlib import closing
from dataclasses import asdict
import base64
import os
from pathlib import Path
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_product_browser_cases_v1 as cases
from gossip_harness import candidate_product_browser_execution_v1 as execution
from gossip_harness import candidate_product_browser_observation_v1 as observer
from gossip_harness import candidate_checkpoint_head_v1 as head
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
from gossip_harness.library_v2_json_reference_v2 import corrected_v2_files, corrected_v2_binary_files, corrected_v2_source_inputs
from tests.test_candidate_clients_docker_v4 import make_store
from tests.test_library_m2_browser_reference_v1 import _runtime

PROTOCOL = 'candidate-product-browser-physical-controls-v1-git-source-batch-v1'
COHORT = ('fixture-original', *('fixture-reserved-'+str(index) for index in range(1,6)))
VARIANTS = {
    'stale-commit': ('library/clients/index.html', "action === 'commit' ? {epoch: job.epoch} : {}",
                     "action === 'commit' ? {epoch: BigInt(job.epoch) - 1n} : {}"),
    'missing-heading': ('library/clients/index.html', '<h1>Local Research Library</h1>', '<h1>Missing required heading</h1>'),
    'incomplete-page': ('library/clients/http.py',
        "            self.send_header('Content-Length', str(len(raw)))",
        "            self.send_header('Content-Length', str(len(raw) + (1 if content_type.startswith('text/html') else 0)))"),
}
CONTROLS = (
    ('P01','m1-browser-intake-state-literal-reload','reference','complete'),
    ('P02','m1-browser-failed-job-action-matrix','reference','complete'),
    ('P03','m1-browser-high-epoch','reference','complete'),
    ('P04','m1-browser-maximum-epoch','reference','complete'),
    ('D01','m1-browser-high-epoch','stale-commit','known-mutation-discrepancy'),
    ('D02','m1-browser-failed-job-action-matrix','missing-heading','unavailable-dom'),
    ('D03','m1-browser-failed-job-action-matrix','incomplete-page','unavailable-wire'),
)


def candidate_files(variant):
    texts,binaries=corrected_v2_files(),corrected_v2_binary_files()
    if set(texts).intersection(binaries):raise ValueError('Candidate text/binary collision')
    original={name:value.encode() for name,value in texts.items()} | binaries
    result=dict(original); mutation=None
    if variant!='reference':
        name,before,after=VARIANTS[variant]
        raw=result[name].decode()
        if raw.count(before)!=1:raise ValueError('Exact prospective mutation seam changed: '+variant)
        result[name]=raw.replace(before,after,1).encode()
        mutation={'path':name,'before':before,'after':after,'original_sha256':cases.sha(original[name]),'mutated_sha256':cases.sha(result[name])}
    return result,{'variant':variant,'generator_inputs':corrected_v2_source_inputs(),'mutation':mutation,
                   'source_manifest':execution.source_manifest(result),'whole_product_acceptance':False,'scientific_samples':0}


def write_new(path,value):
    raw=cases.encoded(value)
    with path.open('xb') as out:
        out.write(raw);out.flush();os.fsync(out.fileno())
    return raw


def stale_commit_witness(request, observed):
    """Pure comparison only; the caller must authenticate the original probe."""
    raw=base64.b64decode(request['request']['body_b64'],validate=True)
    observer.require(request['source']=='browser' and request['request']['method']=='POST'
        and request['request']['target']=='/api/jobs/browser_counter/commit'
        and cases.sha(raw)==request['request']['body_sha256']
        and observer.exact(observer.product_json(raw),{'epoch':9007199254740994}),
        'Registered stale epoch body was not observed')
    expected=execution.wire.request_bytes(execution.bridge_request(request['request']),cases.PORT)
    observer.require(observed.sent_complete and observed.sent==observed.request==expected,
        'Exact stale body was not completely sent to the owned server')
    return {'body_sha256':cases.sha(raw),'body_b64':request['request']['body_b64'],
            'sent_sha256':cases.sha(observed.sent),'epoch_decimal':'9007199254740994'}


def incomplete_page_witness(request, observed):
    response=observed.response
    lengths=[value for name,value in response.headers if name.lower()=='content-length']
    types=[value for name,value in response.headers if name.lower()=='content-type']
    observer.require(request['source']=='browser' and request['request']['method']=='GET'
        and request['request']['target']=='/' and observed.sent_complete and observed.socket_eof
        and not observed.exchange_complete and response.status_code==200 and response.headers_complete
        and response.framing=='content-length' and response.limitation=='incomplete_content_length'
        and len(lengths)==1 and lengths[0].isdigit() and int(lengths[0])==len(response.body)+1
        and len(types)==1 and types[0].startswith('text/html'),
        'Registered one-byte-short HTML page response was not observed')
    expected=execution.wire.request_bytes(execution.bridge_request(request['request']),cases.PORT)
    observer.require(observed.sent==observed.request==expected,'Exact page request differs')
    return {'received_sha256':cases.sha(observed.received),'raw_headers_b64':base64.b64encode(response.raw_headers).decode(),
            'body_bytes':len(response.body),'declared_bytes':int(lengths[0]),'socket_eof':True,
            'limitation':response.limitation}


def missing_heading_witness(action, record, controls):
    observer.require(type(record) is dict and record.get('action_id')==action['id'] and type(record.get('dom')) is dict,
        'Original missing-heading DOM observation unavailable')
    dom=record['dom']
    observer.require(dom.get('current_url')==cases.ORIGIN+'/' and dom.get('context')==1
        and dom.get('heading')=={'count':0,'entries':[]},'Intended absent accessible heading was not observed')
    wrong,missing=observer.dom_findings(action,record,controls.get('/api/jobs'))
    observer.require(not wrong and set(missing)=={
        'heading was not visible within the observation window',
        'Declared action observation window did not establish readiness; no product latency judgment'},
        'Missing-heading witness has unrelated unavailable DOM facts')
    observer.require(observer.api_findings(action,controls)==([],[]),'Complete accompanying API facts unavailable')
    return {'action_id':action['id'],'heading':dom['heading'],'limitations':missing}


class _PhysicalBrowserControl:
    CONTROL_ID=''

    @classmethod
    def setUpClass(cls):
        cls.artifacts=ArtifactDirectory('candidate-product-browser-'+cls.CONTROL_ID.lower(),retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.definition=next(value for value in CONTROLS if value[0]==cls.CONTROL_ID)
        cls.outcome={'control':cls.definition,'status':'not-run','qualification_verified':False,'scientific_samples':0,
                     'whole_product_acceptance':False,'original_execution_id':None}
        cls.addClassCleanup(lambda: write_new(cls.artifacts.root/'completion-census.json',cls.outcome))
        write_new(cls.artifacts.root/'prospective-controls.json',{'protocol':PROTOCOL,'controls':CONTROLS,
            'histories':len(CONTROLS),'source_cases':[case.record for case in cases.all_cases()],
            'purpose':'authored-fixture-observation-qualification','physical_run':True,
            'runtime_pins':{'playwright':'1.62.1','chromium_revision':'1234','image_id':RUNTIME_IMAGE},
            'scope_credit':False,'global_independent_acceptance':False})

    def run_control(self):
        root=self.artifacts.root
        node,modules=_runtime()
        policy=execution.BrowserPolicy(RUNTIME_IMAGE,str(node.resolve()),str(modules.resolve()))
        endpoint=execution.engine.EngineEndpoint.from_environment()
        def retained_runtime(name,raw):
            with (root/name).open('xb') as out:out.write(raw);out.flush();os.fsync(out.fileno())
        runtime=execution.engine.runtime_identity(endpoint,RUNTIME_IMAGE,retain=retained_runtime,label='class-runtime')
        browser_runtime=execution.runtime_identity(policy)
        files,candidate=candidate_files(self.definition[2])
        write_new(root/'candidate-definition.json',candidate)
        store=make_store(root/'candidate.git',files)
        commit=store.head()
        profile=execution.BrowserProfile(cases.case(self.definition[1]),capture_policy=execution.source_capture.BatchCapturePolicy())
        tree,captured=execution.capture_source(store,commit,policy=profile.capture_policy)
        self.assertEqual(captured,files)
        binding=execution.binding_for(files,profile,policy,runtime,browser_runtime)
        subject=registry.Subject('candidate-browser-qualification-v1',COHORT[0],'M4',
            execution.digest({'protocol':PROTOCOL,'control':self.definition,'policy':asdict(policy)}),
            cases.CONTRACT_SHA256,binding.source_sha256)
        prospective=execution.observation_registration_for(binding,profile,policy,subject=subject,
            gate_id='browser-'+self.CONTROL_ID.lower(),commit_oid=commit,tree_oid=tree,repetition_id='physical-batch-v1-1',cohort_trajectory_ids=COHORT)
        registration=execution.BrowserRegistration(binding,commit,tree,'physical-batch-v1-1',prospective)
        original=write_new(root/'prospective-registration.json',{'registration':asdict(prospective),
            'source_registration':asdict(registration),'profile':profile.record(),'policy':asdict(policy),
            'runtime':runtime,'browser_runtime':browser_runtime,'authority_is_synthetic_fixture':True})
        def authenticate():
            admission.require((root/'prospective-registration.json').read_bytes()==original,'Prospective fixture authority changed')
            return prospective
        authority=admission.ObservationAdmission(prospective,verify_registration=authenticate)
        journal,deltas,cleanup=root/'journal',root/'deltas',root/'cleanup'
        with closing(head.ExternalHead.create(root/'head',journal_roots=(journal,deltas))) as anchor, execution.BrowserExecution(
            journal,store,registration,profile,policy,observation_admission=authority,checkpoint_authority=anchor,
            delta_root=deltas,cleanup_root=cleanup,endpoint=endpoint) as owner:
            checkpoint=owner.execute_once()
            terminal=execution.engine.strict_json_loads(owner.read_authenticated('terminal.json'))
            self.outcome.update(original_execution_id=owner.execution_id,planned_actions=terminal['planned_actions'],
                observed_probe_records=len(terminal['request_rows']),
                authenticated_sent_requests=sum(row.get('sent_complete') is True for row in terminal['request_rows']),
                unknown_request_outcomes=terminal['unknown_request_outcomes'],
                attempted_requests=len(terminal['request_intents']), planned_control_requests=terminal['planned_control_requests'],
                confirmed_created_ids=terminal['created_resource_ids'], unconfirmed_creates=terminal['unconfirmed_create_names'],
                browser_requests=sum(row['source']=='browser' for row in terminal['request_rows']),
                control_requests=sum(row['source']=='control' for row in terminal['request_rows']),
                entered_driver_messages=len(terminal['driver_messages']),cleanup_verified=terminal['cleanup_verified'],
                infrastructure=terminal['infrastructure'],status='original-execution-retained')
            write_new(root/'physical-census.json',self.outcome)
            write_new(root/'original-checkpoint.json',asdict(checkpoint))
            self.assertEqual(anchor.read(),checkpoint)
            observed=observer.read_original(owner,checkpoint)
            write_new(root/'original-observation.json',asdict(observed))
            self.outcome.update(status='observed',known_discrepancies=observed.known_discrepancies,unavailable=observed.unavailable)
            self.assertTrue(observed.cleanup_verified)
            expected=self.definition[3]
            if expected=='complete':
                self.assertFalse(observed.known_discrepancies)
                self.assertFalse(observed.unavailable)
                self.assertTrue(all(facet.state=='passed' for facet in observed.facets))
            elif expected=='known-mutation-discrepancy':
                commit_action=next(action for action in profile.case.record['actions'] if action.get('action')=='commit')
                selector=profile.case.identifier+':'+commit_action['id']+':mutation'
                self.assertIn(selector,observed.known_discrepancies)
                reader=observer._Reader(owner,checkpoint)
                selected=[]
                for row in terminal['request_rows']:
                    request=reader.json(row['label']+'-request.json')
                    if row['action_id']==commit_action['id'] and request['source']=='browser' and request['request']['target']=='/api/jobs/browser_counter/commit':
                        original=reader.probe(row,request,owner.server,owner.server_spec)
                        selected.append({'request_label':row['label'],**stale_commit_witness(request,original)})
                self.assertEqual(len(selected),1)
                write_new(root/'specific-negative-witness.json',{'control':'D01','originals':selected})
            else:
                self.assertFalse(observed.known_discrepancies)
                action=profile.case.record['actions'][0]
                prefix=profile.case.identifier+':'+action['id']+':'
                facets={facet.selector:facet for facet in observed.facets}
                # This passed original facet requires both authenticated Chromium
                # launch/source and the reconstructed running candidate server.
                self.assertEqual(facets[prefix+'mutation'].state,'passed')
                self.assertEqual(facets[prefix+'dom'].state,'unavailable')
                reader=observer._Reader(owner,checkpoint)
                pages=[]; controls={}
                for row in terminal['request_rows']:
                    if row['action_id']!=action['id']:continue
                    request=reader.json(row['label']+'-request.json')
                    target=request['request']['target']
                    if request['source']=='browser' and request['request']['method']=='GET' and target=='/':
                        pages.append((row,request,reader.probe(row,request,owner.server,owner.server_spec)))
                    elif request['source']=='control':
                        original=reader.probe(row,request,owner.server,owner.server_spec)
                        execution.bridge_response(original)
                        controls[target]=(original.response.status_code,original.response.body)
                self.assertEqual(len(pages),1)
                row,request,original=pages[0]
                if expected=='unavailable-wire':
                    witness=incomplete_page_witness(request,original)
                else:
                    self.assertEqual(facets[prefix+'api'].state,'passed')
                    execution.bridge_response(original)
                    self.assertIn(b'<h1>Missing required heading</h1>',original.response.body)
                    records=[reader.json('browser-message-'+str(sequence).zfill(5)+'.bin') for sequence in terminal['driver_messages']]
                    records=[record for record in records if record.get('kind')=='observation' and record.get('action_id')==action['id']]
                    self.assertEqual(len(records),1)
                    witness=missing_heading_witness(action,records[0],controls)
                write_new(root/'specific-negative-witness.json',{'control':self.definition[0],'request_label':row['label'],**witness})
            self.assertFalse(observed.acceptance_authority)
            self.assertFalse(observed.independent_purpose_credit)
            self.outcome['qualification_verified']=True
            write_new(root/'qualification-result.json',self.outcome)


_ENABLED=os.environ.get('GOSSIP_RUN_DOCKER_TESTS')=='1'

@unittest.skipUnless(_ENABLED,'Explicit physical Docker/browser lane required')
class CandidateBrowserP01DockerTests(_PhysicalBrowserControl,unittest.TestCase):
    CONTROL_ID='P01'
    def test_intake_literal_reload(self):self.run_control()

@unittest.skipUnless(_ENABLED,'Explicit physical Docker/browser lane required')
class CandidateBrowserP02DockerTests(_PhysicalBrowserControl,unittest.TestCase):
    CONTROL_ID='P02'
    def test_failed_job_matrix(self):self.run_control()

@unittest.skipUnless(_ENABLED,'Explicit physical Docker/browser lane required')
class CandidateBrowserP03DockerTests(_PhysicalBrowserControl,unittest.TestCase):
    CONTROL_ID='P03'
    def test_exact_high_epoch(self):self.run_control()

@unittest.skipUnless(_ENABLED,'Explicit physical Docker/browser lane required')
class CandidateBrowserP04DockerTests(_PhysicalBrowserControl,unittest.TestCase):
    CONTROL_ID='P04'
    def test_exact_maximum_epoch(self):self.run_control()

@unittest.skipUnless(_ENABLED,'Explicit physical Docker/browser lane required')
class CandidateBrowserD01DockerTests(_PhysicalBrowserControl,unittest.TestCase):
    CONTROL_ID='D01'
    def test_actual_stale_browser_token(self):self.run_control()

@unittest.skipUnless(_ENABLED,'Explicit physical Docker/browser lane required')
class CandidateBrowserD02DockerTests(_PhysicalBrowserControl,unittest.TestCase):
    CONTROL_ID='D02'
    def test_actual_missing_heading_unavailable(self):self.run_control()

@unittest.skipUnless(_ENABLED,'Explicit physical Docker/browser lane required')
class CandidateBrowserD03DockerTests(_PhysicalBrowserControl,unittest.TestCase):
    CONTROL_ID='D03'
    def test_actual_incomplete_page_unavailable(self):self.run_control()
