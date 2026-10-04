"""Pure browser definition/bridge/selector controls. No browser or Engine launch."""
from __future__ import annotations
import base64
from dataclasses import replace
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest

from gossip_harness import candidate_product_browser_cases_v1 as cases
from gossip_harness import candidate_product_browser_fixture_v1 as fixture
from gossip_harness import candidate_product_browser_execution_v1 as execution
from gossip_harness import candidate_product_browser_observation_v1 as observer
from gossip_harness import candidate_http_transport_v1 as wire
from gossip_harness import candidate_product_process_observation_v1 as semantic


def message(action, body, target=None):
    expected = observer.expected_mutation(action)
    return {"kind": "request", "source": "browser", "action_id": action["id"], "request": {
        "method": "POST", "target": target or expected[0], "body_b64": base64.b64encode(body).decode(), "body_sha256": cases.sha(body)}}


def fact(text='', enabled=True):
    return {"count": 1, "entries": [{"visible": True, "enabled": enabled, "text": text}]}


def empty():
    return {"count": 0, "entries": []}


def jobs_api(action, jobs=None):
    if jobs is None:
        jobs = action.get('jobs', []) + ([action['job']] if 'job' in action else [])
    return 200, cases.encoded({'jobs': jobs})


def observed_dom(action):
    expected_jobs = action.get('jobs', []) + ([action['job']] if 'job' in action else [])
    jobs = []
    for job in expected_jobs:
        enabled = {'queued': {'prepare','cancel'}, 'running': {'commit','cancel'}, 'failed': {'retry'},
                   'cancelled': {'retry'}, 'completed': set()}[job['state']]
        jobs.append({'job_id': job['job_id'], 'row': fact(f"[{job['job_id']}] {job['completed']}/{job['total']} {job['state']} epoch {job['epoch']}"),
                     'actions': {verb: fact(verb) if verb in enabled else empty() for verb in ('prepare','commit','cancel','retry')}})
    sources = action.get('visible_documents', [pair[0] for pair in action['documents']])
    return {'observation_window_satisfied': True, 'dom': {'heading': fact(), 'status': fact(action.get('error','')),
        'job_list': fact(), 'document_list': fact(), 'job_rows': {'count':len(jobs),'entries':[item['row']['entries'][0] for item in jobs]},
        'controls':{name:fact() for name in ('Source path','Search','Job ID','Local path','Namespace','button:Import local file','button:Search','button:Submit job')},
        'intake':fact(), 'intake_options':[{'tag':'SELECT','values':['zip','directory','json'],'selected':'directory'}], 'jobs':jobs,
        'documents':{source:fact(source) for source in sources}, 'details':fact(action.get('literal','')),
        'document_buttons':{'count':len(sources),'entries':[fact(source)['entries'][0] for source in sources]},
        'literal_transition':({'action_id':action['id'],'source':action['source'],'before_count':0,
            'after_matching_count':0,'new_matching_count':0} if action['op']=='search_open' else None)}}


class CandidateBrowserDefinitionTests(unittest.TestCase):
    def test_four_closed_histories_and_no_old_profile_replacement(self):
        values = cases.all_cases()
        self.assertEqual(len(values), 4)
        self.assertEqual(len({value.identifier for value in values}), 4)
        for value in values:
            with self.assertRaises(ValueError):
                cases.BrowserCase(value.definition + b' ')
            self.assertEqual(value.record['server_argv'], list(cases.SERVER_ARGV))
            self.assertEqual(len({action['id'] for action in value.record['actions']}),len(value.record['actions']))

    def test_counter_bytes_match_independent_public_inventory(self):
        for name in ('high','maximum'):
            raw = fixture.seed(name)
            with tempfile.TemporaryDirectory() as tmp:
                path=Path(tmp)/'initial.sqlite';path.write_bytes(raw)
                with sqlite3.connect('file:'+str(path)+'?mode=ro',uri=True) as db:
                    tables=sorted(row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'"))
                    expected=fixture.inventory(name)
                    self.assertEqual(tables,expected['tables'])
                    for table in tables:
                        self.assertEqual([list(row) for row in db.execute('SELECT * FROM '+table)],expected[table])
                    self.assertEqual(db.execute('PRAGMA integrity_check').fetchall(),[('ok',)])
            self.assertEqual(cases.sha(raw),fixture.inventory(name)['input_sha256'])

    def test_maximum_has_no_retry_or_other_epoch_allocation(self):
        actions=cases.case('m1-browser-maximum-epoch').record['actions']
        self.assertEqual([a['action'] for a in actions if a['op']=='action'],['prepare','cancel','commit'])
        self.assertEqual({a['job']['epoch'] for a in actions if 'job' in a},{9223372036854775807})
        self.assertEqual(next(a for a in actions if a.get('action')=='cancel')['error'],'counter_exhausted')

    def test_high_actions_use_exact_odd_epoch_after_two_allocations(self):
        actions=cases.case('m1-browser-high-epoch').record['actions']
        commit=next(a for a in actions if a.get('action')=='commit')
        self.assertEqual(observer.expected_mutation(commit)[1],{'epoch':9007199254740995})

    def test_seed_setup_is_data_only_and_no_candidate_mount(self):
        case=cases.case('m1-browser-maximum-epoch')
        declaration, argv=execution.setup_argv(case,'a'*64)
        self.assertEqual(argv[:6],['docker','exec','--user=65534:65534','a'*64,'python','-I'])
        self.assertEqual(declaration['seed']['sha256'],fixture.inventory('maximum')['input_sha256'])
        self.assertFalse(declaration['candidate_code_executed'])
        self.assertNotIn('/workspace',' '.join(argv))


class CandidateBrowserObservationTests(unittest.TestCase):
    def test_exact_large_raw_mutation_is_not_number_coerced(self):
        action=next(a for a in cases.case('m1-browser-high-epoch').record['actions'] if a.get('action')=='commit')
        self.assertEqual(observer.mutation_findings(action,[message(action,b'{"epoch":9007199254740995}')]),([],[]))
        for raw in (b'{"epoch":9007199254740996}',b'{"epoch":"9007199254740995"}',b'{"epoch":9.007199254740995e15}'):
            wrong, unknown=observer.mutation_findings(action,[message(action,raw)])
            self.assertTrue(wrong);self.assertFalse(unknown)

    def test_complete_malformed_mutation_is_failure_but_duplicates_are_unknown(self):
        action=cases.case('m1-browser-high-epoch').record['actions'][1]
        wrong,missing=observer.mutation_findings(action,[message(action,b'{')])
        self.assertTrue(wrong);self.assertFalse(missing)
        action=next(a for a in cases.case('m1-browser-high-epoch').record['actions'] if a.get('action')=='commit')
        wrong,missing=observer.mutation_findings(action,[message(action,b'{"epoch":1,"epoch":2}')])
        self.assertFalse(wrong);self.assertTrue(missing)

    def test_wrong_mutation_survives_later_missing_dom_and_api(self):
        action=next(a for a in cases.case('m1-browser-high-epoch').record['actions'] if a.get('action')=='commit')
        self.assertTrue(observer.mutation_findings(action,[message(action,b'{"epoch":1}')])[0])
        self.assertTrue(observer.dom_findings(action,None)[1])
        self.assertTrue(observer.api_findings(action,{})[1])

    def test_complete_wrong_api_and_missing_peer_are_kept_separately(self):
        action=cases.case('m1-browser-high-epoch').record['actions'][0]
        wrong,missing=observer.api_findings(action,{'/api/jobs':(200,b'{"jobs":[]}')})
        self.assertTrue(wrong);self.assertTrue(missing)

    def test_complete_api_invalid_syntax_is_failure(self):
        action=cases.all_cases()[0].record['actions'][0]
        wrong,missing=observer.api_findings(action,{'/api/jobs':(200,b'{'),'/api/documents':(200,b'{"documents":[],"total":0}')})
        self.assertTrue(wrong);self.assertFalse(missing)

    def test_observer_allocation_bound_is_unknown(self):
        raw=b'{"padding":"'+b'a'*(16*1024*1024)+b'"}'
        with self.assertRaises(semantic.ObservationLimit):observer.product_json(raw)

    def test_flexible_job_text_and_disabled_invalid_action_are_accepted(self):
        action=cases.case('m1-browser-high-epoch').record['actions'][1]
        value=observed_dom(action)
        value['dom']['jobs'][0]['row']=fact('epoch 9007199254740994; 0/0 — cancelled [browser_counter]')
        value['dom']['job_rows']['entries'][0]=value['dom']['jobs'][0]['row']['entries'][0]
        value['dom']['jobs'][0]['actions']['commit']=fact('commit browser_counter',enabled=False)
        self.assertEqual(observer.dom_findings(action,value,jobs_api(action)),([],[]))

    def test_rounded_visible_epoch_is_known_wrong_despite_timeout(self):
        action=cases.case('m1-browser-high-epoch').record['actions'][0]
        value=observed_dom(action);value['observation_window_satisfied']=False
        value['dom']['jobs'][0]['row']=fact('browser_counter queued epoch 9007199254740992 0/0')
        value['dom']['job_rows']['entries'][0]=value['dom']['jobs'][0]['row']['entries'][0]
        wrong,missing=observer.dom_findings(action,value,jobs_api(action))
        self.assertIn('Displayed current epoch is not the exact public integer',wrong);self.assertTrue(missing)

    def test_missing_control_does_not_invent_latency_failure(self):
        action=cases.all_cases()[0].record['actions'][0]
        value=observed_dom(action);value['dom']['heading']=empty();value['observation_window_satisfied']=False
        wrong,missing=observer.dom_findings(action,value,jobs_api(action))
        self.assertFalse(wrong);self.assertTrue(missing)

    def test_positive_html_execution_survives_missing_readiness(self):
        action=next(a for a in cases.all_cases()[0].record['actions'] if a['op']=='search_open')
        value=observed_dom(action);value['observation_window_satisfied']=False
        value['dom']['literal_transition'].update(after_matching_count=1,new_matching_count=1)
        wrong,missing=observer.dom_findings(action,value,jobs_api(action))
        self.assertTrue(wrong);self.assertTrue(missing)

    def test_invalid_enabled_action_is_wrong(self):
        action=cases.case('m1-browser-high-epoch').record['actions'][1]
        value=observed_dom(action);value['dom']['jobs'][0]['actions']['commit']=fact('commit browser_counter')
        self.assertTrue(observer.dom_findings(action,value,jobs_api(action))[0])

    def test_complete_job_inventory_rejects_extra_and_duplicate_rows(self):
        action=cases.case('m1-browser-high-epoch').record['actions'][0]
        for extra in (fact('unregistered queued epoch 1 0/0'), fact('browser_counter queued epoch 9007199254740993 0/0')):
            value=observed_dom(action)
            value['dom']['job_rows']['entries'].extend(extra['entries'])
            value['dom']['job_rows']['count']+=1
            wrong,missing=observer.dom_findings(action,value,jobs_api(action))
            self.assertIn('Visible job rows exceed the complete persisted job inventory',wrong)
            self.assertIn('Expected job row identity unavailable: browser_counter',missing)

    def test_job_census_uses_full_inventory_and_allows_flexible_row_text(self):
        action=cases.case('m1-browser-high-epoch').record['actions'][1]
        value=observed_dom(action)
        earlier=dict(action['job'],job_id='earlier',epoch=1,state='completed')
        value['dom']['job_rows']['entries'].append(fact('earlier. completed epoch 1 0/0; see browser_counter')['entries'][0])
        value['dom']['job_rows']['count']=2
        # Real hasText per-ID locator includes the incidental mention too.
        value['dom']['jobs'][0]['row']={'count':2,'entries':list(value['dom']['job_rows']['entries'])}
        inventory=jobs_api(action,[earlier,action['job']])
        self.assertEqual(observer.dom_findings(action,value,inventory),([],[]))
        value['dom']['job_rows']['entries'][1]=dict(value['dom']['job_rows']['entries'][0])
        self.assertIn('Visible job rows duplicate or contradict the complete persisted job identities',
                      observer.dom_findings(action,value,inventory)[0])
        wrong,missing=observer.dom_findings(action,value,'missing wire')
        self.assertFalse(wrong);self.assertTrue(missing)
        value['dom']['job_rows']['entries']=[fact('earlier browser_counter '+text)['entries'][0]
            for text in ('cancelled epoch 9007199254740994 0/0','completed epoch 1 0/0')]
        wrong,missing=observer.dom_findings(action,value,inventory)
        self.assertFalse(wrong);self.assertIn('Job row identity association is ambiguous',missing)

    def test_unrelated_or_preexisting_image_is_not_imported_html_evidence(self):
        action=cases.all_cases()[0].record['actions'][0]
        value=observed_dom(action);value['dom'].update(injected=True,injected_image_count=1)
        self.assertEqual(observer.dom_findings(action,value,jobs_api(action)),([],[]))
        action=next(a for a in cases.all_cases()[0].record['actions'] if a['op']=='search_open')
        value=observed_dom(action)
        value['dom']['literal_transition'].update(before_count=1,after_matching_count=1,new_matching_count=0)
        value['dom'].update(injected=True,injected_image_count=1)
        self.assertEqual(observer.dom_findings(action,value,jobs_api(action)),([],[]))
        value['dom']['literal_transition']=None
        wrong,missing=observer.dom_findings(action,value,jobs_api(action))
        self.assertFalse(wrong);self.assertIn('Selected literal document image provenance unavailable',missing)

    def test_auxiliary_documents_button_is_legal_but_duplicate_result_is_not(self):
        action=next(a for a in cases.all_cases()[0].record['actions'] if a['documents'])
        value=observed_dom(action)
        value['dom']['document_buttons']['entries'].append(fact('Export')['entries'][0])
        value['dom']['document_buttons']['count']+=1
        self.assertEqual(observer.dom_findings(action,value,jobs_api(action)),([],[]))
        first=next(iter(value['dom']['documents'].values()))
        first['entries'].append(dict(first['entries'][0]));first['count']+=1
        self.assertTrue(observer.dom_findings(action,value,jobs_api(action))[0])

    def test_json_default_can_hide_namespace_but_directory_requires_it(self):
        action=cases.all_cases()[0].record['actions'][0]
        value=observed_dom(action);value['dom']['controls']['Namespace']=empty()
        value['dom']['intake_options'][0]['selected']='json'
        self.assertEqual(observer.dom_findings(action,value,jobs_api(action)),([],[]))
        for kind in ('directory','zip'):
            value['dom']['intake_options'][0]['selected']=kind
            wrong,missing=observer.dom_findings(action,value,jobs_api(action))
            self.assertFalse(wrong);self.assertIn('Namespace was not visible within the observation window',missing)

    def test_supplied_record_has_no_original_authority(self):
        with self.assertRaises(ValueError):observer.read_original({},None)

    def test_missing_heading_control_requires_specific_dom_and_complete_peer_facts(self):
        from tests.test_candidate_product_browser_physical_v1 import missing_heading_witness
        action=cases.case('m1-browser-failed-job-action-matrix').record['actions'][0]
        value=observed_dom(action)
        value.update(action_id=action['id'],observation_window_satisfied=False)
        value['dom'].update(heading=empty(),current_url=cases.ORIGIN+'/',context=1)
        controls={'/api/jobs':jobs_api(action),'/api/documents':(200,b'{"documents":[],"total":0}')}
        self.assertEqual(missing_heading_witness(action,value,controls)['action_id'],action['id'])
        for variant in (None, {'action_id':action['id'],'dom':None}):
            with self.assertRaises(ValueError):missing_heading_witness(action,variant,controls)
        with self.assertRaises(ValueError):missing_heading_witness(action,value,{})
        value['dom']['document_list']=empty()
        with self.assertRaises(ValueError):missing_heading_witness(action,value,controls)


class CandidateBrowserBridgeTests(unittest.TestCase):
    def request(self,body=b'{"epoch":9223372036854775807}'):
        return {'url':cases.ORIGIN+'/api/jobs/id/commit','method':'POST','target':'/api/jobs/id/commit',
            'headers':[['content-type','application/json']], 'body_b64':base64.b64encode(body).decode(),'body_sha256':cases.sha(body)}

    def test_broker_preserves_maximum_body_bytes_and_exact_framing(self):
        value=self.request();recipe=execution.bridge_request(value)
        raw=wire.request_bytes(recipe,cases.PORT)
        self.assertTrue(raw.endswith(b'\r\n\r\n{"epoch":9223372036854775807}'))
        self.assertIn(b'Host: 127.0.0.1:8765\r\n',raw)

    def test_broker_rejects_origin_credentials_duplicate_headers_and_raw_substitution(self):
        original=self.request()
        variants=[dict(original,url='http://example.invalid/api/jobs/id/commit'),
            dict(original,headers=[['Cookie','secret']]), dict(original,headers=[['content-type','application/json'],['Content-Type','application/json']]),
            dict(original,body_sha256='0'*64)]
        for value in variants:
            with self.assertRaises(ValueError):execution.bridge_request(value)

    def observation(self,raw,eof=False):
        response=wire.parse_response(raw,'GET',eof=eof)
        listener=wire.ListenerSnapshot(b'',b'',(),False,('pure helper only',))
        return wire.WireObservation(b'GET / HTTP/1.1\r\n\r\n',b'GET / HTTP/1.1\r\n\r\n',raw,response,True,eof,
            'eof' if eof else 'message_complete',response.framing_complete and response.body_complete,(),True,'connected_pre_request',listener,listener)

    def test_lawful_framing_alternatives_have_identical_fulfillment(self):
        raws=[(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}',False),
              (b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n',False),
              (b'HTTP/1.1 200 OK\r\n\r\n{}',True)]
        values=[execution.bridge_response(self.observation(raw,eof)) for raw,eof in raws]
        self.assertEqual(values,[values[0]]*3)

    def test_incomplete_and_ambiguous_wire_are_unavailable(self):
        for raw in (b'HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n{}',
                    b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\n{}'):
            with self.assertRaises(ValueError):execution.bridge_response(self.observation(raw,True))

    def test_unrepresentable_header_semantics_are_unavailable(self):
        for header in (b'Set-Cookie: a=b',b'Content-Encoding: gzip',b'Location: /next',b'X-Other: a\r\nX-Other: b'):
            raw=b'HTTP/1.1 200 OK\r\n'+header+b'\r\nContent-Length: 2\r\n\r\n{}'
            with self.assertRaises(ValueError):execution.bridge_response(self.observation(raw))

    def test_stale_control_requires_exact_body_and_actual_complete_send(self):
        from tests.test_candidate_product_browser_physical_v1 import stale_commit_witness
        request={'source':'browser','request':self.request(b'{"epoch":9007199254740994}')}
        request['request'].update(target='/api/jobs/browser_counter/commit',url=cases.ORIGIN+'/api/jobs/browser_counter/commit')
        raw=wire.request_bytes(execution.bridge_request(request['request']),cases.PORT)
        observed=replace(self.observation(b'HTTP/1.1 409 Conflict\r\nContent-Length: 2\r\n\r\n{}'),request=raw,sent=raw)
        self.assertEqual(stale_commit_witness(request,observed)['epoch_decimal'],'9007199254740994')
        with self.assertRaises(ValueError):stale_commit_witness(request,replace(observed,sent_complete=False))
        for body in (b'{"epoch":9007199254740995}',b'{"epoch":9007199254740992}',b'{"epoch":"9007199254740994"}'):
            changed={'source':'browser','request':dict(request['request'],body_b64=base64.b64encode(body).decode(),body_sha256=cases.sha(body))}
            with self.assertRaises(ValueError):stale_commit_witness(changed,observed)

    def test_incomplete_page_control_requires_exact_one_byte_short_html_exchange(self):
        from tests.test_candidate_product_browser_physical_v1 import incomplete_page_witness
        request={'source':'browser','request':{'url':cases.ORIGIN+'/','method':'GET','target':'/',
                 'headers':[],'body_b64':'','body_sha256':cases.sha(b'')}}
        sent=wire.request_bytes(execution.bridge_request(request['request']),cases.PORT)
        def original(raw,eof=True):
            return replace(self.observation(raw,eof),request=sent,sent=sent)
        observed=original(b'HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 3\r\n\r\nhi')
        self.assertEqual(incomplete_page_witness(request,observed)['declared_bytes'],3)
        for other in (replace(observed,socket_eof=False),replace(observed,sent_complete=False),
                      original(b'HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 4\r\n\r\nhi'),
                      original(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 3\r\n\r\nhi'),
                      original(b'HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 2\r\n\r\nhi')):
            with self.assertRaises(ValueError):incomplete_page_witness(request,other)

    def test_node_data_protocol_roundtrips_raw_counter_without_browser(self):
        from tests.test_library_m2_browser_reference_v1 import _runtime
        node, _ = _runtime()
        script=r'''const assert=require('node:assert/strict');const p=require(process.argv[1]);
const raw=Buffer.from('{"epoch":9223372036854775807}');
const request=p.requestPolicy(p.ORIGIN+'/api/jobs/id/commit','POST',raw,[['Content-Type','application/json']]);
assert.equal(Buffer.from(request.body_b64,'base64').toString(),raw.toString());
const d=new p.LineDecoder(20);assert.deepEqual(d.feed(Buffer.from('{"x":')) ,[]);assert.equal(d.feed(Buffer.from('1}\n'))[0].toString(),'{"x":1}');d.eof();
assert.throws(()=>p.requestPolicy('http://example.invalid/','GET',null,[]));
assert.throws(()=>p.responsePolicy({available:true,status:302,headers:[],body_b64:'',body_sha256:p.sha(Buffer.alloc(0))}));
assert.equal(p.jobIdentityPattern('a-b').test('other_a-b'),'false'==='true');
const response=p.responsePolicy({available:true,status:200,headers:[['__proto__','literal'],['constructor','named']],body_b64:'',body_sha256:p.sha(Buffer.alloc(0))});
assert.equal(Object.getPrototypeOf(response.headers),null);
assert.deepEqual(Object.keys(response.headers),['__proto__','constructor']);
assert.equal(response.headers.__proto__,'literal');assert.equal(response.headers.constructor,'named');
assert.equal(JSON.stringify(response.headers),'{"__proto__":"literal","constructor":"named"}');
'''
        result=subprocess.run([str(node),'-e',script,str(execution.PROTOCOL_SOURCE)],capture_output=True,timeout=15)
        self.assertEqual(result.returncode,0,result.stderr.decode())


class CandidateBrowserExecutionSuccessorTests(unittest.TestCase):
    """Inert admission/owned-pipe controls; no browser, Engine or product credit."""

    def test_legacy_profile_record_is_unchanged_and_batch_is_explicit(self):
        case = cases.all_cases()[0]
        legacy = execution.BrowserProfile(case)
        expected = {'protocol':'candidate-product-browser-profile-v1','definition':case.record,
            'ordered_case_ids':legacy.ordered_case_ids,'original_definition_purpose':cases.ORIGINAL_PURPOSE,
            'whole_product_acceptance':False,'native_browser_network_credit':False,
            'global_independent_acceptance':False,'aggregation':'known-failure-preserved-with-unknown'}
        self.assertEqual(legacy.record(), expected)
        batch = execution.BrowserProfile(case, execution.source_capture.BatchCapturePolicy())
        self.assertEqual(batch.execution_protocol, execution.BATCH_PROTOCOL)
        self.assertEqual(batch.record()['source_capture'], batch.capture_policy.record())
        self.assertNotEqual(batch.sha256, legacy.sha256)
        with self.assertRaises(ValueError):
            execution.BrowserProfile(case, {'timeout_seconds':60})

    def test_missing_wrong_or_undeclared_capture_config_is_rejected(self):
        case = cases.all_cases()[0]
        profile = execution.BrowserProfile(case, execution.source_capture.BatchCapturePolicy())
        valid = {'protocol':profile.execution_protocol,'pipe_diagnostics':execution.pipe_diagnostics_policy(),
                 'source_capture':profile.capture_policy.record()}
        execution.validate_capture_config(profile, valid)
        for altered in ({key:value for key,value in valid.items() if key != 'source_capture'},
                        {**valid,'source_capture':{**valid['source_capture'],'timeout_seconds':61}},
                        {**valid,'protocol':execution.PROTOCOL},
                        {**valid,'pipe_diagnostics':{**valid['pipe_diagnostics'],'dispatch':True}}):
            with self.subTest(altered=altered), self.assertRaises(ValueError):
                execution.validate_capture_config(profile, altered)
        with self.assertRaises(ValueError):
            execution.validate_capture_config(execution.BrowserProfile(case), {**valid,'protocol':execution.PROTOCOL})

    def test_gate_rejects_profile_protocol_mismatch(self):
        from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
        profile = execution.BrowserProfile(cases.all_cases()[0], execution.source_capture.BatchCapturePolicy())
        policy = execution.BrowserPolicy(RUNTIME_IMAGE, '/fixture/node', '/fixture/modules')
        binding = execution.binding_for({'source.txt':b'source'}, profile, policy, {}, {})
        subject = execution.registry.Subject('cohort','trajectory','M4','f'*64,cases.CONTRACT_SHA256,binding.source_sha256)
        arguments = dict(subject=subject,gate_id='browser-unit',commit_oid='a'*40,tree_oid='b'*40,
                         repetition_id='unit',cohort_trajectory_ids=('trajectory','peer-1','peer-2','peer-3','peer-4','peer-5'))
        registered = execution.observation_registration_for(binding, profile, policy, **arguments)
        self.assertEqual(registered.gate.binding.execution_protocol, execution.BATCH_PROTOCOL)
        with self.assertRaises(ValueError):
            execution.observation_registration_for(replace(binding,protocol=execution.PROTOCOL),profile,policy,**arguments)
        with self.assertRaises(ValueError):
            execution.observation_registration_for(binding,execution.BrowserProfile(profile.case),policy,**arguments)

    def test_each_guard_captures_fresh_and_rejects_changed_tree_or_bytes(self):
        from types import SimpleNamespace
        from unittest.mock import Mock, patch
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary)/'runtime'; binary.write_bytes(b'pinned runtime')
            fingerprint = cases.sha(binary.read_bytes())
            for changed in (('new-tree',{'source.txt':b'original'}),('tree',{'source.txt':b'changed'})):
                owner = execution.BrowserExecution.__new__(execution.BrowserExecution)
                owner.checkpoint = Mock(); owner.admission = Mock()
                owner.actual_registration = object(); owner.retained_freeze = object()
                owner.sources = {'unit':'source'}; owner.store = object()
                owner.registration = SimpleNamespace(commit_oid='a'*40)
                owner.profile = execution.BrowserProfile(cases.all_cases()[0],execution.source_capture.BatchCapturePolicy())
                owner.tree, owner.files = 'tree', {'source.txt':b'original'}
                owner.browser_runtime = {'node_executable':str(binary),'browser_executable':str(binary),
                                         'node_sha256':fingerprint,'browser_sha256':fingerprint}
                owner._staging_active = False
                with patch.object(execution,'evaluator_sources',return_value=owner.sources), \
                     patch.object(execution,'_LOADED_SOURCES',owner.sources), \
                     patch.object(execution,'capture_source',side_effect=[(owner.tree,owner.files),changed]) as captured:
                    owner._unchanged()
                    with self.assertRaisesRegex(ValueError,'Registered Git source changed'):
                        owner._unchanged()
                self.assertEqual(captured.call_count,2)
                for call in captured.call_args_list:
                    self.assertEqual(call.args,(owner.store,owner.registration.commit_oid))
                    self.assertIs(call.kwargs['policy'],owner.profile.capture_policy)
                self.assertEqual(owner.admission.check_current.call_count,2)

    def test_broken_stdin_retains_actual_late_bytes_without_dispatch_or_success(self):
        import sys
        import time
        from unittest.mock import Mock, patch
        from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve(); marker=root/'probe-observed'
            owner=execution.BrowserExecution.__new__(execution.BrowserExecution)
            owner.staging=root; owner.profile=execution.BrowserProfile(cases.all_cases()[0])
            owner.policy=execution.BrowserPolicy(RUNTIME_IMAGE,str(Path(sys.executable).resolve()),str(root))
            owner.browser_runtime={'browser_version':'authored inert fixture'}
            owner._work_deadline=time.monotonic()+10;owner._history_deadline=time.monotonic()+25;owner._cleanup_mode=False
            owner.driver_messages=[];owner.request_intents=[]
            retained={}
            def retain(name,raw):
                self.assertNotIn(name,retained);retained[name]=raw
            owner._retain=retain
            def probe(message):
                marker.write_bytes(b'1')
                return {'protocol':execution.IPC_PROTOCOL,'kind':'response','request_id':message['request_id'],'available':False}
            owner._probe=Mock(side_effect=probe)
            rows=[{'kind':'runtime','runtime':owner.browser_runtime,'driver_sha256':cases.sha(execution.DRIVER.read_bytes())},
                  {'kind':'launched','browser_version':owner.browser_runtime['browser_version'],'browser_launches':1},
                  {'kind':'action_start','action_id':'a000'},
                  {'kind':'request','action_id':'a000','request_id':1,'source':'browser','request':{}}]
            rows=[{'protocol':execution.IPC_PROTOCOL,'seq':index+1,**row} for index,row in enumerate(rows)]
            tail=[{'protocol':execution.IPC_PROTOCOL,'seq':5,'kind':'terminal','cleanup':{'browser':'complete'}},
                  {'protocol':execution.IPC_PROTOCOL,'seq':6,'kind':'request','request_id':2,'source':'browser','action_id':'a000'}]
            # This is an actual owned Python pipe child, deliberately NOT a
            # Chromium stand-in or an authenticated product execution.
            script="""import os,sys,time,pathlib,json
os.close(0)
for row in json.loads(sys.argv[1]): print(json.dumps(row),flush=True)
end=time.monotonic()+5
while not pathlib.Path(sys.argv[3]).exists():
 if time.monotonic()>end: raise RuntimeError('unit synchronization expired')
 time.sleep(.001)
for row in json.loads(sys.argv[2]): print(json.dumps(row),flush=True)
sys.stderr.write('original late diagnostic\\n');sys.stderr.flush()
"""
            real_popen=subprocess.Popen; children=[]
            def launch(argv,**kwargs):
                child=real_popen([sys.executable,'-I','-c',script,json.dumps(rows),json.dumps(tail),str(marker)],**kwargs)
                children.append(child);return child
            with patch.object(execution.subprocess,'Popen',side_effect=launch):
                result=owner._browser()
            tail_bytes=retained.get('browser-incomplete-stdout.bin',b'')+retained['browser-tail-stdout.bin']
            self.assertEqual([json.loads(line) for line in tail_bytes.splitlines()],tail)
            self.assertIn(b'original late diagnostic',retained['browser-tail-stderr.bin']+retained['browser-stderr.bin'])
            self.assertEqual(owner._probe.call_count,1)
            self.assertEqual(result['messages'],4);self.assertEqual(result['requests'],1)
            self.assertEqual(result['started_actions'],1);self.assertIsNone(result['terminal'])
            self.assertFalse(result['pipe_complete']);self.assertFalse(result['browser_close_acknowledged'])
            self.assertTrue(any(error.startswith('BrokenPipeError:') for error in result['errors']))
            self.assertEqual(result['diagnostic_tail']['request_dispatches'],0)
            self.assertTrue(result['diagnostic_tail']['ordinary_admission_ended'])
            self.assertEqual(json.loads(retained['browser-process-completion.json']),{key:value for key,value in result.items() if key!='artifacts'})
            self.assertIsNotNone(children[0].poll())
            self.assertTrue(all(pipe.closed for pipe in (children[0].stdin,children[0].stdout,children[0].stderr)))

    def test_diagnostic_drain_bounds_bytes_and_expired_time(self):
        import os
        import selectors
        import time
        for limit,deadline,expected in ((3,time.monotonic()+5,b'abc'),(8,time.monotonic()-1,b'')):
            read_fd,write_fd=os.pipe();selector=selectors.DefaultSelector()
            try:
                os.set_blocking(read_fd,False);selector.register(read_fd,selectors.EVENT_READ,'stdout')
                os.write(write_fd,b'abcdef');os.close(write_fd);write_fd=None
                tails={'stdout':bytearray(),'stderr':bytearray()};eof=set()
                errors=execution.drain_browser_diagnostics(selector,tails,eof,deadline=deadline,limit=limit)
                self.assertEqual(bytes(tails['stdout']),expected)
                self.assertEqual(errors,['stdout:diagnostic-byte-bound'] if limit==3 else [])
                self.assertNotIn('stdout',eof)
            finally:
                selector.close();os.close(read_fd)
                if write_fd is not None:os.close(write_fd)
