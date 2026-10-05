"""Original parser controls and real anchored-state reads; no candidate execution.

Fast record fixtures and mocked composition explicitly lack runtime/source
qualification. Git cases use real state with synthetic review/controller and an
owned refused Engine endpoint. Neither route supplies independent approval.
"""
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import candidate_execution_journal_v1 as journals
from gossip_harness import cumulative_generated_probe_reader_v1 as reader
from gossip_harness import cumulative_generated_probe_values_v2 as values
from gossip_harness import cumulative_generated_probe_wire_v1 as wire
from tests import test_cumulative_generated_probe_execution_v1 as execution_fixture
from tests import test_cumulative_generated_probe_pipe_v1 as pipe_fixture
from tests.test_cumulative_generated_probe_values_v2 import admitted, proposal


class GeneratedProbeOriginalParserTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();raw=self.root/'raw';delta=self.root/'delta'
        self.head=ExternalHead.create(self.root/'head',journal_roots=(raw,delta));self.addCleanup(self.head.close)
        self.journal=journals.OwnerJournal(raw,delta,context={'synthetic_original_parser_fixture':True},authority=self.head)
        self.addCleanup(self.journal.close)
        self.reader=object.__new__(reader._Reader);self.reader.journal=self.journal
        self.reader.docker=['docker','--host','unix:///synthetic-not-a-socket']

    def put(self,name,value):
        self.journal.retain(name,value if type(value) is bytes else values.canonical(value))

    def command(self,*,stdout=b'ok\n',exit_code=0,observed=None,dispatch=None):
        args=['docker','volume','ls'];argv=self.reader.docker+args[1:]
        self.put('query-dispatch.json',dispatch or {'argv':argv,'limit':reader.storage.MAX_STREAM_BYTES})
        streams={}
        for kind,raw in [('stdout',stdout),('stderr',b'')]:
            name='query-'+kind+'.bin';self.put(name,raw);count=observed if kind=='stdout' and observed is not None else len(raw)
            streams[kind]={'path':name,'bytes':len(raw),'observed_bytes':count,'sha256':reader.transport.sha(raw),'truncated':count>len(raw)}
        record={'argv':argv,'arguments':argv,'exit_code':exit_code,'timed_out':False,'capture_complete':True,**streams}
        self.put('query.json',record)
        return args

    def test_command_reads_bound_raw_streams_and_exact_dispatch(self):
        args=self.command();self.assertEqual(self.reader.command('query',args),b'ok\n')
        with self.assertRaisesRegex(reader.OriginalError,'dispatch_differs'):
            self.reader.command('query',['docker','container','ls'])

    def test_boolean_exit_code_cannot_impersonate_integer_zero(self):
        args=self.command(exit_code=False)
        with self.assertRaisesRegex(reader.OriginalError,'control_completion_types'):
            self.reader.command('query',args)

    def test_nonzero_control_is_unavailable_not_success(self):
        args=self.command(exit_code=17)
        with self.assertRaisesRegex(reader.Unavailable,'incomplete_control'):self.reader.command('query',args)

    def test_truncated_control_is_unavailable_even_with_zero_exit(self):
        args=self.command(observed=100)
        with self.assertRaisesRegex(reader.Unavailable,'incomplete_command_stream'):self.reader.command('query',args)

    def test_ack_requires_matching_slot_exact_bytes_count_and_order(self):
        slot='refresh';ack=wire.continuation_bytes(slot)
        self.put(slot+'-staging.json',{});self.put('probe-continue-'+slot+'-intent.bin',ack)
        self.put('probe-continue-'+slot+'-written.json',{'slot':slot,'requested_bytes':len(ack),'written_bytes':len(ack)})
        self.assertEqual(self.reader.acknowledge(slot),'probe-continue-refresh-written.json')

    def test_partial_ack_cannot_continue_a_history(self):
        slot='refresh';ack=wire.continuation_bytes(slot)
        self.put(slot+'-staging.json',{});self.put('probe-continue-'+slot+'-intent.bin',ack)
        self.put('probe-continue-'+slot+'-written.json',{'slot':slot,'requested_bytes':len(ack),'written_bytes':1})
        with self.assertRaisesRegex(reader.Unavailable,'partial_continuation'):self.reader.acknowledge(slot)

    def test_ack_cannot_precede_source_runtime_staging_boundary(self):
        slot='refresh';ack=wire.continuation_bytes(slot)
        self.put('probe-continue-'+slot+'-intent.bin',ack);self.put(slot+'-staging.json',{})
        self.put('probe-continue-'+slot+'-written.json',{'slot':slot,'requested_bytes':len(ack),'written_bytes':len(ack)})
        with self.assertRaisesRegex(reader.OriginalError,'chronology'):self.reader.acknowledge(slot)

    def test_engine_identity_comes_from_exact_http_request_and_response(self):
        value={'ID':'authored-parser-fixture'};body=values.canonical(value)
        self.put('engine-request.bin',reader.process._request('GET','/info'))
        self.put('engine-response.bin',b'HTTP/1.1 200 OK\r\nContent-Length: '+str(len(body)).encode()+b'\r\n\r\n'+body)
        self.assertEqual(self.reader.engine('engine','/info'),value)
        with self.assertRaisesRegex(reader.OriginalError,'engine_request'):self.reader.engine('engine','/version')

    def test_missing_original_is_distinct_from_authenticated_tampering(self):
        with self.assertRaises(reader.Unavailable):self.reader.raw('missing.bin')
        self.put('present.bin',b'real');(self.root/'raw'/'present.bin').write_bytes(b'fake')
        with self.assertRaises(chain.ChainUnknown):self.reader.raw('present.bin')

    def test_ambiguous_direct_and_chunked_blob_is_rejected(self):
        self.put('blob.bin',b'data');self.put('blob.bin-chunks.json',{})
        with self.assertRaisesRegex(reader.OriginalError,'ambiguous'):self.reader.blob('blob.bin')

    def finish_fixture(self, *, forged_value=False, extra_stdout=False, natural=True):
        row=proposal('refresh-noop-v1');probe=admitted(row);frames=pipe_fixture.rows_for(row)
        transcript=wire.ValueTranscript(probe,released_requirements=pipe_fixture.RELEASED,
            limits=wire.WireLimits(16384,131072,256,16))
        for raw in frames:transcript.append(raw)
        stdout=b''.join(frames)+(b'{}\n' if extra_stdout else b'');streams={}
        for kind,raw in [('stdout',stdout),('stderr',b'')]:
            self.put('probe-'+kind+'.bin',raw)
            streams[kind]={'bytes':len(raw),'observed_bytes':len(raw),'sha256':reader.transport.sha(raw),'truncated':False}
        slots=['imported','before','refresh','finished'];value=transcript.result()
        if forged_value:value={**value,'disposition':'fail'}
        terminal={'protocol':reader.pipes.PROTOCOL,'value':value,'streams':streams,
            'acks':[{'slot':slot,'requested_bytes':len(wire.continuation_bytes(slot)),
                     'written_bytes':len(wire.continuation_bytes(slot))} for slot in slots],
            'natural_exit':natural,'mechanics_complete':True,'exit_code':0,'local_pipes_closed':True,'infrastructure':[]}
        self.put('probe-pipe-terminal.json',terminal)
        self.reader.owner=mock.Mock();self.reader.owner.plan.policy.wire_limits=wire.WireLimits(16384,131072,256,16)
        self.reader.owner.plan.policy.stderr_bytes=65536
        return transcript,frames,slots

    def test_runner_value_summary_cannot_replace_recomputed_values(self):
        args=self.finish_fixture(forged_value=True)
        with self.assertRaisesRegex(reader.OriginalError,'value_projection'):self.reader.finish(*args)

    def test_extra_stdout_cannot_hide_behind_complete_frame_summary(self):
        args=self.finish_fixture(extra_stdout=True)
        with self.assertRaisesRegex(reader.OriginalError,'stdout_and_frames'):self.reader.finish(*args)

    def test_zero_exit_without_natural_completion_is_unavailable(self):
        args=self.finish_fixture(natural=False)
        with self.assertRaisesRegex(reader.Unavailable,'pipe_incomplete'):self.reader.finish(*args)

    def test_checkpoint_append_is_not_implicitly_adopted(self):
        self.reader.expected=self.journal.checkpoint();self.reader.checkpoint()
        self.put('extra.bin',b'new')
        with self.assertRaisesRegex(reader.OriginalError,'checkpoint_append'):self.reader.checkpoint()


class GeneratedProbeReconstructionCompositionTests(unittest.TestCase):
    """Synthetic physical boundaries isolate semantic/control composition only."""
    def compose(self,*,bad=False,stop=None):
        row=proposal('refresh-noop-v1');probe=admitted(row);frames=pipe_fixture.rows_for(row)
        if bad:
            value=json.loads(frames[2]);value['value']['status']='refreshed';frames[2]=values.canonical(value)+b'\n'
        spec=mock.Mock();spec.record={'probe':probe,'released_requirements':pipe_fixture.RELEASED}
        slots=['imported','before','refresh','finished'];by_slot=dict(zip(slots,frames))
        def frame(slot,prior):
            if slot==stop:raise reader.Unavailable('missing_later_boundary')
            return by_slot[slot]
        spec.frame.side_effect=frame
        spec.acknowledge.side_effect=lambda slot:'probe-continue-'+slot+'-written.json'
        owner=mock.Mock();owner.plan.policy.wire_limits=wire.WireLimits(16384,131072,256,16)
        # The public API is patched only in these explicitly synthetic composition
        # tests; production exact-type and prefix checks have separate Git cases.
        owner.binding=reader.state.ProbeWindow(1,2)
        checkpoint=reader.state.ProbeWindow(3,4)
        with mock.patch.object(reader,'_Reader',return_value=spec),mock.patch.object(reader,'sources',return_value={'synthetic':'only'}):
            result=reader.reconstruct(owner,expected=checkpoint)
        return result,spec

    def test_complete_good_value_requires_the_whole_physical_finish(self):
        result,spec=self.compose();self.assertEqual(result['disposition'],'pass')
        self.assertTrue(result['mechanics_complete']);spec.finish.assert_called_once()
        self.assertFalse(result['acceptance_authority']);self.assertFalse(result['physically_executed_by_reader'])

    def test_known_bad_value_survives_missing_finished_frame(self):
        result,spec=self.compose(bad=True,stop='finished')
        self.assertEqual(result['disposition'],'fail');self.assertFalse(result['mechanics_complete'])
        spec.finish.assert_not_called()

    def test_good_partial_value_never_becomes_positive_observation(self):
        result,_=self.compose(stop='finished')
        self.assertEqual(result['disposition'],'unavailable');self.assertFalse(result['mechanics_complete'])

    def test_contradictory_runtime_is_not_downgraded_to_unavailable(self):
        with mock.patch.object(reader._Reader,'__init__',side_effect=reader.OriginalError('wrong runtime')):
            with self.assertRaisesRegex(reader.OriginalError,'wrong runtime'):reader.reconstruct(None,expected=None)


class GeneratedProbeOriginalStateGitTests(unittest.TestCase):
    def setUp(self):
        f=execution_fixture.GeneratedProbeExecutionGitTests('test_constructor_binds_physical_environment_but_close_keeps_caller_state')
        self.addCleanup(f.doCleanups);f.setUp();self.fixture=f;self.state=f.probe_state

    def test_unused_state_reconstructs_unavailable_without_appending(self):
        checkpoint=self.state.journal.checkpoint();result=reader.reconstruct(self.state,expected=checkpoint)
        self.assertEqual(result['disposition'],'unavailable');self.assertFalse(result['mechanics_complete'])
        self.assertEqual(self.state.journal.checkpoint(),checkpoint)
        self.assertFalse(self.state.journal.has('intent.json'))

    def test_refused_runtime_is_reconstructed_without_trusting_terminal(self):
        owner=self.fixture.owner();owner.execute_once();checkpoint=self.state.journal.checkpoint()
        result=reader.reconstruct(self.state,expected=checkpoint)
        self.assertEqual(result['disposition'],'unavailable');self.assertFalse(result['qualified_execution_originals'])
        self.assertTrue(result['limitations']);self.assertEqual(self.state.journal.checkpoint(),checkpoint)

    def test_after_deadline_read_is_allowed_but_dispatch_is_still_refused(self):
        checkpoint=self.state.journal.checkpoint()
        with mock.patch.object(reader.state.time,'monotonic_ns',return_value=self.state.binding.window.deadline_ns+1):
            result=reader.reconstruct(self.state,expected=checkpoint)
            self.assertEqual(result['disposition'],'unavailable')
            with self.assertRaisesRegex(ValueError,'absolute_window'):self.state.begin()
        self.assertEqual(self.state.journal.checkpoint(),checkpoint)

    def test_stale_prefix_is_rejected_instead_of_silently_following_append(self):
        checkpoint=self.state.journal.checkpoint();self.state.begin()
        with self.assertRaisesRegex(reader.OriginalError,'checkpoint_append'):reader.reconstruct(self.state,expected=checkpoint)

    def test_revoked_controller_registration_cannot_supply_observations(self):
        checkpoint=self.state.journal.checkpoint();self.fixture.fixture.available=False
        with self.assertRaises(ValueError):reader.reconstruct(self.state,expected=checkpoint)

    def test_changed_original_configuration_is_fatal(self):
        checkpoint=self.state.journal.checkpoint();path=self.state.root/'config.json';path.write_bytes(path.read_bytes()+b' ')
        with self.assertRaises(chain.ChainUnknown):reader.reconstruct(self.state,expected=checkpoint)


if __name__=='__main__':unittest.main()
