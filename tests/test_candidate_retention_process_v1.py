"""Real frozen process control flow with isolated fake Engine sockets."""
from __future__ import annotations

from copy import deepcopy
import threading
import unittest
from unittest.mock import patch

from gossip_harness import candidate_retention_process_v1 as guarded
from gossip_harness import candidate_client_process_v4 as frozen
from tests.test_candidate_client_process_v4 import (
    ARGV, ENDPOINT, IDENTITY, IMAGE, FakeSocket, fixture_inspection,
    finished, frame, runtime_fixture,
)


class RetentionProcessTests(unittest.TestCase):
    def exercise(self, fail_at=None, failure=None, wait_fails=False):
        failure = failure if failure is not None else ValueError("lost checkpoint acknowledgement")
        opened, readers, sent = [], [], []
        retained: dict[str, bytes] = {}
        gate = threading.Event()
        response = (b"HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n"
                    b"Content-Type: application/vnd.docker.multiplexed-stream\r\n\r\n"
                    + frame(1, b"\x00\xffresult"))

        class ClosingSocket(FakeSocket):
            def shutdown(self, how):
                gate.set()
            def sendall(self, raw):
                sent.append(raw)
                super().sendall(raw)

        attachment = ClosingSocket(response, gate=gate)
        sockets = [attachment, ClosingSocket(b"HTTP/1.1 204 No Content\r\n\r\n"),
                   ClosingSocket(b"HTTP/1.1 204 No Content\r\n\r\n")]
        calls = []
        def retain(name, raw):
            calls.append(name)
            if name == fail_at:
                raise failure
            self.assertNotIn(name, retained)
            retained[name] = raw
        def socket_factory(*args, **kwargs):
            value = sockets.pop(0)
            opened.append(value)
            return value
        def control(endpoint, path, **kwargs):
            kwargs['retain'](kwargs['label'] + '-request.bin', frozen._request(kwargs.get('method', 'GET'), path))
            if path.endswith('/wait?condition=not-running'):
                gate.set()
                if wait_fails:
                    raise frozen.ProcessError('fixture wait failure')
                return {'StatusCode': 0}
            return deepcopy(fixture_inspection() if kwargs['label'] == 'inspect-before' else finished())
        original_thread = threading.Thread
        def thread_factory(*args, **kwargs):
            value = original_thread(*args, **kwargs)
            readers.append(value)
            return value
        result, caught = None, None
        with patch.object(frozen, 'runtime_identity', return_value=runtime_fixture()), \
                patch.object(frozen, '_json_control', side_effect=control), \
                patch.object(frozen.EngineEndpoint, 'validate'), \
                patch.object(frozen.socket, 'socket', side_effect=socket_factory), \
                patch.object(frozen.threading, 'Thread', side_effect=thread_factory):
            try:
                result = guarded.run_process(ENDPOINT, container_id=IDENTITY,
                    expected=fixture_inspection(), policy=frozen.ProcessPolicy(IMAGE, timeout_seconds=1,
                    transport_timeout_seconds=1), retain=retain, label='p',
                    expected_runtime=runtime_fixture(), expected_argv=ARGV)
            except BaseException as error:
                caught = error
        self.assertTrue(all(value.closed for value in opened))
        self.assertTrue(all(not value.is_alive() for value in readers))
        if fail_at is not None:
            self.assertIs(caught, failure)
            self.assertIsNone(result)
            self.assertEqual(calls[-1], fail_at)
            self.assertNotIn('p-process.json', retained)
        else:
            self.assertIsNone(caught)
        return result, retained, sent, opened

    def test_success_preserves_binary_capture_and_original_wire_protocol(self):
        result, retained, sent, _ = self.exercise()
        self.assertEqual(result['protocol'], frozen.PROTOCOL)
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(retained['p-stdout.bin'], b'\x00\xffresult')
        self.assertEqual(sum(b'/start ' in raw for raw in sent), 1)

    def test_start_request_retention_failure_never_sends_start_or_cleanup(self):
        _, _, sent, _ = self.exercise('p-start-request.bin')
        self.assertFalse(any(b'/start ' in raw or b'/kill?' in raw for raw in sent))

    def test_lost_start_response_ack_closes_both_sockets_and_blocks_kill(self):
        _, _, sent, opened = self.exercise('p-start-response.bin')
        self.assertEqual(len(opened), 2)
        self.assertEqual(sum(b'/start ' in raw for raw in sent), 1)
        self.assertFalse(any(b'/kill?' in raw for raw in sent))

    def test_first_failure_on_final_attach_capture_still_closes_socket(self):
        _, retained, _, _ = self.exercise('p-attach-response.bin')
        self.assertNotIn('p-attach-response.bin', retained)
        self.assertNotIn('p-stdout.bin', retained)

    def test_cleanup_request_retention_failure_closes_attachment_without_dispatch(self):
        _, _, sent, _ = self.exercise('p-kill-request.bin', wait_fails=True)
        self.assertFalse(any(b'/kill?' in raw for raw in sent))

    def test_cancellation_during_retention_is_reraised_after_reader_teardown(self):
        self.exercise('p-start-response.bin', failure=KeyboardInterrupt('fixture interruption'))

    def test_initial_intent_failure_has_no_engine_effect(self):
        _, _, sent, opened = self.exercise('p-intent.json')
        self.assertEqual((sent, opened), ([], []))

    def test_changed_frozen_source_contract_prevents_dispatch(self):
        with patch.object(guarded, 'FROZEN_V4_SOURCE_SHA256', '0' * 64), \
                patch.object(frozen, 'run_process') as run:
            with self.assertRaisesRegex(frozen.ProcessError, 'teardown ordering'):
                guarded.run_process(ENDPOINT, container_id=IDENTITY, expected=fixture_inspection(),
                    policy=frozen.ProcessPolicy(IMAGE), retain=lambda *_: None, label='p',
                    expected_runtime=runtime_fixture(), expected_argv=ARGV)
        run.assert_not_called()
