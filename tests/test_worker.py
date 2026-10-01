"""Offline protocol tests: injected HTTP only, never credentials or live calls."""

from dataclasses import replace
import json
import unittest
from unittest.mock import patch
import urllib.request

from gossip_harness.worker import (
    CONTEXT_TOKENS, ENDPOINT, HTTPResponse, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES,
    MODEL, OpenAIWorker, WorkerFailure, WorkerRequest, _https_transport,
)


KEY = "test-only-credential-do-not-use"


def request():
    return WorkerRequest("task-1", "Correct the addition function.",
                         ("src/add.py", "src/old.py"),
                         {"src/add.py": "def add(a,b): return a-b\n", "src/old.py": "obsolete\n"},
                         "a" * 40, 1)


def envelope(proposal=None):
    if proposal is None:
        proposal = {"changes": [{"path": "src/add.py", "content": "def add(a,b): return a+b\n"}],
                    "summary": "Corrected addition."}
    text = proposal if isinstance(proposal, str) else json.dumps(proposal)
    return {"id": "resp_test", "model": MODEL, "status": "completed", "service_tier": "default",
            "usage": {"input_tokens": 101, "output_tokens": 20, "total_tokens": 121,
                      "input_tokens_details": {"cached_tokens": 100},
                      "output_tokens_details": {"reasoning_tokens": 10}},
            "output": [{"type": "reasoning", "summary": []},
                       {"type": "message", "role": "assistant", "status": "completed",
                        "content": [{"type": "output_text", "text": text}]}]}


class FakeTransport:
    def __init__(self, value=None, *, body=None, status=200):
        self.value = envelope() if value is None else value
        self.body = body
        self.status = status
        self.calls = []

    def __call__(self, req, timeout, limit):
        self.calls.append((req, timeout, limit))
        data = self.body if self.body is not None else json.dumps(self.value).encode()
        return HTTPResponse(self.status, {"X-Request-Id": "req_test"}, data)


class WorkerTests(unittest.TestCase):
    def assert_failure(self, data=None, *, body=None, status=200, req=None, kind=None, cost=166):
        transport = FakeTransport(data, body=body, status=status)
        with self.assertRaises(WorkerFailure) as caught:
            OpenAIWorker(KEY, transport=transport).run(req or request())
        error = caught.exception
        self.assertEqual(error.usage_units, cost)
        if kind:
            self.assertEqual(error.metadata["failure_kind"], kind)
        self.assertNotIn(KEY, str(error))
        self.assertNotIn(KEY, repr(error.metadata))
        return error, transport

    def test_success_builds_exact_responses_contract_and_prices_reasoning_once(self):
        transport = FakeTransport()
        worker = OpenAIWorker(KEY, transport=transport)
        result = worker.run(request())
        self.assertEqual(result.changes, {"src/add.py": "def add(a,b): return a+b\n"})
        # ceil(101 * .75 + 20 * 4.5): cached input charged uncached;
        # reasoning was already included in output_tokens, never added again.
        self.assertEqual(result.usage_units, 166)
        self.assertEqual(result.metadata["request_id"], "req_test")
        req, timeout, limit = transport.calls[0]
        self.assertEqual(req.full_url, ENDPOINT)
        self.assertEqual(req.get_method(), "POST")
        self.assertEqual(req.get_header("Authorization"), "Bearer " + KEY)
        self.assertEqual((timeout, limit), (120, MAX_RESPONSE_BYTES))
        payload = json.loads(req.data)
        self.assertFalse(payload["store"])
        self.assertFalse(payload["stream"])
        self.assertEqual(payload["service_tier"], "default")
        self.assertEqual(payload["reasoning"], {"effort": "low"})
        self.assertEqual(payload["truncation"], "disabled")
        self.assertNotIn("tools", payload)
        schema = payload["text"]["format"]
        self.assertTrue(schema["strict"])
        self.assertEqual(schema["schema"]["properties"]["changes"]["items"]["properties"]["path"]["enum"],
                         list(request().allowed_paths))
        self.assertNotIn(KEY, req.data.decode())
        self.assertNotIn(KEY, repr(worker))

    def test_reservation_is_full_context_and_max_output_and_does_not_call_provider(self):
        transport = FakeTransport()
        worker = OpenAIWorker(KEY, transport=transport)
        self.assertEqual(worker.reservation_units(request()), 336_864)
        self.assertEqual(worker.reservation_units(replace(request(), files={})), 336_864)
        self.assertEqual(transport.calls, [])

    def test_pinned_model_and_output_bounds_cannot_be_bypassed(self):
        for kwargs in ({"model": "gpt-5.4-mini"}, {"max_output_tokens": 0},
                       {"max_output_tokens": True}, {"max_output_tokens": 128001},
                       {"timeout": float("nan")}, {"timeout": 0}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                OpenAIWorker(KEY, **kwargs)

    def test_refusal_is_not_a_patch_but_known_usage_is_preserved(self):
        data = envelope()
        data["output"][1]["content"] = [{"type": "refusal", "refusal": KEY}]
        self.assert_failure(data, kind="refusal")

    def test_incomplete_is_not_a_patch_but_known_usage_is_preserved(self):
        data = envelope()
        data["status"] = "incomplete"
        data["incomplete_details"] = {"reason": "max_output_tokens", "secret": KEY}
        self.assert_failure(data, kind="incomplete")

    def test_malformed_proposal_keeps_usage_and_does_not_echo_contents(self):
        self.assert_failure(envelope(KEY), kind="proposal")
        self.assert_failure(envelope('{"changes": [], "summary": NaN}'), kind="proposal")
        self.assert_failure(envelope('{"changes": [], "summary":"ok", "summary":"bad"}'), kind="proposal")
        self.assert_failure(envelope({"changes": [{"path": "src/add.py", "content": 42}], "summary": "bad"}),
                  kind="proposal")

    def test_unknown_usage_retains_reservation_and_halts(self):
        for usage in (None, {"input_tokens": True, "output_tokens": 1},
                      {"input_tokens": -1, "output_tokens": 1}):
            data = envelope()
            data["usage"] = usage
            error, _ = self.assert_failure(data, kind="usage", cost=None)
            self.assertTrue(error.metadata["halt"])

    def test_unknown_outer_json_retains_reservation(self):
        for body in (KEY.encode(), b'{"usage":NaN}', b'[]', b'\xff'):
            error, _ = self.assert_failure(body=body, kind="response", cost=None)
            self.assertTrue(error.metadata["halt"])

    def test_http_error_never_echoes_error_body_and_is_not_retried(self):
        error, transport = self.assert_failure({"error": {"message": KEY}}, status=401, kind="http", cost=None)
        self.assertEqual(error.metadata["http_status"], 401)
        self.assertEqual(len(transport.calls), 1)
        self.assertTrue(error.metadata["halt"])

    def test_network_error_never_echoes_exception_and_is_not_retried(self):
        calls = []
        def broken(*args):
            calls.append(1)
            raise OSError(KEY)
        with self.assertRaises(WorkerFailure) as caught:
            OpenAIWorker(KEY, transport=broken).run(request())
        self.assertIsNone(caught.exception.usage_units)
        self.assertNotIn(KEY, repr(caught.exception))
        self.assertTrue(caught.exception.__suppress_context__)
        self.assertEqual(calls, [1])

    def test_response_metadata_cannot_echo_the_credential(self):
        def echoed(*args):
            data = envelope()
            data["id"] = KEY
            return HTTPResponse(200, {"X-Request-Id": KEY}, json.dumps(data).encode())
        result = OpenAIWorker(KEY, transport=echoed).run(request())
        self.assertNotIn(KEY, repr(result.metadata))
        self.assertNotIn("request_id", result.metadata)
        self.assertNotIn("response_id", result.metadata)

    def test_scope_traversal_and_duplicate_paths_are_rejected(self):
        for path in ("../src/add.py", "/src/add.py", "src\\add.py", "src//add.py",
                     "src/./add.py", ".git/config", "tests/test_add.py", "src/add.py\x00"):
            with self.subTest(path=path):
                self.assert_failure(envelope({"changes": [{"path": path, "content": "new"}], "summary": "bad"}),
                          kind="scope")
        self.assert_failure(envelope({"changes": [{"path": "src/add.py", "content": "new"},
                                         {"path": "src/add.py", "content": "other"}], "summary": "bad"}),
                  kind="scope")

    def test_deletion_and_new_file_are_valid_effective_changes(self):
        for path, content in (("src/old.py", None), ("src/new.py", "new\n")):
            data = envelope({"changes": [{"path": path, "content": content}], "summary": "updated"})
            req = replace(request(), allowed_paths=(path,))
            result = OpenAIWorker(KEY, transport=FakeTransport(data)).run(req)
            self.assertEqual(result.changes, {path: content})

    def test_empty_and_ineffective_changes_rejected(self):
        self.assert_failure(envelope({"changes": [], "summary": "nothing"}), kind="proposal")
        self.assert_failure(envelope({"changes": [{"path": "src/add.py", "content": request().files["src/add.py"]}],
                            "summary": "nothing"}), kind="empty")

    def test_invalid_request_is_rejected_before_transport_and_has_zero_usage(self):
        for req in (replace(request(), allowed_paths=("../evil",)),
                    replace(request(), allowed_paths=("src/add.py", "src/add.py")),
                    replace(request(), files={"/etc/passwd": "text"}),
                    replace(request(), instructions="x" * MAX_REQUEST_BYTES),
                    replace(request(), attempt=True)):
            error, transport = self.assert_failure(req=req, kind="request", cost=0)
            self.assertEqual(transport.calls, [])

    def test_oversized_provider_response_retains_reservation(self):
        self.assert_failure(body=b"x" * (MAX_RESPONSE_BYTES + 1), kind="response", cost=None)

    def test_changed_model_or_service_tier_halts_without_guessing_cost(self):
        for field, value in (("model", "other-model"), ("service_tier", "priority")):
            data = envelope()
            data[field] = value
            error, _ = self.assert_failure(data, kind="accounting", cost=None)
            self.assertTrue(error.metadata["halt"])

    def test_impossible_usage_bounds_halt_without_under_settling(self):
        for counts in ((1, 8193), (CONTEXT_TOKENS + 1, 1), (CONTEXT_TOKENS, 1)):
            data = envelope()
            data["usage"] = {"input_tokens": counts[0], "output_tokens": counts[1]}
            error, _ = self.assert_failure(data, kind="accounting", cost=None)
            self.assertTrue(error.metadata["halt"])
        data = envelope()
        data["usage"]["total_tokens"] = 1
        self.assert_failure(data, kind="accounting", cost=None)

    def test_default_http_disables_proxies_and_redirects_and_bounds_reads(self):
        class Response:
            status = 200
            headers = {"X-Request-Id": "req_test"}
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, size):
                self.read_size = size
                return b"{}"
        result = Response()
        with patch("gossip_harness.worker.urllib.request.build_opener") as make:
            make.return_value.open.return_value = result
            req = urllib.request.Request(ENDPOINT, data=b"{}")
            response = _https_transport(req, 19, 20)
            self.assertEqual(response.body, b"{}")
            self.assertEqual(result.read_size, 21)
            make.return_value.open.assert_called_once_with(req, timeout=19)
            handlers = make.call_args.args
            self.assertEqual(handlers[0].proxies, {})
            self.assertIsNone(handlers[1].redirect_request(req, None, 302, "", {}, "https://other.invalid"))
            self.assertIsInstance(handlers[2], urllib.request.HTTPSHandler)


if __name__ == "__main__":
    unittest.main()
