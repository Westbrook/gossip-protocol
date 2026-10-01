"""Pinned model pricing, reservation, and provider-accounting boundaries (offline)."""

from dataclasses import FrozenInstanceError
import json
import unittest

from gossip_harness.worker import (
    ENDPOINT, MODEL, MODEL_PROFILES, STRONG_MODEL, OpenAIWorker, WorkerFailure,
    _micro_usd, model_profile,
)
from tests.test_worker import FakeTransport, KEY, envelope, request


def strong_envelope(input_tokens=101, output_tokens=20):
    data = envelope()
    data["model"] = STRONG_MODEL
    data["usage"].update(input_tokens=input_tokens, output_tokens=output_tokens,
                         total_tokens=input_tokens + output_tokens)
    return data


class ModelProfileTests(unittest.TestCase):
    def test_only_registered_dated_models_can_be_used(self):
        for model in ("gpt-5.4", "gpt-5.4-mini", "gpt-5.4-2026-03-06", "", [], None):
            with self.subTest(model=model), self.assertRaises(ValueError):
                OpenAIWorker(KEY, model=model)
        self.assertEqual(set(MODEL_PROFILES), {MODEL, STRONG_MODEL})
        with self.assertRaises(TypeError):
            MODEL_PROFILES["other"] = model_profile()
        with self.assertRaises(FrozenInstanceError):
            model_profile().context_tokens = 1

    def test_manifest_is_complete_json_safe_and_independent(self):
        worker = OpenAIWorker(KEY, model=STRONG_MODEL, max_output_tokens=4096)
        manifest = json.loads(json.dumps(worker.profile_manifest()))
        self.assertEqual(manifest["model"], STRONG_MODEL)
        self.assertEqual(manifest["configured_max_output_tokens"], 4096)
        self.assertEqual(manifest["context_tokens"], 1_050_000)
        self.assertEqual(manifest["long_context_above_input_tokens"], 272_000)
        self.assertEqual(manifest["source_url"],
                         "https://developers.openai.com/api/docs/models/gpt-5.4")
        self.assertEqual(manifest["endpoint"], ENDPOINT)
        self.assertEqual(manifest["service_tier"], "default")
        self.assertEqual(manifest["cached_input_accounting"], "uncached_rate")
        self.assertEqual(manifest["reservation_units"], 5_342_160)
        self.assertNotIn(KEY, json.dumps(manifest))
        manifest["input_micro_usd_per_million"] = 0
        self.assertEqual(worker.profile_manifest()["input_micro_usd_per_million"], 2_500_000)

    def test_strong_short_request_uses_exact_rates_and_pinned_payload(self):
        transport = FakeTransport(strong_envelope())
        worker = OpenAIWorker(KEY, model=STRONG_MODEL, max_output_tokens=4096,
                              transport=transport)
        result = worker.run(request())
        # Cached tokens are intentionally counted at ordinary input rates;
        # reasoning is already included in output_tokens, never added again.
        self.assertEqual(result.usage_units, 553)  # ceil(101 * 2.5 + 20 * 15)
        self.assertEqual(result.metadata["pricing_context"], "standard")
        payload = json.loads(transport.calls[0][0].data)
        self.assertEqual(payload["model"], STRONG_MODEL)
        self.assertEqual(payload["max_output_tokens"], 4096)
        self.assertEqual(payload["service_tier"], "default")

    def test_long_context_boundary_reprices_all_input_and_output(self):
        for input_tokens, expected, tier in (
            (272_000, 680_300, "standard"),
            (272_001, 1_360_455, "long"),
        ):
            with self.subTest(input_tokens=input_tokens):
                worker = OpenAIWorker(KEY, model=STRONG_MODEL,
                                      transport=FakeTransport(strong_envelope(input_tokens, 20)))
                result = worker.run(request())
                self.assertEqual(result.usage_units, expected)
                self.assertEqual(result.metadata["pricing_context"], tier)

    def test_each_profile_reserves_full_context_at_worst_supported_rates(self):
        for model, expected in ((MODEL, 318_432), (STRONG_MODEL, 5_342_160)):
            with self.subTest(model=model):
                transport = FakeTransport()
                worker = OpenAIWorker(KEY, model=model, max_output_tokens=4096,
                                      transport=transport)
                reserve = worker.reservation_units(request())
                self.assertEqual(reserve, expected)
                profile = model_profile(model)
                for input_tokens in (0, 272_000, 272_001, profile.context_tokens - 4096):
                    self.assertLessEqual(_micro_usd(input_tokens, 4096, model), reserve)
                self.assertEqual(transport.calls, [])

    def test_strong_context_bounds_fail_closed_on_impossible_usage(self):
        for counts in ((1_050_001, 0), (1_050_000, 1), (1, 4097)):
            with self.subTest(counts=counts), self.assertRaises(WorkerFailure) as caught:
                OpenAIWorker(KEY, model=STRONG_MODEL, max_output_tokens=4096,
                             transport=FakeTransport(strong_envelope(*counts))).run(request())
            self.assertIsNone(caught.exception.usage_units)
            self.assertTrue(caught.exception.metadata["halt"])
        # A legitimate long-context request above mini's limit remains supported.
        result = OpenAIWorker(KEY, model=STRONG_MODEL,
                              transport=FakeTransport(strong_envelope(500_000, 20))).run(request())
        self.assertEqual(result.usage_units, 2_500_450)

    def test_model_or_tier_switch_never_uses_the_selected_profiles_rate(self):
        for field, value in (("model", MODEL), ("service_tier", "priority")):
            data = strong_envelope()
            data[field] = value
            transport = FakeTransport(data)
            with self.subTest(field=field), self.assertRaises(WorkerFailure) as caught:
                OpenAIWorker(KEY, model=STRONG_MODEL, transport=transport).run(request())
            self.assertIsNone(caught.exception.usage_units)
            self.assertTrue(caught.exception.metadata["halt"])
            self.assertEqual(len(transport.calls), 1)

    def test_strong_unknown_usage_does_not_settle_zero(self):
        for usage in (None, {"input_tokens": True, "output_tokens": 1}):
            data = strong_envelope()
            data["usage"] = usage
            with self.subTest(usage=usage), self.assertRaises(WorkerFailure) as caught:
                OpenAIWorker(KEY, model=STRONG_MODEL, transport=FakeTransport(data)).run(request())
            self.assertIsNone(caught.exception.usage_units)
            self.assertTrue(caught.exception.metadata["halt"])

    def test_money_rounds_up_and_rejects_noninteger_counts(self):
        self.assertEqual(_micro_usd(1, 0, MODEL), 1)
        self.assertEqual(_micro_usd(1, 0, STRONG_MODEL), 3)
        self.assertEqual(_micro_usd(272_001, 1, STRONG_MODEL), 1_360_028)
        for counts in ((-1, 0), (1, -1), (True, 0), (0, 1.5)):
            with self.subTest(counts=counts), self.assertRaises(ValueError):
                _micro_usd(*counts, STRONG_MODEL)


if __name__ == "__main__":
    unittest.main()
