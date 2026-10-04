"""Offline declarations and compilation only; inert server source is never executed."""
from __future__ import annotations

import ast
import base64
from dataclasses import replace
import hashlib
import json
import types
import unittest
from unittest.mock import patch

from gossip_harness import candidate_http_cases_v1 as catalog
from gossip_harness import candidate_http_cases_core_v1 as core
from gossip_harness import candidate_http_execution_v3 as execution
from gossip_harness import candidate_http_inputs_v1 as inputs
from gossip_harness import candidate_http_transport_v1 as wire
from tests import candidate_http_v3_fixtures as fixtures


class HttpV3ExtensionDeclarationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.row = next(x for x in catalog.definitions() if x.row_id == fixtures.LONG_ROW_ID)
        cls.long = fixtures.fixed_long_recipe()

    def test_fixed_row_identity_and_complete_seventy_requests(self):
        self.assertEqual(fixtures.LONG_ROW_ID, "HTTP-ACTION-STATE/old-token-after-successor-failed")
        self.assertEqual(self.long.definition_sha256,
                         "dc486b5dcfad72782813ddefda8a4a9201e03f2247e3686cc9b97b2acb1e3d97")
        self.assertEqual((len(self.long.steps), sum(s.kind == "probe" for s in self.long.steps)), (72, 70))
        self.assertEqual([s.kind for s in self.long.steps], ["start"] + ["probe"] * 70 + ["stop"])
        self.assertEqual(self.long.definition_json, core.encode(self.row.record()))
        self.assertEqual([s.step_id for s in self.long.steps], [s.step_id for s in self.row.steps])

    def test_fixed_row_keeps_every_step_and_request_byte(self):
        for source, projected in zip(self.row.steps, self.long.steps):
            self.assertEqual(projected.declaration_json, core.encode(source.record()))
            self.assertEqual((projected.epoch, projected.root_path, projected.argv),
                             (source.epoch, source.root, source.argv))
            if source.request:
                self.assertEqual(wire.request_bytes(json.loads(projected.request_json), self.long.port),
                                 source.request.wire_bytes())
        self.assertEqual(sum(len(wire.request_bytes(json.loads(s.request_json), self.long.port))
                             for s in self.long.steps if s.kind == "probe"), 6113)

    def test_fixed_row_preserves_inputs_and_semantics_without_verdict(self):
        self.assertEqual(self.long.input_entries, self.row.fixtures)
        self.assertEqual(self.long.server_argv,
            ("python", "-m", "library", "--db", "/tmp/catalog.sqlite", "--root", "/inputs", "serve", "--port", "18765"))
        self.assertEqual(self.long.record()["definition"], self.row.record())
        self.assertFalse(self.long.record()["acceptance_authority"])
        self.assertFalse(self.long.record()["definition"]["acceptance_authority"])

    def test_changed_row_or_missing_duplicate_row_is_rejected(self):
        changed = replace(self.row, notes=self.row.notes + ("Deliberate definition drift",))
        for rows in ((), (changed,), (self.row, self.row)):
            with self.subTest(count=len(rows)), patch.object(catalog, "definitions", return_value=rows):
                with self.assertRaisesRegex(ValueError, "declaration changed"):
                    fixtures.fixed_long_recipe()

    def test_long_observations_count_every_request_in_literal_order(self):
        expected = fixtures.expected_observations(self.long)
        self.assertEqual(len(expected), 70)
        self.assertEqual([x["step_id"] for x in expected], [s.step_id for s in self.long.steps[1:-1]])
        self.assertEqual([x["json"]["request_index"] for x in expected], list(range(1, 71)))
        self.assertEqual({x["json"]["startup_index"] for x in expected}, {1})
        self.assertEqual(sum(x["json"]["received_length"] for x in expected), 211)
        self.assertTrue(all(x["status"] == 200 and x["json"]["root_view"] is None for x in expected))
        self.assertTrue(any(s.expectation and s.expectation.semantic for s in self.row.steps))

    def test_root_history_has_two_distinct_literal_epochs(self):
        recipe = fixtures.root_link_recipe()
        self.assertEqual([s.kind for s in recipe.steps], ["start", "probe", "probe", "stop"] * 2)
        self.assertEqual([s.epoch for s in recipe.steps], [1] * 4 + [2] * 4)
        self.assertEqual([s.root_path for s in recipe.steps], ["/inputs/north"] * 4 + ["/inputs/south"] * 4)
        self.assertEqual([s.argv for s in recipe.steps if s.kind == "start"], [
            ("python", "-m", "library", "--db", "/tmp/catalog.sqlite", "--root", root, "serve", "--port", "18765")
            for root in ("/inputs/north", "/inputs/south")])
        self.assertFalse(any(s.kind == "cli" for s in recipe.steps))

    def test_root_inputs_include_real_roots_decoy_and_direct_ancestor_links(self):
        recipe = fixtures.root_link_recipe()
        entries = {x.path: x for x in recipe.input_entries}
        self.assertEqual(len(entries), 11)
        self.assertEqual({x.path for x in entries.values() if x.kind == "directory"}, {"north", "south", "shared"})
        self.assertEqual(entries["marker.txt"].data, b"outer-root-decoy-v3\n")
        self.assertEqual(entries["north/marker.txt"].data, b"north-root-v3\n")
        self.assertEqual(entries["south/marker.txt"].data, b"south-root-v3\n")
        self.assertEqual(entries["shared/payload.txt"].data, b"shared-outside-selected-root-inside-inputs-v3\n")
        for name in ("north", "south"):
            self.assertEqual((entries[name + "/direct.txt"].kind, entries[name + "/direct.txt"].target),
                             ("symlink", "../shared/payload.txt"))
            self.assertEqual((entries[name + "/ancestor"].kind, entries[name + "/ancestor"].target),
                             ("symlink", "../shared"))
        inputs.validate(recipe.input_entries)

    def test_root_observations_distinguish_state_counter_and_selected_root(self):
        rows = fixtures.expected_observations(fixtures.root_link_recipe())
        self.assertEqual([x["json"]["request_index"] for x in rows], [1, 2, 3, 4])
        self.assertEqual([x["json"]["startup_index"] for x in rows], [1, 1, 2, 2])
        self.assertEqual({x["json"]["database"] for x in rows}, {"/tmp/catalog.sqlite"})
        for index, name in enumerate(("north", "north", "south", "south")):
            view = rows[index]["json"]["root_view"]
            marker = (name + "-root-v3\n").encode()
            self.assertEqual(view["marker"], {"bytes": len(marker), "sha256": hashlib.sha256(marker).hexdigest()})
            self.assertNotEqual(view["marker"], view["outer_marker"])
            for key, resolved in (("direct", "/inputs/shared/payload.txt"), ("ancestor", "/inputs/shared")):
                self.assertTrue(view[key]["is_symlink"])
                self.assertEqual(view[key]["resolved"], resolved)
                self.assertEqual(view[key]["file"]["bytes"], 46)

    def test_exact_binary_body_boundaries_and_distinct_literal_wire(self):
        for size in (65536, 65537):
            recipe = fixtures.body_recipe(size)
            request = json.loads(recipe.steps[1].request_json)
            raw = base64.b64decode(request["body_b64"], validate=True)
            self.assertEqual(len(raw), size)
            self.assertEqual(raw[:65536], bytes(range(256)) * 256)
            self.assertEqual(raw[65536:], b"\xa5" if size == 65537 else b"")
            wire_raw = wire.request_bytes(request, recipe.port)
            expected_head = (b"POST /fixture/body HTTP/1.1\r\nHost: 127.0.0.1:18765\r\nConnection: close\r\n"
                             b"Content-Type: application/octet-stream\r\nContent-Length: " + str(size).encode() + b"\r\n\r\n")
            self.assertEqual(wire_raw, expected_head + raw)
            facts = fixtures.expected_observations(recipe)[0]
            self.assertEqual(facts["json"]["declared_length"], size)
            self.assertEqual(facts["json"]["received_length"], size)
            self.assertEqual(facts["json"]["body_sha256"], hashlib.sha256(raw).hexdigest())
            self.assertEqual(facts["status"], 200)

    def test_early_close_is_exact_one_request_with_conditional_read_zero(self):
        recipe = fixtures.body_recipe(65537, early_close=True)
        self.assertEqual([s.kind for s in recipe.steps], ["start", "probe", "stop"])
        request = json.loads(recipe.steps[1].request_json)
        self.assertEqual(request["target"], "/fixture/early-close")
        self.assertEqual(len(base64.b64decode(request["body_b64"])), 65537)
        facts, = fixtures.expected_observations(recipe)
        self.assertEqual(facts["status"], 413)
        self.assertTrue(facts["conditional_response"])
        self.assertFalse(facts["guaranteed_partial_send"])
        self.assertEqual((facts["json"]["declared_length"], facts["json"]["received_length"]), (65537, 0))
        self.assertEqual(facts["json"]["body_sha256"], hashlib.sha256(b"").hexdigest())
        self.assertNotIn("sent_complete", facts)
        self.assertNotIn("exchange_complete", facts)

    def test_boundary_controls_reject_unregistered_arguments(self):
        for size in (0, 65535, 65538, True, 65536.0, "65536"):
            with self.subTest(size=size), self.assertRaises(ValueError):
                fixtures.body_recipe(size)
        for size, early in ((65536, True), (65537, 1), (65537, "true")):
            with self.subTest(size=size, early=early), self.assertRaises(ValueError):
                fixtures.body_recipe(size, early_close=early)

    def test_expected_facts_reject_changed_recipe_and_return_fresh_values(self):
        original = fixtures.root_link_recipe()
        changed = replace(original, recipe_id="changed-root-recipe")
        with self.assertRaisesRegex(ValueError, "Unknown or changed"):
            fixtures.expected_observations(changed)
        rows = fixtures.expected_observations(original)
        rows[0]["json"]["request_index"] = 99
        self.assertEqual(fixtures.expected_observations(original)[0]["json"]["request_index"], 1)


class HttpV3InertSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tree = ast.parse(fixtures.SERVER_SOURCE, filename="library.py")

    def test_source_compiles_without_import_or_execution(self):
        compiled = compile(fixtures.SERVER_SOURCE, "library.py", "exec")
        self.assertIsInstance(compiled, types.CodeType)
        # No exec/eval/importlib or subprocess path exists in the wrapper.
        wrapper = ast.parse(__import__("pathlib").Path(fixtures.__file__).read_text())
        calls = {n.func.id for n in ast.walk(wrapper) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        self.assertFalse(calls.intersection({"exec", "eval", "compile", "__import__"}))

    def test_git_source_mapping_is_only_inert_library_module(self):
        files = fixtures.source_files()
        self.assertEqual(set(files), {"library.py"})
        self.assertEqual(files["library.py"], fixtures.SERVER_SOURCE.encode("utf-8"))
        files.clear()
        self.assertEqual(set(fixtures.source_files()), {"library.py"})

    def test_source_has_only_standard_library_imports(self):
        imports = {alias.name for node in ast.walk(self.tree) if isinstance(node, ast.Import) for alias in node.names}
        self.assertEqual(imports, {"argparse", "hashlib", "json", "os", "socket", "socketserver", "sqlite3", "stat"})
        self.assertFalse(any(isinstance(node, ast.ImportFrom) for node in ast.walk(self.tree)))
        calls = {n.func.id for n in ast.walk(self.tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        self.assertFalse(calls.intersection({"exec", "eval", "compile", "__import__"}))

    def test_source_has_no_product_route_answers_or_controller_payload(self):
        strings = {n.value for n in ast.walk(self.tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)}
        self.assertIn("inert-http-v3-transport-facts-v1", strings)
        self.assertIn(b"/fixture/root-links", {n.value for n in ast.walk(self.tree) if isinstance(n, ast.Constant) and isinstance(n.value, bytes)})
        self.assertFalse(any(x.startswith("/api/") for x in strings))
        self.assertNotIn(fixtures.LONG_ROW_ID, fixtures.SERVER_SOURCE)
        self.assertNotIn(fixtures.LONG_DEFINITION_SHA256, fixtures.SERVER_SOURCE)
        self.assertNotIn("expected_observations", fixtures.SERVER_SOURCE)
        self.assertNotIn("definition_json", fixtures.SERVER_SOURCE)

    def test_source_bind_and_database_use_declared_arguments(self):
        calls = [n for n in ast.walk(self.tree) if isinstance(n, ast.Call)]
        connects = [n for n in calls if isinstance(n.func, ast.Attribute) and n.func.attr == "connect"]
        self.assertEqual(len(connects), 2)
        self.assertTrue(all(ast.unparse(n.args[0]) == "args.db" for n in connects))
        servers = [n for n in calls if isinstance(n.func, ast.Name) and n.func.id == "Server"]
        self.assertEqual([ast.literal_eval(n.args[0].elts[0]) for n in servers], ["127.0.0.1"])
        self.assertEqual(ast.unparse(servers[0].args[0].elts[1]), "args.port")

    def test_source_early_close_branch_reads_headers_only_and_never_retries(self):
        handler = next(n for n in self.tree.body if isinstance(n, ast.ClassDef) and n.name == "Handler")
        handle = next(n for n in handler.body if isinstance(n, ast.FunctionDef) and n.name == "handle")
        body_guard = next(n for n in handle.body if isinstance(n, ast.If) and ast.unparse(n.test) == "not early")
        recv_calls = [n for n in ast.walk(body_guard) if isinstance(n, ast.Call)
                      and isinstance(n.func, ast.Attribute) and n.func.attr == "recv"]
        self.assertEqual(len(recv_calls), 1)
        outside = [n for n in handle.body if isinstance(n, ast.While)]
        self.assertEqual(len(outside), 1)
        head_receives = [n for n in ast.walk(outside[0]) if isinstance(n, ast.Call)
                         and isinstance(n.func, ast.Attribute) and n.func.attr == "recv"]
        self.assertEqual([ast.literal_eval(n.args[0]) for n in head_receives], [1])
        early_guard = next(n for n in handle.body if isinstance(n, ast.If) and ast.unparse(n.test) == "early")
        self.assertEqual(len(early_guard.body), 1)
        self.assertEqual(ast.unparse(early_guard.body[0]), "self.request.shutdown(socket.SHUT_RDWR)")
        self.assertFalse(any(isinstance(n, ast.Try) for n in ast.walk(handle)))
