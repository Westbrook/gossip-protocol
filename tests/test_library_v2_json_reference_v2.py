"""Host-only provenance and scope checks; generated application code never runs."""
from __future__ import annotations

import ast
from copy import deepcopy
import hashlib
from pathlib import Path
import unittest
from unittest.mock import patch

from gossip_harness import library_v2_json_reference_v2 as repair


class LibraryV2JsonReferenceSourceTests(unittest.TestCase):
    def test_only_jobs_changes_and_all_returned_maps_are_independent(self):
        original = repair.baseline.v2_files()
        files = repair.corrected_v2_files()
        self.assertEqual(set(original), set(files))
        self.assertEqual([name for name in original if files[name] != original[name]], [repair.JOBS_PATH])
        self.assertEqual(repair.corrected_v2_binary_files(), repair.baseline.v2_binary_files())
        files[repair.JOBS_PATH] = 'changed'
        binaries = repair.corrected_v2_binary_files()
        binaries.clear()
        self.assertEqual(repair.corrected_v2_files()[repair.JOBS_PATH], repair._repair_jobs(original[repair.JOBS_PATH]))
        self.assertTrue(repair.corrected_v2_binary_files())
        for name, text in repair.corrected_v2_files().items():
            if name.endswith('.py'):
                ast.parse(text, filename=name)

    def test_all_existing_code_is_unchanged_except_the_json_reader_call(self):
        old = ast.parse(repair.baseline.v2_files()[repair.JOBS_PATH])
        new = ast.parse(repair.corrected_v2_files()[repair.JOBS_PATH])
        self.assertEqual(len(new.body), len(old.body) + 1)
        helpers = [node for node in new.body if isinstance(node, ast.FunctionDef) and node.name == '_read_json_path']
        self.assertEqual(len(helpers), 1)
        new.body.remove(helpers[0])
        old_manager = next(node for node in old.body if isinstance(node, ast.ClassDef) and node.name == 'JobManager')
        new_manager = next(node for node in new.body if isinstance(node, ast.ClassDef) and node.name == 'JobManager')
        old_submit = next(node for node in old_manager.body if isinstance(node, ast.FunctionDef) and node.name == 'submit_json')
        new_submit = next(node for node in new_manager.body if isinstance(node, ast.FunctionDef) and node.name == 'submit_json')
        self.assertEqual(ast.unparse(new_submit.body[0]), 'raw = _read_json_path(bundle_path)')
        new_submit.body[0] = deepcopy(old_submit.body[0])
        self.assertEqual(ast.dump(new), ast.dump(old))
        # The exact comparison above includes every existing path-confinement,
        # ZIP, decode, schema, decoded-member/total/count and admission operation.

    def test_new_reader_keeps_descriptor_confinement_close_and_io_translation(self):
        tree = ast.parse(repair.corrected_v2_files()[repair.JOBS_PATH])
        helper = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_read_json_path')
        self.assertEqual(ast.unparse(helper.body[1]), 'fd = _path_fd(path)')
        attempt = helper.body[2]
        self.assertIsInstance(attempt, ast.Try)
        self.assertEqual([ast.unparse(node) for node in attempt.finalbody], ['os.close(fd)'])
        self.assertEqual(len(attempt.handlers), 1)
        self.assertEqual(ast.unparse(attempt.handlers[0].type), 'OSError')
        self.assertEqual(ast.unparse(attempt.handlers[0].body[0]), 'raise _io_error(error) from error')
        calls = [ast.unparse(node.func) for node in ast.walk(helper) if isinstance(node, ast.Call)]
        self.assertNotIn('_read_path', calls)
        self.assertNotIn('_read_fd', calls)
        self.assertNotIn('LibraryError', calls)
        self.assertEqual(sum(name == 'os.read' for name in calls), 1)

    def test_input_inventory_covers_transitive_authored_generator_imports(self):
        seen = set()
        def visit(name):
            if name in seen:
                return
            seen.add(name)
            tree = ast.parse((repair._ROOT / name).read_bytes())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module:
                    module = node.module if node.level == 0 else 'gossip_harness.' + node.module
                    if module.startswith('gossip_harness.'):
                        relative = module.replace('.', '/') + '.py'
                        if (repair._ROOT / relative).is_file():
                            visit(relative)
        visit('gossip_harness/library_v2_reference_v1.py')
        pins = repair.corrected_v2_source_inputs()
        self.assertEqual({name for name in pins if name.startswith('gossip_harness/')},
                         seen | {'gossip_harness/library_v2_json_reference_v2.py'})
        self.assertEqual(len(pins), 41)
        pins.clear()
        self.assertEqual(len(repair.corrected_v2_source_inputs()), 41)

    def test_changed_generator_dependency_rejects_before_generation(self):
        original_read = Path.read_bytes
        dependency = repair._ROOT / 'gossip_harness/library_m1_ingestion_reference_v1.py'
        def changed_read(path):
            raw = original_read(path)
            return raw + b'\n# changed\n' if path == dependency else raw
        with patch.object(Path, 'read_bytes', changed_read), patch.object(repair.baseline, 'v2_files') as generation:
            with self.assertRaisesRegex(ValueError, 'Reference source input changed'):
                repair.corrected_v2_files()
            generation.assert_not_called()

    def test_own_source_changed_after_import_rejects_before_generation(self):
        original_read = Path.read_bytes
        def changed_read(path):
            raw = original_read(path)
            return raw + b'\n# modified after import\n' if path == repair._SOURCE else raw
        with patch.object(Path, 'read_bytes', changed_read), patch.object(repair.baseline, 'v2_files') as generation:
            with self.assertRaisesRegex(ValueError, 'Loaded JSON reference repair source changed'):
                repair.corrected_v2_files()
            generation.assert_not_called()

    def test_loaded_generator_output_drift_and_repair_reapplication_reject(self):
        original = repair.baseline.v2_files()
        changed = dict(original)
        changed['library/common.py'] += '\n# loaded-output drift\n'
        with patch.object(repair.baseline, 'v2_files', return_value=changed):
            with self.assertRaisesRegex(ValueError, 'generated source tree changed'):
                repair.corrected_v2_files()
        jobs = original[repair.JOBS_PATH]
        self.assertEqual(hashlib.sha256(jobs.encode()).hexdigest(), repair.BASELINE_JOBS_SHA256)
        for changed_jobs in (jobs + '\n', repair._repair_jobs(jobs)):
            with self.subTest(digest=hashlib.sha256(changed_jobs.encode()).hexdigest()):
                with self.assertRaisesRegex(ValueError, 'Frozen JSON intake source changed'):
                    repair._repair_jobs(changed_jobs)


if __name__ == '__main__':
    unittest.main()
