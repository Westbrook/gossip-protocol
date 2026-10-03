"""V2 authored source composition, immutable inputs and inherited real histories."""
from __future__ import annotations

import ast
import json
from pathlib import Path
import subprocess
import sys
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.library_m3_reference_v1 import m3_files
from gossip_harness.library_m4_reference_v1 import m4_binary_files, m4_files
from gossip_harness.library_project_fixture_v1 import IMMUTABLE_PATHS, public_cases
from gossip_harness.library_v2_reference_v1 import (
    _overlay, schema3_v2_files, v2_binary_files, v2_files,
    write_schema3_v2_project, write_v2_project,
)


class LibraryV2CompositionTests(unittest.TestCase):
    def test_full_v2_and_supported_predecessor_preserve_immutable_inputs(self):
        source, previous = v2_files(), m4_files()
        predecessor = schema3_v2_files()
        self.assertEqual({p: source[p] for p in IMMUTABLE_PATHS}, {p: previous[p] for p in IMMUTABLE_PATHS})
        self.assertEqual({p: predecessor[p] for p in IMMUTABLE_PATHS}, {p: m3_files()[p] for p in IMMUTABLE_PATHS})
        self.assertEqual(v2_binary_files(), m4_binary_files())
        self.assertFalse(set(source).intersection(v2_binary_files()))
        for files in (source, predecessor):
            for name, text in files.items():
                if name.endswith('.py'):
                    compile(text, name, 'exec')
            tree = ast.parse(files['library/catalog/store.py'])
            store = next(node for node in tree.body if isinstance(node, ast.ClassDef))
            self.assertEqual(ast.unparse(store.bases[0]), 'V2MaintenanceMixin')
            self.assertIn('library/counters.py', files)
            self.assertIn('library/catalog/v2_maintenance.py', files)
        source['solution.py'] = 'changed'
        self.assertEqual(v2_files()['solution.py'], previous['solution.py'])

    def test_staging_is_exact_no_clobber_and_transform_ownership_is_exclusive(self):
        with ArtifactDirectory('v2-source-staging') as artifacts:
            root = artifacts.root.resolve() / 'v2'
            write_v2_project(root)
            observed = {str(path.relative_to(root)): path.read_bytes() for path in root.rglob('*') if path.is_file()}
            expected = {name: text.encode() for name, text in v2_files().items()} | v2_binary_files()
            self.assertEqual(observed, expected)
            with self.assertRaises(FileExistsError):
                write_v2_project(root)
            self.assertEqual(observed, {str(path.relative_to(root)): path.read_bytes() for path in root.rglob('*') if path.is_file()})
            schema3 = artifacts.root.resolve() / 'schema3'
            write_schema3_v2_project(schema3)
            self.assertEqual({str(path.relative_to(schema3)): path.read_bytes() for path in schema3.rglob('*') if path.is_file()},
                             {name: text.encode() for name, text in schema3_v2_files().items()})
        base, owners = {'old.py': 'historical'}, set()
        _overlay(base, {'old.py': 'prospective'}, owners)
        with self.assertRaises(ValueError):
            _overlay(base, {'old.py': 'second owner'}, owners)
        self.assertEqual(base, {'old.py': 'prospective'})


class LibraryV2InheritedReferenceTests(unittest.TestCase):
    def test_frozen_m1_public_histories_execute_against_composed_v2(self):
        cases = public_cases('m1')
        with ArtifactDirectory('v2-inherited-m1') as artifacts:
            root = artifacts.root.resolve() / 'app'
            write_v2_project(root)
            script = ('import json,resource,sys\n'
                      'resource.setrlimit(resource.RLIMIT_CPU,(15,15))\n'
                      'resource.setrlimit(resource.RLIMIT_FSIZE,(67108864,67108864))\n'
                      'resource.setrlimit(resource.RLIMIT_NOFILE,(96,96))\n'
                      'sys.path.insert(0,sys.argv[1])\nfrom solution import solve\n'
                      + f'cases=json.loads({json.dumps(cases)!r})\n'
                      + 'print(json.dumps({"count":len(cases),"failures":[c["id"] for c in cases if solve(c["input"])!=c["expected"]]}))\n')
            result = subprocess.run([sys.executable, '-I', '-B', '-c', script, str(root)], cwd=root,
                                    capture_output=True, timeout=30, check=False)
            (artifacts.root / 'stdout.txt').write_bytes(result.stdout)
            (artifacts.root / 'stderr.txt').write_bytes(result.stderr)
            self.assertEqual(result.returncode, 0, result.stderr.decode(errors='replace')[:6000])
            self.assertLessEqual(len(result.stdout), 65536)
            self.assertEqual(json.loads(result.stdout), {'count': 8, 'failures': []})
