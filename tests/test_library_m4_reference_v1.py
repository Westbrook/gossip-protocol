"""Cumulative authored M4 source and binary fixture integration checks."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m3_reference_v1 import m3_files
from gossip_harness.library_m4_fixture_v1 import fixture_files
from gossip_harness.library_m4_reference_v1 import m4_binary_files, m4_files, write_m4_project
from gossip_harness.library_project_fixture_v1 import IMMUTABLE_PATHS, public_cases


class LibraryM4CompositionTests(unittest.TestCase):
    def test_complete_composition_preserves_frozen_inputs_and_public_binary_bytes(self):
        files, previous = m4_files(), m3_files()
        self.assertEqual({p: files[p] for p in IMMUTABLE_PATHS},
                         {p: previous[p] for p in IMMUTABLE_PATHS})
        self.assertEqual(set(m4_binary_files()), {"compatibility/v0.sqlite3", "compatibility/m2.sqlite3"})
        self.assertFalse(set(files).intersection(m4_binary_files()))
        for name, source in files.items():
            if name.endswith(".py"):
                compile(source, name, "exec")
        with tempfile.TemporaryDirectory(prefix="authored-m4-compose-") as parent:
            root = Path(parent) / "app"
            write_m4_project(root)
            observed = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}
            expected = {p: s.encode("utf-8") for p, s in files.items()} | m4_binary_files()
            self.assertEqual(observed, expected)
            for name, data in fixture_files().items():
                self.assertEqual(hashlib.sha256((root/name).read_bytes()).hexdigest(), hashlib.sha256(data).hexdigest())
            with self.assertRaises(FileExistsError):
                write_m4_project(root)
            self.assertEqual(observed, {str(p.relative_to(root)):p.read_bytes() for p in root.rglob("*") if p.is_file()})
        files["solution.py"] = "changed"
        self.assertEqual(m4_files()["solution.py"], previous["solution.py"])


class LibraryM4CumulativeReferenceTests(unittest.TestCase):
    def test_inherited_m1_histories_execute_against_full_m4(self):
        cases = public_cases("m1")
        with tempfile.TemporaryDirectory(prefix="authored-m4-cumulative-") as parent:
            root = Path(parent).resolve() / "app"
            write_m4_project(root)
            script = (
                "import json, resource, sys\n"
                "resource.setrlimit(resource.RLIMIT_CPU, (15,15))\n"
                "resource.setrlimit(resource.RLIMIT_FSIZE, (67108864,67108864))\n"
                "resource.setrlimit(resource.RLIMIT_NOFILE, (96,96))\n"
                "sys.path.insert(0,sys.argv[1])\n"
                "from solution import solve\n"
                + f"cases=json.loads({json.dumps(cases)!r})\n"
                + "print(json.dumps({'count':len(cases),'failures':[c['id'] for c in cases if solve(c['input']) != c['expected']]}))\n"
            )
            result = subprocess.run([sys.executable,"-I","-c",script,str(root)],cwd=root,
                                    capture_output=True,text=True,timeout=30,check=False)
            self.assertEqual(result.returncode,0,result.stderr[:6000])
            self.assertLessEqual(len(result.stdout.encode()),65536)
            self.assertEqual(json.loads(result.stdout),{"count":8,"failures":[]})
