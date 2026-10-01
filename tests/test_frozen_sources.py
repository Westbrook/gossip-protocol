"""A frozen-source audit never imports or executes an archived Python file."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from devtools import frozen_sources


class FrozenSourcesTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.run, self.output = self.root / 'run', self.root / 'frozen'
        self.source = self.run / 'source-snapshot/gossip_harness/example.py'
        self.source.parent.mkdir(parents=True)
        self.contents = b"raise RuntimeError('Archived code must not execute')\r\n"
        self.source.write_bytes(self.contents)
        self.contract = dict(protocol='test-contract-v1', sources={'example.py': hashlib.sha256(self.contents).hexdigest()})
        self.write_contract()

    def write_contract(self):
        result = dict(contract=self.contract, contract_sha=frozen_sources._digest(self.contract))
        (self.run / 'results.json').write_text(json.dumps(result))
        (self.run / 'manifest.json').write_text(json.dumps(self.contract))

    def test_only_contract_core_bytes_are_materialized_without_execution(self):
        (self.source.parent / 'unlisted.py').write_text('DO_NOT_COPY = True')
        (self.run / 'candidate.py').write_text('raise RuntimeError("Never import candidates")')
        manifest = frozen_sources.materialize(self.run, self.output)
        self.assertFalse(manifest['executed'])
        self.assertFalse(manifest['complete_runtime'])
        self.assertEqual(manifest['protocol'], 'test-contract-v1')
        self.assertEqual(manifest['contract'], self.contract)
        self.assertEqual(manifest['input_sha256']['source-snapshot/gossip_harness/example.py'], hashlib.sha256(self.contents).hexdigest())
        self.assertEqual((self.output / 'gossip_harness/example.py').read_bytes(), self.contents)
        self.assertEqual(sorted(p.relative_to(self.output).as_posix() for p in self.output.rglob('*') if p.is_file()),
                         ['gossip_harness/example.py', 'replay-manifest.json'])
        self.assertEqual(manifest['source_sha256'], {'gossip_harness/example.py': hashlib.sha256(self.contents).hexdigest()})

    def test_tampered_source_rejected_before_output(self):
        self.source.write_text('tampered')
        with self.assertRaisesRegex(ValueError, 'differs from its contract'):
            frozen_sources.materialize(self.run, self.output)
        self.assertFalse(self.output.exists())

    def test_manifest_and_contract_digest_must_agree(self):
        (self.run / 'manifest.json').write_text(json.dumps({'sources': {}}))
        with self.assertRaisesRegex(ValueError, 'source contracts differ'):
            frozen_sources.materialize(self.run, self.output)
        self.assertFalse(self.output.exists())
        self.write_contract()
        (self.run / 'results.json').write_text(json.dumps(dict(contract=self.contract, contract_sha='wrong')))
        with self.assertRaisesRegex(ValueError, 'contract digest'):
            frozen_sources.materialize(self.run, self.output)
        self.assertFalse(self.output.exists())

    def test_symlink_and_traversal_sources_are_rejected(self):
        self.source.unlink()
        outside = self.root / 'outside.py'
        outside.write_bytes(self.contents)
        self.source.symlink_to(outside)
        with self.assertRaisesRegex(ValueError, 'symlinks'):
            frozen_sources.materialize(self.run, self.output)
        self.assertFalse(self.output.exists())
        self.contract['sources'] = {'../outside.py': hashlib.sha256(self.contents).hexdigest()}
        self.write_contract()
        with self.assertRaisesRegex(ValueError, 'path component'):
            frozen_sources.materialize(self.run, self.output)
        self.assertFalse(self.output.exists())

    def test_mid_export_change_is_rejected_and_staging_removed(self):
        write = frozen_sources._write_export
        def mutate(root, files):
            write(root, files)
            self.source.write_text('changed')
        with patch.object(frozen_sources, '_write_export', side_effect=mutate):
            with self.assertRaisesRegex(ValueError, 'Input evidence changed'):
                frozen_sources.materialize(self.run, self.output)
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.glob('.frozen.staging-*')), [])

    def test_case_colliding_contract_sources_are_rejected_without_output(self):
        (self.source.parent / 'EXAMPLE.py').write_bytes(self.contents)
        self.contract['sources']['EXAMPLE.py'] = hashlib.sha256(self.contents).hexdigest()
        self.write_contract()
        with self.assertRaisesRegex(ValueError, 'colliding paths'):
            frozen_sources.materialize(self.run, self.output)
        self.assertFalse(self.output.exists())

    def test_existing_destination_is_preserved(self):
        self.output.mkdir()
        marker = self.output / 'keep'
        marker.write_text('user data')
        with self.assertRaises(FileExistsError):
            frozen_sources.materialize(self.run, self.output)
        self.assertEqual(marker.read_text(), 'user data')


if __name__ == '__main__':
    unittest.main()
