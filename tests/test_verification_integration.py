import json
from pathlib import Path
import tempfile
import unittest

from gossip_harness.gitstore import GitStore
from gossip_harness.verification_integration import (
    PolicyIntegrationRejected, _exact_text_validator, integrate_policy,
    receipt_path, run_text_conflict_probe, text_hash,
)


class PolicyIntegrationTests(unittest.TestCase):
    def test_real_independent_merge_preserves_backend_and_rejects_stale_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            initial_files={'policy.json':'{"version":1}\n','backend.py':'# initial\n'}
            initial=GitStore.create(root/'initial.git',initial_files)
            current=GitStore.fork(initial,root/'current.git')
            # If any candidate code were executed this file would fail immediately.
            backend='raise RuntimeError("candidate code must not execute")\n'
            tip=current.propose({'backend.py':backend},base_sha=current.head(),message='Existing backend implementation')
            candidate=current.prepare(current,tip,current.head(),_exact_text_validator({**initial_files,'backend.py':backend}),('backend.py',))
            self.assertEqual(current.accept(candidate).status,'accepted')
            before=current.head(); original=initial.head()
            updates={'policy.json':'{"version":2,"include_unknown":true}\n'}
            store,receipt=integrate_policy(initial,current,updates,root/'integrated.git')
            self.assertEqual(store.read_files(),{**initial_files,'backend.py':backend,**updates})
            self.assertEqual(receipt['merge_parents'],[before,receipt['incoming_tip']])
            self.assertEqual(receipt['incoming_changed_paths'],['policy.json'])
            self.assertEqual(receipt['old_evidence_prepare_status'],'prepared')
            self.assertEqual(receipt['stale_probe_status'],'stale')
            self.assertEqual(receipt['merged_head'],receipt['exact_tested_sha'])
            self.assertEqual(receipt['merged_head'],receipt['stale_probe_head'])
            self.assertEqual(receipt['merged_files_sha256'],text_hash(store.read_files()))
            self.assertEqual(current.head(),before); self.assertEqual(initial.head(),original)
            self.assertEqual(json.loads(receipt_path(root/'integrated.git').read_text()),receipt)
            for repo in (store.path,receipt_path(root/'integrated.git').parent/'incoming.git'):
                self.assertFalse((repo/'objects/info/alternates').exists())
            with self.assertRaises(FileExistsError): integrate_policy(initial,current,updates,root/'integrated.git')

    def test_only_complete_policy_update_is_allowed_before_sources_are_read(self):
        for updates in ({},{'backend.py':'unsafe'},{'policy.json':None},{'policy.json':1},[],{'policy.json':'{}','extra':'x'}):
            with self.assertRaises(ValueError): integrate_policy(None,None,updates,Path('/unused'))

    def test_exact_tree_validator_rejects_extra_files_and_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); (root/'policy.json').write_text('{}')
            validator=_exact_text_validator({'policy.json':'{}'})
            self.assertTrue(validator(root)[0])
            (root/'extra').write_text('x'); self.assertFalse(validator(root)[0]); (root/'extra').unlink()
            (root/'policy.json').unlink(); (root/'policy.json').symlink_to('/not/read')
            self.assertFalse(validator(root)[0])

    def test_scripted_real_text_conflict_never_reaches_validator_or_changes_head(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt=run_text_conflict_probe(Path(directory)/'probe')
            self.assertEqual(receipt['prepare_status'],'text_conflict')
            self.assertEqual(receipt['accept_status'],'text_conflict')
            self.assertEqual(receipt['accepted_head_before'],receipt['accepted_head_after'])
            self.assertEqual(receipt['validator_calls'],0)
            self.assertEqual(receipt['model_calls'],0)
            self.assertIn('does not test agent',receipt['claim'])


if __name__=='__main__': unittest.main()
