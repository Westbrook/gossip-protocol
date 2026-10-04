from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.cumulative_rehearsal_inventory_v1 import audit_child_inventory
from gossip_harness.peer_financial_terminal_v1 import FinancialError, sha


class CumulativeRehearsalInventoryV1Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name).resolve()
        self.path=self.root/'roles'/'actor'/'journal.sqlite'
        self.path.parent.mkdir(parents=True)
        self.path.write_bytes(b'original bytes')
        self.files=[{'path':str(self.path),'sha256':sha(self.path.read_bytes()),'size':self.path.stat().st_size}]
        self.addCleanup(self.temp.cleanup)

    def test_complete_original_inventory_is_read_only_scoped_evidence(self):
        proof=audit_child_inventory(self.root,self.files)
        self.assertEqual((proof.file_count,proof.total_bytes),(1,14))
        self.assertFalse(proof.live_qualification)

    def test_missing_or_changed_bytes_are_rejected(self):
        self.path.write_bytes(b'changed bytes!')
        with self.assertRaises(FinancialError):
            audit_child_inventory(self.root,self.files)

    def test_foreign_suffix_is_rejected(self):
        (self.path.parent/'unregistered').write_bytes(b'new')
        with self.assertRaises(FinancialError):
            audit_child_inventory(self.root,self.files)

    def test_indirect_file_is_rejected(self):
        self.path.rename(self.path.parent/'original')
        self.path.symlink_to(self.path.parent/'original')
        with self.assertRaises(FinancialError):
            audit_child_inventory(self.root,self.files)

    def test_outside_root_or_duplicate_manifest_is_rejected(self):
        with self.assertRaises(FinancialError):
            audit_child_inventory(self.root,self.files*2)
        with self.assertRaises(FinancialError):
            audit_child_inventory(self.root,[{**self.files[0],'path':str(self.root.parent/'outside')}])

    def test_oversize_declared_original_rejects_before_open(self):
        with patch('gossip_harness.cumulative_rehearsal_inventory_v1.os.open',side_effect=AssertionError('no byte read')):
            with self.assertRaises(FinancialError):
                audit_child_inventory(self.root,[{**self.files[0],'size':1073741825}])

    def test_shared_ledger_cannot_be_pinned_as_child_file(self):
        ledger=self.root/'ledger.sqlite'
        ledger.write_bytes(b'cumulative ledger')
        with self.assertRaises(FinancialError):
            audit_child_inventory(self.root,[{'path':str(ledger),'sha256':sha(ledger.read_bytes()),'size':ledger.stat().st_size}])
