"""Owned-host staging controls only: no candidate, container, provider, or verdict.

Real filesystem fixtures are isolated under a canonical temporary parent. Unsafe
link targets in negative controls point only at disposable owned sentinels.
"""
from __future__ import annotations

from dataclasses import FrozenInstanceError
import hashlib
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock

from gossip_harness import candidate_http_cases_core_v1 as core
from gossip_harness import candidate_http_cases_v1 as catalog
from gossip_harness import candidate_http_inputs_v1 as inputs


def ordered(*entries: core.Fixture) -> tuple[core.Fixture, ...]:
    return tuple(sorted(entries, key=lambda item: item.path))


def forged(**changes: object) -> core.Fixture:
    """An exact typed object whose frozen constructor was deliberately bypassed."""
    value = object.__new__(core.Fixture)
    fields: dict[str, object] = {"path": "leaf.txt", "kind": "file", "data": b"x", "target": ""}
    fields.update(changes)
    for name, content in fields.items():
        object.__setattr__(value, name, content)
    return value


def layout() -> tuple[core.Fixture, ...]:
    return ordered(core.Fixture("a-link", "symlink", target="z-dir/file.txt"),
                   core.Fixture("empty", "directory"), core.Fixture("z-dir", "directory"),
                   core.Fixture("z-dir/file.txt", "file", b"line\r\n\0\xff"),
                   core.Fixture("zero.txt", "file", b""))


class HttpInputDeclarationsV1Tests(unittest.TestCase):
    def test_empty_manifest_and_exact_file_directory_link_records(self) -> None:
        self.assertIsNone(inputs.validate(()))
        self.assertEqual(inputs.manifest(()), {"protocol": "candidate-http-inputs-v1", "entries": [],
                                             "entry_count": 0, "file_bytes": 0})
        entries = layout()
        expected_records = [
            {"path": "a-link", "kind": "symlink", "bytes": None, "sha256": None, "target": "z-dir/file.txt"},
            {"path": "empty", "kind": "directory", "bytes": None, "sha256": None, "target": None},
            {"path": "z-dir", "kind": "directory", "bytes": None, "sha256": None, "target": None},
            {"path": "z-dir/file.txt", "kind": "file", "bytes": 8,
             "sha256": hashlib.sha256(b"line\r\n\0\xff").hexdigest(), "target": None},
            {"path": "zero.txt", "kind": "file", "bytes": 0,
             "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "target": None},
        ]
        self.assertEqual(inputs.manifest(entries), {"protocol": "candidate-http-inputs-v1", "entries": expected_records,
                                                  "entry_count": 5, "file_bytes": 8})
        mutated = inputs.manifest(entries)
        mutated["entries"][0]["target"] = "other"
        self.assertEqual(inputs.manifest(entries)["entries"], expected_records)
        with self.assertRaises(FrozenInstanceError):
            entries[0].target = "other"

    def test_typed_immutable_entries_and_duplicate_destinations_are_required(self) -> None:
        item = core.Fixture("file", "file", b"x")
        for entries in ([item], (object(),), (item, item)):
            with self.subTest(entries=entries), self.assertRaises(inputs.InputError):
                inputs.validate(entries)

    def test_forged_fixture_is_revalidated_at_the_staging_boundary(self) -> None:
        invalid = (forged(path="/absolute"), forged(path="../escape"), forged(path="a//b"),
                   forged(path="a/./b"), forged(path="bad\0name"), forged(path="back\\slash"),
                   forged(kind="fifo"), forged(data=bytearray(b"x")),
                   forged(kind="directory", data=b"unexpected"),
                   forged(kind="file", target="unexpected"),
                   forged(kind="symlink", data=b"", target="/absolute"),
                   forged(kind="symlink", data=b"", target="../outer"))
        for item in invalid:
            with self.subTest(path=item.path, kind=item.kind), self.assertRaises(inputs.InputError):
                inputs.validate((item,))

    def test_count_bound_is_isolated_with_unique_valid_zero_byte_files(self) -> None:
        entries = tuple(core.Fixture(f"f{i:04d}", "file") for i in range(inputs.MAX_ENTRIES + 1))
        self.assertEqual(inputs.MAX_ENTRIES, 1024)
        self.assertEqual(len({item.path for item in entries}), 1025)
        self.assertIsNone(inputs.validate(entries[:-1]))
        with self.assertRaises(inputs.InputError):
            inputs.validate(entries)

    def test_total_byte_bound_is_isolated_from_member_and_count_bounds(self) -> None:
        self.assertEqual(inputs.MAX_BYTES, 8 * 1024 * 1024)
        half = inputs.MAX_BYTES // 2
        exact = (core.Fixture("a", "file", b"a" * half), core.Fixture("b", "file", b"b" * half))
        over = (exact[0], core.Fixture("b", "file", b"b" * (half + 1)))
        self.assertIsNone(inputs.validate(exact))
        self.assertEqual(sum(len(item.data) for item in over), inputs.MAX_BYTES + 1)
        self.assertTrue(all(len(item.data) < inputs.MAX_BYTES for item in over))
        with self.assertRaises(inputs.InputError):
            inputs.validate(over)

    def test_every_parent_must_be_explicit_and_a_real_directory(self) -> None:
        examples = (
            (core.Fixture("a/b", "file", b"x"),),
            ordered(core.Fixture("a", "file", b"x"), core.Fixture("a/b", "file", b"x")),
            ordered(core.Fixture("a", "directory"), core.Fixture("a/b/c", "file", b"x")),
            ordered(core.Fixture("a", "symlink", target="real"), core.Fixture("a/child", "file", b"x"),
                    core.Fixture("real", "directory")),
        )
        for entries in examples:
            with self.subTest(entries=entries), self.assertRaises(inputs.InputError):
                inputs.validate(entries)

    def test_link_chains_cycles_self_links_and_missing_targets_are_rejected(self) -> None:
        examples = (
            ordered(core.Fixture("a", "symlink", target="b"), core.Fixture("b", "symlink", target="target"),
                    core.Fixture("target", "file", b"x")),
            ordered(core.Fixture("a", "symlink", target="b"), core.Fixture("b", "symlink", target="a")),
            (core.Fixture("a", "symlink", target="a"),),
            (core.Fixture("a", "symlink", target="missing"),),
            ordered(core.Fixture("a", "symlink", target="real/missing"), core.Fixture("real", "directory")),
        )
        for entries in examples:
            with self.subTest(entries=entries), self.assertRaises(inputs.InputError):
                inputs.validate(entries)

    def test_raw_link_then_parent_traversal_cannot_be_normalized_safe(self) -> None:
        entries = ordered(core.Fixture("dir", "directory"),
            core.Fixture("dir/jump", "symlink", target="../elsewhere"), core.Fixture("elsewhere", "directory"),
            core.Fixture("dir/safe.txt", "file", b"safe"),
            core.Fixture("link", "symlink", target="dir/jump/../safe.txt"))
        with self.assertRaises(inputs.InputError):
            inputs.validate(entries)
        through_file = ordered(core.Fixture("file", "file", b"x"), core.Fixture("target", "file", b"target"),
                               core.Fixture("link", "symlink", target="file/../target"))
        with self.assertRaises(inputs.InputError):
            inputs.validate(through_file)

    def test_literal_parent_traversal_through_real_owned_directories_is_allowed(self) -> None:
        entries = ordered(core.Fixture("dir", "directory"), core.Fixture("target", "file", b"x"),
                          core.Fixture("link", "symlink", target="dir/../target"))
        self.assertIsNone(inputs.validate(entries))
        cross_root = ordered(core.Fixture("root", "directory"), core.Fixture("other", "directory"),
            core.Fixture("other/target", "file", b"owned outer-input sentinel"),
            core.Fixture("root/link", "symlink", target="../other/target"))
        self.assertIsNone(inputs.validate(cross_root))

    def test_all_held_catalog_fixtures_fit_the_new_input_contract(self) -> None:
        cases = catalog.definitions()
        self.assertEqual(len(cases), 276)
        for case in cases:
            with self.subTest(row=case.row_id):
                self.assertIsNone(inputs.validate(case.fixtures))
                manifest = inputs.manifest(case.fixtures)
                self.assertEqual(manifest["entry_count"], len(case.fixtures))
                self.assertEqual(manifest["file_bytes"], sum(len(item.data) for item in case.fixtures))
                self.assertLessEqual(len(case.fixtures), inputs.MAX_ENTRIES)


class HttpInputStagingV1Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="gossip-http-inputs-stage-")
        self.addCleanup(self.temporary.cleanup)
        self.parent = Path(self.temporary.name).resolve()
        self.root = self.parent / "inputs"
        self.root.mkdir()

    def test_stage_then_verify_preserves_exact_bytes_types_targets_and_modes(self) -> None:
        entries = layout()
        self.assertIsNone(inputs.stage(self.root, entries))
        self.assertIsNone(inputs.verify(self.root, entries))
        self.assertEqual(os.readlink(self.root / "a-link"), "z-dir/file.txt")
        self.assertTrue(stat.S_ISLNK((self.root / "a-link").lstat().st_mode))
        self.assertEqual((self.root / "z-dir/file.txt").read_bytes(), b"line\r\n\0\xff")
        self.assertEqual((self.root / "zero.txt").read_bytes(), b"")
        self.assertEqual(list((self.root / "empty").iterdir()), [])
        for path in ("z-dir/file.txt", "zero.txt"):
            info = (self.root / path).lstat()
            self.assertTrue(stat.S_ISREG(info.st_mode))
            self.assertEqual(stat.S_IMODE(info.st_mode), 0o444)
            self.assertEqual(info.st_nlink, 1)
        for path in ("empty", "z-dir"):
            self.assertEqual(stat.S_IMODE((self.root / path).lstat().st_mode), 0o755)
        self.assertEqual({path.relative_to(self.root).as_posix() for path in self.root.rglob("*")},
                         {item.path for item in entries})

    def test_empty_stage_and_root_permissions_are_not_product_constraints(self) -> None:
        os.chmod(self.root, 0o700)
        self.assertIsNone(inputs.stage(self.root, ()))
        self.assertIsNone(inputs.verify(self.root, ()))
        self.assertEqual(tuple(self.root.iterdir()), ())
        self.assertEqual(stat.S_IMODE(self.root.lstat().st_mode), 0o700)

    def test_nonempty_root_is_never_overwritten_or_cleared(self) -> None:
        sentinel = self.root / "sentinel"
        sentinel.write_bytes(b"keep exactly")
        for entries in ((), (core.Fixture("sentinel", "file", b"replace"),)):
            with self.subTest(entries=entries), self.assertRaises(inputs.InputError):
                inputs.stage(self.root, entries)
            self.assertEqual(sentinel.read_bytes(), b"keep exactly")
            self.assertEqual(tuple(path.name for path in self.root.iterdir()), ("sentinel",))

    def test_root_must_exist_and_be_canonical_absolute_real_directory(self) -> None:
        outside = self.parent / "real"
        outside.mkdir()
        linked = self.parent / "linked"
        linked.symlink_to(outside, target_is_directory=True)
        regular = self.parent / "file"
        regular.write_bytes(b"regular")
        invalid = (self.parent / "missing", regular, linked, Path("relative-inputs"),
                   Path(str(self.root) + "/../inputs"))
        for root in invalid:
            with self.subTest(root=root), self.assertRaises(inputs.InputError):
                inputs.stage(root, ())
        self.assertFalse((self.parent / "missing").exists())
        self.assertEqual(tuple(outside.iterdir()), ())
        self.assertEqual(regular.read_bytes(), b"regular")

    def test_symlink_in_an_ancestor_is_rejected_before_staging(self) -> None:
        real = self.parent / "real"
        real.mkdir()
        (real / "child").mkdir()
        alias = self.parent / "alias"
        alias.symlink_to(real, target_is_directory=True)
        with self.assertRaises(inputs.InputError):
            inputs.stage(alias / "child", (core.Fixture("file", "file", b"x"),))
        self.assertEqual(tuple((real / "child").iterdir()), ())

    def test_invalid_graph_is_rejected_before_any_tree_mutation(self) -> None:
        entries = ordered(core.Fixture("a", "file", b"would be created"), core.Fixture("z", "symlink", target="missing"))
        with self.assertRaises(inputs.InputError):
            inputs.stage(self.root, entries)
        self.assertEqual(tuple(self.root.iterdir()), ())

    def test_links_are_created_only_after_all_regular_targets_exist(self) -> None:
        entries = layout()
        real_symlink = os.symlink
        seen: list[str] = []

        def inspecting_symlink(target: str, destination: object, *args: object, **kwargs: object) -> None:
            self.assertEqual((self.root / "z-dir/file.txt").read_bytes(), b"line\r\n\0\xff")
            self.assertTrue((self.root / "empty").is_dir())
            self.assertTrue((self.root / "zero.txt").is_file())
            seen.append(target)
            real_symlink(target, destination, *args, **kwargs)

        with mock.patch.object(inputs.os, "symlink", side_effect=inspecting_symlink):
            inputs.stage(self.root, entries)
        self.assertEqual(seen, ["z-dir/file.txt"])
        inputs.verify(self.root, entries)

    def test_partial_creation_is_retained_after_an_injected_filesystem_failure(self) -> None:
        entries = (core.Fixture("a.txt", "file", b"first"), core.Fixture("b.txt", "file", b"second"))
        original_write = os.write
        rejected = False

        def failing_write(descriptor: int, data: bytes) -> int:
            nonlocal rejected
            if data == b"second":
                rejected = True
                raise OSError("controlled owned staging write failure")
            return original_write(descriptor, data)

        with mock.patch.object(inputs.os, "write", side_effect=failing_write):
            with self.assertRaises(inputs.InputError):
                inputs.stage(self.root, entries)
        self.assertTrue(rejected)
        self.assertEqual((self.root / "a.txt").read_bytes(), b"first")
        self.assertEqual((self.root / "b.txt").read_bytes(), b"")
        self.assertEqual({path.name for path in self.root.iterdir()}, {"a.txt", "b.txt"})
        with self.assertRaises(inputs.InputError):
            inputs.stage(self.root, entries)  # A second call cannot erase partial evidence.
        self.assertEqual((self.root / "a.txt").read_bytes(), b"first")

    def test_short_regular_file_writes_preserve_all_declared_bytes(self) -> None:
        original_write = os.write
        calls = 0

        def short_write(descriptor: int, data: bytes) -> int:
            nonlocal calls
            calls += 1
            return original_write(descriptor, data[:2])

        entries = (core.Fixture("file.txt", "file", b"0123456789"),)
        with mock.patch.object(inputs.os, "write", side_effect=short_write):
            inputs.stage(self.root, entries)
        self.assertEqual(calls, 5)
        self.assertEqual((self.root / "file.txt").read_bytes(), b"0123456789")
        inputs.verify(self.root, entries)

    def test_unicode_and_binary_data_have_no_text_or_newline_conversion(self) -> None:
        entries = ordered(core.Fixture("unicode", "directory"),
                          core.Fixture("unicode/é.txt", "file", "café\r\n".encode()),
                          core.Fixture("binary", "file", bytes(range(256))))
        inputs.stage(self.root, entries)
        inputs.verify(self.root, entries)
        self.assertEqual((self.root / "unicode/é.txt").read_bytes(), b"caf\xc3\xa9\r\n")
        self.assertEqual((self.root / "binary").read_bytes(), bytes(range(256)))

    def test_actual_six_catalog_data_link_layouts_stage_in_separate_owned_trees(self) -> None:
        chosen = tuple(case for case in catalog.definitions() if case.row_id.startswith("HTTP-ROOT-PATH/")
                       and case.row_id.endswith(("-direct-link", "-ancestor-link")))
        self.assertEqual(len(chosen), 6)
        for index, case in enumerate(chosen):
            with self.subTest(row=case.row_id):
                root = self.root / f"case-{index}"
                root.mkdir()
                inputs.stage(root, case.fixtures)
                inputs.verify(root, case.fixtures)
                links = tuple(item for item in case.fixtures if item.kind == "symlink")
                self.assertEqual(len(links), 1)
                link = links[0]
                self.assertTrue(link.path.startswith("root/"))
                self.assertTrue(link.target.startswith("../other"))
                self.assertEqual(os.readlink(root / link.path), link.target)
                target = (root / link.path).resolve(strict=True)
                self.assertTrue(target.is_relative_to(root))
                self.assertFalse(target.is_relative_to(root / "root"))


class HttpInputVerificationV1Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="gossip-http-inputs-verify-")
        self.addCleanup(self.temporary.cleanup)
        self.parent = Path(self.temporary.name).resolve()
        self.root = self.parent / "inputs"
        self.root.mkdir()
        self.entries = ordered(core.Fixture("dir", "directory"), core.Fixture("dir/file.txt", "file", b"original"),
                               core.Fixture("link", "symlink", target="dir/file.txt"))
        inputs.stage(self.root, self.entries)

    def test_changed_bytes_with_restored_mode_are_rejected(self) -> None:
        path = self.root / "dir/file.txt"
        os.chmod(path, 0o644)
        path.write_bytes(b"modified")  # Same byte length isolates byte identity from size/mode.
        os.chmod(path, 0o444)
        self.assertEqual(path.stat().st_size, len(b"original"))
        with self.assertRaises(inputs.InputError):
            inputs.verify(self.root, self.entries)

    def test_changed_size_with_restored_mode_is_rejected(self) -> None:
        path = self.root / "dir/file.txt"
        os.chmod(path, 0o644)
        path.write_bytes(b"original plus extra")
        os.chmod(path, 0o444)
        with self.assertRaises(inputs.InputError):
            inputs.verify(self.root, self.entries)

    def test_equivalent_but_different_literal_readlink_target_is_rejected(self) -> None:
        link = self.root / "link"
        link.unlink()
        link.symlink_to("./dir/file.txt")
        self.assertEqual(link.read_bytes(), b"original")
        with self.assertRaises(inputs.InputError):
            inputs.verify(self.root, self.entries)

    def test_regular_file_replaced_by_link_to_identical_owned_bytes_is_rejected(self) -> None:
        outside = self.parent / "outside.txt"
        outside.write_bytes(b"original")
        os.chmod(outside, 0o444)
        path = self.root / "dir/file.txt"
        path.unlink()
        path.symlink_to("../../outside.txt")
        with self.assertRaises(inputs.InputError):
            inputs.verify(self.root, self.entries)
        self.assertEqual(outside.read_bytes(), b"original")

    def test_directory_replaced_by_link_to_identical_owned_tree_is_rejected(self) -> None:
        original = self.root / "dir"
        outside = self.parent / "outside-dir"
        original.rename(outside)
        original.symlink_to("../outside-dir", target_is_directory=True)
        with self.assertRaises(inputs.InputError):
            inputs.verify(self.root, self.entries)
        self.assertEqual((outside / "file.txt").read_bytes(), b"original")

    def test_declared_link_replaced_by_a_regular_file_is_rejected(self) -> None:
        path = self.root / "link"
        path.unlink()
        path.write_bytes(b"original")
        os.chmod(path, 0o444)
        with self.assertRaises(inputs.InputError):
            inputs.verify(self.root, self.entries)

    def test_missing_entry_is_rejected(self) -> None:
        (self.root / "dir/file.txt").unlink()
        with self.assertRaises(inputs.InputError):
            inputs.verify(self.root, self.entries)

    def test_extra_nested_entry_is_rejected_without_erasing_it(self) -> None:
        extra = self.root / "dir/unregistered.txt"
        extra.write_bytes(b"keep me")
        os.chmod(extra, 0o444)
        with self.assertRaises(inputs.InputError):
            inputs.verify(self.root, self.entries)
        self.assertEqual(extra.read_bytes(), b"keep me")

    def test_extra_dangling_link_is_inventory_drift(self) -> None:
        (self.root / "extra-link").symlink_to("missing-owned-target")
        with self.assertRaises(inputs.InputError):
            inputs.verify(self.root, self.entries)
        self.assertEqual(os.readlink(self.root / "extra-link"), "missing-owned-target")

    def test_file_and_directory_mode_changes_are_rejected_independently(self) -> None:
        path = self.root / "dir/file.txt"
        os.chmod(path, 0o644)
        with self.assertRaises(inputs.InputError):
            inputs.verify(self.root, self.entries)
        os.chmod(path, 0o444)
        os.chmod(self.root / "dir", 0o700)
        with self.assertRaises(inputs.InputError):
            inputs.verify(self.root, self.entries)
        os.chmod(self.root / "dir", 0o755)
        inputs.verify(self.root, self.entries)

    def test_extra_hardlink_is_rejected_even_when_bytes_and_modes_match(self) -> None:
        outside = self.parent / "hardlink.txt"
        os.link(self.root / "dir/file.txt", outside)
        self.assertEqual(outside.read_bytes(), b"original")
        self.assertEqual(stat.S_IMODE(outside.stat().st_mode), 0o444)
        self.assertEqual(outside.stat().st_nlink, 2)
        with self.assertRaises(inputs.InputError):
            inputs.verify(self.root, self.entries)

    def test_fifo_replacing_a_regular_file_is_rejected_without_blocking_read(self) -> None:
        path = self.root / "dir/file.txt"
        path.unlink()
        os.mkfifo(path, 0o444)
        self.assertTrue(stat.S_ISFIFO(path.lstat().st_mode))
        with self.assertRaises(inputs.InputError):
            inputs.verify(self.root, self.entries)

    def test_same_bytes_in_a_replaced_inode_during_read_are_rejected(self) -> None:
        original_read = os.read
        replaced = False

        def replacing_read(descriptor: int, count: int) -> bytes:
            nonlocal replaced
            data = original_read(descriptor, count)
            if data == b"original" and not replaced:
                path = self.root / "dir/file.txt"
                path.unlink()
                path.write_bytes(b"original")
                os.chmod(path, 0o444)
                replaced = True
            return data

        with mock.patch.object(inputs.os, "read", side_effect=replacing_read):
            with self.assertRaises(inputs.InputError):
                inputs.verify(self.root, self.entries)
        self.assertTrue(replaced)
        self.assertEqual((self.root / "dir/file.txt").read_bytes(), b"original")

    def test_symlink_root_and_parent_alias_are_rejected_without_following(self) -> None:
        link = self.parent / "root-link"
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(inputs.InputError):
            inputs.verify(link, self.entries)
        parent_alias = self.parent / "parent-link"
        parent_alias.symlink_to(self.parent, target_is_directory=True)
        with self.assertRaises(inputs.InputError):
            inputs.verify(parent_alias / "inputs", self.entries)

    def test_regular_file_swap_to_link_at_open_is_rejected(self) -> None:
        outside = self.parent / "outside.txt"
        outside.write_bytes(b"original")
        os.chmod(outside, 0o444)
        original_open = os.open
        swapped = False

        def swapping_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
            nonlocal swapped
            if path == "file.txt" and kwargs.get("dir_fd") is not None and not swapped:
                target = self.root / "dir/file.txt"
                target.unlink()
                target.symlink_to("../../outside.txt")
                swapped = True
            return original_open(path, flags, *args, **kwargs)

        with mock.patch.object(inputs.os, "open", side_effect=swapping_open):
            with self.assertRaises(inputs.InputError):
                inputs.verify(self.root, self.entries)
        self.assertTrue(swapped, "the controlled replacement must occur at the real file-open boundary")
        self.assertEqual(outside.read_bytes(), b"original")


if __name__ == "__main__":
    unittest.main()
