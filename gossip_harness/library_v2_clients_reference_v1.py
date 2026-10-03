"""Authored prospective-v2 interface overlays; not candidate acceptance evidence.

The frozen cumulative-v1 modules remain unchanged. This composition preserves
inherited routes while validating exact signed64 tokens before Store access.
"""
from __future__ import annotations

from textwrap import dedent


def _source(value: str) -> str:
    return dedent(value).lstrip("\n").rstrip() + "\n"


_SERVICE = _source(r'''
    """Prospective-v2 transport validation over the durable catalog."""
    from library.common import LibraryError
    from library.counters import counter
    from library.query.legacy_v1_service import Service as LegacyService, decimal, canonical_export_bytes

    _V2_CONFLICTS = frozenset(('counter_exhausted', 'stale_instance',
                              'backup_root_unbound', 'backup_root_mismatch'))

    def decimal_counter(value):
        return counter(decimal(value))

    def decimal_version(value):
        return counter(decimal(value), minimum=1)

    class Service(LegacyService):
        def __init__(self, store, root, *, backup_dir=None):
            super().__init__(store, root, backup_dir=backup_dir)
            self._explicit_backup_target = backup_dir is not None

        def lifecycle_list(self, query='', *, tag=None, collection=None,
                           deleted='active', offset=0, limit=100, generation=None):
            if generation is not None:
                counter(generation)
            return super().lifecycle_list(query, tag=tag, collection=collection,
                deleted=deleted, offset=offset, limit=limit, generation=generation)

        def list_v1(self, query='', *, tag=None, collection=None,
                    deleted='active', offset=0, limit=100, generation=None):
            if generation is not None:
                counter(generation)
            return super().list_v1(query, tag=tag, collection=collection,
                deleted=deleted, offset=offset, limit=limit, generation=generation)

        def refresh_document(self, document_id, expected_version, *, text=None, path=None):
            counter(expected_version, minimum=1)
            return super().refresh_document(document_id, expected_version, text=text, path=path)

        def replace_annotations(self, document_id, expected_version, notes, tags, collections):
            counter(expected_version, minimum=1)
            return super().replace_annotations(document_id, expected_version, notes, tags, collections)

        def delete_document(self, document_id, expected_version):
            counter(expected_version, minimum=1)
            return super().delete_document(document_id, expected_version)

        def restore_document(self, document_id, expected_version):
            counter(expected_version, minimum=1)
            return super().restore_document(document_id, expected_version)

        def create_collection(self, name, expected_generation):
            counter(expected_generation)
            return super().create_collection(name, expected_generation)

        def remove_collection(self, name, expected_generation):
            counter(expected_generation)
            return super().remove_collection(name, expected_generation)

        def restore_backup(self, name, expected_generation):
            counter(expected_generation)
            return super().restore_backup(name, expected_generation)

        def commit_job(self, job_id, epoch):
            counter(epoch, minimum=1)
            return super().commit_job(job_id, epoch)

        def adopt_backup_root(self, expected_root):
            # Process-only API: no HTTP/browser route delegates to this method.
            if not self._explicit_backup_target:
                raise LibraryError('invalid_request')
            return self.store.adopt_backup_root(expected_root)

        def request(self, method, target, body=None):
            status, value = super().request(method, target, body)
            if type(value) is dict and set(value) == {'error'} and value['error'] in _V2_CONFLICTS:
                return 409, value
            return status, value
''')

_ADOPT_PARSER = _source(r'''
    command = commands.add_parser('backup-root-adopt')
    expectation = command.add_mutually_exclusive_group(required=True)
    expectation.add_argument('--expect-unbound', action='store_true')
    expectation.add_argument('--expect-root')
''')


def _replace(source: str, old: str, new: str, *, count: int = 1) -> str:
    if source.count(old) != count:
        raise ValueError(f"Frozen cumulative-v1 interface anchor changed: {old!r}")
    return source.replace(old, new, count)


def clients_files(base: dict[str, str]) -> dict[str, str]:
    """Return owned query/client replacements; leave ``base`` unchanged."""
    cli = base["library/clients/cli.py"]
    cli = _replace(cli, "from library.query.service import Service, decimal, canonical_export_bytes",
                   "from library.query.service import (Service, decimal, canonical_export_bytes,\n"
                   "    decimal_counter, decimal_version)")
    cli = _replace(cli, "command.add_argument('--generation', type=decimal)",
                   "command.add_argument('--generation', type=decimal_counter)", count=2)
    cli = _replace(cli, "command.add_argument('--expected-generation', type=decimal, required=True)",
                   "command.add_argument('--expected-generation', type=decimal_counter, required=True)", count=2)
    cli = _replace(cli, "command.add_argument('--expected-version', type=decimal, required=True)",
                   "command.add_argument('--expected-version', type=decimal_version, required=True)")
    cli = _replace(cli, "command.add_argument('epoch', type=int)",
                   "command.add_argument('epoch', type=decimal_version)")
    cli = _replace(cli, "    raw_args = list(sys.argv[1:] if argv is None else argv)",
                   "".join("    " + line + "\n" for line in _ADOPT_PARSER.splitlines()) +
                   "    raw_args = list(sys.argv[1:] if argv is None else argv)")
    cli = _replace(cli, "new_commands = {'documents-v1'",
                   "new_commands = {'backup-root-adopt','job-commit','documents-v1'")
    cli = _replace(cli, "    args = parser.parse_args(raw_args)",
                   "    args = parser.parse_args(raw_args)\n"
                   "    if args.command == 'backup-root-adopt' and args.backup_dir is None:\n"
                   "        raise LibraryError('invalid_request')")
    cli = _replace(cli, "        if args.command == 'migrate':",
                   "        if args.command == 'backup-root-adopt':\n"
                   "            expected = None if args.expect_unbound else args.expect_root\n"
                   "            print(json.dumps(store.adopt_backup_root(expected), sort_keys=True))\n"
                   "            return 0\n"
                   "        if args.command == 'migrate':")
    cli = _replace(cli,
                   "            print(json.dumps(store.migrate(), ensure_ascii=True, sort_keys=True))",
                   "            # This process command includes construction-time migration.\n"
                   "            # A later public Store.migrate() call reports its own current no-op.\n"
                   "            activated = store._pending_migration\n"
                   "            result = store.migrate()\n"
                   "            if activated is not None and activated['migrated']:\n"
                   "                result = dict(activated)\n"
                   "            print(json.dumps(result, ensure_ascii=True, sort_keys=True))")
    return {
        "library/query/legacy_v1_service.py": base["library/query/service.py"],
        "library/query/service.py": _SERVICE,
        "library/clients/cli.py": cli,
    }
