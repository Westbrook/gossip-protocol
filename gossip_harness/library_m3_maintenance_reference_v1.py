"""Trusted authored M3 maintenance overlay; never model or acceptance input.

The generated mixin uses schema3's durable control/ownership layer. Derived
search generations never become the authority for catalog answers.
"""
from __future__ import annotations

from textwrap import dedent


_MAINTENANCE = dedent(r'''
    """Bounded derived indexing, complete export validation and read-only status."""
    import json
    import sqlite3

    from library.common import LibraryError

    _RECOVERY = {
        'worker': 'restart_worker', 'reindex': 'retry_reindex',
        'backup': 'choose_new_backup', 'restore': 'validate_backup',
        'migrate': 'run_migration',
    }

    def canonical_export_bytes(payload):
        try:
            return json.dumps(payload, ensure_ascii=False, sort_keys=True,
                              separators=(',', ':'), allow_nan=False).encode('utf-8')
        except (UnicodeError, ValueError) as error:
            raise LibraryError('invalid_utf8') from error

    class MaintenanceMixin:
        def _index_obsolete(self, generation):
            if generation is None:
                return
            self.db.execute('UPDATE maintenance_artifacts SET state=? '
                            'WHERE kind=? AND root_kind=? AND relative_path=?',
                            ('obsolete', 'index_generation', 'maintenance', str(generation)))
            # This method is called only after dropping both durable pointers
            # to the old generation, under the same SQLite writer transaction.
            self.db.execute('DELETE FROM search_entries WHERE generation=?', (generation,))

        def _reindex_step(self, limit):
            if type(limit) is not int or not 1 <= limit <= 64:
                raise LibraryError('invalid_request')
            with self._write():
                generation = self._generation()
                state = dict(self.db.execute('SELECT * FROM search_state WHERE singleton=1').fetchone())
                if state['target_generation'] != generation:
                    old_shadow = state['target_generation']
                    # Register ownership before creating the first derived row.
                    self.register_artifact('index_generation', 'maintenance', str(generation))
                    total = self.db.execute('SELECT COUNT(*) FROM lifecycle WHERE deleted=0').fetchone()[0]
                    self.db.execute('UPDATE search_state SET target_generation=?,processed=0,total=?,cursor=NULL '
                                    'WHERE singleton=1', (generation, total))
                    if old_shadow is not None and old_shadow != state['published_generation']:
                        self._index_obsolete(old_shadow)
                    state.update(target_generation=generation, processed=0, total=total, cursor=None)
                if state['published_generation'] == generation and state['processed'] == state['total']:
                    return {'state': 'completed', 'generation': generation,
                            'target_generation': generation, 'processed': state['processed'],
                            'total': state['total'], 'cursor': state['cursor']}

                # IDs are not in source order. Resolve the persisted cursor to
                # its stable source key in the unchanged captured generation.
                cursor = state['cursor']
                after_source = None
                if cursor is not None:
                    row = self.db.execute('SELECT source FROM documents WHERE document_id=?', (cursor,)).fetchone()
                    if row is None:
                        raise LibraryError('io_error')
                    after_source = row['source']
                rows = self.db.execute(
                    'SELECT d.document_id,d.source,b.content FROM documents d '
                    'JOIN lifecycle l USING(document_id) JOIN blobs b USING(blob_id) '
                    'WHERE l.deleted=0 AND (? IS NULL OR d.source>? OR (d.source=? AND d.document_id>?)) '
                    'ORDER BY d.source,d.document_id LIMIT ?',
                    (after_source, after_source, after_source, cursor, limit)).fetchall()
                for row in rows:
                    self.db.execute('INSERT INTO search_entries VALUES (?,?,?,?)',
                                    (generation, row['document_id'], row['source'],
                                     bytes(row['content']).decode('utf-8')))
                processed = state['processed'] + len(rows)
                cursor = rows[-1]['document_id'] if rows else cursor
                self.db.execute('UPDATE search_state SET processed=?,cursor=? WHERE singleton=1',
                                (processed, cursor))
                # BEGIN IMMEDIATE prevents a competing catalog writer; retain
                # the explicit publication fence rather than assuming this.
                if self._generation() != generation:
                    raise LibraryError('stale_generation')
                completed = processed == state['total']
                published = state['published_generation']
                if completed:
                    self.db.execute('UPDATE search_state SET published_generation=? WHERE singleton=1', (generation,))
                    self.db.execute('UPDATE maintenance_artifacts SET state=? '
                                    'WHERE kind=? AND root_kind=? AND relative_path=?',
                                    ('published', 'index_generation', 'maintenance', str(generation)))
                    if published is not None and published != generation:
                        self._index_obsolete(published)
                    published = generation
                return {'state': 'completed' if completed else 'running',
                        'generation': published, 'target_generation': generation,
                        'processed': processed, 'total': state['total'], 'cursor': cursor}

        def reindex_step(self, limit=64):
            try:
                with self._write():
                    result = self._reindex_step(limit)
                    self.cleanup_artifacts()
                    self.record_error('reindex', None)
            except LibraryError as error:
                self.record_error('reindex', error.code)
                raise
            except (OSError, sqlite3.Error, UnicodeError) as error:
                self.record_error('reindex', 'io_error')
                raise LibraryError('io_error') from error
            return result

        def export_bundle(self, ids=None, *, include_deleted=False,
                          include_history=False, max_bytes=16777216):
            if (type(include_deleted) is not bool or type(include_history) is not bool
                    or type(max_bytes) is not int or not 1 <= max_bytes <= 16777216):
                raise LibraryError('invalid_request')
            if ids is not None:
                if type(ids) is not list or len(ids) > 256 or any(type(value) is not str for value in ids):
                    raise LibraryError('invalid_request')
                try:
                    for value in ids:
                        value.encode('utf-8')
                except UnicodeError as error:
                    raise LibraryError('invalid_utf8') from error
                if len(set(ids)) != len(ids):
                    raise LibraryError('invalid_request')
            with self._read():
                generation = self._generation()
                if ids is None:
                    selected = [doc['document_id'] for doc in self.all_documents()]
                else:
                    selected = ids
                records = []
                # Validate all explicit IDs before encoding or returning a
                # byte, including disallowed tombstones and later missing IDs.
                for identifier in selected:
                    record = self._record(identifier)
                    if record['deleted'] and not include_deleted:
                        if ids is not None:
                            raise LibraryError('not_found')
                        continue
                    records.append(record)
                records.sort(key=lambda record: (record['document']['source'], record['document']['document_id']))
                payload = {'format': 'local-research-library-export-v2', 'generation': generation,
                           'documents': []}
                # Count canonical envelope + items before retaining more than
                # the admitted output. Shared blobs can otherwise expand into
                # 4096 repeated history strings, despite the storage byte cap.
                encoded_size = len(canonical_export_bytes(payload))
                if encoded_size > max_bytes:
                    raise LibraryError('too_large')
                for record in records:
                    item = {'record': record, 'revisions': self.revision_history(
                        record['document']['document_id'])['revisions'] if include_history else []}
                    encoded_size += len(canonical_export_bytes(item)) + bool(payload['documents'])
                    if encoded_size > max_bytes:
                        raise LibraryError('too_large')
                    payload['documents'].append(item)
                return payload

        def diagnostics(self):
            with self._read():
                schema = int(self.db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0])
                generation = self._generation()
                counts = self.db.execute('SELECT COALESCE(SUM(deleted=0),0),'
                                         'COALESCE(SUM(deleted=1),0) FROM lifecycle').fetchone()
                blobs = self.db.execute('SELECT COUNT(*),COALESCE(SUM(length(content)),0) FROM blobs').fetchone()
                jobs = dict.fromkeys(('queued', 'running', 'completed', 'cancelled', 'failed'), 0)
                for row in self.db.execute('SELECT state,COUNT(*) AS total FROM jobs GROUP BY state'):
                    jobs[row['state']] = row['total']
                search = self.db.execute('SELECT * FROM search_state WHERE singleton=1').fetchone()
                if search['published_generation'] == generation:
                    index_state = 'current'
                elif search['target_generation'] == generation and search['processed'] < search['total']:
                    index_state = 'building'
                elif search['published_generation'] is None and search['target_generation'] is None:
                    index_state = 'absent'
                else:
                    index_state = 'stale'
                last_error = json.loads(self._control('last_error'))
                return {'schema': schema, 'generation': generation,
                        'documents': {'active': counts[0], 'deleted': counts[1]},
                        'revisions': self.db.execute('SELECT COUNT(*) FROM revisions').fetchone()[0],
                        'blobs': {'count': blobs[0], 'bytes': blobs[1]}, 'jobs': jobs,
                        'worker_generation': int(self._control('worker_generation')),
                        # The OS lock is observed once during the SQLite read
                        # snapshot. Persisted running rows cannot invent a live
                        # process; idle/stopped distinctions belong to CoreStore.
                        'worker_state': self.worker_state(), 'index_state': index_state,
                        'last_error': last_error,
                        'recovery_action': 'none' if last_error is None else _RECOVERY[last_error['operation']]}
''').lstrip()


def maintenance_files() -> dict[str, str]:
    """Fresh additive maintenance sources for the cumulative authored reference."""
    return {"library/catalog/maintenance.py": _MAINTENANCE}
