"""Authored M4 logical backup adaptation; never candidate acceptance evidence."""
from __future__ import annotations

from textwrap import dedent


_BACKUP = dedent(r'''
    """Keep logical v3 interchange while retaining normalized physical schema4."""
    import json

    from library.common import LibraryError
    from library.catalog.backup import BackupMixin, MAX_BACKUP_BYTES
    from library.catalog.backup_format import canonical_bytes
    from library.catalog.m4_store import revision_id

    class M4BackupMixin(BackupMixin):
        def _backup_payload(self):
            # This method runs inside the inherited snapshot transaction. Only
            # numbered logical history crosses the backup boundary; physical
            # revision IDs are reproducible from its immutable identities.
            payload = {'schema':3, 'generation':self._generation(), 'documents':[],
                       'revisions':[], 'blobs':[], 'collections':[], 'jobs':[]}
            encoded_size = len(canonical_bytes(payload))
            def append(key, value):
                nonlocal encoded_size
                encoded_size += len(canonical_bytes(value)) + bool(payload[key])
                if encoded_size > MAX_BACKUP_BYTES:
                    raise LibraryError('too_large')
                payload[key].append(value)
            for row in self.db.execute('SELECT document_id FROM documents ORDER BY source,document_id'):
                append('documents', self._record(row[0]))
            for row in self.db.execute('SELECT document_id,revision,blob_id FROM document_revisions '
                                       'ORDER BY document_id,revision'):
                append('revisions', dict(row))
            for row in self.db.execute('SELECT blob_id,content FROM blobs ORDER BY blob_id'):
                append('blobs', {'blob_id':row['blob_id'], 'text':bytes(row['content']).decode('utf-8')})
            for row in self.db.execute('SELECT name FROM collections ORDER BY name'):
                append('collections', row[0])
            for row in self.db.execute('SELECT j.*,c.enrolled FROM jobs j JOIN job_control c USING(job_id) ORDER BY job_id'):
                append('jobs', {'job':self._job(row), 'manifest_json':row['manifest'],
                               'content_hashes_json':row['content_hashes'],
                               'receipt_json':row['receipt'], 'enrolled':bool(row['enrolled'])})
            return payload

        def _activate_backup(self, payload):
            # The inherited restore owns validation, authority, the transaction,
            # catalog generation and exactly one installation-fence rotation.
            # This hook replaces only logical content. Target fencing tombstones,
            # backup ownership, registry and the physical version stay in place.
            schema = self.db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0]
            if schema != '4':
                raise LibraryError('unsupported_schema')
            document_highwater = {row[0]:row[1] for row in self.db.execute(
                'SELECT document_id,edit_high_water FROM document_control')}
            job_highwater = {row[0]:row[1] for row in self.db.execute(
                'SELECT job_id,epoch_high_water FROM job_control')}
            for row in self.db.execute('SELECT document_id,edit_version FROM document_state'):
                document_highwater[row[0]] = max(document_highwater.get(row[0],0),row[1])
            for row in self.db.execute('SELECT job_id,epoch FROM jobs'):
                job_highwater[row[0]] = max(job_highwater.get(row[0],0),row[1])
            for identifier, highwater in document_highwater.items():
                self.db.execute('INSERT INTO document_control VALUES (?,?) ON CONFLICT(document_id) '
                    'DO UPDATE SET edit_high_water=max(edit_high_water,excluded.edit_high_water)', (identifier,highwater))
            for identifier, highwater in job_highwater.items():
                self.db.execute('INSERT INTO job_control VALUES (?,?,0) ON CONFLICT(job_id) '
                    'DO UPDATE SET epoch_high_water=max(epoch_high_water,excluded.epoch_high_water),enrolled=0',
                    (identifier,highwater))
            for table in ('search_entries','document_state','document_revisions','documents','blobs','collections','jobs'):
                self.db.execute('DELETE FROM ' + table)
            self.db.execute('UPDATE search_state SET published_generation=NULL,target_generation=NULL,'
                            'processed=0,total=0,cursor=NULL WHERE singleton=1')
            for blob in payload['blobs']:
                self.db.execute('INSERT INTO blobs VALUES (?,?)', (blob['blob_id'],blob['text'].encode('utf-8')))
            for name in payload['collections']:
                self.db.execute('INSERT INTO collections VALUES (?)', (name,))
            for record in payload['documents']:
                document = record['document']
                self.db.execute('INSERT INTO documents VALUES (?,?,?,?,?)',
                    tuple(document[key] for key in ('document_id','source_id','source','blob_id','title')))
            # Heads cannot be inserted until their normalized revision rows exist.
            for revision in payload['revisions']:
                self.db.execute('INSERT INTO document_revisions VALUES (?,?,?,?)',
                    (revision_id(revision['document_id'],revision['revision'],revision['blob_id']),
                     revision['document_id'],revision['revision'],revision['blob_id']))
            for record in payload['documents']:
                document = record['document']
                identifier = document['document_id']
                edit_version = max(record['edit_version'],document_highwater.get(identifier,0)) + 1
                head = revision_id(identifier,record['revision'],document['blob_id'])
                self.db.execute('INSERT INTO document_state VALUES (?,?,?,?,?,?,?)',
                    (identifier,head,edit_version,int(record['deleted']),record['notes'],
                     json.dumps(record['tags'],ensure_ascii=True,separators=(',',':')),
                     json.dumps(record['collections'],ensure_ascii=True,separators=(',',':'))))
                self.db.execute('INSERT INTO document_control VALUES (?,?) ON CONFLICT(document_id) '
                    'DO UPDATE SET edit_high_water=max(edit_high_water,excluded.edit_high_water)', (identifier,edit_version))
            for item in payload['jobs']:
                job = item['job']
                identifier = job['job_id']
                epoch = (job['epoch'] if job['state'] == 'completed'
                         else max(job['epoch'],job_highwater.get(identifier,0)) + 1)
                self.db.execute('INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?)',
                    (identifier,epoch,job['state'],job['total'],job['completed'],job['error'],
                     item['manifest_json'],item['content_hashes_json'],item['receipt_json']))
                self.db.execute('INSERT INTO job_control VALUES (?,?,?) ON CONFLICT(job_id) '
                    'DO UPDATE SET epoch_high_water=max(epoch_high_water,excluded.epoch_high_water),'
                    'enrolled=excluded.enrolled',
                    (identifier,max(epoch,job_highwater.get(identifier,0)),int(item['enrolled'])))
''')


def backup_files() -> dict[str, str]:
    """Return the M4-only mixin; qualified M3 publication code is inherited."""
    return {"library/catalog/m4_backup.py": _BACKUP}
