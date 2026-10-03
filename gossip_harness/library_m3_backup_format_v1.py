"""Strict authored M3 logical-backup validation, independent of storage layout.

These generated sources are trusted offline reference code, never candidate
inputs or independent model-quality evidence. Their only dependencies are the
frozen public identity functions; validation does not instantiate a Store.
"""
from __future__ import annotations

from textwrap import dedent


_FORMAT = dedent(r'''
    """Closed, bounded v3 logical interchange; never SQL or executable input."""
    import hashlib
    import json
    from pathlib import PurePosixPath
    import re
    import unicodedata

    from library.common import LibraryError, blob_id, identity, source_key

    FORMAT = 'local-research-library-backup-v3'
    MAX_BACKUP_BYTES = 67108864
    _FAILURES = frozenset(('invalid_source', 'unsupported_type', 'invalid_utf8',
                          'too_large', 'invalid_batch', 'source_changed', 'capacity'))
    _DOC_KEYS = frozenset(('document_id', 'source_id', 'source', 'blob_id', 'title', 'text'))
    _JOB_KEYS = frozenset(('job_id', 'epoch', 'state', 'total', 'completed', 'error'))

    def _bad():
        raise LibraryError('invalid_backup')

    def canonical_bytes(value):
        """The contract's canonical UTF-8 representation, with no newline/BOM."""
        try:
            return json.dumps(value, ensure_ascii=False, allow_nan=False,
                              sort_keys=True, separators=(',', ':')).encode('utf-8')
        except (ValueError, TypeError, UnicodeError, RecursionError):
            _bad()

    def _pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                _bad()
            result[key] = value
        return result

    def _constant(value):
        _bad()

    def _json(text):
        try:
            return json.loads(text, object_pairs_hook=_pairs, parse_constant=_constant)
        except (ValueError, TypeError, UnicodeError, RecursionError):
            _bad()

    def _shape(value, keys):
        if type(value) is not dict or set(value) != set(keys):
            _bad()
        return value

    def _array(value, maximum=None):
        if type(value) is not list or (maximum is not None and len(value) > maximum):
            _bad()
        return value

    def _integer(value, minimum=0, maximum=None):
        if (type(value) is not int or value < minimum
                or (maximum is not None and value > maximum)):
            _bad()
        return value

    def _text(value, maximum=None):
        if type(value) is not str:
            _bad()
        try:
            raw = value.encode('utf-8')
        except UnicodeError:
            _bad()
        if maximum is not None and len(raw) > maximum:
            _bad()
        return raw

    def _names(values, maximum):
        values = _array(values, maximum)
        for name in values:
            _text(name, 64)
            normalized = unicodedata.normalize('NFC', unicodedata.normalize('NFC', name).strip().casefold())
            if (not name or name != normalized
                    or any(ord(char) < 32 or ord(char) == 127 for char in name)):
                _bad()
        if values != sorted(set(values)):
            _bad()
        return values

    def _document(value, blobs):
        _shape(value, _DOC_KEYS)
        for item in value.values():
            _text(item)
        source = value['source']
        try:
            source_key(source)
            did, sid = identity('document', source), identity('source', source)
        except LibraryError:
            _bad()
        # V0 imports predate the M1 16-segment intake limit. A valid inherited
        # document may be deeper; the restriction is applied to M1 receipts.
        if PurePosixPath(source).suffix.lower() not in ('.txt', '.md', '.html'):
            _bad()
        if (value['document_id'] != did or value['source_id'] != sid
                or value['title'] != source.rsplit('/', 1)[-1]):
            _bad()
        raw = _text(value['text'], 32768)
        bid = blob_id(raw)
        if value['blob_id'] != bid or blobs.get(bid) != value['text']:
            _bad()
        return did, bid

    def _manifest(raw):
        _text(raw)
        entries = _array(_json(raw))
        hashes = []
        for entry in entries:
            _shape(entry, ('source', 'text'))
            # Surrogates in these nested JSON values are admitted by M1 submit
            # and rejected only by prepare/commit. The outer string is UTF-8.
            if type(entry['source']) is not str or type(entry['text']) is not str:
                _bad()
            try:
                hashes.append(hashlib.sha256(entry['text'].encode('utf-8')).hexdigest())
            except UnicodeError:
                hashes.append(None)
        if entries != sorted(entries, key=lambda item: (item['source'], item['text'])):
            _bad()
        return entries, hashes

    def _completed_entries(entries):
        if len(entries) > 64 or len({entry['source'] for entry in entries}) != len(entries):
            _bad()
        size = 0
        for entry in entries:
            try:
                source_key(entry['source'])
            except LibraryError:
                _bad()
            if (len(entry['source'].split('/')) > 16
                    or PurePosixPath(entry['source']).suffix.lower() not in ('.txt', '.md', '.html')):
                _bad()
            size += len(_text(entry['text'], 32768))
        if size > 524288:
            _bad()

    def _job_row(row, blobs, records):
        _shape(row, ('job', 'manifest_json', 'content_hashes_json', 'receipt_json', 'enrolled'))
        if type(row['enrolled']) is not bool:
            _bad()
        job = _shape(row['job'], _JOB_KEYS)
        jid = job['job_id']
        if type(jid) is not str or re.fullmatch(r'[A-Za-z0-9_-]{1,64}', jid) is None:
            _bad()
        _integer(job['epoch'], 1)
        total = _integer(job['total'])
        completed = _integer(job['completed'])
        state = job['state']
        if type(state) is not str or state not in ('queued', 'running', 'completed', 'cancelled', 'failed'):
            _bad()
        if state == 'cancelled' and job['epoch'] < 2:
            _bad()
        if state == 'failed':
            if type(job['error']) is not str or job['error'] not in _FAILURES:
                _bad()
        elif job['error'] is not None:
            _bad()
        if completed != (total if state == 'completed' else 0):
            _bad()
        entries, hashes = _manifest(row['manifest_json'])
        if total != len(entries):
            _bad()
        _text(row['content_hashes_json'])
        supplied_hashes = _array(_json(row['content_hashes_json']))
        if supplied_hashes != hashes:
            _bad()
        receipt = row['receipt_json']
        if state != 'completed':
            if receipt is not None:
                _bad()
            return jid, set()
        _text(receipt)
        receipt = _shape(_json(receipt), ('job', 'documents'))
        receipt_job = _shape(receipt['job'], _JOB_KEYS)
        # Equality alone would equate true with 1, so require the receipt's
        # exact JSON scalar types as well as its complete original JOB values.
        if any(type(receipt_job[key]) is not type(job[key]) or receipt_job[key] != job[key]
               for key in _JOB_KEYS):
            _bad()
        documents = _array(receipt['documents'])
        if len(documents) != total:
            _bad()
        _completed_entries(entries)
        refs = set()
        for document, entry in zip(documents, entries):
            did, bid = _document(document, blobs)
            if (document['source'] != entry['source'] or document['text'] != entry['text']
                    or did not in records):
                _bad()
            refs.add(bid)
        return jid, refs

    def _validate_payload(payload):
        _shape(payload, ('schema', 'generation', 'documents', 'revisions', 'blobs', 'collections', 'jobs'))
        _integer(payload['generation'])
        collections = set(_names(payload['collections'], 64))
        blobs, blob_sizes, blob_order = {}, {}, []
        for row in _array(payload['blobs']):
            _shape(row, ('blob_id', 'text'))
            _text(row['blob_id'])
            raw = _text(row['text'], 32768)
            bid = blob_id(raw)
            if row['blob_id'] != bid or bid in blobs:
                _bad()
            blobs[bid] = row['text']
            blob_sizes[bid] = len(raw)
            blob_order.append(bid)
        if blob_order != sorted(blob_order):
            _bad()
        records, document_order = {}, []
        for record in _array(payload['documents'], 256):
            _shape(record, ('document', 'revision', 'edit_version', 'deleted', 'notes', 'tags', 'collections'))
            did, _ = _document(record['document'], blobs)
            if did in records:
                _bad()
            revision = _integer(record['revision'], 1, 16)
            _integer(record['edit_version'], revision)
            if type(record['deleted']) is not bool:
                _bad()
            _text(record['notes'], 16384)
            _names(record['tags'], 32)
            memberships = _names(record['collections'], 16)
            if not set(memberships).issubset(collections):
                _bad()
            # Content changes, annotation writes, and deletion are distinct
            # effective transactions; restore can only increase edit versions.
            minimum_edits = revision + int(record['deleted']) + int(bool(
                record['notes'] or record['tags'] or memberships))
            _integer(record['edit_version'], minimum_edits)
            records[did] = record
            document_order.append((record['document']['source'], did))
        if document_order != sorted(document_order):
            _bad()
        revisions = {did: [] for did in records}
        revision_order, revision_refs = [], set()
        for row in _array(payload['revisions'], 256 * 16):
            _shape(row, ('document_id', 'revision', 'blob_id'))
            _text(row['document_id'])
            _text(row['blob_id'])
            number = _integer(row['revision'], 1, 16)
            did, bid = row['document_id'], row['blob_id']
            if did not in records or bid not in blobs:
                _bad()
            revisions[did].append((number, bid))
            revision_order.append((did, number))
            revision_refs.add(bid)
        if revision_order != sorted(set(revision_order)):
            _bad()
        for did, record in records.items():
            history = revisions[did]
            if ([number for number, bid in history] != list(range(1, record['revision'] + 1))
                    or history[-1][1] != record['document']['blob_id']
                    or any(before[1] == after[1] for before, after in zip(history, history[1:]))):
                _bad()
        if sum(blob_sizes[bid] for bid in revision_refs) > 16777216:
            _bad()
        job_ids, receipt_refs = [], set()
        for row in _array(payload['jobs']):
            jid, refs = _job_row(row, blobs, records)
            job_ids.append(jid)
            receipt_refs.update(refs)
        if job_ids != sorted(set(job_ids)):
            _bad()
        if set(blobs) != revision_refs | receipt_refs:
            _bad()
        return payload

    def validate_backup(raw):
        """Validate before activation; return values retaining exact job strings."""
        if type(raw) is not bytes:
            _bad()
        if len(raw) > MAX_BACKUP_BYTES:
            raise LibraryError('too_large')
        try:
            text = raw.decode('utf-8')
        except UnicodeError:
            _bad()
        envelope = _shape(_json(text), ('format', 'payload', 'payload_sha256'))
        _text(envelope['format'])
        if not envelope['format']:
            _bad()
        digest = envelope['payload_sha256']
        if type(digest) is not str or re.fullmatch(r'[0-9a-f]{64}', digest) is None:
            _bad()
        payload = envelope['payload']
        if type(payload) is not dict or 'schema' not in payload:
            _bad()
        _integer(payload['schema'])
        if hashlib.sha256(canonical_bytes(payload)).hexdigest() != digest:
            _bad()
        if envelope['format'] != FORMAT or payload['schema'] != 3:
            raise LibraryError('unsupported_schema')
        try:
            return _validate_payload(payload)
        except (KeyError, TypeError, ValueError, UnicodeError, RecursionError):
            _bad()
''').lstrip()


def format_files() -> dict[str, str]:
    """Return the independently authored logical-format module as a fresh map."""
    return {"library/catalog/backup_format.py": _FORMAT}
