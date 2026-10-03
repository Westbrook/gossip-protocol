"""Adversarial logical-backup checks; authored code only, no candidate execution."""
from __future__ import annotations

from pathlib import Path
import json
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m3_backup_format_v1 import format_files
from gossip_harness.library_project_fixture_v1 import seed_files


_PRELUDE = r'''
from copy import deepcopy
import hashlib
import json
from library.common import LibraryError, identity, blob_id
from library.catalog.backup_format import canonical_bytes, validate_backup


def doc(source='a.txt', text='first'):
    return {'document_id':identity('document',source),'source_id':identity('source',source),
            'source':source,'blob_id':blob_id(text.encode()),'title':source.rsplit('/',1)[-1],'text':text}

def rec(source='a.txt', text='first'):
    return {'document':doc(source,text),'revision':1,'edit_version':1,'deleted':False,
            'notes':'','tags':[],'collections':[]}

def empty():
    return {'schema':3,'generation':0,'documents':[],'revisions':[],'blobs':[],
            'collections':[],'jobs':[]}

def job(entries=None, state='queued', jid='job-one', epoch=1):
    if entries is None: entries=[{'source':'a.txt','text':'first'}]
    entries=sorted(entries,key=lambda entry:(entry['source'],entry['text']))
    hashes=[]
    for entry in entries:
        try: hashes.append(hashlib.sha256(entry['text'].encode()).hexdigest())
        except UnicodeError: hashes.append(None)
    if state=='cancelled' and epoch==1: epoch=2
    value={'job_id':jid,'epoch':epoch,'state':state,'total':len(entries),
           'completed':len(entries) if state=='completed' else 0,
           'error':'invalid_utf8' if state=='failed' else None}
    receipt={'job':value,'documents':[doc(entry['source'],entry['text']) for entry in entries]} if state=='completed' else None
    return {'job':value,'manifest_json':json.dumps(entries,ensure_ascii=True,sort_keys=True,separators=(',',':')),
            'content_hashes_json':json.dumps(hashes),'receipt_json':json.dumps(receipt,ensure_ascii=True,sort_keys=True) if receipt else None,
            'enrolled':False}

def fixture():
    value=empty()
    head=rec(text='second'); head.update(revision=2,edit_version=4,deleted=True,
                                          notes='exact\nnotes',tags=['résumé'],collections=['study'])
    value['documents']=[head]
    did=head['document']['document_id']
    value['revisions']=[{'document_id':did,'revision':n,'blob_id':blob_id(text.encode())}
                        for n,text in enumerate(('first','second'),1)]
    value['blobs']=sorted([{'blob_id':blob_id(text.encode()),'text':text} for text in ('first','second')],key=lambda r:r['blob_id'])
    value['collections']=['study']; value['generation']=6
    value['jobs']=[job(state='completed')]
    return value

def pack(value, format='local-research-library-backup-v3'):
    envelope={'format':format,'payload':value,'payload_sha256':hashlib.sha256(canonical_bytes(value)).hexdigest()}
    return canonical_bytes(envelope)

out={}
def check(name,value,*,raw=False):
    try:
        result=validate_backup(value if raw else pack(value))
        out[name]='ok' if result == (json.loads(value)['payload'] if raw else value) else 'changed'
    except LibraryError as exc:
        out[name]=exc.code

def mutate(name,change):
    value=fixture(); change(value); check(name,value)
'''


def _authored(script):
    """Run only the checked-in validator and frozen identity helpers in isolation."""
    with tempfile.TemporaryDirectory(prefix="trusted-library-m3-format-") as directory:
        root = Path(directory)
        sources = {"library/__init__.py": "", "library/catalog/__init__.py": "",
                   "library/common.py": seed_files()["library/common.py"]} | format_files()
        for name, source in sources.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding="utf-8")
        bootstrap = (
            "import resource, sys\n"
            "resource.setrlimit(resource.RLIMIT_CPU, (30, 30))\n"
            "resource.setrlimit(resource.RLIMIT_FSIZE, (67108864, 67108864))\n"
            "resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))\n"
            "sys.path.insert(0,sys.argv[1])\n"
        )
        result = subprocess.run([sys.executable, "-I", "-c", bootstrap + _PRELUDE + script
                                 + "\nprint(json.dumps(out,sort_keys=True))\n", str(root)],
                                cwd=root, capture_output=True, text=True, timeout=40, check=False)
        if result.returncode or len(result.stdout.encode()) > 65536:
            raise AssertionError("Trusted backup-format child failed: " + result.stderr[:6000])
        return json.loads(result.stdout)


class LibraryM3BackupFormatTests(unittest.TestCase):
    def assert_invalid(self, results):
        self.assertTrue(results)
        self.assertEqual(results, dict.fromkeys(results, "invalid_backup"))

    def test_source_is_closed_overlay_with_fresh_maps(self):
        files = format_files()
        self.assertEqual(set(files), {"library/catalog/backup_format.py"})
        compile(files["library/catalog/backup_format.py"], "backup_format.py", "exec")
        files.clear()
        self.assertTrue(format_files())

    def test_valid_empty_and_complete_snapshot_preserve_original_receipt_bytes(self):
        result = _authored(r'''
check('empty',empty())
value=fixture()
# Neither historical whitespace nor escaping is rewritten by restoration.
value['jobs'][0]['manifest_json']='[ { "text": "first", "source": "a.txt" } ]'
value['jobs'][0]['content_hashes_json']='[ "'+hashlib.sha256(b'first').hexdigest()+'" ]'
value['jobs'][0]['receipt_json']=json.dumps(json.loads(value['jobs'][0]['receipt_json']),indent=2)
restored=validate_backup(pack(value))
out['exact']=restored == value
out['original_text']=json.loads(restored['jobs'][0]['receipt_json'])['documents'][0]['text']
out['current_text']=restored['documents'][0]['document']['text']
check('complete',value)
# Inherited V0 paths did not have M1's later sixteen-segment intake restriction.
value=empty(); record=rec('/'.join(['deep']*17+['a.txt'])); value['documents']=[record]
value['revisions']=[{'document_id':record['document']['document_id'],'revision':1,'blob_id':record['document']['blob_id']}]
value['blobs']=[{'blob_id':record['document']['blob_id'],'text':'first'}]
check('v0_deep',value)
''')
        self.assertEqual(result, {"empty": "ok", "complete": "ok", "exact": True,
                                  "original_text": "first", "current_text": "second", "v0_deep": "ok"})

    def test_closed_shapes_and_exact_scalar_types(self):
        result = _authored(r'''
mutate('payload_extra',lambda p:p.update(extra=1))
mutate('missing_generation',lambda p:p.pop('generation'))
mutate('generation_bool',lambda p:p.update(generation=True))
mutate('generation_negative',lambda p:p.update(generation=-1))
mutate('documents_object',lambda p:p.update(documents={}))
mutate('document_extra',lambda p:p['documents'][0]['document'].update(extra='x'))
mutate('record_extra',lambda p:p['documents'][0].update(extra=0))
mutate('deleted_integer',lambda p:p['documents'][0].update(deleted=1))
mutate('edit_bool',lambda p:p['documents'][0].update(edit_version=True))
mutate('edit_zero',lambda p:p['documents'][0].update(edit_version=0))
mutate('edit_before_revision',lambda p:p['documents'][0].update(edit_version=1))
mutate('edit_before_deletion_annotations',lambda p:p['documents'][0].update(edit_version=3))
mutate('revision_bool',lambda p:p['revisions'][0].update(revision=True))
mutate('blob_extra',lambda p:p['blobs'][0].update(extra=1))
mutate('jobs_object',lambda p:p.update(jobs={}))
mutate('job_extra',lambda p:p['jobs'][0]['job'].update(extra=None))
mutate('enrolled_integer',lambda p:p['jobs'][0].update(enrolled=0))
''')
        self.assert_invalid(result)

    def test_json_envelope_digest_utf8_and_version_error_distinctions(self):
        result = _authored(r'''
raw=pack(fixture())
check('utf8',b'\xff',raw=True)
check('bom',b'\xef\xbb\xbf'+raw,raw=True)
check('trailing',raw+b'{}',raw=True)
check('duplicate',raw.replace(b'"format":',b'"format":"first","format":',1),raw=True)
check('nonfinite',raw.replace(b'"generation":6',b'"generation":NaN'),raw=True)
check('overflow_float',raw.replace(b'"generation":6',b'"generation":1e999'),raw=True)
envelope=json.loads(raw); envelope['payload_sha256']='0'*64
check('digest',canonical_bytes(envelope),raw=True)
envelope=json.loads(raw); envelope['payload_sha256']=envelope['payload_sha256'].upper()
check('uppercase_digest',canonical_bytes(envelope),raw=True)
envelope=json.loads(raw); envelope['extra']=0
check('envelope_extra',canonical_bytes(envelope),raw=True)
value=fixture(); value['schema']=4; check('schema4',value)
value=fixture(); value['schema']=True; check('schema_bool',value)
value=fixture(); value['schema']=-1; check('schema_negative',value)
check('future_format',pack(fixture(),format='local-research-library-backup-v9'),raw=True)
check('empty_format',pack(fixture(),format=''),raw=True)
check('wrong_raw_type',bytearray(raw),raw=True)
check('over_bound',b' '*(67108864+1),raw=True)
# The digest is the canonical PAYLOAD, independent of envelope indentation.
check('outer_whitespace',json.dumps(json.loads(raw),indent=2).encode(),raw=True)
out['canonical']=canonical_bytes({'z':'é','a':1}) == b'{"a":1,"z":"\xc3\xa9"}'
''')
        expected = dict.fromkeys(result, "invalid_backup")
        expected.update(schema4="unsupported_schema", future_format="unsupported_schema",
                        over_bound="too_large", outer_whitespace="ok", canonical=True)
        self.assertEqual(result, expected)

    def test_graph_identity_content_references_and_original_provenance(self):
        result = _authored(r'''
mutate('source_id',lambda p:p['documents'][0]['document'].update(source_id='src-'+'0'*64))
mutate('document_id',lambda p:p['documents'][0]['document'].update(document_id='doc-'+'0'*64))
mutate('title',lambda p:p['documents'][0]['document'].update(title='wrong.txt'))
mutate('head_text',lambda p:p['documents'][0]['document'].update(text='forged'))
mutate('blob_text',lambda p:p['blobs'][0].update(text='forged'))
mutate('missing_blob',lambda p:p['blobs'].pop(0))
mutate('missing_record',lambda p:p.update(documents=[],revisions=[]))
mutate('orphan_blob',lambda p:p['blobs'].append({'blob_id':blob_id(b'orphan'),'text':'orphan'}))
value=fixture(); receipt=json.loads(value['jobs'][0]['receipt_json'])
receipt['documents'][0]=deepcopy(value['documents'][0]['document'])
value['jobs'][0]['receipt_json']=json.dumps(receipt); check('receipt_current_head_for_original',value)
value=fixture(); receipt=json.loads(value['jobs'][0]['receipt_json'])
receipt['documents'][0]=doc('another.txt','first')
value['jobs'][0]['receipt_json']=json.dumps(receipt); check('receipt_wrong_source',value)
# An original receipt may be the sole retained reference to a blob: no head equality.
value=fixture(); value['revisions']=[value['revisions'][1]]; value['revisions'][0]['revision']=1
value['documents'][0]['revision']=1; check('receipt_only_reference',value)
value['blobs']=[r for r in value['blobs'] if r['text']!='first']; check('missing_receipt_blob',value)
''')
        expected = dict.fromkeys(result, "invalid_backup")
        expected["receipt_only_reference"] = "ok"
        self.assertEqual(result, expected)

    def test_revision_continuity_order_and_head(self):
        result = _authored(r'''
mutate('missing_first',lambda p:p['revisions'].pop(0))
mutate('missing_all',lambda p:p.update(revisions=[]))
mutate('missing_last',lambda p:p['revisions'].pop())
mutate('duplicate_revision',lambda p:p['revisions'].insert(0,deepcopy(p['revisions'][0])))
mutate('out_of_order',lambda p:p['revisions'].reverse())
mutate('unknown_document',lambda p:p['revisions'][0].update(document_id='doc-'+'0'*64))
mutate('head_mismatch',lambda p:p['revisions'][-1].update(blob_id=p['revisions'][0]['blob_id']))
mutate('unchanged_refresh_cannot_append',lambda p:p['revisions'][0].update(blob_id=p['revisions'][-1]['blob_id']))
mutate('revision_over_limit',lambda p:p['documents'][0].update(revision=17,edit_version=17))
mutate('zero_revision',lambda p:p['revisions'][0].update(revision=0))
''')
        self.assert_invalid(result)

    def test_all_row_orders_uniqueness_and_annotation_normalization(self):
        result = _authored(r'''
mutate('blobs_reverse',lambda p:p['blobs'].reverse())
mutate('blob_duplicate',lambda p:p['blobs'].insert(0,deepcopy(p['blobs'][0])))
mutate('record_duplicate',lambda p:p['documents'].append(deepcopy(p['documents'][0])))
mutate('job_duplicate',lambda p:p['jobs'].append(deepcopy(p['jobs'][0])))
value=fixture(); second=rec('b.txt','first'); value['documents'].append(second)
value['revisions'].append({'document_id':second['document']['document_id'],'revision':1,'blob_id':second['document']['blob_id']})
value['revisions'].sort(key=lambda row:(row['document_id'],row['revision']))
check('multiple_documents_valid',value); value['documents'].reverse(); check('documents_reverse',value)
value=fixture(); value['jobs'].append(job(jid='z-last')); check('multiple_jobs_valid',value)
value['jobs'].reverse(); check('jobs_reverse',value)
value=fixture(); value['jobs']=[job([{'source':'a.txt','text':'a'},{'source':'b.txt','text':'b'}])]
entries=json.loads(value['jobs'][0]['manifest_json']); entries.reverse()
value['jobs'][0]['manifest_json']=json.dumps(entries)
value['jobs'][0]['content_hashes_json']=json.dumps([hashlib.sha256(e['text'].encode()).hexdigest() for e in entries])
check('manifest_reverse',value)
mutate('collection_duplicate',lambda p:p['collections'].append('study'))
mutate('collection_reverse',lambda p:p.update(collections=['study','earlier']))
mutate('unknown_membership',lambda p:p['documents'][0].update(collections=['missing']))
mutate('non_normalized',lambda p:p['documents'][0].update(tags=[' Résumé ']))
mutate('decomposed',lambda p:p['documents'][0].update(tags=['re\u0301sume\u0301']))
mutate('duplicate_tag',lambda p:p['documents'][0].update(tags=['tag','tag']))
mutate('tags_reverse',lambda p:p['documents'][0].update(tags=['z','a']))
mutate('control_tag',lambda p:p['documents'][0].update(tags=['x\x7fy']))
mutate('empty_tag',lambda p:p['documents'][0].update(tags=['']))
mutate('oversize_name_utf8',lambda p:p['documents'][0].update(tags=['é'*33]))
mutate('notes_utf8_bound',lambda p:p['documents'][0].update(notes='é'*8193))
mutate('tags_count',lambda p:p['documents'][0].update(tags=[f'x{n:02}' for n in range(33)]))
mutate('collections_count',lambda p:p.update(collections=[f'x{n:02}' for n in range(65)]))
value=fixture(); value['collections']=[f'x{n:02}' for n in range(17)]
value['documents'][0]['collections']=value['collections']; check('membership_count',value)
value=fixture(); value['documents'][0].update(notes='é'*8192,tags=['é'*32]); check('annotation_boundaries',value)
''')
        expected = dict.fromkeys(result, "invalid_backup")
        expected["annotation_boundaries"] = "ok"
        expected["multiple_documents_valid"] = "ok"
        expected["multiple_jobs_valid"] = "ok"
        self.assertEqual(result, expected)

    def test_job_state_manifest_hash_and_receipt_invariants(self):
        result = _authored(r'''
mutate('epoch_bool',lambda p:p['jobs'][0]['job'].update(epoch=True))
mutate('epoch_zero',lambda p:p['jobs'][0]['job'].update(epoch=0))
mutate('job_id_invalid',lambda p:p['jobs'][0]['job'].update(job_id='../job'))
mutate('job_total',lambda p:p['jobs'][0]['job'].update(total=2))
mutate('completed_count',lambda p:p['jobs'][0]['job'].update(completed=0))
mutate('state',lambda p:p['jobs'][0]['job'].update(state='complete'))
mutate('completed_error',lambda p:p['jobs'][0]['job'].update(error='capacity'))
mutate('missing_receipt',lambda p:p['jobs'][0].update(receipt_json=None))
mutate('hash_tamper',lambda p:p['jobs'][0].update(content_hashes_json='["'+('0'*64)+'"]'))
mutate('hash_prefix',lambda p:p['jobs'][0].update(content_hashes_json='["blob-'+hashlib.sha256(b'first').hexdigest()+'"]'))
mutate('hash_null',lambda p:p['jobs'][0].update(content_hashes_json='[null]'))
mutate('manifest_extra',lambda p:p['jobs'][0].update(manifest_json='[{"source":"a.txt","text":"first","extra":0}]'))
mutate('manifest_duplicate_key',lambda p:p['jobs'][0].update(manifest_json='[{"source":"a.txt","source":"a.txt","text":"first"}]'))
mutate('manifest_wrong_type',lambda p:p['jobs'][0].update(manifest_json='[{"source":"a.txt","text":1}]'))
mutate('hash_nonfinite',lambda p:p['jobs'][0].update(content_hashes_json='[NaN]'))
value=fixture(); receipt=json.loads(value['jobs'][0]['receipt_json']); receipt['job']['epoch']=True
value['jobs'][0]['receipt_json']=json.dumps(receipt); check('receipt_bool_epoch',value)
value=fixture(); receipt=json.loads(value['jobs'][0]['receipt_json']); receipt['job']['epoch']=2
value['jobs'][0]['receipt_json']=json.dumps(receipt); check('receipt_epoch_changed',value)
value=fixture(); receipt=json.loads(value['jobs'][0]['receipt_json']); receipt['documents']=[]
value['jobs'][0]['receipt_json']=json.dumps(receipt); check('receipt_incomplete',value)
value=fixture(); value['jobs'][0]['receipt_json']='{"job":{},"job":{},"documents":[]}'
check('receipt_duplicate_key',value)
value=fixture(); value['jobs']=[job(state='queued')]; value['jobs'][0]['receipt_json']='{}'; check('noncompleted_receipt',value)
value=fixture(); value['jobs']=[job(state='failed')]; value['jobs'][0]['job']['error']='made_up'; check('failed_code',value)
value=fixture(); value['jobs']=[job(state='running')]; value['jobs'][0]['job']['completed']=1; check('partial_progress',value)
value=fixture(); value['jobs']=[job(state='cancelled')]; value['jobs'][0]['job']['epoch']=1; check('cancelled_initial_epoch',value)
''')
        self.assert_invalid(result)

    def test_deferred_invalid_admissions_remain_representable_in_all_noncompleted_states(self):
        result = _authored(r'''
entries=[{'source':'../bad.txt','text':'\ud800'}, {'source':'duplicate.bin','text':'x'},
         {'source':'duplicate.bin','text':'y'}, {'source':'\ud800','text':'valid'}]
for state in ('queued','running','failed','cancelled'):
    value=fixture(); value['jobs']=[job(entries,state)]
    check(state,value)
    restored=validate_backup(pack(value))
    out[state+'_exact']=restored['jobs']==value['jobs']
# Admission is not semantic validation; M1 submit admits too many or oversized entries.
for label,entries in [('too_many',[{'source':f'{n}.txt','text':'x'} for n in range(65)]),
                       ('oversized',[{'source':'a.txt','text':'x'*32769}]),
                       ('duplicate',[{'source':'a.txt','text':'x'},{'source':'a.txt','text':'y'}])]:
    value=fixture(); value['jobs']=[job(entries)]; check(label,value)
value=fixture(); value['jobs']=[job(state='failed')]; value['jobs'][0]['job']['error']='source_changed'
check('historical_failure_not_rederived',value)
# Raw invalid Unicode in outer fields is distinct from escaped nested JSON text.
raw=pack(fixture()).replace(b'"notes":"exact\\nnotes"',b'"notes":"\\ud800"')
check('outer_surrogate',raw,raw=True)
''')
        expected = {key: (True if key.endswith("_exact") else "ok") for key in result}
        expected["outer_surrogate"] = "invalid_backup"
        self.assertEqual(result, expected)

    def test_completed_manifests_cannot_smuggle_deferred_errors_or_false_receipts(self):
        result = _authored(r'''
for label,entries in [('too_many',[{'source':'a.txt','text':'first'}]*65),
                       ('duplicate',[{'source':'a.txt','text':'first'}]*2),
                       ('empty',[])]:
    value=fixture(); value['jobs']=[job(entries,'completed')]; check(label,value)
# Change nested manifest while retaining its shape and recomputing recorded hashes.
value=fixture(); value['jobs'][0]['manifest_json']='[{"source":"a.txt","text":"\\ud800"}]'
value['jobs'][0]['content_hashes_json']='[null]'; check('completed_invalid_utf8',value)
value=fixture(); value['jobs'][0]['manifest_json']='[{"source":"../a.txt","text":"first"}]'
check('completed_invalid_source',value)
value=fixture(); value['jobs'][0]['manifest_json']='[{"source":"a.bin","text":"first"}]'
check('completed_unsupported',value)
''')
        expected = dict.fromkeys(result, "invalid_backup")
        expected["empty"] = "ok"
        self.assertEqual(result, expected)

    def test_document_and_distinct_revision_blob_capacity_boundaries(self):
        result = _authored(r'''
def catalog(count, histories=1, width=1):
    value=empty(); blobs={}
    for n in range(count):
        history=[]
        for revision in range(1,histories+1):
            text=(f'{n:04}:{revision:02}:').ljust(width,'x')
            document=doc(f'{n:04}.txt',text); bid=document['blob_id']
            blobs[bid]=text
            history.append({'document_id':document['document_id'],'revision':revision,'blob_id':bid})
        record=rec(f'{n:04}.txt',text); record.update(revision=histories,edit_version=histories)
        value['documents'].append(record); value['revisions'].extend(history)
    value['revisions'].sort(key=lambda row:(row['document_id'],row['revision']))
    value['blobs']=[{'blob_id':bid,'text':text} for bid,text in sorted(blobs.items())]
    return value
check('documents_256',catalog(256)); check('documents_257',catalog(257))
check('revisions_16',catalog(1,16)); check('file_over_bound',catalog(1,1,32769))
value=catalog(32,16,32768); check('retained_exact_bound',value)
extra=catalog(33,16,32768); check('retained_over_bound',extra)
# Reused old bytes are one retained blob even when many revision references exist.
value=fixture(); value['documents'][0].update(revision=16,edit_version=18)
did=value['documents'][0]['document']['document_id']; current=value['documents'][0]['document']['blob_id']
first=blob_id(b'first'); value['revisions']=[{'document_id':did,'revision':n,'blob_id':current if n%2==0 else first} for n in range(1,17)]
check('shared_bytes',value)
''')
        self.assertEqual(result, {"documents_256": "ok", "documents_257": "invalid_backup",
                                 "revisions_16": "ok", "file_over_bound": "invalid_backup",
                                 "retained_exact_bound": "ok", "retained_over_bound": "invalid_backup",
                                 "shared_bytes": "ok"})
