"""Portable Node controls for the authored v2 browser's exact JSON boundary.

These execute the shipped inline codec/request function without DOM mocks.
Physical browser/release workflows remain a separate, required browser lane.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import unittest

from gossip_harness.library_m4_reference_v1 import m4_files
from gossip_harness.library_v2_browser_reference_v1 import LOSSLESS_JSON_SCRIPT, browser_files


def _node(script: str) -> dict:
    selected = os.environ.get("GOSSIP_BROWSER_NODE")
    node = Path(selected).expanduser() if selected else (
        Path.home() / ".cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node")
    if not selected and not node.is_file():
        discovered = shutil.which("node")
        if discovered:
            node = Path(discovered)
    if not node.is_file():
        raise AssertionError("Existing Node runtime required; set GOSSIP_BROWSER_NODE")
    result = subprocess.run([str(node), "-e", "const assert=require('node:assert/strict');\n" +
                             LOSSLESS_JSON_SCRIPT + "\n" + script], capture_output=True, text=True,
                            timeout=10, check=False,
                            env={"PATH": os.defpath, "LANG": "en_US.UTF-8"})
    if result.returncode or len(result.stdout.encode()) > 100000:
        raise AssertionError(f"Authored v2 codec control failed: {result.stderr[:8000]}")
    return json.loads(result.stdout)


class LibraryV2BrowserCodecTests(unittest.TestCase):
    def test_generated_page_compiles_and_preserves_only_owned_page(self):
        base = m4_files()
        before = dict(base)
        overlay = browser_files(base)
        self.assertEqual(base, before)
        self.assertEqual(set(overlay), {"library/clients/index.html"})
        page = overlay["library/clients/index.html"]
        script = page.split("<script>", 1)[1].split("</script>", 1)[0]
        result = _node("new (require('node:vm').Script)(" + json.dumps(script) +
                       ");console.log(JSON.stringify({compiled:true}));")
        self.assertEqual(result, {"compiled": True})
        for invariant in ("expected_version:draftBase.edit_version", "String(pageGeneration)",
                          "const submittedGeneration = generation", "if (!discard && drafts.has(id))"):
            self.assertIn(invariant, page)
        self.assertNotIn("await response.json()", page)
        self.assertNotIn("backup-root-adopt", page)

    def test_safe_and_unsafe_integer_round_trip_is_exact(self):
        result = _node(r'''
const raw='{"epoch":9223372036854775807,"edit_version":9007199254740993,"generation":0,"revision":16}';
const value=LosslessJSON.validate(LosslessJSON.parse(raw));
assert.equal(typeof value.epoch,'bigint');assert.equal(typeof value.edit_version,'bigint');
assert.equal(typeof value.generation,'number');assert.equal(value.revision,16);
assert.equal(LosslessJSON.stringify(value),raw);
assert.equal('edit_version '+value.edit_version,'edit_version 9007199254740993');
assert.equal(new URLSearchParams({generation:String(value.epoch)}).toString(),'generation=9223372036854775807');
console.log(JSON.stringify({exact:true}));
''')
        self.assertEqual(result, {"exact": True})

    def test_integer_boundaries_and_wrong_types_are_rejected(self):
        result = _node(r'''
for(const raw of ['{"epoch":0}','{"epoch":-1}','{"epoch":true}','{"epoch":"1"}',
 '{"generation":9223372036854775808}','{"edit_version":9223372036854775808}',
 '{"generation":null}','{"worker_generation":-1}'])
 assert.throws(()=>LosslessJSON.validate(LosslessJSON.parse(raw)),/invalid_response/);
for(const raw of ['{"epoch":1}','{"epoch":9223372036854775806}','{"epoch":9223372036854775807}',
 '{"generation":0}','{"generation":9007199254740992}',
 '{"published_generation":null,"target_generation":null}'])
 assert.doesNotThrow(()=>LosslessJSON.validate(LosslessJSON.parse(raw)));
console.log(JSON.stringify({bounded:true}));
''')
        self.assertEqual(result, {"bounded": True})

    def test_parser_rejects_malformed_duplicate_and_float_spellings(self):
        result = _node(r'''
const bad=['', ' ', '[1,]', '{"a":1,}', '{"a":1,"a":2}', '{"a":1,"\u0061":2}',
 '01','-01','+1','NaN','Infinity','1.0','1e0','1E3','[1]junk','{}{}','[true false]',
 '"line\nx"'.replace('\\n','\n'),'"\u0001"','"\\x"','"unterminated',
 '{"a" 1}','[','{','null x','[undefined]'];
for(const raw of bad)assert.throws(()=>LosslessJSON.parse(raw),/invalid_response/,raw);
assert.throws(()=>LosslessJSON.parse('['.repeat(130)+'0'+']'.repeat(130)),/invalid_response/);
console.log(JSON.stringify({rejected:bad.length}));
''')
        self.assertEqual(result["rejected"], 26)

    def test_string_escapes_unicode_and_prototype_names_stay_literal(self):
        result = _node(r'''
const ordinary={text:'雪 <script> 9223372036854775807 " \\ \n', nested:['true',null,false,[]]};
const raw=JSON.stringify(ordinary),value=LosslessJSON.parse(raw);
assert.equal(LosslessJSON.stringify(value),raw);
const escaped=LosslessJSON.parse('{"text":"\\\"\\\\\\n\\t\\u96ea"}');
assert.equal(escaped.text,'"\\\n\t雪');
const poison=LosslessJSON.parse('{"__proto__":{"polluted":true},"constructor":"literal"}');
assert.equal(Object.getPrototypeOf(poison),null);assert.equal({}.polluted,undefined);
assert.equal(poison.constructor,'literal');assert.equal(poison.__proto__.polluted,true);
assert.equal(LosslessJSON.parse(LosslessJSON.stringify(poison)).constructor,'literal');
console.log(JSON.stringify({literal:true}));
''')
        self.assertEqual(result, {"literal": True})

    def test_serializer_never_emits_rounded_or_string_tokens(self):
        result = _node(r'''
for(const value of [9007199254740992,Infinity,NaN,1.5,undefined,()=>0])
 assert.throws(()=>LosslessJSON.stringify({value}),/invalid_request/);
const cycle={};cycle.self=cycle;assert.throws(()=>LosslessJSON.stringify(cycle),/invalid_request/);
const value={expected_version:9007199254740993n,expected_generation:9223372036854775807n,epoch:1};
const raw=LosslessJSON.stringify(value,null,2);
assert.ok(raw.includes('"expected_version": 9007199254740993'));
assert.ok(!raw.includes('"9007199254740993"'));assert.ok(!raw.includes('e+'));
assert.equal(LosslessJSON.parse(raw).expected_generation,9223372036854775807n);
console.log(JSON.stringify({unrounded:true}));
''')
        self.assertEqual(result, {"unrounded": True})

    def test_actual_request_function_resubmits_loaded_token_exactly(self):
        page = browser_files(m4_files())["library/clients/index.html"]
        request = page[page.index("async function request(url, body)"):page.index("async function list()")]
        result = _node(request + r'''
(async()=>{
 const calls=[];
 global.fetch=async(url,options)=>{calls.push({url,options});return {
  ok:true,text:async()=>'{"edit_version":9007199254740993,"generation":9223372036854775807,"epoch":9007199254740995}'}};
 const loaded=await request('/api/v1/documents/id');
 await request('/api/v1/documents/id/annotations',{expected_version:loaded.edit_version,notes:'draft',tags:[],collections:[]});
 await request('/api/maintenance/restore',{name:'saved.json',expected_generation:loaded.generation});
 await request('/api/jobs/job/commit',{epoch:loaded.epoch});
 assert.equal(calls[1].options.body,'{"expected_version":9007199254740993,"notes":"draft","tags":[],"collections":[]}');
 assert.equal(calls[2].options.body,'{"name":"saved.json","expected_generation":9223372036854775807}');
 assert.equal(calls[3].options.body,'{"epoch":9007199254740995}');
 console.log(JSON.stringify({calls:calls.length}));
})().catch(error=>{console.error(error);process.exit(1)});
''')
        self.assertEqual(result, {"calls": 4})

    def test_first_partial_reindex_allows_only_its_unpublished_generation(self):
        page = browser_files(m4_files())["library/clients/index.html"]
        request = page[page.index("async function request(url, body)"):page.index("async function list()")]
        result = _node(request + r'''
(async()=>{
 const pending='{ "state":"running", "generation":null, "target_generation":9007199254740993, "processed":64, "total":65, "cursor":"last-indexed.txt" }';
 global.fetch=async()=>({ok:true,text:async()=>pending});
 const observed=await request('/api/maintenance/reindex',{limit:64});
 assert.equal(observed.generation,null);assert.equal(observed.target_generation,9007199254740993n);
 assert.equal(observed.processed,64);assert.equal(observed.total,65);
 await assert.rejects(request('/api/maintenance/diagnostics'),/invalid_response/);
 await assert.rejects(request('/api/v1/documents'),/invalid_response/);
 for(const raw of ['{"generation":null}',pending.replace('"running"','"completed"'),
  pending.replace('"target_generation":9007199254740993','"target_generation":null'),
  pending.replace('"total":65','"total":64'),
  pending.replace('"cursor":"last-indexed.txt"','"cursor":{"generation":null}')]){
  global.fetch=async()=>({ok:true,text:async()=>raw});
  await assert.rejects(request('/api/maintenance/reindex',{limit:64}),/invalid_response/);
 }
 global.fetch=async()=>({ok:true,text:async()=>'{"state":"completed","generation":9007199254740993,"target_generation":9007199254740993,"processed":65,"total":65,"cursor":"last-indexed.txt"}'});
 const complete=await request('/api/maintenance/reindex',{limit:64});
 assert.equal(complete.generation,9007199254740993n);
 console.log(JSON.stringify({unpublished:true}));
})().catch(error=>{console.error(error);process.exit(1)});
''')
        self.assertEqual(result, {"unpublished": True})

    def test_adjacent_large_versions_do_not_alias_and_errors_stay_visible(self):
        page = browser_files(m4_files())["library/clients/index.html"]
        request = page[page.index("async function request(url, body)"):page.index("async function list()")]
        result = _node(request + r'''
(async()=>{
 const draft=LosslessJSON.parse('{"edit_version":9007199254740992}');
 const current=LosslessJSON.parse('{"edit_version":9007199254740993}');
 assert.notEqual(draft.edit_version,current.edit_version);
 assert.equal(LosslessJSON.stringify({expected_version:draft.edit_version}),'{"expected_version":9007199254740992}');
 global.fetch=async()=>({ok:false,text:async()=>'{"error":"counter_exhausted"}'});
 await assert.rejects(request('/api/v1/documents/id/delete',{expected_version:9223372036854775807n}),/counter_exhausted/);
 global.fetch=async()=>({ok:true,text:async()=>'{"edit_version":9223372036854775808}'});
 await assert.rejects(request('/api/v1/documents/id'),/invalid_response/);
 console.log(JSON.stringify({distinct:true}));
})().catch(error=>{console.error(error);process.exit(1)});
''')
        self.assertEqual(result, {"distinct": True})


if __name__ == "__main__":
    unittest.main()
