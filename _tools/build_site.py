#!/usr/bin/env python3
"""Build a read-only public projection; never deploy or modify input workspaces."""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

REPOSITORY = "https://github.com/Westbrook/gossip-protocol"
PAGES = ["benchmark-comparison", "continuation-comparison", "verification-pilot", "sustained-pilot", "swarm-pilot", "experiments", "pilot", "investigation", "research", "swarm", "investigation-roadmap"]
TITLES = {"benchmark-comparison":"Evidence frontier benchmark", "continuation-comparison":"Continuation and maintenance", "verification-pilot":"Adaptive verification", "sustained-pilot":"Sustained project work", "swarm-pilot":"Candidate swarm trial", "experiments":"Shared discoveries and repair", "pilot":"First live coding trial", "investigation":"Distributed Git integration", "research":"Research and prior art", "swarm":"Swarm selection design", "investigation-roadmap":"Investigation roadmap"}
CSS = """:root{color-scheme:light;--paper:#f5f3ec;--surface:#fffef9;--ink:#173836;--muted:#5d6f6b;--line:#d3dbd3;--teal:#176c60;--wash:#e7eee5;--gold:#a05b14}*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);font:16px/1.65 system-ui,-apple-system,sans-serif}main{max-width:1120px;margin:auto;padding:40px 28px 90px}a{color:var(--teal);text-underline-offset:3px}a:hover{color:var(--gold)}a:focus-visible,summary:focus-visible{outline:3px solid var(--gold);outline-offset:4px}.site-nav{display:flex;gap:12px 26px;flex-wrap:wrap;border-bottom:1px solid var(--line);padding:0 0 18px;margin:0 0 32px;font-size:14px}.site-nav .brand{font-weight:750;margin-right:auto}h1{font:600 clamp(38px,5vw,64px)/1.08 Georgia,serif;letter-spacing:-.045em;margin:12px 0 18px}h2{font:600 29px/1.2 Georgia,serif;letter-spacing:-.02em;margin:40px 0 18px}h3{font-size:18px;line-height:1.35;margin:0 0 8px}p{max-width:900px}.lead{font-size:21px;color:var(--muted);max-width:850px}.eyebrow{font:12px/1.4 ui-monospace,monospace;letter-spacing:.1em;text-transform:uppercase;color:var(--muted)}.small{font-size:13px;color:var(--muted)}.pill{display:inline-block;padding:4px 10px;border-radius:30px;background:var(--wash);color:var(--teal);font-size:12px}.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}.panel{padding:24px;background:var(--surface);border:1px solid var(--line);border-radius:10px}.overview{display:grid;grid-template-columns:300px 1fr;gap:22px}.percent{font:600 70px/1 Georgia,serif;letter-spacing:-.05em;margin:15px 0}.bar{height:7px;border-radius:4px;background:var(--line);overflow:hidden}.bar span{height:100%;display:block;background:var(--teal)}.chart{width:100%;height:145px}.banner{border-left:4px solid var(--teal);background:var(--wash);padding:14px 20px;margin:20px 0}.task{display:grid;grid-template-columns:1fr 120px;gap:20px;padding:18px 0;border-bottom:1px solid var(--line)}.task p{margin:5px 0;font-size:14px;color:var(--muted)}.task-status{text-align:right}details{border:1px solid var(--line);padding:18px 22px;border-radius:8px;margin:18px 0}summary{cursor:pointer;font-weight:650}details[open]>summary{margin-bottom:18px}.cards article{margin-bottom:0}.cards ul{padding-left:20px;font-size:14px}.cards .version{font:11px/1.5 ui-monospace,monospace;overflow-wrap:anywhere;color:var(--muted)}code{font:13px ui-monospace,monospace;background:var(--wash);border-radius:3px;padding:2px 4px;overflow-wrap:anywhere}pre{background:#173836;color:#f5f3ec;padding:20px;border-radius:8px;overflow:auto;font:13px/1.6 ui-monospace,monospace}pre code{background:none;color:inherit;white-space:pre;overflow-wrap:normal}table{border-collapse:collapse;width:100%;display:block;overflow:auto;font-size:14px}th,td{text-align:left;padding:12px;border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--teal)}blockquote{border-left:4px solid var(--line);margin:20px 0;padding:1px 20px;color:var(--muted)}li{margin-bottom:7px}.missing-evidence{color:var(--muted);text-decoration:underline dotted;cursor:help}.source-banner{font:12px/1.6 system-ui,sans-serif;background:#e7eee5;color:#173836;padding:12px 20px;margin:0;border-bottom:1px solid #d3dbd3}.source-banner a{color:#176c60}footer{border-top:1px solid var(--line);padding-top:20px;margin-top:45px;font-size:13px;color:var(--muted)}.return{position:fixed;right:18px;bottom:max(18px,env(safe-area-inset-bottom));background:var(--ink);color:white;padding:10px 16px;border-radius:30px;text-decoration:none;z-index:9}.return[hidden]{display:none}.return:hover{background:var(--teal);color:white}@media(max-width:700px){main{padding:26px 18px 90px}.overview,.grid{grid-template-columns:1fr}.site-nav .brand{width:100%}.task{grid-template-columns:1fr}.task-status{text-align:left}.panel{padding:20px}.lead{font-size:19px}}"""

def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def esc(value: object) -> str:
    return html.escape(str(value), quote=True)

def command(*args: str) -> str:
    return subprocess.check_output(args, text=True).strip()

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--pandoc", default="pandoc")
    parser.add_argument("--report-state", type=Path, help="Optional immutable local snapshot of canonical report state; never published")
    args = parser.parse_args()
    repo, report, out = args.repo.resolve(), args.report.resolve(), args.output.resolve()
    if out == repo or out == report or repo in out.parents or report in out.parents:
        raise SystemExit("Output must be separate from source and report workspaces")
    pandoc = Path(command("which", args.pandoc)).resolve()
    version = command(str(pandoc), "--version").splitlines()[0]
    if version != "pandoc 3.10":
        raise SystemExit("This publication builder is pinned to existing pandoc 3.10")
    original_state = (args.report_state or (report / "data/project.json")).read_bytes()
    state = json.loads(original_state)
    commit = command("git", "-C", str(repo), "rev-parse", "HEAD")
    tracked = set(command("git", "-C", str(repo), "ls-files").splitlines())
    exported = datetime.now(timezone.utc).isoformat()
    manifest = {"protocol":"gossip-pages-publication-v1", "source_repository":REPOSITORY, "source_commit":commit, "report_revision":state["revision"], "report_source_sha256":digest(original_state), "exported_at":exported, "renderer":{"version":version,"sha256":digest(pandoc.read_bytes())}, "snapshot_semantics":"Read-only publication. Report revision reflects recorded work; no background updates or shared review controls.", "entries":[], "generated_entries":[]}
    artifacts: set[str] = set()
    known_artifacts = {p.name for p in (report / "artifacts").glob("*.html") if re.fullmatch(r"[a-z0-9-]+-[a-f0-9]{12}\.html",p.name)}
    doc_paths = sorted({p for p in tracked if p.startswith("docs/") and p.endswith(".md")} | {"docs/peer-library-project-v4.md"})
    doc_paths = [p for p in doc_paths if (repo / p).is_file()]
    doc_map = {p: p[:-3] + ".html" for p in doc_paths}
    doc_map["VERIFICATION.md"] = "docs/development-verification.html"
    doc_map["README.md"] = "docs/repository-guide.html"

    def clean(value: str) -> str:
        # Exact known prefixes preserve repo-relative evidence identities.
        for prefix, replacement in sorted([(str(repo)+"/", ""),(str(report)+"/", "report/"),(str(repo),"[project workspace]"),(str(report),"[report workspace]")],key=lambda x:-len(x[0])):
            value = value.replace(prefix,replacement)
        # Other personal workspace references are not public evidence identities.
        value = re.sub(r"/Users/[^\s<>\"'`]+", "[local path omitted]", value)
        value = re.sub(r"/private/(?:tmp|var)/[^\s<>\"'`]+", "[local runtime path omitted]", value)
        return value

    def relative(target: str, current: str) -> str:
        return os.path.relpath(target, str(Path(current).parent)).replace(os.sep,"/")

    def public_url(url: str, current: str, source: str = "") -> str | None:
        parsed = urlsplit(html.unescape(url))
        if parsed.scheme in {"http","https"} and parsed.hostname not in {"127.0.0.1","localhost"}:
            return url
        if parsed.scheme and parsed.scheme not in {"http","https"}:
            return url if parsed.scheme == "mailto" else None
        if parsed.scheme or parsed.path.startswith("/"):
            path = parsed.path.lstrip("/")
            if not path or path == "index.html":
                target = "index.html"
            elif path.startswith("artifacts/"):
                name = Path(path).name
                if name not in known_artifacts:
                    return None
                artifacts.add(name)
                target = "artifacts/"+name
            elif path[:-5] in PAGES and path.endswith(".html"):
                target = "outputs/"+path
            else:
                return None
        elif not parsed.path:
            return url
        else:
            resolved = os.path.normpath(str(Path(source).parent / parsed.path)).replace(os.sep,"/")
            if resolved in doc_map:
                target = doc_map[resolved]
            elif resolved.endswith(".html") and Path(resolved).stem in PAGES:
                target = "outputs/"+Path(resolved).name
            elif resolved in tracked:
                # Source-only evidence remains at its immutable published Git identity.
                return REPOSITORY+"/blob/"+commit+"/"+quote(resolved)+("#"+parsed.fragment if parsed.fragment else "")
            else:
                return None
        return urlunsplit(("","",relative(target,current),parsed.query,parsed.fragment))

    def links(content: str,current: str,source: str="") -> str:
        def replace(match: re.Match[str]) -> str:
            before, attr, delimiter, url, after, label = match.groups()
            target = public_url(url,current,source)
            if target is None:
                return '<span class="missing-evidence" title="Retained source evidence; this operational artifact is not part of the public site">'+label+'</span>'
            return '<a'+before+attr+'='+delimiter+esc(target)+delimiter+after+'>'+label+'</a>'
        # Only authored static documents enter this transformer, never candidate code.
        return re.sub(r'<a([^>]*?)(href)=("|\')([^"\']+)\3([^>]*)>(.*?)</a>', replace, content,flags=re.S|re.I)

    def write(path: str,content: str,source: Path|None=None,source_name: str="",provenance: str="generated",source_bytes: bytes|None=None) -> None:
        content = clean(content)
        target = out / path
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_text(content)
        entry = {"path":path,"exported_sha256":digest(target.read_bytes())}
        if source is not None:
            entry.update(source=source_name,source_sha256=digest(source_bytes if source_bytes is not None else source.read_bytes()),provenance=provenance)
            manifest["entries"].append(entry)
        else:
            manifest["generated_entries"].append(entry)

    def provenance(source: str,source_bytes: bytes) -> str:
        try:
            old = subprocess.check_output(["git","-C",str(repo),"show",commit+":"+source],stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            return "uncommitted-working-tree-snapshot"
        return "committed-source" if old == source_bytes else "uncommitted-working-tree-snapshot"

    def shell(title: str,body: str,current: str) -> str:
        nav = ''.join('<a href="'+relative(path,current)+'">'+label+'</a>' for path,label in [("index.html","Progress"),("outputs/index.html","Study outputs"),("docs/index.html","Documentation")])
        return '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>'+esc(title)+' · Gossip × Agents</title><link rel="stylesheet" href="'+relative("assets/site.css",current)+'"></head><body><main><nav class="site-nav" aria-label="Site"><a class="brand" href="'+relative("index.html",current)+'">Gossip × Agents</a>'+nav+'<a href="'+REPOSITORY+'">Repository ↗</a></nav>'+body+'<footer>Published snapshot · '+esc(exported)+' · <a href="'+relative("publication.json",current)+'">Publication provenance</a><br>This static site is updated when a new snapshot is published. It has no shared review or feedback service.</footer></main></body></html>'

    def card(item: dict,current: str) -> str:
        target = public_url(item.get("url",""),current)
        if not target:
            return ""
        return '<article class="panel"><h3><a href="'+esc(target)+'">'+esc(item['title'])+'</a></h3><p>'+esc(clean(item.get('change','')))+'</p>'+('<ul>'+''.join('<li>'+esc(clean(x))+'</li>' for x in item.get('inspect',[]))+'</ul>' if item.get('inspect') else '')+'<div class="version">Version '+esc(item.get('version',''))+' · '+esc(item.get('updatedAt',item.get('at','')))+'</div></article>'

    write("assets/site.css",CSS)
    write(".nojekyll","")
    # Public state is a narrow projection, never the local handoff or journal.
    tasks = [{k:clean(t[k]) if isinstance(t[k],str) else t[k] for k in ("id","title","weight","status","note","phase","inScope","updatedAt","tier") if k in t} for t in state['tasks']]
    total = sum(t['weight'] for t in tasks if t.get('inScope',True))
    done = sum(t['weight'] for t in tasks if t.get('inScope',True) and t['status']=='complete')
    percent = round(done/total*100) if total else 0
    public_state = {"protocol":"gossip-public-progress-v1","title":state['title'],"source_revision":state['revision'],"snapshot_at":exported,"last_meaningful_update":state['lastMeaningfulUpdate'],"recorded_status":state['status'],"verified_effort":done,"total_effort":total,"percent":percent,"tasks":tasks,"history":[{k:clean(p[k]) if isinstance(p[k],str) else p[k] for k in ("at","remaining","total","reason") if k in p} for p in state['history']],"review_semantics":"Read-only public projection; no change to any local user review checkpoint."}
    write("progress.json",json.dumps(public_state,indent=2,ensure_ascii=False)+"\n")
    latest = {}
    for c in state['cards']:
        latest[c['itemId']] = c
    cards = list(latest.values())
    current_cards = list(reversed(cards))[:4]
    old_cards = [c for c in reversed(cards) if c not in current_cards]
    points = state['history']
    max_y = max([p['total'] for p in points]+[1])
    points_text = ' '.join(f'{i*600/max(len(points)-1,1):.1f},{130-p["remaining"]*110/max_y:.1f}' for i,p in enumerate(points))
    chart = '<svg class="chart" viewBox="0 0 600 150" role="img" aria-label="Recorded remaining weighted effort across scope and completion changes"><path d="M0 130H600" stroke="#d3dbd3"/><polyline points="'+points_text+'" fill="none" stroke="#176c60" stroke-width="3"/></svg>'
    def task_rows(items: list[dict]) -> str:
        return ''.join('<article class="task"><div><h3>'+esc(t['title'])+'</h3><p>'+esc(t.get('note',''))+'</p></div><div class="task-status"><span class="pill">'+esc(t['status'].replace('_',' '))+'</span><div class="small">'+str(t['weight'])+' effort units</div></div></article>' for t in items)
    active_tasks = [t for t in tasks if t.get('inScope',True) and t['status']!='complete']
    complete_tasks = [t for t in tasks if t.get('inScope',True) and t['status']=='complete']
    body = '<header><div class="eyebrow">Public progress report · snapshot revision '+str(state['revision'])+'</div><h1>Can a swarm finish<br>better software?</h1><p class="lead">A research harness for distributed Git integration, gossip evidence, and many-candidate agent teams. Quality and sustained completion come first.</p><p class="small">Last recorded work: '+esc(state['lastMeaningfulUpdate'])+' · Recorded status: '+esc(state['status'])+'</p></header><div class="banner"><strong>Published snapshot, not a live monitor.</strong> The 20-role project is under development. Scripted rehearsals establish execution plumbing; they do not establish model quality or a statistical advantage over orchestration.</div><section class="overview" aria-label="Progress"><div class="panel"><div class="eyebrow">Verified effort</div><div class="percent">'+str(percent)+'%</div><div class="bar"><span style="width:'+str(percent)+'%"></span></div><p class="small">'+str(done)+' / '+str(total)+' weighted units. Estimates of project scope, not hours.</p></div><div class="panel"><div class="eyebrow">Remaining work</div>'+chart+'<p class="small">'+str(total-done)+' units remain. Scope additions are preserved; this curve implies no completion date.</p></div></section><section><h2>Current checkpoints</h2><div class="grid cards">'+''.join(card(c,"index.html") for c in current_cards)+'</div></section><section><h2>Active work</h2>'+task_rows(active_tasks)+'</section><details><summary>Completed work · '+str(len(complete_tasks))+' tasks</summary>'+task_rows(complete_tasks)+'</details><details><summary>Earlier published checkpoints</summary><div class="grid cards">'+''.join(card(c,"index.html") for c in old_cards)+'</div></details><p><a href="progress/archive.html">Browse recorded scope changes →</a></p>'
    write("index.html",shell("Progress",body,"index.html"))
    history = ''.join('<li><time>'+esc(p['at'])+'</time> · '+str(p['remaining'])+'/'+str(p['total'])+' remaining<p>'+esc(clean(p.get('reason','')))+'</p></li>' for p in reversed(points))
    write("progress/archive.html",shell("Scope history",'<div class="eyebrow">Historical scope and completion records</div><h1>How the work expanded</h1><p>This preserves the burndown history. A published snapshot does not change local reviews or mark outstanding work complete.</p><ol>'+history+'</ol>',"progress/archive.html"))
    output_cards = ''.join('<article class="panel"><h3><a href="'+name+'.html?progress-report">'+TITLES[name]+'</a></h3><p class="small">Authored study report. Findings and limitations retain their original scientific scope.</p></article>' for name in PAGES)
    development = ''
    checkpoint = 'analysis/peer-library-project-v4-checkpoint.json'
    if checkpoint in tracked and 'docs/peer-library-project-v4.md' in doc_map:
        development = '<section class="panel"><div class="eyebrow">Development qualification</div><h2>20-role M1 harness</h2><p>Read the <a href="../docs/peer-library-project-v4.html?progress-report">versioned methodology</a> and <a href="'+REPOSITORY+'/blob/'+commit+'/'+checkpoint+'">exact published checkpoint</a> for completed checks, retained failures and outstanding rehearsal or live evaluation. A development checkpoint is separate from a comparative model-quality result.</p></section>'
    write("outputs/index.html",shell("Study outputs",'<div class="eyebrow">Research, experiments and comparisons</div><h1>Study outputs</h1><p class="lead">Read the latest comparative reports first; earlier studies remain separate evidence rather than one pooled leaderboard.</p>'+development+'<div class="grid">'+output_cards+'</div>',"outputs/index.html"))

    def export_html(source: Path,path: str,source_name: str,kind: str|None=None) -> None:
        raw = source.read_bytes()
        kind = kind or provenance(source_name,raw)
        content = raw.decode()
        # Replace the local-only navigation script with an explicit static adapter.
        content = re.sub(r'<script\b[^>]*>.*?</script>', '', content, flags=re.S|re.I)
        content = links(content,path,source_name)
        content = clean(content)
        banner = '<aside class="source-banner" style="font:13px/1.6 system-ui;background:#e7eee5;color:#173836;padding:12px 20px;border-bottom:1px solid #d3dbd3"><a href="'+relative("outputs/index.html",path)+'">Study outputs</a> · <a href="'+relative("index.html",path)+'">Progress snapshot</a> · Public export of a historical report; the report’s original findings and qualification limits follow.</aside>'
        content = re.sub(r'(<body[^>]*>)',r'\1'+banner,content,count=1,flags=re.I)
        script = '<script>if(new URLSearchParams(location.search).has("progress-report")){const a=document.querySelector(".return");if(a)a.hidden=false;for(const link of document.querySelectorAll("a[href]")){const target=new URL(link.href,location.href);if(target.origin===location.origin&&target.pathname.endsWith(".html")){target.searchParams.set("progress-report","");link.href=target.href;}}}</script>'
        content = content.replace('</body>',script+'</body>')
        write(path,content,source,source_name,kind,raw)

    for name in PAGES:
        source = repo/(name+".html")
        export_html(source,"outputs/"+name+".html",name+".html")
    completed_artifacts: set[str] = set()
    while artifacts-completed_artifacts:
        name = sorted(artifacts-completed_artifacts)[0]
        export_html(report/"artifacts"/name,"artifacts/"+name,"report/artifacts/"+name,"independent-report-artifact")
        completed_artifacts.add(name)

    docs_index = []
    for source_name,path in sorted(doc_map.items()):
        source = repo/source_name
        if not source.is_file():
            continue
        raw = source.read_bytes()
        original = clean(raw.decode())
        historical_context = '`3019bcf`, before the retained-topic continuation change.\nThe [published-source checkpoint](../analysis/peer-library-project-v4-checkpoint.json)'
        if source_name == 'docs/peer-library-project-v4.md' and historical_context in original:
            if original.count(historical_context) != 1:
                raise SystemExit('Historical checkpoint context is ambiguous')
            historical_target = REPOSITORY+'/blob/3019bcf97db8d0f37b3bd1f045c8826155ad438e/analysis/peer-library-project-v4-checkpoint.json'
            corrected = historical_context.replace('../analysis/peer-library-project-v4-checkpoint.json',historical_target)
            original = original.replace(historical_context,corrected,1)
            manifest.setdefault('derivations',[]).append({'path':path,'source':source_name,'change':'Pin the historical 3019bcf / 361-check paragraph to its original immutable checkpoint; current checkpoint link remains bound to current source commit.','target':historical_target,'source_unchanged':True})
        fragment = subprocess.check_output([str(pandoc),"--from=gfm","--to=html5","--wrap=none","--syntax-highlighting=none"],input=original,text=True)
        fragment = links(fragment,path,source_name)
        title_match = re.search(r'^#\s+(.+)',original,re.M)
        title = title_match.group(1) if title_match else source.stem
        kind = provenance(source_name,raw)
        qualifier = "Uncommitted working-tree documentation snapshot; not included in the source commit below." if kind != "committed-source" else "Documentation from source commit "+commit[:12]+"."
        intro = '<p class="banner small">'+esc(qualifier)+' Exact input and exported hashes are recorded in the <a href="'+relative("publication.json",path)+'">publication manifest</a>. Operational evidence without a public artifact is shown as text.</p>'
        write(path,shell(title,intro+fragment,path),source,source_name,kind,raw)
        docs_index.append('<article class="panel"><h3><a href="'+esc(relative(path,"docs/index.html"))+'">'+esc(title)+'</a></h3><p class="small">'+esc(source_name)+' · '+esc(kind.replace('-',' '))+'</p></article>')
    write("docs/index.html",shell("Documentation",'<div class="eyebrow">Protocols, study plans and implementation boundaries</div><h1>Documentation</h1><p class="lead">Versioned design notes distinguish what was implemented, physically tested, and still proposed. New working documentation is labeled separately from committed evidence.</p><div class="grid">'+''.join(docs_index)+'</div>',"docs/index.html"))
    for p in sorted((out/"_tools").glob("*")):
        if p.is_file():
            manifest.setdefault("tools_entries",[]).append({"path":p.relative_to(out).as_posix(),"exported_sha256":digest(p.read_bytes())})
    for entry in manifest['entries']:
        source_path = report/entry['source'][len('report/'):] if entry['source'].startswith('report/') else repo/entry['source']
        if digest(source_path.read_bytes()) != entry['source_sha256']:
            raise SystemExit('Input changed during export: '+entry['source']+'; preserve output and rebuild once source is stable')
    if command("git","-C",str(repo),"rev-parse","HEAD") != commit:
        raise SystemExit('Source HEAD changed during export; rebuild from a stable checkpoint')
    (out/"publication.json").write_text(json.dumps(manifest,indent=2,ensure_ascii=False)+"\n")
    print(json.dumps({"status":"built","source_commit":commit,"report_revision":state['revision'],"public_files":len(manifest['entries'])+len(manifest['generated_entries'])+1,"artifact_count":len(completed_artifacts),"renderer":version,"output":str(out)}))

if __name__ == "__main__":
    main()
