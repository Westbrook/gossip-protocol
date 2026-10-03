#!/usr/bin/env python3
"""Check a read-only GitHub Pages export without third-party dependencies."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit


SHA256 = re.compile(r"[0-9a-f]{64}\Z")
LOOPBACK = re.compile(r"\blocalhost\b|\b127(?:\.|%2e)0(?:\.|%2e)0(?:\.|%2e)1\b", re.I)
TEXT_SUFFIXES = {".html", ".htm", ".css", ".js", ".mjs", ".json", ".md", ".txt", ".svg", ".xml", ".yml", ".yaml"}
FORBIDDEN_TEXT = {
    "absolute_user_path": re.compile(r"/Users/", re.I),
    "temporary_host_path": re.compile(r"/private/tmp(?:/|\b)", re.I),
    "provider_secret": re.compile(r"\bsk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{20,}\b"),
    "bearer_secret": re.compile(r"\bBearer\s+[A-Za-z0-9._~+/-]{24,}={0,2}\b", re.I),
    "aws_access_key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "private_key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    "assigned_api_secret": re.compile(r"\b(?:OPENAI_API_KEY|ANTHROPIC_API_KEY)\s*[=:]\s*[\"']?[A-Za-z0-9_-]{20,}", re.I),
}
ACTIVE_SCRIPT = {
    "api_endpoint_in_executable_script": re.compile(r"[\"'`](?:https?://[^/\s\"'`]+)?/?(?:[^\s\"'`]+/)?api(?:/|[\"'`])", re.I),
    "post_request": re.compile(r"\bmethod\s*:\s*[\"'`]POST[\"'`]|\.open\s*\(\s*[\"'`]POST[\"'`]|\.post\s*\(", re.I),
}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class Document(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[tuple[str, str]] = []
        self.anchors: set[str] = set()
        self.duplicate_ids: set[str] = set()
        self.base: str | None = None
        self.scripts: list[str] = []
        self.styles: list[str] = []
        self.write_controls: list[str] = []
        self._script: list[str] | None = None
        self._style: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        identifier = values.get("id")
        if identifier:
            if identifier in self.anchors:
                self.duplicate_ids.add(identifier)
            self.anchors.add(identifier)
        if tag == "a" and values.get("name"):
            self.anchors.add(str(values["name"]))
        if tag == "base" and values.get("href"):
            if self.base is not None:
                self.write_controls.append("multiple base elements")
            self.base = values["href"]
        else:
            for attr in ("href", "src", "poster", "action", "formaction"):
                if values.get(attr):
                    self.links.append((attr, str(values[attr])))
            # Data URLs have commas; they are self-contained and need no target check.
            if values.get("srcset") and not str(values["srcset"]).lstrip().startswith("data:"):
                self.links.extend(("srcset", part.strip().split()[0]) for part in str(values["srcset"]).split(",") if part.strip())
        if tag == "form" and str(values.get("method", "get")).lower() == "post":
            self.write_controls.append("POST form")
        if str(values.get("formmethod", "")).lower() == "post":
            self.write_controls.append("POST form control")
        if "data-review" in values or "data-feedback" in values:
            self.write_controls.append("local review/feedback write control")
        for attr, value in attrs:
            if attr.startswith("on") and value:
                self.scripts.append(value)
        if values.get("style"):
            self.styles.append(str(values["style"]))
        if tag == "script" and str(values.get("type", "")).lower() not in {"application/json", "application/ld+json"}:
            self._script = []
        if tag == "style":
            self._style = []

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_data(self, data: str) -> None:
        if self._script is not None:
            self._script.append(data)
        if self._style is not None:
            self._style.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self._script is not None:
            self.scripts.append("".join(self._script))
            self._script = None
        if tag == "style" and self._style is not None:
            self.styles.append("".join(self._style))
            self._style = None


class Checker:
    def __init__(self, root: Path, base_path: str, source_root: Path | None, report_root: Path | None, report_state: Path | None, require_sources: bool, renderer_executable: Path | None = None) -> None:
        self.root = root.resolve()
        self.base_path = "/" + base_path.strip("/") + "/"
        self.origin = "https://pages.example.invalid"
        self.source_root = source_root
        self.report_root = report_root
        self.report_state = report_state
        self.require_sources = require_sources
        self.renderer_executable = renderer_executable
        self.counts: Counter[str] = Counter()
        self.errors: list[dict[str, str]] = []
        self.documents: dict[str, Document] = {}
        self.public_files: set[str] = set()

    def error(self, kind: str, path: str, detail: str) -> None:
        self.errors.append({"kind": kind, "path": path, "detail": detail})

    def scan(self) -> None:
        for path in sorted(self.root.rglob("*")):
            relative = path.relative_to(self.root)
            if relative.parts[0] in {".git", "_tools"}:
                continue
            name = relative.as_posix()
            if path.is_symlink():
                self.error("symlink", name, "Published payload must contain regular files only.")
                continue
            if not path.is_file():
                continue
            self.public_files.add(name)
            self.counts["public_files"] += 1
            lowered = path.name.lower()
            if lowered == ".ds_store" or lowered.startswith(".env") or path.suffix.lower() in {".sqlite", ".sqlite3", ".db", ".pem", ".key", ".p12", ".pfx"} or (re.search(r"(?:capabilit(?:y|ies)|credentials?|secrets?)(?:[-_.]|$)", lowered) and path.suffix.lower() in {".json", ".jsonl", ".bin", ".txt"}):
                self.error("forbidden_file", name, "Environment, database, capability or credential files cannot be published.")
            if path.suffix.lower() not in TEXT_SUFFIXES and path.name not in {".nojekyll", "CNAME"}:
                continue
            try:
                content = path.read_text(encoding="utf-8")
            except (UnicodeError, OSError):
                self.error("unreadable_text", name, "Expected UTF-8 public text.")
                continue
            self.counts["text_files"] += 1
            # JSON may escape slashes and Unicode characters; decode before scanning too.
            inspect = content
            if path.suffix.lower() in {".html", ".htm", ".svg", ".xml"}:
                inspect += "\n" + unescape(content)
            if path.suffix.lower() == ".json":
                try:
                    inspect += "\n" + json.dumps(json.loads(content), ensure_ascii=False)
                except json.JSONDecodeError:
                    self.error("invalid_json", name, "JSON failed to parse.")
            for kind, pattern in FORBIDDEN_TEXT.items():
                if pattern.search(inspect):
                    self.error(kind, name, "Forbidden public content detected; matching bytes are not printed.")
            mentions = len(LOOPBACK.findall(content))
            if mentions:
                self.counts["loopback_mentions_including_documentation"] += mentions
            if path.suffix.lower() in {".html", ".htm"}:
                document = Document()
                document.feed(content)
                document.close()
                self.documents[name] = document
                self.counts["html_documents"] += 1
                for control in document.write_controls:
                    self.error("write_control", name, control)
                for identifier in document.duplicate_ids:
                    self.error("duplicate_id", name, identifier)
                self.check_scripts(name, document.scripts)
                self.check_styles(name, document.styles)
            elif path.suffix.lower() in {".js", ".mjs"}:
                self.check_scripts(name, [content])
            elif path.suffix.lower() == ".css":
                self.check_styles(name, [content])

    def check_scripts(self, name: str, scripts: list[str]) -> None:
        for source in scripts:
            if LOOPBACK.search(source):
                self.error("active_loopback_script", name, "Executable script contains a local host endpoint.")
            for kind, pattern in ACTIVE_SCRIPT.items():
                if pattern.search(source):
                    self.error(kind, name, "Executable script contains a prohibited API endpoint or write request.")

    def check_styles(self, name: str, styles: list[str]) -> None:
        for source in styles:
            for match in re.finditer(r"url\s*\(\s*([^)]+)\)", source, re.I):
                if LOOPBACK.search(match.group(1)):
                    self.error("active_loopback_style", name, "CSS requests a local host endpoint.")

    def check_links(self) -> None:
        for name, document in self.documents.items():
            page_url = self.origin + self.base_path + name
            base = urljoin(page_url, document.base) if document.base else page_url
            if document.base and LOOPBACK.search(document.base):
                self.error("active_loopback_link", name, "Base URL points to a local host endpoint.")
            if document.base and not base.startswith(self.origin + self.base_path):
                self.error("base_path", name, "Base element escapes the published project path.")
            for attr, link in document.links:
                self.counts["links"] += 1
                if LOOPBACK.search(unquote(link)):
                    self.error("active_loopback_link", name, attr + " points to a local host endpoint.")
                try:
                    parsed = urlsplit(urljoin(base, link))
                except ValueError:
                    self.error("invalid_link", name, "Could not parse " + attr + " URL.")
                    continue
                if parsed.scheme in {"mailto", "tel"}:
                    self.counts["external_links"] += 1
                    continue
                if parsed.scheme == "data" and attr in {"src", "srcset", "poster"}:
                    self.counts["embedded_assets"] += 1
                    continue
                if parsed.scheme not in {"http", "https"}:
                    self.error("unsafe_link_scheme", name, "Unsupported " + attr + " URL scheme.")
                    continue
                if parsed.netloc != urlsplit(self.origin).netloc:
                    self.counts["external_links"] += 1
                    continue
                decoded = unquote(parsed.path)
                if not decoded.startswith(self.base_path):
                    self.error("base_path", name, "Internal link escapes " + self.base_path + ": " + parsed.path)
                    continue
                target_name = decoded[len(self.base_path):]
                target = self.root / target_name
                if not target.resolve().is_relative_to(self.root) or target_name.split("/")[0] in {".git", "_tools"}:
                    self.error("private_target", name, "Internal link reaches non-public content.")
                    continue
                if target.is_dir():
                    target_name = target_name.rstrip("/") + "/index.html" if target_name else "index.html"
                if target_name not in self.public_files:
                    self.error("missing_target", name, "Missing published target: " + target_name)
                    continue
                self.counts["internal_links_verified"] += 1
                fragment = unquote(parsed.fragment)
                if fragment and target_name in self.documents:
                    # Browser text-fragment directives are not element identifiers.
                    anchor = fragment.split(":~:text=", 1)[0]
                    if anchor and anchor not in self.documents[target_name].anchors:
                        self.error("missing_fragment", name, "Missing anchor " + anchor + " in " + target_name)
                    else:
                        self.counts["fragments_verified"] += 1

    def hash_file(self, name: object, expected: object, kind: str) -> bool:
        if not isinstance(name, str) or not name or Path(name).is_absolute() or ".." in Path(name).parts:
            self.error("manifest_path", "publication.json", "Manifest contains a non-relative file path.")
            return False
        if not isinstance(expected, str) or not SHA256.fullmatch(expected):
            self.error("manifest_hash", name, "Expected a lowercase SHA-256 digest.")
            return False
        path = self.root / name
        if not path.is_file() or path.is_symlink() or not path.resolve().is_relative_to(self.root):
            self.error("manifest_target", name, "Manifest target is missing or unsafe.")
            return False
        if digest(path) != expected:
            self.error("hash_mismatch", name, kind + " digest does not match file bytes.")
            return False
        self.counts[kind + "_hashes_verified"] += 1
        return True

    def source_path(self, source: str) -> Path | None:
        if source in {"report/data/project.json", "report/project.json"} and self.report_state is not None:
            return self.report_state
        if source.startswith("report/"):
            return self.report_root / source.removeprefix("report/") if self.report_root else None
        return self.source_root / source if self.source_root else None

    def check_manifest(self) -> None:
        path = self.root / "publication.json"
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            self.error("manifest", "publication.json", "Missing or invalid publication manifest.")
            return
        if not isinstance(manifest, dict):
            self.error("manifest", "publication.json", "Manifest must be an object.")
            return
        for field in ("protocol", "source_repository", "source_commit", "exported_at"):
            if not isinstance(manifest.get(field), str) or not manifest[field]:
                self.error("manifest_metadata", "publication.json", "Missing text metadata: " + field)
        if not re.fullmatch(r"[0-9a-f]{40}", str(manifest.get("source_commit", ""))):
            self.error("manifest_metadata", "publication.json", "Source commit must be an exact 40-digit SHA.")
        if type(manifest.get("report_revision")) is not int or manifest["report_revision"] < 0:
            self.error("manifest_metadata", "publication.json", "Report revision must be a nonnegative integer.")
        report_snapshot = self.report_state or (self.report_root / "data/project.json" if self.report_root else None)
        if not SHA256.fullmatch(str(manifest.get("report_source_sha256", ""))):
            self.error("manifest_metadata", "publication.json", "Invalid report source SHA-256.")
        elif report_snapshot is not None:
            if not report_snapshot.is_file() or digest(report_snapshot) != manifest["report_source_sha256"]:
                self.error("report_snapshot_hash", "publication.json", "Report source digest differs from the supplied snapshot.")
            else:
                self.counts["report_snapshot_hashes_verified"] += 1
                try:
                    snapshot = json.loads(report_snapshot.read_text(encoding="utf-8"))
                    if snapshot.get("revision") != manifest.get("report_revision"):
                        self.error("report_snapshot_revision", "publication.json", "Report revision differs from the source snapshot.")
                except (UnicodeError, json.JSONDecodeError, AttributeError):
                    self.error("report_snapshot_json", "publication.json", "Report snapshot must be a JSON object.")
        else:
            self.counts["report_snapshot_hashes_unverified"] += 1
            if self.require_sources:
                self.error("report_snapshot_unavailable", "publication.json", "No report root or immutable snapshot supplied.")
        renderer = manifest.get("renderer")
        if not isinstance(renderer, dict) or not renderer.get("version"):
            self.error("renderer", "publication.json", "Missing renderer identity.")
        elif not SHA256.fullmatch(str(renderer.get("sha256", ""))):
            self.error("renderer", "publication.json", "Invalid renderer SHA-256.")
        elif self.renderer_executable is not None:
            if not self.renderer_executable.is_file() or digest(self.renderer_executable) != renderer["sha256"]:
                self.error("renderer_hash", "publication.json", "Renderer digest differs from the supplied executable.")
            else:
                self.counts["renderer_hashes_verified"] += 1
        else:
            self.counts["renderer_hashes_unverified"] += 1
        covered: set[str] = set()
        for group in ("entries", "generated_entries", "tools_entries"):
            entries = manifest.get(group, [] if group == "tools_entries" else None)
            if not isinstance(entries, list):
                self.error("manifest_entries", "publication.json", group + " must be an array.")
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    self.error("manifest_entry", "publication.json", group + " contains a non-object.")
                    continue
                name = entry.get("path")
                if isinstance(name, str):
                    if name in covered:
                        self.error("manifest_duplicate", name, "File occurs more than once in manifest.")
                    covered.add(name)
                self.hash_file(name, entry.get("exported_sha256"), "exported")
                if group != "entries":
                    continue
                source = entry.get("source")
                source_hash = entry.get("source_sha256")
                if not isinstance(source, str) or not source or Path(source).is_absolute() or ".." in Path(source).parts:
                    self.error("source_path", str(name), "Source identity must be repository/report relative.")
                    continue
                if not isinstance(source_hash, str) or not SHA256.fullmatch(source_hash):
                    self.error("source_hash", str(name), "Source identity lacks a valid SHA-256 digest.")
                    continue
                if not entry.get("provenance"):
                    self.error("source_provenance", str(name), "Missing derivative provenance.")
                original = self.source_path(source)
                if original is None:
                    self.counts["source_hashes_unverified"] += 1
                    if self.require_sources:
                        self.error("source_unavailable", str(name), "No source root supplied for " + source)
                elif not original.is_file() or digest(original) != source_hash:
                    self.error("source_hash_mismatch", str(name), "Source digest differs from the supplied original: " + source)
                else:
                    self.counts["source_hashes_verified"] += 1
        for name in sorted(self.public_files - covered - {"publication.json"}):
            self.error("unmanifested_payload", name, "Public file is absent from the publication manifest.")

    def run(self) -> dict[str, object]:
        self.scan()
        self.check_links()
        self.check_manifest()
        return {"protocol": "gossip-pages-integrity-v1", "passed": not self.errors, "base_path": self.base_path, "counts": dict(sorted(self.counts.items())), "errors": self.errors}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, nargs="?", default=Path(__file__).resolve().parents[1])
    parser.add_argument("--base-path", default="/gossip-protocol/")
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--report-root", type=Path)
    parser.add_argument("--report-state", type=Path)
    parser.add_argument("--renderer-executable", type=Path)
    parser.add_argument("--require-sources", action="store_true")
    args = parser.parse_args()
    checker = Checker(args.root, args.base_path, args.source_root, args.report_root, args.report_state, args.require_sources, args.renderer_executable)
    result = checker.run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
