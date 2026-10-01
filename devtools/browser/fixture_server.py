"""Serve a relocated report snapshot with its actual handler on an allocated port.

Only the dedicated fixture directory is written. The report implementation and
unit tests remain independent of the product, and canonical state is never opened
for writing. Environment URLs are relocated in copied HTML/state, not in source.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
from http.server import ThreadingHTTPServer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--locator", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    locator = json.loads(args.locator.read_text())
    source = Path(locator["reportWorkspace"]).resolve()
    repo = args.locator.resolve().parent.parent
    root = args.directory.resolve()
    root.mkdir(parents=True, exist_ok=False)
    (root / "data").mkdir()
    products = root / "pages"
    products.mkdir()
    shutil.copy2(source / "report.py", root / "report.py")
    shutil.copy2(source / "index.html", root / "index.html")
    state = json.loads(Path(locator["stateLocation"]).read_text())
    for page in locator["apps"]:
        shutil.copy2(repo / f"{page}.html", products / f"{page}.html")
    if (source / "artifacts").is_dir():
        shutil.copytree(source / "artifacts", root / "artifacts")
    spec = importlib.util.spec_from_file_location("isolated_report", root / "report.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load the independent report implementation")
    report = importlib.util.module_from_spec(spec)
    sys.dont_write_bytecode = True
    spec.loader.exec_module(report)
    setattr(report, "REPO", products)
    server = ThreadingHTTPServer(("127.0.0.1", 0), report.Handler)
    base = f"http://127.0.0.1:{server.server_port}/"
    original = locator["reportUrl"].rstrip("/") + "/"
    setattr(report, "REPORT_URL", base)
    # Relocate the trusted environment configuration in snapshot files. All UI
    # and server logic still executes the real report implementation.
    for html in root.rglob("*.html"):
        html.write_text(html.read_text().replace(original, base))
    serialized = json.dumps(state).replace(original, base)
    (root / "data" / "project.json").write_text(serialized + "\n")
    print(json.dumps({"url": base, "directory": str(root), "pid": os.getpid()}), flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
