#!/usr/bin/env python3
"""Serve only the public site, with its real project-site base path."""
import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
parser.add_argument("--port", type=int, default=4279)
args = parser.parse_args()

class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        path = unquote(urlsplit(self.path).path)
        if not path.startswith("/gossip-protocol/"):
            self.send_error(404)
            return
        relative = path[len("/gossip-protocol/"):]
        if any(part.startswith(".") or part == "_tools" for part in Path(relative).parts):
            self.send_error(404)
            return
        self.path = self.path.replace("/gossip-protocol/", "/", 1)
        super().do_GET()

ThreadingHTTPServer(("127.0.0.1", args.port), partial(Handler, directory=str(args.root))).serve_forever()
