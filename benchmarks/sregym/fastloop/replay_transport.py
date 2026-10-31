"""Local subprocess command transport for captured worker replay."""

from __future__ import annotations

import json
import os
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path


@contextmanager
def captured_commands(root: Path, command: Callable[[list[str]], str]) -> Iterator[dict[str, str]]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            pass

        def do_POST(self):
            try:
                value = command(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
                result = {"value": value}
            except Exception as exc:
                result = {"error": str(exc)}
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    binary = root / "captured-bin"
    binary.mkdir()
    for name in ("sdo", "kubectl", "sdo-operator"):
        path = binary / name
        path.write_text(f"""#!{sys.executable}
import json, sys, urllib.request
payload = json.dumps([{name!r}, *sys.argv[1:]]).encode()
result = json.load(urllib.request.urlopen('http://127.0.0.1:{server.server_port}', data=payload))
if 'error' in result:
    print(result['error'], file=sys.stderr)
    raise SystemExit(1)
print(result['value'], end='')
""")
        path.chmod(0o755)
    try:
        yield {**os.environ, "PATH": str(binary) + os.pathsep + os.environ["PATH"]}
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
