"""A synthetic notes service for the SDO onboarding fast gate. Standard library only.

Users sign in with a seeded account and get a bearer token. Each seeded user owns three seeded
notes. Signed-in users list, create, read and delete their own notes. State is in memory.
"""

import json
import secrets
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

USERS = {f"user{n:02d}": f"pass-{n:02d}-synthetic" for n in range(1, 11)}
LOCK = threading.Lock()
TOKENS: dict[str, str] = {}
NOTES: dict[str, dict[str, str]] = {}
NEXT = [1]


def _seed() -> None:
    for user in USERS:
        for title in ("welcome", "groceries", "ideas"):
            _create(user, f"{title} of {user}", f"seeded {title}")


def _create(owner: str, title: str, body: str) -> dict[str, str]:
    note = {"id": f"n{NEXT[0]}", "owner": owner, "title": title, "body": body}
    NEXT[0] += 1
    NOTES[note["id"]] = note
    return note


class Handler(BaseHTTPRequestHandler):
    @property
    def route(self) -> str:
        return urlsplit(self.path).path

    def _send(self, status: int, payload: object) -> None:
        data = b"" if status == 204 else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict[str, str]:
        length = int(self.headers.get("Content-Length") or 0)
        try:
            data = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            return {}
        return data if isinstance(data, dict) else {}

    def _user(self) -> str | None:
        header = self.headers.get("Authorization", "")
        return TOKENS.get(header.removeprefix("Bearer ").strip()) if header.startswith("Bearer ") else None

    def do_GET(self) -> None:
        if self.route == "/healthz":
            return self._send(200, {"ok": True})
        user = self._user()
        if user is None:
            return self._send(401, {"error": "sign in first"})
        with LOCK:
            if self.route == "/api/notes":
                mine = [note for note in NOTES.values() if note["owner"] == user]
                return self._send(200, {"notes": mine, "count": len(mine)})
            if self.route.startswith("/api/notes/"):
                note = NOTES.get(self.route.removeprefix("/api/notes/"))
                if note is None or note["owner"] != user:
                    return self._send(404, {"error": "no such note"})
                return self._send(200, note)
        return self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        body = self._body()
        if self.route == "/api/login":
            if USERS.get(body.get("user", "")) != body.get("password"):
                return self._send(401, {"error": "wrong user or password"})
            token = secrets.token_hex(16)
            with LOCK:
                TOKENS[token] = body["user"]
            return self._send(200, {"token": token, "user": body["user"]})
        user = self._user()
        if user is None:
            return self._send(401, {"error": "sign in first"})
        if self.route == "/api/notes":
            if not body.get("title"):
                return self._send(400, {"error": "title is required"})
            with LOCK:
                note = _create(user, body["title"], body.get("body", ""))
            return self._send(201, note)
        return self._send(404, {"error": "not found"})

    def do_DELETE(self) -> None:
        user = self._user()
        if user is None:
            return self._send(401, {"error": "sign in first"})
        with LOCK:
            note = NOTES.get(self.route.removeprefix("/api/notes/"))
            if not self.route.startswith("/api/notes/") or note is None or note["owner"] != user:
                return self._send(404, {"error": "no such note"})
            del NOTES[note["id"]]
        return self._send(204, {})

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 (the base signature)
        pass


if __name__ == "__main__":
    _seed()
    ThreadingHTTPServer(("", 8080), Handler).serve_forever()
