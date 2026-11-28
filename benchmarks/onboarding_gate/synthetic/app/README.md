# Synthetic notes service

A small synthetic HTTP/JSON service (`server.py`, standard library only) for testing onboarding.
It runs as one Deployment and the `notes` Service on port 8080 (`deploy/app.yaml`).

API (JSON bodies):

- `POST /api/login` `{"user": "...", "password": "..."}` returns `{"token": "..."}`; a wrong password is 401.
  Ten seeded accounts: `user01` .. `user10`, password `pass-NN-synthetic` (for example `pass-01-synthetic`).
- Every other `/api` call needs `Authorization: Bearer TOKEN`.
- `GET /api/notes` lists the signed-in user's notes: `{"notes": [{"id", "owner", "title", "body"}], "count": N}`.
  Each seeded user starts with three notes titled `welcome of USER`, `groceries of USER` and `ideas of USER`.
- `POST /api/notes` `{"title": "...", "body": "..."}` creates a note and returns it (201).
- `GET /api/notes/ID` reads one note; `DELETE /api/notes/ID` deletes it (204); another user's note is 404.
- `GET /healthz` is the liveness check.
