# Proteomes API deployment (Docker)

This repo contains a static website (GitHub Pages) and a FastAPI backend (must be hosted elsewhere).

## Files you need on the server

- `api/` (FastAPI code + Dockerfile + requirements)
- `docker-compose.api.yml`
- `api/.env` (create from `api/.env.example`)

You can either:
- `git clone` the whole repo on the server (simplest), or
- copy only the files above.

## Prerequisites on the server

- Docker Engine
- Docker Compose plugin (`docker compose`)

On Debian/Ubuntu, install via Docker’s official instructions.

## Run

From the repo root on the server:

1) Create env file:

```bash
cp api/.env.example api/.env
nano api/.env
```

2) Start:

```bash
docker compose --env-file api/.env -f docker-compose.api.yml up -d --build
```

3) Test:

```bash
curl -sS http://localhost:8000/health
curl -sS http://localhost:8000/columns | head
```

## TLS (required for GitHub Pages)

GitHub Pages is HTTPS, so your API must also be HTTPS.

Recommended: put a reverse proxy in front (Caddy or Nginx) and expose only 443.
Example (conceptual):
- Public: `https://api.example.org` -> proxy to `http://127.0.0.1:8000`

## Frontend config

Edit `assets/config.js` in the GitHub Pages repo to set:

```js
window.BBC_PROTEOMES_API_BASE = 'https://api.example.org';
```
