# AppChat Server - Local development

This server can run using a local SQLite file (no external DB required) or Postgres when `DATABASE_URL` is set.

Quick start (SQLite, no Docker)

1) Install dependencies:

```powershell
cd .\server
npm install
```

2) Start the server using the built-in SQLite fallback:

```powershell
$env:USE_SQLITE = "true"
npm start
```

The server will create `server/database/appchat.sqlite` and the necessary tables automatically.

If you prefer Postgres, set `DATABASE_URL` in your environment before starting the server. The server will use Postgres when `DATABASE_URL` is present.

Security note: remove or rotate credentials in `.env` before publishing the repository and avoid committing DB files to VCS.
