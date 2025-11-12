# Running multiple server instances locally (no Docker)

This launcher starts two Node server instances on different ports so you can locally test cross-instance behavior (Redis pub/sub required for cross-instance messaging).

Requirements:
- Node.js 18+
- Redis server running locally (or set REDIS_URL environment variable to a remote Redis).

Start two instances:

PowerShell:

```powershell
# optional: start redis locally (if you have it installed)
# redis-server &
$env:REDIS_URL = 'redis://127.0.0.1:6379'
node run-multiple-instances.js
```

The launcher will start two child Node processes running `server.js` on ports 8080 and 8081. You can then connect clients to either port to simulate users in different regions.

Notes:
- The launcher does not provide automatic load balancing; use your client to switch between ports or add a lightweight reverse-proxy if needed.
- For cross-instance message delivery ensure Redis is reachable and `REDIS_URL` is set.
