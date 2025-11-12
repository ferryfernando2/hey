// Simple launcher to start two Node server instances (no Docker, no nginx).
// Usage: node run-multiple-instances.js
// It will spawn two child Node processes running server.js with PORT=8080 and PORT=8081.

const { spawn } = require('child_process');
const path = require('path');

const instances = [
  { name: 'server-1', port: process.env.PORT1 || '8080' },
  { name: 'server-2', port: process.env.PORT2 || '8081' }
];

const serverPath = path.resolve(__dirname, 'server.js');

function spawnInstance(inst) {
  const env = { ...process.env, PORT: inst.port, PUBSUB_URL: `http://127.0.0.1:9000` };
  console.log(`Starting ${inst.name} on port ${inst.port}`);
  const proc = spawn('node', [serverPath], { env, stdio: ['ignore', 'pipe', 'pipe'] });

  proc.stdout.on('data', (d) => process.stdout.write(`[${inst.name}] ${d}`));
  proc.stderr.on('data', (d) => process.stderr.write(`[${inst.name}][ERR] ${d}`));

  proc.on('exit', (code, sig) => console.log(`${inst.name} exited code=${code} sig=${sig}`));
  return proc;
}

const children = instances.map(spawnInstance);

// Forward SIGINT/SIGTERM to children
const shutdown = () => {
  console.log('Shutting down children...');
  for (const c of children) {
    try { c.kill('SIGTERM'); } catch(e){}
  }
  process.exit();
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

console.log('Launcher started. Press Ctrl+C to stop.');

// --- In-process pub/sub relay (HTTP poll-based) ---
// Simple memory queues: messages and presence. Servers will POST to /publish/* and poll /poll/*
const relayPort = process.env.PUBSUB_RELAY_PORT || 9000;
const express = require('express');
const bodyParser = require('body-parser');
const relayApp = express();
relayApp.use(bodyParser.json({ limit: '1mb' }));

const queues = { messages: [], presence: [] };

relayApp.post('/publish/messages', (req, res) => {
  const m = req.body;
  if (m) queues.messages.push(m);
  return res.json({ ok: true });
});

relayApp.post('/publish/presence', (req, res) => {
  const p = req.body;
  if (p) queues.presence.push(p);
  return res.json({ ok: true });
});

relayApp.get('/poll/messages', (req, res) => {
  const out = queues.messages.splice(0, queues.messages.length);
  res.json(out);
});

relayApp.get('/poll/presence', (req, res) => {
  const out = queues.presence.splice(0, queues.presence.length);
  res.json(out);
});

relayApp.listen(relayPort, '127.0.0.1', () => console.log(`Local pubsub relay listening on http://127.0.0.1:${relayPort}`));
