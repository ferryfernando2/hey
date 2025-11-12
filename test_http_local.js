#!/usr/bin/env node
// Lightweight diagnostic script to run on the VPS. It will:
//  - check what process (if any) is listening on port 8080
//  - perform HTTP GETs to http://127.0.0.1:8080/health and /
//  - print concise, copy-paste-friendly output
// Usage: on the VPS run: node test_http_local.js

const { exec } = require('child_process');
const http = require('http');

const TIMEOUT_MS = 5000;

function runCmd(cmd) {
  return new Promise((resolve) => {
    exec(cmd, { maxBuffer: 1024 * 1024 }, (err, stdout, stderr) => {
      resolve({ err: err ? String(err) : null, stdout: stdout ? String(stdout) : '', stderr: stderr ? String(stderr) : '' });
    });
  });
}

function httpGet(url) {
  return new Promise((resolve) => {
    const req = http.get(url, { timeout: TIMEOUT_MS }, (res) => {
      let buf = '';
      res.on('data', (c) => (buf += c.toString()));
      res.on('end', () => {
        resolve({ ok: true, statusCode: res.statusCode, headers: res.headers, body: buf.slice(0, 2000) });
      });
    });
    req.on('error', (e) => resolve({ ok: false, error: String(e) }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, error: 'timeout' }); });
  });
}

async function main() {
  console.log('=== VPS local HTTP diagnostic (appchat) ===');
  console.log(`Timestamp: ${new Date().toISOString()}`);

  console.log('\n1) Check listeners on port 8080 (ss then netstat fallback)');
  let out = await runCmd("ss -tulpn | grep ':8080' || true");
  if (!out.stdout || !out.stdout.trim()) {
    out = await runCmd("netstat -tulpn 2>/dev/null | grep 8080 || true");
  }
  console.log('--- listeners ---');
  console.log(out.stdout || '(no output)');

  console.log('\n2) HTTP GET to http://127.0.0.1:8080/health');
  const r1 = await httpGet('http://127.0.0.1:8080/health');
  if (r1.ok) {
    console.log(`OK status=${r1.statusCode}`);
    console.log('body:', r1.body.replace(/\n/g,'\\n').slice(0,1000));
  } else {
    console.log('FAILED:', r1.error || 'unknown');
  }

  console.log('\n3) HTTP GET to http://127.0.0.1:8080/ (root)');
  const r2 = await httpGet('http://127.0.0.1:8080/');
  if (r2.ok) {
    console.log(`OK status=${r2.statusCode}`);
    console.log('body:', r2.body.replace(/\n/g,'\\n').slice(0,1000));
  } else {
    console.log('FAILED:', r2.error || 'unknown');
  }

  console.log('\n4) If the above GETs fail but ss/netstat show a listener, the process may be accepting then resetting connections.');
  console.log('If tests show no listener on 0.0.0.0:8080 but only 127.0.0.1:8080, restart the app to bind to 0.0.0.0 or update proxy config.');

  console.log('\n=== End diagnostic ===');
}

main().catch((e) => { console.error('fatal', e); process.exitCode = 2; });
