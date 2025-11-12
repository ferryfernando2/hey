const http = require('http');
const WebSocket = require('ws');

const HOST = 'localhost';
const PORT = process.env.PORT || 8080;
const BASE = `http://${HOST}:${PORT}`;

function postJSON(path, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(`${BASE}${path}`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } }, (res) => {
      let buf = '';
      res.setEncoding('utf8');
      res.on('data', d => buf += d);
      res.on('end', () => {
        try { resolve(JSON.parse(buf)); } catch (e) { resolve(buf); }
      });
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function run() {
  console.log('E2E test starting against', BASE);
  // Register two users
  const a = await postJSON('/register', { email: `e2e_a_${Date.now()}@test`, password: 'pw', username: 'e2eA' });
  const b = await postJSON('/register', { email: `e2e_b_${Date.now()}@test`, password: 'pw', username: 'e2eB' });
  console.log('Registered users', a.id, b.id);

  // Login A
  const la = await postJSON('/login', { email: a.email, password: 'pw' });
  console.log('Login A OK:', la.id);

  // Connect both websockets
  const wsA = new WebSocket(`ws://${HOST}:${PORT}`);
  const wsB = new WebSocket(`ws://${HOST}:${PORT}`);

  await Promise.all(['open','open'].map((_, i) => new Promise(res => {
    const ws = i===0?wsA:wsB; ws.on('open', res);
  })));
  console.log('Both websockets connected');

  const messagesB = [];
  wsB.on('message', (m) => {
    try { const d = JSON.parse(m.toString()); messagesB.push(d); } catch (e) { messagesB.push(m.toString()); }
  });

  // Auth both sockets
  wsA.send(JSON.stringify({ type: 'auth', userId: a.id }));
  wsB.send(JSON.stringify({ type: 'auth', userId: b.id }));

  // Wait briefly for auth to process
  await new Promise(r => setTimeout(r, 300));

  // Send a chat from A -> B
  wsA.send(JSON.stringify({ type: 'chat', fromId: a.id, toId: b.id, message: JSON.stringify({ text: 'E2E hello', encrypted: false }) }));

  // Wait up to 2s for delivery
  await new Promise(r => setTimeout(r, 1000));

  console.log('Messages received by B count:', messagesB.length);
  if (!messagesB.find(m => m && m.type === 'chat')) {
    console.error('No chat message delivered to recipient via WebSocket');
    process.exit(2);
  }

  console.log('E2E test passed');
  wsA.close(); wsB.close();
  // allow sockets to close cleanly
  setTimeout(() => process.exit(0), 300);
}

run().catch(e => { console.error('E2E failed', e); process.exit(1); });
