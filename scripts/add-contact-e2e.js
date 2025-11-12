const http = require('http');
const WebSocket = require('ws');
const BASE = 'http://72.61.141.84:8080';

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

(async () => {
  try {
    console.log('Creating user A and B');
    const a = await postJSON('/register', { email: `a_${Date.now()}@test`, password: 'pw', username: 'A' });
    const b = await postJSON('/register', { email: `b_${Date.now()}@test`, password: 'pw', username: 'B' });
    console.log('Users:', a.id, b.id);

    console.log('A adds B as contact');
    const add = await postJSON(`/users/${a.id}/contacts`, { contactId: b.id });
    console.log('Add contact response:', add);

    console.log('Open WS for B and auth');
  const wsB = new WebSocket('ws://72.61.141.84:8080');
    await new Promise(r => wsB.on('open', r));
    wsB.send(JSON.stringify({ type: 'auth', userId: b.id }));
    const received = [];
    wsB.on('message', (m) => { try { received.push(JSON.parse(m.toString())); } catch(e) { received.push(m.toString()); } });

    console.log('Open WS for A and auth');
  const wsA = new WebSocket('ws://72.61.141.84:8080');
    await new Promise(r => wsA.on('open', r));
    wsA.send(JSON.stringify({ type: 'auth', userId: a.id }));

    console.log('A sends chat to B');
    wsA.send(JSON.stringify({ type: 'chat', fromId: a.id, toId: b.id, message: JSON.stringify({ text: 'hello B' }), encrypted: false }));

    await new Promise(r => setTimeout(r, 800));
    console.log('Received by B:', received.filter(x => x && x.type === 'chat').length, JSON.stringify(received));
    wsA.close(); wsB.close();
    process.exit(0);
  } catch (e) {
    console.error('Test failed', e);
    process.exit(1);
  }
})();
