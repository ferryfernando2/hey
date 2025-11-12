// Simple signaling test script for the appchat server
// Connects two clients (userA and userB), authenticates, sends a call offer, and logs received messages.

const WebSocket = require('ws');

const SERVER = process.env.SERVER_URL || 'ws://localhost:3000';

function mkClient(name) {
  const ws = new WebSocket(SERVER);
  ws._name = name;
  ws.on('open', () => {
    console.log(`[${name}] open`);
    // auth
    ws.send(JSON.stringify({ type: 'auth', userId: name, sharePresence: true }));
  });
  ws.on('error', (err) => console.log(`[${name}] error:`, err && err.message || err));
  ws.on('close', () => console.log(`[${name}] closed`));
  ws.on('message', (m) => {
    try {
      const msg = JSON.parse(m.toString());
      console.log(`[${name}] received:`, JSON.stringify(msg));
    } catch (e) {
      console.log(`[${name}] received (raw):`, m.toString());
    }
  });
  return ws;
}

async function delay(ms){ return new Promise(r=>setTimeout(r, ms)); }

(async function(){
  const a = mkClient('userA');
  const b = mkClient('userB');

  // wait for both to open and auth
  await delay(1200);

  // userA sends a call offer to userB
  const callId = `call_${Date.now()}_userA_userB`;
  console.log('[test] userA sending call:offer');
  a.send(JSON.stringify({ type: 'call:offer', fromId: 'userA', toId: 'userB', callId, sdp: 'dummyOfferSDP' }));

  await delay(800);

  console.log('[test] userB sending call:answer');
  b.send(JSON.stringify({ type: 'call:answer', fromId: 'userB', toId: 'userA', callId, sdp: 'dummyAnswerSDP' }));

  await delay(800);

  console.log('[test] userA sending call:end');
  a.send(JSON.stringify({ type: 'call:end', fromId: 'userA', toId: 'userB', callId }));

  await delay(600);

  a.close();
  b.close();

  // exit after a short delay
  await delay(400);
  process.exit(0);
})();
