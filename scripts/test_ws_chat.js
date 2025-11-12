// Simple test client to send a chat message via WebSocket to the local server.
// Usage: node test_ws_chat.js <fromId> <toId> "message text"

const WebSocket = require('ws');

const args = process.argv.slice(2);
const fromId = args[0] || 'terminal_sender';
const toId = args[1] || 'terminal_recipient';
const text = args[2] || `Hello from terminal at ${new Date().toISOString()}`;

const candidates = ['ws://72.61.141.84:8080/ws', 'ws://72.61.141.84:8080'];

(async function tryConnect() {
  for (const url of candidates) {
    try {
      console.log(`Attempting connect to ${url}`);
      const ws = new WebSocket(url);

      ws.on('open', () => {
        console.log('Connected to', url);
        // send auth
        ws.send(JSON.stringify({ type: 'auth', userId: fromId }));
        setTimeout(() => {
          const payload = { type: 'chat', fromId, toId, message: text };
          console.log('Sending payload:', payload);
          ws.send(JSON.stringify(payload));
        }, 200);
      });

      ws.on('message', (msg) => {
        try {
          const data = JSON.parse(msg.toString());
          console.log('Received:', JSON.stringify(data, null, 2));
        } catch (e) {
          console.log('Raw message:', msg.toString());
        }
      });

      ws.on('error', (err) => {
        console.error('WebSocket error on', url, err.message || err);
      });

      ws.on('close', (code, reason) => {
        console.log(`Connection to ${url} closed:`, code, reason && reason.toString());
      });

      // keep process alive for a short while to receive responses
      setTimeout(() => {
        try { ws.close(); } catch (e) {}
        process.exit(0);
      }, 3000);

      // if we reached here, don't try next candidate
      return;
    } catch (e) {
      console.error('Connect failed to', url, e.message || e);
      // try next
    }
  }

  console.error('All connection attempts failed');
  process.exit(2);
})();
