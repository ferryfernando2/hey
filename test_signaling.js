// Small test utility to send signaling messages via WebSocket to the server.
// Usage:
//   node test_signaling.js --server ws://72.61.141.84:8080 --user callerId --target targetUserId --mode incoming
//   node test_signaling.js --server ws://72.61.141.84:8080 --user callerId --target targetUserId --mode offer --sdp offer.sdp
// The script connects, authenticates as --user, then sends either an incomingCall notice
// or forwards a call:offer payload read from an SDP file (optional).

const WebSocket = require('ws');
const fs = require('fs');
const argv = require('minimist')(process.argv.slice(2));

const server = argv.server || argv.s || 'ws://72.61.141.84:8080';
const userId = argv.user || argv.u || 'test_caller';
const target = argv.target || argv.t || 'test_target';
const mode = (argv.mode || 'incoming');
const sdpFile = argv.sdp || null;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

(async function main() {
  console.log('Connecting to', server, 'as', userId);
  const ws = new WebSocket(server);
  ws.on('open', async () => {
    console.log('Connected');
    // authenticate
    ws.send(JSON.stringify({ type: 'auth', userId }));
    await sleep(300);

    if (mode === 'incoming') {
      const callId = `call_test_${Date.now()}_${Math.random().toString(36).slice(2,6)}`;
      const meta = { fromId: userId, callId, fromName: userId, fromAvatar: null };
      console.log('Sending incomingCall notice to', target, 'callId=', callId);
      ws.send(JSON.stringify({ type: 'incomingCall', data: meta, to: target, toId: target }));
      // Also send as call:offer wrapper (without real SDP) so clients that expect call:offer receive it
      ws.send(JSON.stringify({ type: 'call:offer', fromId: userId, toId: target, callId, sdp: '', sdpType: 'offer' }));
      console.log('Done');
      process.exit(0);
    }

    if (mode === 'offer') {
      const callId = `call_test_${Date.now()}_${Math.random().toString(36).slice(2,6)}`;
      let sdp = '';
      if (sdpFile) {
        try { sdp = fs.readFileSync(sdpFile, 'utf8'); } catch (e) { console.error('Failed to read sdp file', e); }
      }
      console.log('Sending call:offer to', target, 'callId=', callId, 'sdpFile=', sdpFile);
      const payload = { type: 'call:offer', fromId: userId, toId: target, callId, sdp: sdp || '', sdpType: 'offer' };
      ws.send(JSON.stringify(payload));
      console.log('Done');
      process.exit(0);
    }

    console.log('Unknown mode', mode);
    process.exit(1);
  });

  ws.on('message', (m) => {
    try { const data = JSON.parse(m.toString()); console.log('RECV>', JSON.stringify(data)); } catch (e) { console.log('RAW>', m.toString()); }
  });

  ws.on('error', (err) => { console.error('WS ERR', err); process.exit(2); });
  ws.on('close', () => { console.log('WS closed'); process.exit(0); });
})();
