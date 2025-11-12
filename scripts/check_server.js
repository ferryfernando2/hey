const http = require('http');

function get(path) {
  return new Promise((resolve, reject) => {
    const options = { hostname: 'localhost', port: 8080, path, method: 'GET' };
    const req = http.request(options, res => {
      let chunks = '';
      res.on('data', c => chunks += c);
      res.on('end', () => resolve({ status: res.statusCode, body: chunks }));
    });
    req.on('error', reject);
    req.end();
  });
}

function putJson(path, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const options = {
      hostname: 'localhost', port: 8080, path, method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) }
    };
    const req = http.request(options, res => { let chunks=''; res.on('data', c => chunks+=c); res.on('end', () => resolve({ status: res.statusCode, body: chunks })); });
    req.on('error', reject);
    req.write(data); req.end();
  });
}

(async function() {
  try {
    const h = await get('/health');
    console.log('Health:', h.status, h.body);
    const testUserId = 'test-user-legacy-pref';
    const r = await putJson(`/users/${encodeURIComponent(testUserId)}/profile`, { preference: { theme: 'dark' } });
    console.log('PUT profile:', r.status, r.body);
  } catch (e) { console.error('Error contacting server', e); process.exit(1); }
})();
