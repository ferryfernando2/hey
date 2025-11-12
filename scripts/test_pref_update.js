const http = require('http');

function putJson(path, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const options = {
      hostname: 'localhost',
      port: 8080,
      path,
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
      },
    };

    const req = http.request(options, (res) => {
      let chunks = '';
      res.on('data', (c) => (chunks += c));
      res.on('end', () => resolve({ status: res.statusCode, body: chunks }));
    });

    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function test() {
  const testUserId = 'test-user-legacy-pref';
  console.log('Posting legacy preference update...');
  const result = await putJson(`/users/${encodeURIComponent(testUserId)}/profile`, { preference: { theme: 'dark', notifications: true } });
  console.log('Status:', result.status);
  console.log('Body:', result.body);
}

test().catch(err => { console.error(err); process.exit(1); });
