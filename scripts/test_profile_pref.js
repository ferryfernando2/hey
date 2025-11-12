const http = require('http');

function request(options, body) {
  return new Promise((resolve, reject) => {
    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => data += chunk);
      res.on('end', () => {
        let parsed = null;
        try { parsed = data ? JSON.parse(data) : null; } catch (e) { parsed = data; }
        resolve({ status: res.statusCode, body: parsed });
      });
    });
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

(async () => {
  try {
    console.log('Registering test user...');
    const rnd = Math.floor(Math.random()*1000000);
    const reg = await request({ hostname: 'localhost', port: 8080, path: '/register', method: 'POST', headers: { 'Content-Type': 'application/json' } }, { email: `test+${Date.now()}${rnd}@example.com`, password: 'Password123!', username: `test${rnd}` });
    console.log('Register response', reg.status, reg.body);
    if (!reg.body || !reg.body.id) throw new Error('register failed or no id returned');
    const userId = reg.body.id;

    console.log('Updating profile (normal fields)...');
    const up1 = await request({ hostname: 'localhost', port: 8080, path: `/users/${userId}/profile`, method: 'PUT', headers: { 'Content-Type': 'application/json' } }, { fullName: 'Automated Tester', phoneNumber: '+62123456789', location: 'Jakarta' });
    console.log('PUT normal', up1.status, up1.body ? (up1.body.id ? 'user returned' : up1.body) : up1.body);

    console.log('GET profile');
    const get1 = await request({ hostname: 'localhost', port: 8080, path: `/users/${userId}/profile`, method: 'GET' });
    console.log('GET profile', get1.status, get1.body);

    console.log('Updating profile with legacy singular "preference" key');
    const up2 = await request({ hostname: 'localhost', port: 8080, path: `/users/${userId}/profile`, method: 'PUT', headers: { 'Content-Type': 'application/json' } }, { preference: { theme: 'dark', sessions: [] } });
    console.log('PUT legacy pref', up2.status, up2.body);

    console.log('GET profile after legacy pref');
    const get2 = await request({ hostname: 'localhost', port: 8080, path: `/users/${userId}/profile`, method: 'GET' });
    console.log('GET after', get2.status, get2.body);

    console.log('Test completed');
  } catch (e) {
    console.error('Test failed', e);
    process.exit(2);
  }
})();
