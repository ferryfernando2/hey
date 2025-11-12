const path = require('path');

// Ensure SQLite backend is used for this test
process.env.USE_SQLITE = '1';

const DatabaseHandler = require('../database/handler');

(async () => {
  const db = new DatabaseHandler();
  try {
    await db.init();
    console.log('DB initialized (backend):', db.useSqlite ? 'sqlite' : (db.usePostgres ? 'postgres' : 'none'));

    // Create two users
    const u1 = await db.createUser(`test1_${Date.now()}@example.com`, 'pass1', 'tester1');
    const u2 = await db.createUser(`test2_${Date.now()}@example.com`, 'pass2', 'tester2');
    console.log('Created users:', u1.id, u2.id);

    // Login user1
    const logged = await db.loginUser(u1.email, 'pass1');
    console.log('Login success for:', logged.id, logged.username);

    // Send a message from u1 to u2
    const sent = await db.saveMessage(u1.id, u2.id, JSON.stringify({ text: 'Hello from smoke test', encrypted: false }));
    console.log('Sent message id:', sent.id);

    // Fetch messages in the chat
    const msgs = await db.getMessages(u1.id, u2.id, { limit: 50 });
    console.log('Fetched messages count:', msgs.length);
    if (msgs.length) console.log('Sample message:', msgs[msgs.length-1]);

    // Check undelivered for u2
    const und = await db.getUndeliveredMessages(u2.id);
    console.log('Undelivered messages for recipient:', und.length);

    // Flush DB to disk
    await db.flushAll();

    console.log('\nSmoke test completed successfully');
    try {
      // try to flush and clear timers to allow clean shutdown
      if (db && db.flushAll) await db.flushAll();
      if (db && db._sqliteFlushTimer) try { clearInterval(db._sqliteFlushTimer); } catch(e){}
      if (db && db._flushSqliteWritesSync) try { db._flushSqliteWritesSync(); } catch(e){}
    } catch(e) { /* best-effort */ }
    // allow Node to close handles cleanly
    setTimeout(() => process.exit(0), 200);
  } catch (e) {
    console.error('Smoke test failed', e);
    try {
      if (db && db._sqliteFlushTimer) try { clearInterval(db._sqliteFlushTimer); } catch(e){}
      if (db && db._flushSqliteWritesSync) try { db._flushSqliteWritesSync(); } catch(e){}
    } catch(e) {}
    setTimeout(() => process.exit(1), 200);
  }
})();
