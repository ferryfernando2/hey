const fs = require('fs');
const path = require('path');

(async () => {
  try {
    const SQL = await require('sql.js')();
    const dbFile = path.join(__dirname, '..', 'database', 'appchat.sqlite');
    let db = null;
    if (fs.existsSync(dbFile)) {
      const buf = fs.readFileSync(dbFile);
      db = new SQL.Database(new Uint8Array(buf));
      console.log('Loaded existing sqlite DB');
    } else {
      db = new SQL.Database();
      console.log('Created new sqlite DB in-memory');
    }

    // Ensure tables exist
    db.run(`CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      email TEXT,
      password TEXT,
      username TEXT,
      publickey TEXT,
      lastseen TEXT
    );`);

    db.run(`CREATE TABLE IF NOT EXISTS messages (
      id TEXT PRIMARY KEY,
      chatid TEXT,
      fromid TEXT,
      toid TEXT,
      message TEXT,
      timestamp TEXT,
      encrypted INTEGER DEFAULT 0,
      status TEXT
    );`);

  // Legacy JSON migration is deprecated. If you have JSON backups to import,
  // place them in 'server/database/backup/' and run a custom migration. This script
  // no longer automatically reads users.json/messages.json to avoid accidental use.
  const users = null;
  const messages = null;

    let usersCount = 0;
    if (users && users.users) {
      const stmt = db.prepare('INSERT OR REPLACE INTO users (id,email,password,username,publickey,lastseen) VALUES (?, ?, ?, ?, ?, ?)');
      for (const u of Object.values(users.users)) {
        stmt.run([
          u.id || null,
          u.email || null,
          u.password || null,
          u.username || null,
          u.publicKey || u.publickey || null,
          u.lastSeen || u.lastseen || null
        ]);
        usersCount++;
      }
      stmt.free && stmt.free();
    }

    let messagesCount = 0;
    if (messages) {
      const stmt = db.prepare('INSERT OR REPLACE INTO messages (id,chatid,fromid,toid,message,timestamp,encrypted,status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
      for (const chatId of Object.keys(messages)) {
        const arr = messages[chatId] || [];
        for (const m of arr) {
          const id = m.id || `msg_${Date.now()}_${Math.floor(Math.random()*1000)}`;
          const encryptedFlag = (m.encrypted === true || m.encrypted === 1 || m.encrypted === 'true') ? 1 : 0;
          stmt.run([
            id,
            chatId,
            m.fromId || m.fromid || null,
            m.toId || m.toid || null,
            typeof m.message === 'object' ? JSON.stringify(m.message) : (m.message || null),
            m.timestamp || new Date().toISOString(),
            encryptedFlag,
            m.status || null
          ]);
          messagesCount++;
        }
      }
      stmt.free && stmt.free();
    }

    // Persist DB to file
    const data = db.export();
    fs.writeFileSync(dbFile, Buffer.from(data));
    console.log(`Wrote sqlite file: ${dbFile}`);
    console.log(`Imported users: ${usersCount}, messages: ${messagesCount}`);

    db.close && db.close();
    process.exit(0);
  } catch (e) {
    console.error('Migration failed', e);
    process.exit(1);
  }
})();
