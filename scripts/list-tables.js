const fs = require('fs');
const path = require('path');

const dbPath = path.join(__dirname, '..', 'database', 'appchat.sqlite');

if (!fs.existsSync(dbPath)) {
  console.log('No sqlite file found at', dbPath);
  console.log('\nLegacy JSON fallback support is deprecated and users.json/messages.json are no longer used.');
  console.log('If you have backups to import, place them under server/database/backup/ and run a migration script.');
  process.exit(0);
}

// If file exists, use sql.js to list tables
(async () => {
  try {
    const initSqlJs = require('sql.js');
    const SQL = await initSqlJs();
    const buf = fs.readFileSync(dbPath);
    const db = new SQL.Database(new Uint8Array(buf));
    const res = db.exec("SELECT name FROM sqlite_master WHERE type='table';");
    if (!res || !res[0]) {
      console.log('No tables found');
    } else {
      console.log('Tables:', res[0].values.map(r => r[0]));
    }
  } catch (e) {
    console.error('Error loading sql.js or reading DB', e);
  }
})();
