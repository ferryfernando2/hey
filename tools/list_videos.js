// Usage: node tools/list_videos.js
// Prints recent rows from the 'videos' table inside server/database/appchat.sqlite

const fs = require('fs');
const path = require('path');

(async function() {
  try {
    const initSqlJs = require('sql.js');
    const SQL = await initSqlJs();
    const dbPath = path.join(__dirname, '..', 'database', 'appchat.sqlite');
    if (!fs.existsSync(dbPath)) {
      console.error('SQLite DB not found at', dbPath);
      process.exit(2);
    }
    const buf = fs.readFileSync(dbPath);
    const db = new SQL.Database(new Uint8Array(buf));

    const q = `SELECT id, userId, url, title, description, thumbnailUrl, duration, views, likes, createdAt FROM videos ORDER BY createdAt DESC LIMIT 200`;
    const res = db.exec(q);
    if (!res || !res[0]) {
      console.log('No videos table or no rows found');
      process.exit(0);
    }

    const cols = res[0].columns;
    const rows = res[0].values;

    console.log('Found', rows.length, 'video(s)');
    for (const r of rows) {
      const obj = {};
      for (let i = 0; i < cols.length; i++) obj[cols[i]] = r[i];
      console.log('---');
      console.log('id:', obj.id);
      console.log('userId:', obj.userId);
      console.log('url:', obj.url);
      console.log('thumbnailUrl:', obj.thumbnailUrl);
      console.log('title:', obj.title);
      console.log('duration:', obj.duration);
      console.log('views:', obj.views, 'likes:', obj.likes);
      console.log('createdAt:', obj.createdAt);
    }
  } catch (e) {
    console.error('Failed to read SQLite DB:', e && e.message ? e.message : e);
    process.exit(1);
  }
})();
