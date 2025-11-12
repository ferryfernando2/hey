(async function(){
  try {
    const fs = require('fs');
    const path = require('path');
    const initSqlJs = require('sql.js');
    const SQL = await initSqlJs();
    const dbPath = path.join(__dirname, '..','database','appchat.sqlite');
    if (!fs.existsSync(dbPath)) {
      console.error(JSON.stringify({ error: 'db_not_found', path: dbPath }));
      process.exit(1);
    }
    const buf = fs.readFileSync(dbPath);
    const db = new SQL.Database(new Uint8Array(buf));
    function run(q){
      try{ const r = db.exec(q); return r; } catch(e){ return { error: String(e) }; }
    }
    // get counts
    const msgs = run("SELECT COUNT(1) AS cnt FROM messages");
    const sched = run("SELECT COUNT(1) AS cnt FROM scheduled_messages");
    // get last 5 messages
    const lastMsgs = run("SELECT id,chatid,fromid,toid,message,timestamp,status FROM messages ORDER BY timestamp DESC LIMIT 5");
    const lastSched = run("SELECT id,chatid,fromid,toid,content,scheduledat,stampenabled,status,createdat FROM scheduled_messages ORDER BY createdat DESC LIMIT 10");
    const out = { messagesCount: msgs && msgs[0] && msgs[0].values && msgs[0].values[0] ? msgs[0].values[0][0] : null, scheduledCount: sched && sched[0] && sched[0].values && sched[0].values[0] ? sched[0].values[0][0] : null, lastMessages: [], lastScheduled: [] };
    if (lastMsgs && lastMsgs[0]){
      const cols = lastMsgs[0].columns;
      for (const row of lastMsgs[0].values){ const o={}; for(let i=0;i<cols.length;i++) o[cols[i]]=row[i]; out.lastMessages.push(o);} }
    if (lastSched && lastSched[0]){
      const cols = lastSched[0].columns;
      for (const row of lastSched[0].values){ const o={}; for(let i=0;i<cols.length;i++) o[cols[i]]=row[i]; out.lastScheduled.push(o);} }
    console.log(JSON.stringify(out,null,2));
  } catch (e){ console.error(JSON.stringify({ error: String(e) })); process.exit(2); }
})();
