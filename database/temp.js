// Export a standalone function that operates on a provided dbHandler object.
// This avoids referencing `this` when the snippet was extracted from a class.
module.exports.getMessage = async function getMessage(dbHandler, messageId) {
    if (!messageId) return null;
    if (dbHandler && dbHandler.usePostgres) {
        const client = await dbHandler.pgPool.connect();
        try {
            const res = await client.query('SELECT * FROM messages WHERE id = $1 LIMIT 1', [messageId]);
            if (!res.rows || !res.rows.length) return null;
            return res.rows[0];
        } finally { client.release(); }
    }
    if (dbHandler && dbHandler.useSqlite && dbHandler.sqliteDb) {
        const res = dbHandler.sqliteDb.exec('SELECT * FROM messages WHERE id = ? LIMIT 1', [messageId]);
        if (!res || !res[0] || !res[0].values.length) return null;
        const cols = res[0].columns;
        const vals = res[0].values[0];
        const msg = {};
        for (let i = 0; i < cols.length; i++) {
            msg[cols[i]] = vals[i];
        }
        return msg;
    }
    return null;
}