const WebSocket = require('ws');
const express = require('express');
const DatabaseHandler = require('./database/handler');
require('dotenv').config();

const app = express();
const db = new DatabaseHandler();
let dbReady = false;
// in-memory statuses fallback
const statuses = [];
// In-memory pending (undelivered) messages: toId -> [message]
const pendingMessages = new Map();
// Max pending messages to keep per user to bound memory (can be tuned via env)
const MAX_PENDING_PER_USER = Number(process.env.MAX_PENDING_PER_USER || 500);
// Policy flags
const ENFORCE_E2E = (process.env.ENFORCE_E2E || 'false') === 'true';
// Default to persisting messages so scheduled deliveries appear in message history.
// Set NO_PERSIST_MESSAGES=true in env if you want transient-only behavior.
const NO_PERSIST_MESSAGES = (process.env.NO_PERSIST_MESSAGES || 'false') === 'true';
const port = process.env.PORT || 8080;

// Placeholder shown when an encrypted message lacks key metadata
const PLACEHOLDER_KEY_LOST = (process.env.PLACEHOLDER_KEY_LOST || 'Maaf, kami kehilangan kunci pesan ini');

// Helper: check common key fields in an object payload
const _hasKeyMetadata = (obj) => {
    if (!obj || typeof obj !== 'object') return false;
    const keyFields = ['keyId', 'kid', 'key', 'k', 'key_id', 'keyIdV1'];
    for (const f of keyFields) if (Object.prototype.hasOwnProperty.call(obj, f)) return true;
    return false;
};

// Basic anti-abuse: token-bucket rate limiter (in-memory) with optional Redis extension later
// Bucket map keyed by ip|ua (or other fingerprint)
const ipBuckets = new Map(); // key -> { tokens, maxTokens, lastRefill, bannedUntil, requestCount, lastSeen }
const MAX_REQUESTS_PER_MIN = Number(process.env.MAX_REQ_PER_MIN || 2000); // Increased to 2000/min
const BAN_SECONDS = Number(process.env.BAN_SECONDS || 60);  // Reduced to 1 minute
// token-bucket parameters - optimized for stability
const RATE_WINDOW_MS = 60 * 1000;
const BURST_MULTIPLIER = Number(process.env.RATE_BURST_MULTIPLIER || 10); // Increased burst allowance
const TOKEN_REFILL_PER_MS = MAX_REQUESTS_PER_MIN / RATE_WINDOW_MS; // tokens added per ms

const getIpKey = (req) => {
    const ip = (req.headers['x-forwarded-for'] || req.connection.remoteAddress || req.socket.remoteAddress || '').toString();
    const ua = (req.headers['user-agent'] || '').toString();
    return ip + '|' + ua.slice(0, 64);
};

const refillBucket = (bucket) => {
    const now = Date.now();
    const elapsed = Math.max(0, now - (bucket.lastRefill || now));
    const added = elapsed * TOKEN_REFILL_PER_MS;
    bucket.tokens = Math.min(bucket.maxTokens, (bucket.tokens || bucket.maxTokens) + added);
    bucket.lastRefill = now;
};

const consumeToken = (key, cost = 1) => {
    let bucket = ipBuckets.get(key);
    if (!bucket) {
        bucket = {
            tokens: MAX_REQUESTS_PER_MIN, // start full
            maxTokens: MAX_REQUESTS_PER_MIN * BURST_MULTIPLIER,
            lastRefill: Date.now(),
            bannedUntil: 0,
            requestCount: 0,
            lastSeen: Date.now()
        };
        ipBuckets.set(key, bucket);
    }
    bucket.lastSeen = Date.now();
    if (bucket.bannedUntil && bucket.bannedUntil > Date.now()) {
        return { allowed: false, retryAfter: Math.ceil((bucket.bannedUntil - Date.now()) / 1000) };
    }
    refillBucket(bucket);
    if ((bucket.tokens || 0) >= cost) {
        bucket.tokens -= cost;
        return { allowed: true, remaining: bucket.tokens };
    }

    // out of tokens: increment request counter for potential banning if abusive
    const windowAge = Date.now() - (bucket.requestWindowStart || 0);
    if (!bucket.requestWindowStart || windowAge > RATE_WINDOW_MS) {
        bucket.requestWindowStart = Date.now();
        bucket.requestCount = 1;
    } else {
        bucket.requestCount = (bucket.requestCount || 0) + 1;
    }
    // temporary ban if extreme abuse
    if (bucket.requestCount > MAX_REQUESTS_PER_MIN * 3) {
        bucket.bannedUntil = Date.now() + BAN_SECONDS * 1000;
        return { allowed: false, retryAfter: Math.ceil(BAN_SECONDS) };
    }

    // compute approximate seconds until one token is available
    const deficit = 1 - (bucket.tokens || 0);
    const msUntilOne = Math.ceil(Math.max(0, deficit / TOKEN_REFILL_PER_MS));
    return { allowed: false, retryAfter: Math.max(1, Math.ceil(msUntilOne / 1000)) };
};

// Basic middleware
// Performance optimizations
// Configure compression but skip compressing video media (which breaks Range requests)
try {
    const compression = require('compression');
    app.use(compression({
        filter: (req, res) => {
            try {
                const url = req.url || '';
                if (url.match(/\.(mp4|webm|ogg|mov|m4v)$/i)) return false;
            } catch (e) {}
            return compression.filter(req, res);
        }
    }));
} catch (e) {}

// Enable caching headers globally
app.use((req, res, next) => {
    // Cache static assets for 1 hour
    if (req.url.match(/\.(jpg|jpeg|gif|png|ico|css|js|woff2)$/)) {
        res.setHeader('Cache-Control', 'public, max-age=3600');
    }
    next();
});

// stricter JSON body limits but increased for chat
app.use(express.json({ limit: process.env.JSON_LIMIT || '1mb' }));
// XSS cleaning
try { const xss = require('xss-clean'); app.use(xss()); } catch(e) {}

// CORS whitelist support (comma-separated list in env: CORS_WHITELIST)
const corsWhitelist = (process.env.CORS_WHITELIST || '').split(',').map(s => s.trim()).filter(Boolean);
app.use((req, res, next) => {
    const origin = req.headers.origin || '';
    if (!corsWhitelist.length || corsWhitelist.includes(origin)) res.header('Access-Control-Allow-Origin', origin || '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    if (req.method === 'OPTIONS') return res.status(200).end();
    next();
});

// Force HTTPS redirect when behind proxy and HTTPS_REQUIRED=true
if (process.env.HTTPS_REQUIRED === 'true') {
    app.use((req, res, next) => {
        if (req.headers['x-forwarded-proto'] && req.headers['x-forwarded-proto'] !== 'https') {
            const httpsUrl = `https://${req.headers.host}${req.url}`;
            return res.redirect(301, httpsUrl);
        }
        next();
    });
}
// security headers
try { const helmet = require('helmet'); app.use(helmet()); } catch(e) {}

// rate limiter and slow down
try {
    const rateLimit = require('express-rate-limit');
    // Removed slowDown middleware to eliminate delays
    const limiter = rateLimit({ windowMs: 60 * 1000, max: MAX_REQUESTS_PER_MIN, standardHeaders: true, legacyHeaders: false });
    app.use(limiter);
} catch (e) {}
// IP ban check and basic fingerprinting
app.use((req, res, next) => {
    const key = getIpKey(req);
    const result = consumeToken(key, 1);
    if (!result.allowed) {
        res.setHeader('Retry-After', String(result.retryAfter || 1));
        return res.status(429).json({ error: 'Too many requests', retryAfter: result.retryAfter || 1 });
    }
    next();
});

// Serve uploaded status files
const path = require('path');
const fs = require('fs');
// mime type helper (optional)
let mime = null;
try { mime = require('mime-types'); } catch (e) { mime = null; }
const UPLOAD_DIR = path.join(__dirname, 'uploads', 'statuses');
try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); } catch (e) {}
// Explicit range-aware handler for video files to ensure browsers can request byte ranges.
// This takes precedence over express.static and guarantees correct headers for streaming.
app.get('/uploads/statuses/:filename', (req, res) => {
    try {
        const filename = req.params.filename || '';
        // Prevent path traversal
        if (filename.includes('..') || filename.includes('/')) return res.status(400).end('Invalid filename');
        const filePath = path.join(UPLOAD_DIR, filename);
        // ensure the file is inside the upload dir
        if (!filePath.startsWith(UPLOAD_DIR)) return res.status(403).end('Forbidden');
        if (!fs.existsSync(filePath)) return res.status(404).end('Not found');

        const stat = fs.statSync(filePath);
        const total = stat.size;

        const range = req.headers.range;
        const contentType = (mime && mime.lookup ? mime.lookup(filePath) : null) || 'application/octet-stream';
        res.setHeader('Content-Type', contentType);
        res.setHeader('Accept-Ranges', 'bytes');

        if (!range) {
            // No range header - send entire file
            res.setHeader('Content-Length', total);
            res.status(200);
            const stream = fs.createReadStream(filePath);
            return stream.pipe(res);
        }

        // Parse Range header: bytes=start-end
        const parts = range.replace(/bytes=/, '').split('-');
        const start = parseInt(parts[0], 10);
        const end = parts[1] ? parseInt(parts[1], 10) : (total - 1);
        if (isNaN(start) || isNaN(end) || start > end || start < 0) return res.status(416).setHeader('Content-Range', `bytes */${total}`).end();

        const chunkSize = (end - start) + 1;
        res.status(206);
        res.setHeader('Content-Range', `bytes ${start}-${end}/${total}`);
        res.setHeader('Content-Length', chunkSize);

        const stream = fs.createReadStream(filePath, { start, end });
        return stream.pipe(res);
    } catch (e) {
        console.error('video stream error', e);
        return res.status(500).end('Server error');
    }
});

// Fallback static serving for other assets in uploads/statuses (if any)
app.use('/uploads/statuses', express.static(UPLOAD_DIR));
 
 // Serve uploaded avatars
 const AVATAR_DIR = path.join(__dirname, 'uploads', 'avatars');
 try { fs.mkdirSync(AVATAR_DIR, { recursive: true }); } catch (e) {}
 app.use('/uploads/avatars', express.static(AVATAR_DIR));
 
 // Serve uploaded thumbnails
 const THUMB_DIR = path.join(__dirname, 'uploads', 'thumbnails');
 try { fs.mkdirSync(THUMB_DIR, { recursive: true }); } catch (e) {}
 app.use('/uploads/thumbnails', express.static(THUMB_DIR));

// File-backed videos metadata store (preferred when DB-backed videos cause issues)
const VIDEO_META_DIR = path.join(__dirname, 'data');
const VIDEOS_JSON = path.join(VIDEO_META_DIR, 'videos.json');
try { fs.mkdirSync(VIDEO_META_DIR, { recursive: true }); } catch (e) {}

async function loadVideosFromFile() {
    try {
        const txt = await fs.promises.readFile(VIDEOS_JSON, 'utf8');
        const arr = JSON.parse(txt || '[]');
        return Array.isArray(arr) ? arr : [];
    } catch (e) {
        return [];
    }
}

async function saveVideoToFile(obj) {
    try {
        const arr = await loadVideosFromFile();
        // prepend new video so most-recent appear first
        arr.unshift(obj);
        await fs.promises.writeFile(VIDEOS_JSON, JSON.stringify(arr, null, 2), 'utf8');
        return obj;
    } catch (e) {
        throw e;
    }
}

// Multer for multipart/form-data file uploads
let multer = null;
try { multer = require('multer'); } catch (e) { multer = null; }
if (multer) {
    const storage = multer.diskStorage({
        destination: function (req, file, cb) { cb(null, UPLOAD_DIR); },
        filename: function (req, file, cb) { cb(null, `${Date.now()}_${file.originalname}`); }
    });
    const upload = multer({ storage, limits: { fileSize: 50 * 1024 * 1024 } }); // 50MB

    // POST /statuses - create a new status (file + optional text). expires after 24 hours.
    app.post('/statuses', upload.single('file'), async (req, res) => {
        try {
            console.log('[POST /statuses] received upload');
            console.log('[POST /statuses] body:', req.body);
            console.log('[POST /statuses] file:', req.file ? { fieldname: req.file.fieldname, originalname: req.file.originalname, encoding: req.file.encoding, mimetype: req.file.mimetype, size: req.file.size, destination: req.file.destination, filename: req.file.filename } : 'none');
            const userId = req.body.userId || req.query.userId || (req.headers['x-user-id'] || '').toString();
            if (!userId) return res.status(400).json({ error: 'userId required' });
            const caption = req.body.caption || '';
            const file = req.file;
            if (!file) return res.status(400).json({ error: 'file is required' });

    // If video, enforce 1 minute limit by checking duration is not easily available server-side here;
    // client should enforce duration before upload. We'll accept and store; node-side duration check requires ffmpeg.

    // Optimize file handling - add cache headers
    const url = `/uploads/statuses/${encodeURIComponent(file.filename)}`;
    const statusObj = { 
        id: `status_${Date.now()}`, 
        userId, 
        url, 
        caption, 
        created: new Date().toISOString(), 
        expiresAt: new Date(Date.now() + 24*60*60*1000).toISOString(),
        cacheControl: 'public, max-age=3600' // Add 1-hour cache
    };
            console.log('[POST /statuses] statusObj:', statusObj);
            console.log('[POST /statuses] file saved to:', file.destination + '/' + file.filename);
            // Persist via DB handler if available
            if (db && db.saveStatus) {
                try {
                    await db.saveStatus(statusObj);
                } catch (e) {
                    console.error('db.saveStatus failed', e);
                }
            } else {
                statuses.push(statusObj);
            }

            return res.json({ success: true, status: statusObj });
        } catch (e) {
            console.error('statuses upload failed', e);
            return res.status(500).json({ error: e.message || 'upload failed' });
        }
    });
 
     // Avatar upload endpoint: POST /users/:userId/avatar accepts multipart 'avatar' and updates profileImageUrl
     const avatarStorage = multer.diskStorage({
         destination: function (req, file, cb) { cb(null, AVATAR_DIR); },
         filename: function (req, file, cb) { cb(null, `${Date.now()}_${file.originalname}`); }
     });
     const uploadAvatar = multer({ storage: avatarStorage, limits: { fileSize: 10 * 1024 * 1024 } }); // 10MB
 
     app.post('/users/:userId/avatar', uploadAvatar.single('avatar'), async (req, res) => {
         try {
             const userId = req.params.userId;
             if (!userId) return res.status(400).json({ error: 'userId required' });
             const file = req.file;
             if (!file) return res.status(400).json({ error: 'avatar file is required' });
             const url = `/uploads/avatars/${encodeURIComponent(file.filename)}`;
 
             // Update user's profileImageUrl via DB handler
             if (db && db.updateProfile) {
                 try {
                     await db.updateProfile(userId, { profileImageUrl: url });
                 } catch (e) {
                     console.error('db.updateProfile for avatar failed', e);
                 }
             }
 
             return res.json({ success: true, url });
         } catch (e) {
             console.error('avatar upload failed', e);
             return res.status(500).json({ error: e.message || 'avatar upload failed' });
         }
     });
}

// GET /statuses/:userId - list active statuses for a user
app.get('/statuses/:userId', async (req, res) => {
    const uid = req.params.userId;
    try {
        if (db && db.getStatuses) {
            const s = await db.getStatuses(uid);
            const now = new Date().toISOString();
            const active = (s || []).filter(st => !st.expiresAt || st.expiresAt > now);
            return res.json(active);
        }
        const active = statuses.filter(st => st.userId === uid && (!st.expiresAt || st.expiresAt > new Date().toISOString()));
        return res.json(active);
    } catch (e) {
        return res.status(500).json({ error: e.message });
    }
});

// --- Video endpoints and For You feed ---
// POST /videos - register a new video metadata record (client typically uploads file to /uploads/statuses and sends URL)
app.post('/videos', async (req, res) => {
    try {
        console.log('[POST /videos] received request');
        console.log('[POST /videos] body:', req.body);
        const body = req.body || {};
        const userId = body.userId || (req.headers['x-user-id'] || '') || null;
        const url = body.url || body.videoUrl || null;
        if (!userId || !url) {
            console.error('[POST /videos] missing userId or url. userId:', userId, 'url:', url);
            return res.status(400).json({ error: 'userId and url are required' });
        }
        const title = body.title || '';
        const description = body.description || '';
        const duration = body.duration !== undefined ? Number(body.duration) : null;
        const thumbnailUrl = body.thumbnailUrl || body.thumbnail || null;
        const metadata = body.metadata || {};

        // Fetch uploader's profile info to include in video metadata
        let uploaderName = null;
        let uploaderAvatar = null;
        try {
            if (db && db.getUserById) {
                const user = await db.getUserById(String(userId)).catch(() => null);
                if (user) {
                    uploaderName = user.username || user.name || null;
                    uploaderAvatar = user.profileImageUrl || user.profileimageurl || null;
                }
            }
        } catch (e) {
            console.error('Failed to fetch uploader profile:', e);
        }

        const useFileStore = (process.env.USE_FILE_VIDEO_STORE || '').toLowerCase() === 'true' || !db || !db.saveVideo;

        let vid = null;
        if (useFileStore) {
            // Build a minimal video object and save to JSON file store
            const videoObj = {
                id: (Date.now() + '_' + Math.random().toString(36).slice(2)),
                userId: String(userId),
                url,
                title,
                description,
                duration: duration || null,
                thumbnailUrl: thumbnailUrl || null,
                uploaderName: uploaderName || null,
                uploaderAvatar: uploaderAvatar || null,
                metadata: metadata || {},
                likes: 0,
                views: 0,
                createdAt: new Date().toISOString()
            };
            console.log('[POST /videos] saving video to file store:', videoObj.id);
            vid = await saveVideoToFile(videoObj);
        } else {
            console.log('[POST /videos] calling db.saveVideo with: userId=' + userId + ', url=' + url + ', title=' + title);
            vid = await db.saveVideo(String(userId), url, title, description, duration, thumbnailUrl, metadata);
            console.log('[POST /videos] saveVideo returned:', vid);
            // Augment response with uploader info
            if (vid && uploaderName) vid.uploaderName = uploaderName;
            if (vid && uploaderAvatar) vid.uploaderAvatar = uploaderAvatar;
        }

        // Notify local process or other instances about the new video
        try {
            if (globalThis && typeof globalThis.broadcastNewVideo === 'function') {
                try { globalThis.broadcastNewVideo(vid); } catch(e) { console.error('broadcastNewVideo error', e); }
            }
            // Also attempt to publish to Redis if available (some deployments may not have redisPub in this scope)
            try {
                const REDIS_URL = process.env.REDIS_URL || process.env.REDIS || null;
                if (REDIS_URL) {
                    const { createClient } = require('redis');
                    const tmpPub = createClient({ url: REDIS_URL });
                    tmpPub.connect().then(()=>tmpPub.publish('appchat:new_video', JSON.stringify({ video: vid })).catch(()=>{})).catch(()=>{}).finally(()=>{ try { tmpPub.disconnect().catch(()=>{}); } catch(_){} });
                }
            } catch(e) { console.error('redis publish new_video error', e); }

        } catch (e) { console.error('notify new video failed', e); }

        return res.json({ success: true, video: vid });
    } catch (e) {
        console.error('POST /videos failed', e);
        return res.status(500).json({ error: e.message || 'failed' });
    }
});

// GET /tags/popular - return a simple list of popular tags (client can use for suggestions)
app.get('/tags/popular', (req, res) => {
    try {
        const limit = req.query.limit ? Number(req.query.limit) : 20;
        const popular = ['travel','food','music','dance','funny','tutorial','asmr','life','fashion','sports','news','gaming','art','diy','beauty','fitness','comedy','pets','education','tech'];
        return res.json({ success: true, tags: popular.slice(0, limit) });
    } catch (e) {
        console.error('GET /tags/popular failed', e);
        return res.status(500).json({ error: 'failed' });
    }
});

// POST /videos/:id/view - record a view/watch event (increments view counter)
app.post('/videos/:id/view', async (req, res) => {
    try {
        if (!db || !db.incrementVideoViews) return res.status(501).json({ error: 'Video view tracking not implemented' });
        const videoId = req.params.id;
        const inc = req.body && req.body.increment ? Number(req.body.increment) : 1;
        const watchSeconds = req.body && req.body.watchSeconds ? Number(req.body.watchSeconds) : null;
        // We currently only increment a simple counter. In future we can persist per-user views/watch-time
        const updated = await db.incrementVideoViews(videoId, inc);
        if (!updated) return res.status(404).json({ error: 'Video not found' });
        return res.json({ success: true, video: updated });
    } catch (e) {
        console.error('POST /videos/:id/view failed', e);
        return res.status(500).json({ error: e.message || 'failed' });
    }
});

// POST /videos/:id/like - like or unlike a video. One like per user enforced. body: { increment: 1 | -1, userId? }
app.post('/videos/:id/like', async (req, res) => {
    try {
        if (!db || !db.incrementVideoLikes) return res.status(501).json({ error: 'Like support not implemented' });
        const videoId = req.params.id;
        const body = req.body || {};
        let userId = body.userId || (req.headers['x-user-id'] || '') || null;
        if (!userId) return res.status(400).json({ error: 'userId is required' });
        
        const inc = body.increment;
        const doLike = (inc === undefined || inc === 1 || inc > 0) ? true : false;

        // Check/enforce one-like-per-user constraint
        if (db.hasUserLikedVideo && db.toggleUserVideoLike) {
            try {
                const result = await db.toggleUserVideoLike(String(userId), String(videoId), doLike);
                if (!result) return res.status(500).json({ error: 'Failed to process like' });
                
                // Only increment/decrement the video likes count based on the actual state change
                const wasLiked = await db.hasUserLikedVideo(String(userId), String(videoId));
                let likeIncrement = 0;
                if (doLike && result.liked) {
                    // User just liked: increment by 1
                    likeIncrement = 1;
                } else if (!doLike && !result.liked) {
                    // User just unliked: decrement by 1
                    likeIncrement = -1;
                }
                // if (doLike && !result.liked) or (!doLike && result.liked): no change to count (already was/wasn't liked)

                let updated = null;
                if (likeIncrement !== 0) {
                    updated = await db.incrementVideoLikes(videoId, likeIncrement);
                } else {
                    // Still fetch the video to return current state
                    if (db.getVideoById) {
                        updated = await db.getVideoById(videoId);
                    }
                }

                if (!updated) return res.status(404).json({ error: 'Video not found' });

                // Broadcast like update to connected clients
                try {
                    const payload = { type: 'video_like', data: { id: updated.id || videoId, likes: updated.likes || 0, userLiked: result.liked, userId: String(userId) } };
                    const payloadStr = JSON.stringify(payload);
                    for (const [uid, set] of clients.entries()) {
                        for (const s of set) {
                            try { if (s && s.readyState === WebSocket.OPEN) s.send(payloadStr); } catch(e) {}
                        }
                    }
                    if (redisPub) {
                        try { redisPub.publish('appchat:video_likes', JSON.stringify({ id: updated.id || videoId, likes: updated.likes || 0, userLiked: result.liked, userId: String(userId) })).catch(()=>{}); } catch(e) { console.error('redis publish video_likes error', e); }
                    }
                } catch (e) { console.error('broadcast like failed', e); }

                return res.json({ success: true, video: updated, liked: result.liked });
            } catch (e) {
                console.error('Like constraint check failed:', e);
                // Fall back to simple increment if constraint system fails
                const updated = await db.incrementVideoLikes(videoId, inc || 1);
                if (!updated) return res.status(404).json({ error: 'Video not found' });
                return res.json({ success: true, video: updated });
            }
        } else {
            // Fallback: simple increment (old behavior)
            const updated = await db.incrementVideoLikes(videoId, inc || 1);
            if (!updated) return res.status(404).json({ error: 'Video not found' });
            return res.json({ success: true, video: updated });
        }
    } catch (e) {
        console.error('POST /videos/:id/like failed', e);
        return res.status(500).json({ error: e.message || 'failed' });
    }
});

// GET /feed/for-you - simple For You feed. Query: ?userId=&limit=&recencyDays=&weightViews=&weightRecency=
app.get('/feed/for-you', async (req, res) => {
    try {
        const useFileStore = (process.env.USE_FILE_VIDEO_STORE || '').toLowerCase() === 'true' || !db || !db.getForYouFeed;
        const userId = req.query.userId || (req.headers['x-user-id'] || '') || null;
        const limit = req.query.limit ? Number(req.query.limit) : undefined;
        const recencyDays = req.query.recencyDays ? Number(req.query.recencyDays) : undefined;
        const weightViews = req.query.weightViews ? Number(req.query.weightViews) : undefined;
        const weightRecency = req.query.weightRecency ? Number(req.query.weightRecency) : undefined;
        const opts = { limit, recencyDays, weightViews, weightRecency };
        let feed = [];
        if (useFileStore) {
            // Read from file-backed JSON store and respect simple filters (limit)
            let arr = await loadVideosFromFile();
            if (!Array.isArray(arr)) arr = [];
            if (limit && Number(limit) > 0) arr = arr.slice(0, Number(limit));
            feed = arr;
        } else {
            feed = await db.getForYouFeed(userId, opts);
        }
        return res.json({ success: true, feed });
    } catch (e) {
        console.error('GET /feed/for-you failed', e);
        return res.status(500).json({ error: e.message || 'failed' });
    }
});

// --- Comments endpoints for videos ---
// GET /videos/:id/comments?limit=&before=
app.get('/videos/:id/comments', async (req, res) => {
    try {
        if (!db || !db.getCommentsForVideo) return res.status(501).json({ error: 'Comments not implemented' });
        const videoId = req.params.id;
        const limit = req.query.limit ? Number(req.query.limit) : undefined;
        const before = req.query.before || undefined; // could be timestamp or comment id depending on DB handler
        const opts = { limit, before };
        const comments = await db.getCommentsForVideo(videoId, opts);
        return res.json({ success: true, comments });
    } catch (e) {
        console.error('GET /videos/:id/comments failed', e);
        return res.status(500).json({ error: e.message || 'failed' });
    }
});

// POST /videos/:id/comments { text, metadata?, userId? }
app.post('/videos/:id/comments', async (req, res) => {
    try {
        if (!db || !db.saveComment) return res.status(501).json({ error: 'Comments not implemented' });
        const videoId = req.params.id;
        const body = req.body || {};
        const userId = body.userId || (req.headers['x-user-id'] || '') || null;
        const text = body.text || body.comment || '';
        const parentId = body.parentId || null;
        const metadata = body.metadata || (body.meta || null);
        if (!videoId || !userId || !text) return res.status(400).json({ error: 'videoId, userId and text are required' });

        // Try to obtain author's username/avatar for convenience
        let username = null; let avatar = null;
        try { const u = await db.getUserById(String(userId)).catch(()=>null); if (u) { username = u.username || u.name || null; avatar = u.profileImageUrl || u.profileimageurl || null; } } catch (e) {}

        const saved = await db.saveComment(videoId, String(userId), String(text), parentId || null, username, avatar, metadata || {});
        // Broadcast to local WebSocket subscribers of this video's comment stream
        try {
            const subs = getVideoSubscribers(videoId);
            const payload = { type: 'video_comment', data: saved };
            const payloadStr = JSON.stringify(payload);
            for (const s of subs) {
                try { if (s && s.readyState === WebSocket.OPEN) s.send(payloadStr); } catch (e) { console.error('deliver video_comment to ws subscriber failed', e); }
            }
            // Publish to Redis so other instances can forward to their subscribers
            if (redisPub) {
                try { redisPub.publish('appchat:video_comments', JSON.stringify({ videoId, comment: saved })).catch(()=>{}); } catch(e) { console.error('redis publish video_comment error', e); }
            } else if (useHttpRelay && PUBSUB_URL) {
                try { const fetch = globalThis.fetch || require('node-fetch'); fetch(`${PUBSUB_URL}/publish/video_comments`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ videoId, comment: saved }) }).catch(()=>{}); } catch(e) {}
            }
        } catch (e) {
            console.error('Failed to broadcast video comment', e);
        }

        return res.json({ success: true, comment: saved });
    } catch (e) {
        console.error('POST /videos/:id/comments failed', e);
        return res.status(500).json({ error: e.message || 'failed' });
    }
});

// POST /comments/:id/like { increment: 1|-1, userId? }
app.post('/comments/:id/like', async (req, res) => {
    try {
        if (!db || !db.incrementCommentLikes) return res.status(501).json({ error: 'Comment likes not implemented' });
        const commentId = req.params.id;
        const body = req.body || {};
        const inc = body.increment === undefined ? 1 : Number(body.increment || 0);
        if (!commentId || !inc) return res.status(400).json({ error: 'commentId and increment are required' });

        const updated = await db.incrementCommentLikes(String(commentId), inc);

        // Broadcast to subscribers of the video's comment stream (if we can find videoId)
        try {
            const vid = updated && (updated.videoId || updated.videoid) ? String(updated.videoId || updated.videoid) : null;
            const payload = { type: 'comment_like', data: { comment: updated } };
            const payloadStr = JSON.stringify(payload);
            if (vid) {
                const subs = getVideoSubscribers(vid);
                for (const s of subs) {
                    try { if (s && s.readyState === WebSocket.OPEN) s.send(payloadStr); } catch (e) { console.error('deliver comment_like to ws subscriber failed', e); }
                }
            }
            if (redisPub) {
                try { redisPub.publish('appchat:comment_likes', JSON.stringify({ comment: updated })).catch(()=>{}); } catch(e) { console.error('redis publish comment_like error', e); }
            } else if (useHttpRelay && PUBSUB_URL) {
                try { const fetch = globalThis.fetch || require('node-fetch'); fetch(`${PUBSUB_URL}/publish/comment_likes`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ comment: updated }) }).catch(()=>{}); } catch(e) {}
            }
        } catch (e) {
            console.error('broadcast comment_like failed', e);
        }

        return res.json({ success: true, comment: updated });
    } catch (e) {
        console.error('POST /comments/:id/like failed', e);
        return res.status(500).json({ error: e.message || 'failed' });
    }
});

// Register a device token for push notifications
app.post('/users/:userId/deviceToken', async (req, res) => {
    try {
        const userId = req.params.userId;
        const token = (req.body && req.body.token) || (req.query && req.query.token) || (req.headers['x-device-token'] || '').toString();
        if (!token) return res.status(400).json({ error: 'token is required' });
        if (!db || !db.getUserById) return res.status(501).json({ error: 'DB not available' });
        const user = await db.getUserById(userId).catch(() => null);
        if (!user) return res.status(404).json({ error: 'User not found' });
        const prefs = user.preferences || {};
        if (!prefs || typeof prefs !== 'object') {
            try { prefs = JSON.parse(user.preferences || '{}'); } catch(e) { prefs = {}; }
        }
        prefs.deviceTokens = prefs.deviceTokens || [];
        if (!prefs.deviceTokens.includes(token)) prefs.deviceTokens.push(token);
        // persist via updateProfile (it will stringify preferences when needed)
        await db.updateProfile(userId, { preferences: JSON.stringify(prefs) });
        return res.json({ success: true });
    } catch (e) {
        console.error('register device token failed', e);
        return res.status(500).json({ error: e.message || 'failed' });
    }
});

const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const requireAdminToken = (req, res, next) => {
    if (!ADMIN_TOKEN) return res.status(403).json({ error: 'Admin endpoints are disabled' });
    const token = (req.headers['x-admin-token'] || req.query.adminToken || '').toString();
    if (!token || token !== ADMIN_TOKEN) return res.status(401).json({ error: 'Unauthorized' });
    next();
};

// In-memory runtime metrics
let connectionCount = 0;
const recentErrors = [];
const messageTimestamps = [];
const recordMessage = () => { const now = Date.now(); messageTimestamps.push(now); const cutoff = now - 60 * 1000; while (messageTimestamps.length && messageTimestamps[0] < cutoff) messageTimestamps.shift(); };
const computeMessagesPerSec = () => { const now = Date.now(); const cutoff = now - 10 * 1000; const recent = messageTimestamps.filter(t => t >= cutoff).length; return recent/10.0; };

const getLiveMetrics = async () => {
    try {
        const stats = await db.getStats();
        const pendingWrites = db.getPendingWriteCount ? db.getPendingWriteCount() : 0;
        const mem = process.memoryUsage();
        return {
            timestamp: new Date().toISOString(),
            activeConnections: connectionCount,
            usersCount: stats.usersCount || 0,
            chatsCount: stats.chatsCount || 0,
            messagesCount: stats.messagesCount || 0,
            pendingWrites,
            memory: { rss: mem.rss, heapTotal: mem.heapTotal, heapUsed: mem.heapUsed, external: mem.external || 0 },
            uptime: process.uptime(),
            recentErrors: recentErrors.slice(0,50)
        };
    } catch (e) {
        console.error('getLiveMetrics error', e);
        return { timestamp: new Date().toISOString(), error: String(e) };
    }
    // HTTP poll-based relay (if provided and Redis not used)
    const PUBSUB_URL = process.env.PUBSUB_URL || null;
    let useHttpRelay = false;
    if (!redisPub && PUBSUB_URL) {
        useHttpRelay = true;
        console.log('Using HTTP pubsub relay at', PUBSUB_URL);
        // polling loop
        (async function pollRelay() {
            const fetch = globalThis.fetch || (() => { try { return require('node-fetch'); } catch (e) { return null; } })();
            if (!fetch) { console.warn('No fetch available to poll PUBSUB_URL'); return; }
            while (true) {
                try {
                    const msgs = await fetch(`${PUBSUB_URL}/poll/messages`).then(r => r.json()).catch(()=>[]);
                    for (const m of msgs || []) {
                        try {
                            const sockets = getClientSockets(m.toId);
                            for (const recipientWs of sockets) {
                                try { if (recipientWs && recipientWs.readyState === WebSocket.OPEN) recipientWs.send(JSON.stringify({ type: 'chat', data: m })); } catch(e) { console.error('relay deliver error', e); }
                            }
                        } catch (e) { console.error('relay deliver error', e); }
                    }
                    const pres = await fetch(`${PUBSUB_URL}/poll/presence`).then(r => r.json()).catch(()=>[]);
                    for (const p of pres || []) {
                        try {
                            const sockets = getClientSockets(p.userId);
                            for (const ws of sockets) {
                                try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'presence', data: p })); } catch(e) { console.error('relay presence deliver error', e); }
                            }
                        } catch (e) { console.error('relay presence deliver error', e); }
                    }
                    // profile updates via HTTP relay
                    const profs = await fetch(`${PUBSUB_URL}/poll/profile_updates`).then(r => r.json()).catch(()=>[]);
                    for (const pu of profs || []) {
                        try {
                            const userId = String(pu.userId || pu.id || '');
                            if (!userId) continue;
                            const userSockets = getClientSockets(userId);
                            for (const recipientWs of userSockets) {
                                try { if (recipientWs && recipientWs.readyState === WebSocket.OPEN) recipientWs.send(JSON.stringify({ type: 'profile_update', data: pu })); } catch(e) { console.error('relay profile_update deliver error', e); }
                            }
                            // notify contacts
                            if (db && db.getUserContacts) {
                                try {
                                    const contacts = await db.getUserContacts(userId).catch(()=>[]);
                                    for (const c of contacts || []) {
                                        const cid = String((c && (c.id || c.userId || c)) || '');
                                        if (!cid) continue;
                                        const sockets = getClientSockets(cid);
                                        for (const s of sockets) {
                                            try { if (s && s.readyState === WebSocket.OPEN) s.send(JSON.stringify({ type: 'profile_update', data: pu })); } catch(e) { console.error('relay profile_update contact deliver error', e); }
                                        }
                                    }
                                } catch (e) { console.error('relay profile_update contacts error', e); }
                            }
                        } catch (e) { console.error('relay profile_update handling error', e); }
                    }
                } catch (e) {
                    console.error('pollRelay error', e);
                }
                await new Promise(r => setTimeout(r, 300));
            }
        })();
    }
};

// --- FCM (Firebase Cloud Messaging) support (optional) ---
let fcmAdmin = null;
let fcmAvailable = false;
try {
    if (process.env.FCM_SERVICE_ACCOUNT_JSON) {
        try {
            const admin = require('firebase-admin');
            const sa = JSON.parse(process.env.FCM_SERVICE_ACCOUNT_JSON);
            admin.initializeApp({ credential: admin.credential.cert(sa) });
            fcmAdmin = admin;
            fcmAvailable = true;
            console.log('FCM initialized from FCM_SERVICE_ACCOUNT_JSON');
        } catch (e) {
            console.warn('Failed to initialize FCM from FCM_SERVICE_ACCOUNT_JSON', e && e.message ? e.message : e);
        }
    }
} catch (e) {}

// Helper: get registered device tokens for a user (stored inside preferences.deviceTokens)
const getDeviceTokensForUser = async (userId) => {
    try {
        if (!db || !db.getUserById) return [];
        const u = await db.getUserById(String(userId));
        if (!u) return [];
        const prefs = u.preferences || {};
        if (!prefs) return [];
        const tokens = prefs.deviceTokens || [];
        return Array.isArray(tokens) ? tokens : [];
    } catch (e) { return []; }
};

// Send a push to a user's registered device tokens (if FCM configured)
const sendPushToUser = async (userId, title, body, data = {}) => {
    try {
        if (!fcmAvailable || !fcmAdmin) return false;
        const tokens = await getDeviceTokensForUser(userId);
        if (!tokens || !tokens.length) return false;
        // Build multicast message
        const message = {
            tokens,
            notification: { title: String(title || ''), body: String(body || '') },
            data: Object.assign({}, (data || {}))
        };
        const resp = await fcmAdmin.messaging().sendMulticast(message);
        // log successes/failures for debugging
        if (resp.failureCount && resp.failureCount > 0) {
            console.warn('FCM some failures', resp.responses.filter(r => !r.success).map(r => r.error && r.error.message));
        }
        return true;
    } catch (e) {
        console.error('sendPushToUser failed', e && e.message ? e.message : e);
        return false;
    }
};

// Helper to track connections (simple logging/trim)
const trackConnections = () => {
    // keep recentErrors trimmed
    while (recentErrors.length > 200) recentErrors.shift();
};

// HTTP routes
app.get('/health', (req, res) => res.json({ status: 'ok' }));

// Root route: provide a small human-readable page to confirm the server is running
app.get('/', (req, res) => {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.send(`<!doctype html><html><head><meta charset="utf-8"><title>AppChat Server</title></head><body><h1>AppChat signaling server</h1><p>Status: OK</p><p>Try <a href="/health">/health</a> for JSON.</p></body></html>`);
});

// Help center articles (lightweight stub)
app.get('/help/articles', async (req, res) => {
    try {
        const articles = [
            { id: 'a1', title: 'Getting started', excerpt: 'How to use the app and set up your account.' },
            { id: 'a2', title: 'Privacy & Security', excerpt: 'Manage your privacy settings and data.' },
            { id: 'a3', title: 'Troubleshooting', excerpt: 'Common issues and fixes.' },
        ];
        return res.json(articles);
    } catch (e) { return res.status(500).json({ error: e.message }); }
});

// Contact form -> send email to admin (uses emailjs)
const { body, validationResult } = (() => { try { return require('express-validator'); } catch(e) { return { body: null, validationResult: null }; } })();

// stricter limiter for contact form
let contactLimiter = null;
try { const rateLimit = require('express-rate-limit'); contactLimiter = rateLimit({ windowMs: 60*1000, max: Number(process.env.CONTACT_MAX_PER_MIN || 6), standardHeaders: true, legacyHeaders: false }); } catch(e) { contactLimiter = null; }

app.post('/help/contact', contactLimiter ? contactLimiter : (req, res, next) => next(),
    body ? [
        body('name').optional().isString().isLength({ max: 100 }).trim().escape(),
        body('email').optional().isEmail().normalizeEmail(),
        body('subject').optional().isString().isLength({ max: 150 }).trim().escape(),
        body('message').isString().isLength({ min: 5, max: 5000 }).trim().escape(),
        body('userId').optional().isString().isLength({ max: 128 }).trim().escape()
    ] : (req, res, next) => next(),
    async (req, res) => {
    try {
        // validation
        if (validationResult && validationResult(req)) {
            const errs = validationResult(req);
            if (!errs.isEmpty()) return res.status(400).json({ error: 'Validation failed', details: errs.array() });
        }
        const { name, email, subject, message, userId } = req.body || {};
        if (!message || (!email && !userId)) return res.status(400).json({ error: 'message and email or userId required' });

        const adminEmail = process.env.ADMIN_EMAIL || 'ferryfernando2208@gmail.com';

        // Build email content
        const fullSubject = subject ? `[Help] ${subject}` : '[Help] New message from appchat';
        const bodyText = `From: ${name || 'Anonymous'}\nEmail: ${email || ''}\nUserId: ${userId || ''}\n\n${message}`;

        // Suspicious content detection: simple heuristics
        const suspiciousPatterns = [/\b(select|drop|insert|update|delete)\b/i, /<\s*script\b/i, /https?:\/\//i];
        let suspiciousScore = 0;
        for (const p of suspiciousPatterns) if (p.test(message)) suspiciousScore++;
        if (suspiciousScore >= 2) {
            // Penalize the token bucket for suspicious content; this allows short term penalization without immediate global ban
            const key = getIpKey(req);
            // consume several tokens to slow the actor
            const penalize = consumeToken(key, Math.max(4, Math.round(MAX_REQUESTS_PER_MIN / 30)));
            // if already banned by consumeToken, reflect that
            if (!penalize.allowed) {
                return res.status(400).json({ error: 'Suspicious content detected' });
            }
            return res.status(400).json({ error: 'Suspicious content detected' });
        }

        // Try to send via SMTP if configured
        const SMTP_HOST = process.env.SMTP_HOST || '';
        const SMTP_PORT = process.env.SMTP_PORT ? Number(process.env.SMTP_PORT) : 587;
        const SMTP_USER = process.env.SMTP_USER || '';
        const SMTP_PASS = process.env.SMTP_PASS || '';

    if (SMTP_HOST && SMTP_USER && SMTP_PASS) {
            try {
                const nodemailer = require('nodemailer');
                const transporter = nodemailer.createTransport({ host: SMTP_HOST, port: SMTP_PORT, secure: SMTP_PORT === 465, auth: { user: SMTP_USER, pass: SMTP_PASS } });
                await transporter.sendMail({ from: `${name || 'AppChat'} <${SMTP_USER}>`, to: adminEmail, subject: fullSubject, text: bodyText });
                return res.json({ success: true });
            } catch (e) {
                console.error('email send failed', e);
                return res.status(500).json({ error: 'Failed to send email', detail: String(e) });
            }
        }

        // Fallback: just log the message on the server
        console.log('Contact form submission', { name, email, userId, subject, message });
        return res.json({ success: true, note: 'SMTP not configured, message logged on server' });
    } catch (e) {
        console.error('contact endpoint error', e);
        return res.status(500).json({ error: e.message || 'Contact failed' });
    }
});

// Group video rooms listing (lightweight stub)
app.get('/groupRooms', async (req, res) => {
    try {
        const rooms = [
            { id: 'r1', name: 'Open Hangout', participants: 2 },
            { id: 'r2', name: 'Study Group', participants: 5 },
        ];
        return res.json(rooms);
    } catch (e) { return res.status(500).json({ error: e.message }); }
});

app.post('/register', async (req, res) => {
    try {
    if (!dbReady) return res.status(503).json({ error: 'Database not ready' });
        const { email, password, username } = req.body;
        const user = await db.createUser(email, password, username);
        res.json(user);
    } catch (e) {
        res.status(400).json({ error: e.message });
    }
});

// Scheduled messages endpoints
app.post('/scheduled_messages', async (req, res) => {
    try {
        if (!db || !db.saveScheduledMessage) return res.status(501).json({ error: 'Scheduled messages not supported' });
        const body = req.body || {};
        // require basic fields
        const fromId = body.fromId || (req.headers['x-user-id'] || req.query.userId || null);
        const toId = body.toId || body.to || null;
        const content = body.content || body.message || '';
        const scheduledAt = body.scheduledAt || body.scheduled_at || null;
        if (!fromId || !toId || !scheduledAt) return res.status(400).json({ error: 'fromId, toId and scheduledAt are required' });
        const obj = { id: body.id || null, chatId: body.chatId || null, fromId: String(fromId), toId: String(toId), content, scheduledAt, stampEnabled: body.stampEnabled === false ? false : true };
        const saved = await db.saveScheduledMessage(obj);
        return res.json({ success: true, scheduled: saved });
    } catch (e) {
        console.error('scheduled_messages create failed', e);
        return res.status(500).json({ error: e.message || 'failed' });
    }
});

app.get('/users/:userId/scheduled_messages', async (req, res) => {
    try {
        if (!db || !db.getScheduledMessagesForUser) return res.status(501).json({ error: 'Not supported' });
        const rows = await db.getScheduledMessagesForUser(req.params.userId);
        return res.json(rows);
    } catch (e) {
        return res.status(500).json({ error: e.message });
    }
});

app.delete('/scheduled_messages/:id', async (req, res) => {
    try {
        if (!db || !db.markScheduledMessageStatus) return res.status(501).json({ error: 'Not supported' });
        const id = req.params.id;
        await db.markScheduledMessageStatus(id, 'cancelled');
        return res.json({ success: true });
    } catch (e) {
        return res.status(500).json({ error: e.message });
    }
});

app.post('/login', async (req, res) => {
    try {
    if (!dbReady) return res.status(503).json({ error: 'Database not ready' });
        const { email, password } = req.body;
        const user = await db.loginUser(email, password);
        res.json(user);
    } catch (e) {
        res.status(401).json({ error: e.message });
    }
});

// --- Heyyow integration: link account and login via Heyyow token ---
// Expected env vars:
// HEYYOW_API_URL (e.g. https://api.heyyow.example)
// HEYYOW_API_KEY (optional - if Heyyow requires server-side API key)
const HEYYOW_API_URL = process.env.HEYYOW_API_URL || '';
const HEYYOW_API_KEY = process.env.HEYYOW_API_KEY || '';

// Helper to validate Heyyow token and fetch remote profile
const fetchHeyyowProfile = async (token) => {
    if (!HEYYOW_API_URL) throw new Error('HEYYOW_API_URL not configured');
    const fetcher = globalThis.fetch || (() => { try { return require('node-fetch'); } catch (e) { return null; } })();
    if (!fetcher) throw new Error('fetch not available');
    const url = `${HEYYOW_API_URL.replace(/\/$/, '')}/me`;
    const headers = { 'authorization': `Bearer ${token}`, 'accept': 'application/json' };
    if (HEYYOW_API_KEY) headers['x-api-key'] = HEYYOW_API_KEY;
    const resp = await fetcher(url, { method: 'GET', headers }).then(r => r).catch(e => { throw e; });
    if (!resp || (resp.status && resp.status >= 400)) {
        const body = resp && resp.text ? await resp.text().catch(()=>null) : null;
        throw new Error(`Heyyow validation failed: ${resp && resp.status} ${body || ''}`);
    }
    const data = await resp.json().catch(() => null);
    if (!data) throw new Error('Invalid Heyyow profile response');
    return data;
};

// Link a Heyyow account to an existing local user
app.post('/users/:userId/link/heyyow', async (req, res) => {
    try {
        const userId = req.params.userId;
        const { heyyowToken } = req.body || {};
        if (!userId) return res.status(400).json({ error: 'userId is required' });
        if (!heyyowToken) return res.status(400).json({ error: 'heyyowToken is required' });
        // validate token with Heyyow
        let profile;
        try {
            profile = await fetchHeyyowProfile(heyyowToken);
        } catch (e) {
            return res.status(401).json({ error: 'Invalid Heyyow token', detail: String(e && e.message ? e.message : e) });
        }

        // persist linkage into user's profile. We store under preferences.linkedAccounts.heyyow
        try {
            // read existing profile if available
            const existing = await (db.getUserById ? db.getUserById(String(userId)).catch(()=>null) : Promise.resolve(null));
            const prefsRaw = existing && existing.preferences ? (typeof existing.preferences === 'string' ? JSON.parse(existing.preferences || '{}') : existing.preferences) : {};
            prefsRaw.linkedAccounts = prefsRaw.linkedAccounts || {};
            prefsRaw.linkedAccounts.heyyow = { id: profile.id || profile.userId || profile.sub || null, profile };
            if (db && db.updateProfile) {
                await db.updateProfile(String(userId), { preferences: JSON.stringify(prefsRaw) });
            } else {
                return res.status(501).json({ error: 'DB updateProfile not implemented on server' });
            }
        } catch (e) {
            console.error('Failed to persist Heyyow link', e);
            return res.status(500).json({ error: 'Failed to persist link', detail: String(e && e.message ? e.message : e) });
        }

        return res.json({ success: true, linked: { provider: 'heyyow', profile } });
    } catch (e) {
        console.error('heyyow link error', e);
        return res.status(500).json({ error: e.message || 'heyyow link failed' });
    }
});

// Login / create via Heyyow token. Attempts to find an existing user linked to the Heyyow profile.
app.post('/auth/heyyow', async (req, res) => {
    try {
        const { heyyowToken } = req.body || {};
        if (!heyyowToken) return res.status(400).json({ error: 'heyyowToken is required' });
        let profile;
        try { profile = await fetchHeyyowProfile(heyyowToken); } catch (e) { return res.status(401).json({ error: 'Invalid Heyyow token', detail: String(e && e.message ? e.message : e) }); }

        // Try to find an existing user linked with this heyyow id
        const heyyowId = profile.id || profile.userId || profile.sub || null;
        if (!heyyowId) return res.status(500).json({ error: 'Heyyow profile missing id' });

        // Prefer a DB helper if available
        let user = null;
        try {
            if (db && db.findUserByLinkedAccount) {
                user = await db.findUserByLinkedAccount('heyyow', heyyowId).catch(()=>null);
            }
            // fallback to a generic search (DB handlers may provide different helpers)
            if (!user && db && db.searchUsers) {
                const uname = profile.username || profile.name || '';
                const results = await db.searchUsers(uname || '').catch(()=>[]);
                user = (results || []).find(u => {
                    try {
                        const prefs = u.preferences ? (typeof u.preferences === 'string' ? JSON.parse(u.preferences) : u.preferences) : {};
                        return prefs && prefs.linkedAccounts && prefs.linkedAccounts.heyyow && String(prefs.linkedAccounts.heyyow.id) === String(heyyowId);
                    } catch (e) { return false; }
                }) || null;
            }

            // If user not found and DB supports creation from external profile, create a lightweight account
            if (!user) {
                if (db && db.createUserFromExternal) {
                    user = await db.createUserFromExternal('heyyow', heyyowId, { displayName: profile.name || profile.username || '', email: profile.email || null, avatar: profile.avatar || profile.picture || null });
                } else {
                    // As a last resort, return an informative error so client can ask user to link manually
                    return res.status(404).json({ error: 'No local account linked to this Heyyow profile. Please link your Heyyow account from app settings.' });
                }
            }
        } catch (e) {
            console.error('heyyow login DB lookup/create error', e);
            return res.status(500).json({ error: 'Internal server error', detail: String(e && e.message ? e.message : e) });
        }

        // At this point, `user` should be a local user object. Respond with it (client handles session storage)
        return res.json({ success: true, user });
    } catch (e) {
        console.error('heyyow auth error', e);
        return res.status(500).json({ error: e.message || 'heyyow auth failed' });
    }
});

app.get('/users/search', async (req, res) => {
    try { const { query } = req.query; const users = await db.searchUsers(query || ''); res.json(users); } catch (e) { res.status(500).json({ error: e.message }); }
});
// Helper: determine if requester can view target's profile image
const canViewProfileImage = async (requesterId, targetId) => {
    try {
        if (!requesterId) return false;
        if (requesterId === targetId) return true;
        const profile = await db.getUserById(targetId);
        if (!profile) return false;
        const vis = (profile.profileImageUrl && (profile.profileVisibility || 'public')) ? (profile.profileVisibility || 'public') : 'public';
        if (vis === 'public') return true;
        if (vis === 'private') return false;
        // contacts only
        const contacts = await db.getUserContacts(targetId);
        return (contacts || []).some(c => c.id === requesterId);
    } catch (e) { return false; }
};

app.get('/users/:userId', async (req, res) => {
    try {
        const targetId = req.params.userId;
        const requesterId = (req.headers['x-user-id'] || req.query.requesterId || '').toString() || null;
        const user = await db.getUserById(targetId);
        if (!user) return res.status(404).json({ error: 'User not found' });

        // enforce visibility for profileImageUrl
        const allowed = await canViewProfileImage(requesterId, targetId);
        if (!allowed) {
            user.profileImageUrl = null;
        }
        res.json(user);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// Get account preferences
app.get('/users/:userId/preferences', async (req, res) => {
    try {
        if (db && db.getUserById) {
            const user = await db.getUserById(req.params.userId);
            if (!user) return res.status(404).json({ error: 'User not found' });
            return res.json({ preferences: user.preferences || {} });
        }
        return res.status(501).json({ error: 'Not implemented' });
    } catch (e) { return res.status(500).json({ error: e.message }); }
});

// Update account preferences
app.put('/users/:userId/preferences', async (req, res) => {
    try {
        if (!db || !db.updateProfile) return res.status(501).json({ error: 'Not implemented' });
        const prefs = req.body || {};
        // Store as JSON in profile via updateProfile
        const updated = await db.updateProfile(req.params.userId, { preferences: JSON.stringify(prefs) });
        return res.json({ success: true, preferences: prefs, user: updated });
    } catch (e) { return res.status(500).json({ error: e.message }); }
});

// Delete account (admin token or owner only)
app.delete('/users/:userId', async (req, res) => {
    try {
        const userId = req.params.userId;
        const requester = (req.headers['x-user-id'] || req.query.requesterId || '').toString();
        const adminToken = (req.headers['x-admin-token'] || req.query.adminToken || '').toString();
        if (ADMIN_TOKEN && adminToken === ADMIN_TOKEN) {
            // admin allowed
        } else if (requester !== userId) {
            return res.status(401).json({ error: 'Unauthorized' });
        }
        if (db && db.deleteUser) {
            const ok = await db.deleteUser(userId);
            return res.json({ success: ok });
        }
        return res.status(501).json({ error: 'Delete user not implemented in DB handler' });
    } catch (e) { return res.status(500).json({ error: e.message }); }
});

app.get('/users/:userId/contacts', async (req, res) => { try { const contacts = await db.getUserContacts(req.params.userId); res.json(contacts); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/users/:userId/contacts', async (req, res) => {
    try {
        // accept multiple possible field names from client for tolerance
        const contactId = req.body.contactId || req.body.id || req.body.userId || req.body.contact;
        if (!contactId) return res.status(400).json({ error: 'contactId is required' });

        // verify both users exist
        const owner = await db.getUserById(req.params.userId).catch(() => null);
        if (!owner) return res.status(404).json({ error: 'Owner user not found' });

        const target = await db.getUserById(contactId).catch(() => null);
        if (!target) return res.status(404).json({ error: 'Contact user not found' });

        // add contact (db.addContact should validate duplication)
        const added = await db.addContact(req.params.userId, contactId);

        // return full profile for client convenience
        const profile = await db.getUserProfile(contactId);
        return res.json({ success: true, contact: profile });
    } catch (e) {
        console.error('add contact error', e);
        if (e && e.message && e.message.includes('already')) return res.status(409).json({ error: 'Contact already added' });
        return res.status(500).json({ error: e.message || 'Failed to add contact' });
    }
});
// Create or return an existing chat between two users
app.post('/chats', async (req, res) => {
    try {
        const { userId, contactId } = req.body || {};
        if (!userId || !contactId) return res.status(400).json({ error: 'userId and contactId are required' });

        // verify users exist
        const me = await db.getUserById(userId).catch(() => null);
        if (!me) return res.status(404).json({ error: 'User not found' });
        const other = await db.getUserById(contactId).catch(() => null);
        if (!other) return res.status(404).json({ error: 'Contact not found' });

        // Build a lightweight Chat object that the client expects
        const chatId = [userId, contactId].sort().join('_');
        const chatPayload = {
            id: chatId,
            userId: other.id,
            username: other.username || '',
            email: other.email || '',
            lastSeen: other.lastseen || other.lastSeen || 'offline',
            color: other.color || 0xFF2196F3,
            lastMessage: '',
            time: new Date().toISOString(),
            unread: 0,
        };

        return res.json(chatPayload);
    } catch (e) {
        console.error('Failed to create/get chat', e);
        return res.status(500).json({ error: 'Failed to create chat' });
    }
});

// Create a group
app.post('/groups', async (req, res) => {
    try {
        if (!db || !db.createGroup) return res.status(501).json({ error: 'Group support not implemented' });
        const { id, name, ownerId, members } = req.body || {};
        if (!id || !name || !ownerId) return res.status(400).json({ error: 'id, name, and ownerId are required' });
        const mlist = Array.isArray(members) ? members : (members ? [members] : []);
        const group = await db.createGroup(id, name, ownerId, mlist);
        return res.json({ success: true, group });
    } catch (e) { console.error('create group failed', e); return res.status(500).json({ error: e.message || 'create group failed' }); }
});

// Get groups for a user
app.get('/users/:userId/groups', async (req, res) => {
    try {
        if (!db || !db.getUserGroups) return res.status(501).json({ error: 'Not implemented' });
        const groups = await db.getUserGroups(req.params.userId);
        return res.json(groups);
    } catch (e) { return res.status(500).json({ error: e.message }); }
});
app.get('/users/:userId/profile', async (req, res) => { try { const p = await db.getUserProfile(req.params.userId); res.json(p); } catch (e) { res.status(400).json({ error: e.message }); } });
app.put('/users/:userId/profile', async (req, res) => { try { const u = await db.updateProfile(req.params.userId, req.body); res.json(u); } catch (e) { res.status(400).json({ error: e.message }); } });
app.put('/users/:userId/publicKey', async (req, res) => { try { const { publicKey } = req.body; if (!publicKey) return res.status(400).json({ error: 'publicKey is required' }); const updated = await db.saveUserPublicKey(req.params.userId, publicKey); res.json({ success: true, user: updated }); } catch (e) { res.status(500).json({ error: e.message }); } });

// Change password
app.post('/users/:userId/change-password', async (req, res) => {
    try {
        const userId = req.params.userId;
        const { oldPassword, newPassword } = req.body || {};
        if (!newPassword) return res.status(400).json({ error: 'newPassword is required' });
        if (!db || !db.changePassword) return res.status(501).json({ error: 'Not implemented' });
        try {
            const ok = await db.changePassword(userId, oldPassword, newPassword);
            return res.json({ success: ok });
        } catch (e) {
            return res.status(400).json({ error: e.message });
        }
    } catch (e) { return res.status(500).json({ error: e.message }); }
});

// TOTP setup - generate secret and return otpauth URL for QR
app.post('/users/:userId/totp/setup', async (req, res) => {
    try {
        const userId = req.params.userId;
        if (!db || !db.generateTOTPSecret) return res.status(501).json({ error: 'Not implemented' });
        const secretObj = await db.generateTOTPSecret(userId);
            // return otpauth_url and base32 secret (also expose as 'secret' for client)
            return res.json({ success: true, otpauth_url: secretObj.otpauth_url, secret: secretObj.base32, base32: secretObj.base32 });
    } catch (e) { return res.status(500).json({ error: e.message }); }
});

// TOTP confirm - verify the provided token
app.post('/users/:userId/totp/confirm', async (req, res) => {
    try {
        const userId = req.params.userId;
        const { code } = req.body || {};
        if (!code) return res.status(400).json({ error: 'code is required' });
        if (!db || !db.verifyTOTP) return res.status(501).json({ error: 'Not implemented' });
        const ok = await db.verifyTOTP(userId, code);
        return res.json({ success: ok });
    } catch (e) { return res.status(500).json({ error: e.message }); }
});

// Sessions (multi-device) - lightweight in-memory stub using DB preferences
app.get('/users/:userId/sessions', async (req, res) => {
    try {
        if (!db || !db.getUserById) return res.status(501).json({ error: 'Not implemented' });
        const user = await db.getUserById(req.params.userId);
        if (!user) return res.status(404).json({ error: 'User not found' });
        const prefs = user.preferences ? (typeof user.preferences === 'string' ? JSON.parse(user.preferences) : user.preferences) : {};
        const sessions = prefs['sessions'] ?? [];
        return res.json({ sessions });
    } catch (e) { return res.status(500).json({ error: e.message }); }
});

app.delete('/users/:userId/sessions/:sessionId', async (req, res) => {
    try {
        const user = await db.getUserById(req.params.userId);
        if (!user) return res.status(404).json({ error: 'User not found' });
        const prefs = user.preferences ? (typeof user.preferences === 'string' ? JSON.parse(user.preferences) : user.preferences) : {};
        let sessions = prefs['sessions'] || [];
        if (!Array.isArray(sessions)) sessions = [];
        const filtered = sessions.filter(s => {
            try {
                const sid = (s && (s.id || s.sessionId || s.sid || s['id'] || s['sessionId'] || s['sid']));
                return String(sid) !== String(req.params.sessionId);
            } catch (e) { return true; }
        });
        prefs['sessions'] = filtered;
        await db.updateProfile(req.params.userId, { preferences: JSON.stringify(prefs) });
        return res.json({ success: true });
    } catch (e) { return res.status(500).json({ error: e.message }); }
});

// Data export stub
app.post('/users/:userId/export', async (req, res) => {
    try {
        // For now, return a small payload or indicate export is queued
        // Real impl: spawn job, collect user data, provide download URL
        return res.json({ success: true, download: `/exports/${req.params.userId}/data.json` });
    } catch (e) { return res.status(500).json({ error: e.message }); }
});

app.get('/admin/stream', async (req, res) => {
    if (!ADMIN_TOKEN) return res.status(403).end('Admin endpoints are disabled');
    const token = (req.headers['x-admin-token'] || req.query.adminToken || '').toString();
    if (!token || token !== ADMIN_TOKEN) return res.status(401).end('Unauthorized');
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders && res.flushHeaders();
    let closed = false;
    const send = async () => { if (closed) return; const payload = await getLiveMetrics(); res.write(`data: ${JSON.stringify(payload)}\n\n`); };
    send(); const iv = setInterval(send, 2000); req.on('close', () => { closed = true; clearInterval(iv); });
});

app.get('/admin/stats', requireAdminToken, async (req, res) => { try { const stats = await db.getStats(); const pendingWrites = db.getPendingWriteCount(); res.json({ success: true, stats: { ...stats, pendingWrites } }); } catch (e) { res.status(500).json({ error: 'Failed to collect stats' }); } });
app.post('/admin/flush', requireAdminToken, async (req, res) => { try { const ok = await db.flushAll(); res.json({ success: ok }); } catch (e) { res.status(500).json({ error: 'Flush failed' }); } });

// Admin: reset rate limiter buckets (global or specific key). Useful to recover from accidental bans.
app.post('/admin/resetRate', requireAdminToken, async (req, res) => {
    try {
        const { key } = req.body || {};
        if (key) {
            ipBuckets.delete(key);
            return res.json({ success: true, note: `bucket ${key} reset` });
        }
        ipBuckets.clear();
        return res.json({ success: true, note: 'all buckets cleared' });
    } catch (e) {
        console.error('resetRate failed', e);
        return res.status(500).json({ error: 'resetRate failed' });
    }
});

// Clear only messages/chats but keep user accounts intact
app.post('/admin/clearMessages', requireAdminToken, async (req, res) => {
    try {
        if (!db.clearMessages) return res.status(501).json({ error: 'clearMessages not implemented in DB handler' });
        const ok = await db.clearMessages();
        res.json({ success: ok });
    } catch (e) {
        console.error('clearMessages failed', e);
        res.status(500).json({ error: 'clearMessages failed' });
    }
});

// Start server after DB init
(async function start() {
    try {
        await db.init();
        console.log('Database cache initialized');
        // Only mark ready if either Postgres pool or sqliteDb is available
        if ((db.usePostgres && db.pgPool) || (db.useSqlite && db.sqliteDb)) {
            dbReady = true;
        } else {
            console.warn('DB init returned but backend not initialized fully');
            dbReady = false;
        }
    } catch (e) {
        console.error('DB init failed', e);
        dbReady = false;
    }

    // Explicitly bind to 0.0.0.0 so the server listens on all network interfaces
    // (this allows phone devices on a hotspot to connect to the PC server).
    const server = app.listen(port, '0.0.0.0', () => console.log(`Server is running on port ${port} and bound to 0.0.0.0`));
    const wss = new WebSocket.Server({ server });
    // Map userId -> Set of WebSocket connections (support multi-device)
    const clients = new Map();
    // Map videoId -> Set of WebSocket connections subscribed to comment updates
    const videoSubscribers = new Map();

    // Expose a process-global helper so handlers defined earlier can notify connected clients
    try {
        globalThis.broadcastNewVideo = (video) => {
            try {
                const payload = JSON.stringify({ type: 'new_video', data: video });
                for (const [uid, set] of clients.entries()) {
                    for (const s of set) {
                        try { if (s && s.readyState === WebSocket.OPEN) s.send(payload); } catch(e) { }
                    }
                }
                // publish to redisPub if available (redisPub is set later in this scope)
                try { if (redisPub) { redisPub.publish('appchat:new_video', JSON.stringify({ video })).catch(()=>{}); } } catch(e) {}
            } catch (e) { console.error('broadcastNewVideo failed', e); }
        };
    } catch (e) {}

    const addClient = (userId, socket) => {
        const key = String(userId);
        let set = clients.get(key);
        if (!set) {
            set = new Set();
            clients.set(key, set);
        }
        set.add(socket);
    };

    const removeClient = (userId, socket) => {
        const key = String(userId);
        const set = clients.get(key);
        if (!set) return;
        set.delete(socket);
        if (set.size === 0) clients.delete(key);
    };

    const getClientSockets = (userId) => {
        const set = clients.get(String(userId));
        return set ? Array.from(set) : [];
    };

    const addVideoSubscriber = (videoId, socket) => {
        if (!videoId) return;
        const key = String(videoId);
        let set = videoSubscribers.get(key);
        if (!set) { set = new Set(); videoSubscribers.set(key, set); }
        set.add(socket);
    };

    const removeVideoSubscriber = (videoId, socket) => {
        if (!videoId) return;
        const key = String(videoId);
        const set = videoSubscribers.get(key);
        if (!set) return;
        set.delete(socket);
        if (set.size === 0) videoSubscribers.delete(key);
    };

    const getVideoSubscribers = (videoId) => {
        const set = videoSubscribers.get(String(videoId));
        return set ? Array.from(set) : [];
    };

    // Admin debug endpoint: list connected clients and socket counts
    // Requires ADMIN_TOKEN (reuse existing requireAdminToken middleware defined above)
    try {
        app.get('/admin/clients', requireAdminToken, (req, res) => {
            try {
                const out = [];
                for (const [userId, set] of clients.entries()) {
                    out.push({ userId, sockets: set.size });
                }
                res.json({ success: true, clients: out, totalUsers: out.length });
            } catch (e) {
                console.error('admin/clients error', e);
                res.status(500).json({ success: false, error: 'internal' });
            }
        });
    } catch (e) {
        // If requireAdminToken is not defined for some reason, skip silently
        console.warn('Could not register /admin/clients endpoint', e);
    }

    // Local-only debug endpoint: list connected clients when accessed from localhost
    try {
        app.get('/debug/clients', (req, res) => {
            try {
                const remote = req.ip || req.connection && req.connection.remoteAddress || '';
                // Allow only local requests for this endpoint
                if (!remote || !(remote.includes('127.0.0.1') || remote.includes('::1') || remote.includes('::ffff:127.0.0.1'))) {
                    return res.status(403).json({ error: 'forbidden' });
                }
                const out = [];
                for (const [userId, set] of clients.entries()) {
                    out.push({ userId, sockets: set.size });
                }
                res.json({ success: true, clients: out, totalUsers: out.length });
            } catch (e) {
                console.error('debug/clients error', e);
                res.status(500).json({ success: false, error: 'internal' });
            }
        });
    } catch (e) {
        console.warn('Could not register /debug/clients endpoint', e);
    }

    // Optional Redis pub/sub for cross-instance message broadcasting
    let redisPub = null;
    let redisSub = null;
    const REDIS_URL = process.env.REDIS_URL || process.env.REDIS || null;

    // Optional HTTP poll-based relay when Redis is not available
    const PUBSUB_URL = process.env.PUBSUB_URL || null;
    let useHttpRelay = false;

    if (REDIS_URL) {
        try {
            const { createClient } = require('redis');
            redisPub = createClient({ url: REDIS_URL });
            redisSub = createClient({ url: REDIS_URL });
            redisPub.connect().catch(err => console.error('redisPub connect error', err));
            redisSub.connect().catch(err => console.error('redisSub connect error', err));

            // messages channel
            redisSub.subscribe('appchat:messages', (msg) => {
                try {
                    const data = JSON.parse(msg);
                    const sockets = getClientSockets(data.toId);
                    for (const recipientWs of sockets) {
                        try { if (recipientWs && recipientWs.readyState === WebSocket.OPEN) recipientWs.send(JSON.stringify({ type: 'chat', data })); } catch(e) { console.error('redis message deliver error', e); }
                    }
                } catch (e) { console.error('redis message handling error', e); }
            }).catch(e => console.error('redisSub subscribe error', e));

            // presence channel
            redisSub.subscribe('appchat:presence', (msg) => {
                try {
                    const p = JSON.parse(msg);
                    const sockets = getClientSockets(p.userId);
                    for (const ws of sockets) {
                        try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'presence', data: p })); } catch(e) { console.error('redis presence deliver error', e); }
                    }
                } catch (e) { console.error('redis presence handling error', e); }
            }).catch(e => console.error('redisSub subscribe error', e));

            // profile_update channel - notify user devices and their contacts about profile changes
            redisSub.subscribe('appchat:profile_update', (msg) => {
                try {
                    const p = JSON.parse(msg);
                    const userId = String(p.userId || p.id || '');
                    if (!userId) return;

                    // notify all sockets for the updated user
                    const userSockets = getClientSockets(userId);
                    for (const recipientWs of userSockets) {
                        try { if (recipientWs && recipientWs.readyState === WebSocket.OPEN) recipientWs.send(JSON.stringify({ type: 'profile_update', data: p })); } catch(e) { console.error('redis profile_update deliver error', e); }
                    }

                    // also notify contacts (if DB supports it)
                    (async () => {
                        try {
                            if (db && db.getUserContacts) {
                                const contacts = await db.getUserContacts(userId).catch(()=>[]);
                                for (const c of contacts || []) {
                                    const cid = String((c && (c.id || c.userId || c)) || '');
                                    if (!cid) continue;
                                    const sockets = getClientSockets(cid);
                                    for (const s of sockets) {
                                        try { if (s && s.readyState === WebSocket.OPEN) s.send(JSON.stringify({ type: 'profile_update', data: p })); } catch(e) { console.error('redis profile_update contact deliver error', e); }
                                    }
                                }
                            }
                        } catch (e) { console.error('redis profile_update contacts error', e); }
                    })();
                } catch (e) { console.error('redis profile_update handling error', e); }
            }).catch(e => console.error('redisSub subscribe error', e));

            // video comments channel - broadcast new comments across instances
            redisSub.subscribe('appchat:video_comments', (msg) => {
                try {
                    const data = JSON.parse(msg);
                    const vid = String(data.videoId || (data.comment && data.comment.videoId) || '');
                    if (!vid) return;
                    const subs = getVideoSubscribers(vid);
                    for (const s of subs) {
                        try { if (s && s.readyState === WebSocket.OPEN) s.send(JSON.stringify({ type: 'video_comment', data: data.comment })); } catch(e) { console.error('redis deliver video_comment error', e); }
                    }
                } catch (e) { console.error('redisSub video_comments handling error', e); }
            }).catch(e => console.error('redisSub subscribe error', e));

            // status_update channel - notify user devices and their contacts about status changes
            redisSub.subscribe('appchat:status_update', (msg) => {
                try {
                    const p = JSON.parse(msg);
                    const userId = String(p.userId || p.id || '');
                    if (!userId) return;

                    // deliver to user's own sockets
                    const userSockets = getClientSockets(userId);
                    for (const recipientWs of userSockets) {
                        try { if (recipientWs && recipientWs.readyState === WebSocket.OPEN) recipientWs.send(JSON.stringify({ type: 'status_update', data: p })); } catch(e) { console.error('redis status_update deliver error', e); }
                    }

                    // notify contacts
                    (async () => {
                        try {
                            if (db && db.getUserContacts) {
                                const contacts = await db.getUserContacts(userId).catch(()=>[]);
                                for (const c of contacts || []) {
                                    const cid = String((c && (c.id || c.userId || c)) || '');
                                    if (!cid) continue;
                                    const sockets = getClientSockets(cid);
                                    for (const s of sockets) {
                                        try { if (s && s.readyState === WebSocket.OPEN) s.send(JSON.stringify({ type: 'status_update', data: p })); } catch(e) { console.error('redis status_update contact deliver error', e); }
                                    }
                                }
                            }
                        } catch (e) { console.error('redis status_update contacts error', e); }
                    })();
                } catch (e) { console.error('redis status_update handling error', e); }
            }).catch(e => console.error('redisSub subscribe error', e));

            console.log('Redis pub/sub initialized');
        } catch (e) {
            console.error('Failed to initialize Redis pub/sub', e);
            redisPub = null; redisSub = null;
        }
        // forward new_video channel to connected clients
        try {
            if (redisSub) {
                redisSub.subscribe('appchat:new_video', (msg) => {
                    try {
                        const data = JSON.parse(msg);
                        const payload = JSON.stringify({ type: 'new_video', data: data.video || data });
                        // send to all connected clients
                        for (const [uid, set] of clients.entries()) {
                            for (const s of set) {
                                try { if (s && s.readyState === WebSocket.OPEN) s.send(payload); } catch(e) { }
                            }
                        }
                    } catch (e) { console.error('redisSub new_video handling error', e); }
                }).catch(e => console.error('redisSub subscribe new_video error', e));
            }
        } catch (e) {}
    } else if (PUBSUB_URL) {
        useHttpRelay = true;
        console.log('Using HTTP pubsub relay at', PUBSUB_URL);
        (async function pollRelay() {
            const fetch = globalThis.fetch || (() => { try { return require('node-fetch'); } catch (e) { return null; } })();
            if (!fetch) { console.warn('No fetch available to poll PUBSUB_URL'); return; }
            while (true) {
                try {
                    const msgs = await fetch(`${PUBSUB_URL}/poll/messages`).then(r => r.json()).catch(()=>[]);
                    for (const m of msgs || []) {
                        try {
                            const recipientSockets = getClientSockets(m.toId);
                            for (const recipientWs of recipientSockets) {
                                try { if (recipientWs && recipientWs.readyState === WebSocket.OPEN) recipientWs.send(JSON.stringify({ type: 'chat', data: m })); } catch(e) { console.error('relay deliver error', e); }
                            }
                        } catch (e) { console.error('relay deliver error', e); }
                    }
                    const pres = await fetch(`${PUBSUB_URL}/poll/presence`).then(r => r.json()).catch(()=>[]);
                    for (const p of pres || []) {
                        try {
                            const sockets = getClientSockets(p.userId);
                            for (const ws of sockets) {
                                try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'presence', data: p })); } catch(e) { console.error('relay presence deliver error', e); }
                            }
                        } catch (e) { console.error('relay presence deliver error', e); }
                    }
                } catch (e) {
                    console.error('pollRelay error', e);
                }
                await new Promise(r => setTimeout(r, 300));
            }
        })();
    }

    // Scheduled messages delivery worker
    // Periodically query for due scheduled messages, attempt to claim them atomically,
    // persist as regular messages (when supported), deliver via existing socket/pubsub logic,
    // and mark scheduled row as 'sent'. This enables server-side delivery even when clients are offline.
    (function startScheduledDeliveryWorker() {
        const intervalMs = Number(process.env.SCHEDULED_WORKER_INTERVAL_MS || 15000); // default 15s
        const worker = async () => {
            try {
                if (!db || !db.getDueScheduledMessages || !db.claimScheduledMessage || !db.markScheduledMessageStatus) return;
                const nowIso = new Date().toISOString();
                const due = await db.getDueScheduledMessages(nowIso).catch(() => []);
                if (!due || !due.length) return;
                for (const s of due) {
                    try {
                        // Try to claim; if another instance claimed it, skip
                        const claimed = await db.claimScheduledMessage(s.id).catch(() => false);
                        if (!claimed) continue;

                        const fromId = String(s.fromid || s.fromId || s.from || '');
                        const toId = String(s.toid || s.toId || s.to || '');
                        const content = s.content || s.message || '';
                        const stampEnabled = (s.stampenabled === 1 || s.stampenabled === true || String(s.stampenabled) === '1');

                        // Persist as a normal message when possible (to have history). SaveMessage will return a stored message object.
                        let savedMsg = null;
                        try {
                            if (db && db.saveMessage && !(process.env.NO_PERSIST_MESSAGES === 'true')) {
                                savedMsg = await db.saveMessage(fromId, toId, content);
                                // Ensure sqlite in-memory cache is flushed to disk so other tools/processes
                                // that read the DB file see the new message immediately.
                                try { if (db.flushAll) await db.flushAll(); } catch (e) { /* ignore flush errors */ }
                            }
                        } catch (e) {
                            // fallback to transient payload
                            console.error('Failed to persist scheduled message, will deliver transiently', e);
                        }

                        const transientMsg = savedMsg ? Object.assign({}, savedMsg) : {
                            id: `schedmsg_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
                            fromId: fromId,
                            toId: toId,
                            message: content,
                            timestamp: new Date().toISOString()
                        };

                        // Add scheduled metadata so clients can show the stamp if desired
                        transientMsg.scheduled = true;
                        transientMsg.scheduledMessageId = s.id;
                        transientMsg.stampEnabled = !!stampEnabled;

                        const recipientSockets = getClientSockets(toId);
                        if (recipientSockets.length > 0) {
                            for (const recipientWs of recipientSockets) {
                                try { if (recipientWs && recipientWs.readyState === WebSocket.OPEN) recipientWs.send(JSON.stringify({ type: 'chat', data: transientMsg })); } catch(e) { console.error('scheduled deliver error', e); }
                            }

                            // Notify sender sockets (if any) about delivery status
                            try {
                                const senderSockets = getClientSockets(fromId);
                                for (const senderWs of senderSockets) {
                                    try { if (senderWs && senderWs.readyState === WebSocket.OPEN) { senderWs.send(JSON.stringify({ type: 'messageConfirmation', data: { id: transientMsg.id, status: 'delivered' }, note: 'scheduled: delivered' })); senderWs.send(JSON.stringify({ type: 'messageStatus', data: { id: transientMsg.id, status: 'delivered' } })); } } catch(e) {}
                                }
                            } catch (e) {}

                            // publish to pubsub so other instances can be aware
                            if (redisPub) {
                                try { redisPub.publish('appchat:messages', JSON.stringify(transientMsg)).catch(()=>{}); } catch(e) { console.error('redis publish scheduled error', e); }
                            } else if (useHttpRelay && PUBSUB_URL) {
                                try { const fetch = globalThis.fetch || require('node-fetch'); fetch(`${PUBSUB_URL}/publish/messages`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(transientMsg) }).catch(()=>{}); } catch(e) {}
                            }
                        } else {
                            // recipient offline: queue into in-memory pendingMessages so it will be delivered when they connect
                            const q = pendingMessages.get(toId) || [];
                            q.push(transientMsg);
                            pendingMessages.set(toId, q);

                            // Also publish to pubsub so other instances may deliver
                            if (redisPub) {
                                try { redisPub.publish('appchat:messages', JSON.stringify(transientMsg)).catch(()=>{}); } catch(e) { console.error('redis publish scheduled queue error', e); }
                            } else if (useHttpRelay && PUBSUB_URL) {
                                try { const fetch = globalThis.fetch || require('node-fetch'); fetch(`${PUBSUB_URL}/publish/messages`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(transientMsg) }).catch(()=>{}); } catch(e) {}
                            }
                        }

                        // Mark scheduled row as sent and then delete it to free space/keep table small
                        try {
                            await db.markScheduledMessageStatus(s.id, 'sent');
                        } catch (e) {
                            console.error('Failed to mark scheduled message sent', e);
                        }
                        try {
                            if (db && db.deleteScheduledMessage) {
                                await db.deleteScheduledMessage(s.id).catch(() => {});
                            }
                        } catch (e) {
                            // non-fatal: leaving the row with status 'sent' is acceptable
                            console.error('Failed to delete scheduled message after send', e);
                        }
                    } catch (e) {
                        console.error('Error processing scheduled row', s && s.id, e);
                        try { await db.markScheduledMessageStatus(s.id, 'error'); } catch (er) {}
                    }
                }
            } catch (e) {
                console.error('Scheduled delivery worker error', e);
            }
        };
        // run immediately and then on interval
        setTimeout(() => worker().catch(()=>{}), 1000);
        setInterval(() => worker().catch(()=>{}), intervalMs);
    })();

    wss.on('connection', (ws, req) => {
    // assign a lightweight id to each socket for logging
    ws._instanceId = `${Date.now()}_${Math.random().toString(36).slice(2,6)}`;
    // per-connection message rate limiting
    ws._msgTimestamps = [];
    ws._msgLimit = Number(process.env.WS_MSG_LIMIT_PER_MIN || 120);
    ws._lastPing = Date.now();
        connectionCount++;
        trackConnections();
        console.log('New client connected');

    ws.on('error', (err) => console.error('WebSocket Client Error:', err));

    // track which video comment streams this socket subscribed to
    ws._subscribedVideos = new Set();

        ws.on('message', async (message) => {
            // per-connection bucket refill and enforcement. Use a per-WS token bucket to allow bursts.
            try {
                if (!ws._bucket) {
                    ws._bucket = {
                        tokens: ws._msgLimit || Number(process.env.WS_MSG_LIMIT_PER_MIN || 120),
                        maxTokens: (ws._msgLimit || Number(process.env.WS_MSG_LIMIT_PER_MIN || 120)) * BURST_MULTIPLIER,
                        lastRefill: Date.now(),
                    };
                }
                // refill
                const now = Date.now();
                const elapsed = now - (ws._bucket.lastRefill || now);
                const refillRate = (ws._msgLimit || Number(process.env.WS_MSG_LIMIT_PER_MIN || 120)) / RATE_WINDOW_MS;
                const add = elapsed * refillRate;
                ws._bucket.tokens = Math.min(ws._bucket.maxTokens, (ws._bucket.tokens || ws._bucket.maxTokens) + add);
                ws._bucket.lastRefill = now;
                if ((ws._bucket.tokens || 0) < 1) {
                    // not enough tokens: send a soft error and drop message
                    try { ws.send(JSON.stringify({ type: 'error', error: 'rate limit exceeded' })); } catch(e){}
                    return;
                }
                ws._bucket.tokens -= 1;
            } catch (e) {}
            try {
                const data = JSON.parse(message);
                if (!data.type) throw new Error('Message type is required');

                switch (data.type) {
                    case 'auth':
                        // Allow auth with or without userId (guests allowed)
                        if (!data.userId) {
                            console.log(`WS auth: guest connection (socket=${ws._instanceId})`);
                            ws._sharePresence = (data.sharePresence === undefined) ? true : !!data.sharePresence;
                            break;
                        }
                        // normalize and store as string keys to avoid numeric/string mismatch
                        const normalizedUserId = data.userId ? String(data.userId) : data.userId;
                        // Log mapping event
                        // include remote address when available to correlate client
                        const remoteAddr = (req && req.socket && (req.socket.remoteAddress || req.socket.remotePort)) ? `${req.socket.remoteAddress || 'unknown'}:${req.socket.remotePort || 'unknown'}` : 'unknown';
                        console.log(`WS auth: adding mapping user=${normalizedUserId} -> socket=${ws._instanceId} (remote=${remoteAddr})`);
                        addClient(normalizedUserId, ws);
                        ws.userId = normalizedUserId;
                        // honor client preference whether to share presence publicly
                        ws._sharePresence = (data.sharePresence === undefined) ? true : !!data.sharePresence;
                        await db.updateUserStatus(data.userId, true);

                        // announce presence to other instances only if client allows it
                        if (ws._sharePresence) {
                            if (redisPub) {
                                try { redisPub.publish('appchat:presence', JSON.stringify({ userId: data.userId, status: 'online', ts: new Date().toISOString() })).catch(()=>{}); } catch(e) { console.error('redis presence publish error', e); }
                            } else if (useHttpRelay && PUBSUB_URL) {
                                try { const fetch = globalThis.fetch || require('node-fetch'); fetch(`${PUBSUB_URL}/publish/presence`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ userId: data.userId, status: 'online', ts: new Date().toISOString() }) }).catch(()=>{}); } catch(e) {}
                            }
                        }

                        // confirm auth - server is relay-only and does not deliver stored messages
                        try {
                            ws.send(JSON.stringify({ type: 'authConfirmation', success: true, note: 'relay-only server: no stored messages will be delivered', sharePresence: ws._sharePresence }));
                            console.log(`Sent authConfirmation to user=${normalizedUserId} socket=${ws._instanceId}`);
                        } catch (e) { console.error('Failed to send authConfirmation', e); }

                        // deliver any pending (undelivered) messages that we kept in memory
                        try {
                            const queue = pendingMessages.get(String(data.userId)) || [];
                            if (queue.length) {
                                console.log(`Delivering ${queue.length} pending message(s) to user=${String(data.userId)} on socket=${ws._instanceId}`);
                                // deliver each and then remove from pending storage
                                for (const m of queue) {
                                    try {
                                        if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'chat', data: m }));
                                        // notify original sender (if connected) that we've delivered to recipient (all their sockets)
                                        const senderSockets = getClientSockets(m.fromId);
                                        for (const senderWs of senderSockets) {
                                            try { if (senderWs && senderWs.readyState === WebSocket.OPEN) { senderWs.send(JSON.stringify({ type: 'messageConfirmation', data: { id: m.id, status: 'delivered', tempId: m.tempId || null }, note: 'transient: delivered to recipient' })); senderWs.send(JSON.stringify({ type: 'messageStatus', data: { id: m.id, status: 'delivered' } })); } } catch(e){}
                                        }
                                    } catch (e) { console.error('pending deliver error', e); }
                                }
                            }
                            pendingMessages.delete(String(data.userId));
                            console.log(`Cleared pending messages for user=${String(data.userId)}; remaining=${pendingMessages.get(String(data.userId)) ? pendingMessages.get(String(data.userId)).length : 0}`);
                        } catch (e) { console.error('failed to deliver pending messages', e); }
                        break;

                    case 'subscribeVideoComments':
                        try {
                            const videoId = data.videoId || data.id || null;
                            if (!videoId) break;
                            addVideoSubscriber(videoId, ws);
                            ws._subscribedVideos.add(String(videoId));
                            try { ws.send(JSON.stringify({ type: 'subscribeVideoComments:ack', videoId, success: true })); } catch (e) {}
                        } catch (e) { console.error('subscribeVideoComments handling error', e); }
                        break;

                    case 'unsubscribeVideoComments':
                        try {
                            const videoId = data.videoId || data.id || null;
                            if (!videoId) break;
                            removeVideoSubscriber(videoId, ws);
                            ws._subscribedVideos.delete(String(videoId));
                            try { ws.send(JSON.stringify({ type: 'unsubscribeVideoComments:ack', videoId, success: true })); } catch (e) {}
                        } catch (e) { console.error('unsubscribeVideoComments handling error', e); }
                        break;

                    case 'chat':
                        // Expect opaque payload. If ENFORCE_E2E is true, reject plaintext messages.
                        try {
                            // normalize ids to strings to prevent numeric/string mismatches
                            const fromId = data.fromId ? String(data.fromId) : data.fromId;
                            const toId = data.toId ? String(data.toId) : data.toId;
                            // DEBUG: log received chat payload for routing diagnosis
                            try { console.log('SERVER chat received:', JSON.stringify({ fromId: fromId, toId: toId, tempId: data.tempId || null, encrypted: data.encrypted || false })); } catch(e) {}
                            const payload = data.message || data.payload || data.body || null;
                            const encryptedFlag = !!data.encrypted;
                            if (!fromId || !toId || !payload) throw new Error('fromId, toId, and message payload are required for chat messages');
                            if (ENFORCE_E2E && !encryptedFlag) {
                                try { ws.send(JSON.stringify({ type: 'error', error: 'ENFORCE_E2E: plaintext messages are rejected' })); } catch(e){}
                                return;
                            }

                            // Keep 'message' as the canonical field name for client compatibility
                            // Validate and normalize timestamp
                                const normalizedTimestamp = (() => {
                                    try {
                                        // Check if client provided a timestamp
                                        if (data.timestamp) {
                                            const clientDate = new Date(data.timestamp);
                                            if (!isNaN(clientDate.getTime())) {
                                                // Use client timestamp if within reasonable bounds (±5 min from now)
                                                const now = Date.now();
                                                const diff = Math.abs(now - clientDate.getTime());
                                                if (diff <= 5 * 60 * 1000) {
                                                    return clientDate.toISOString();
                                                }
                                            }
                                        }
                                    } catch (e) {}
                                    // Fallback to server time
                                    return new Date().toISOString();
                                })();

                                const transientMsg = {
                                    id: `tmp_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
                                    fromId,
                                    toId,
                                    message: payload,
                                    timestamp: normalizedTimestamp,
                                    sequence: Date.now(), // Add sequence number for strict ordering
                                    encrypted: encryptedFlag,
                                tempId: data.tempId || null,
                                // Accept multiple possible field names from clients
                                replyToId: data.replyToId || data.reply_to_id || null,
                                replyToSender: data.replyToSender || data.reply_to_sender || null,
                                // reply text may be sent as replyToText or replyToMessage (compat)
                                replyToMessage: data.replyToText || data.replyToMessage || data.reply_to_text || data.reply_text || null,
                            };
                            try { recordMessage(); } catch(e) {}

                            // If message is encrypted but lacks key metadata, DO NOT replace the encrypted payload.
                            // Preserve the original payload so it can be delivered to offline devices and stored
                            // for later decryption. Instead, mark the transient message with a keyMissing flag
                            // and attach a human-visible notice if clients prefer to show it.
                            try {
                                let parsed = null;
                                if (typeof payload === 'string') {
                                    try { parsed = JSON.parse(payload); } catch (e) { parsed = null; }
                                } else if (typeof payload === 'object') {
                                    parsed = payload;
                                }

                                if (encryptedFlag && !_hasKeyMetadata(parsed)) {
                                    transientMsg.keyMissing = true;
                                    transientMsg.keyMissingNotice = PLACEHOLDER_KEY_LOST;
                                    console.warn(`Message ${transientMsg.id} from ${fromId} appears encrypted but missing key metadata. Preserving payload and marking as keyMissing.`);
                                    // Notify sender devices that the recipient may be missing keys so the sender
                                    // can take action (e.g. re-share key metadata or prompt user). This is a
                                    // lightweight hint; clients should handle key exchange out-of-band.
                                    try {
                                        const senderSockets = getClientSockets(fromId);
                                        const notice = JSON.stringify({ type: 'keyRequest', data: { id: transientMsg.id, toId, note: 'recipient missing key metadata, please provide key or re-share' } });
                                        for (const s of senderSockets) {
                                            try { if (s && s.readyState === WebSocket.OPEN) s.send(notice); } catch(e) {}
                                        }
                                    } catch (e) { console.error('notify sender keyRequest failed', e); }
                                }
                            } catch (e) { console.error('key metadata check failed', e); }

                            // Persist the message when DB backend is available and persistence is enabled (do not block delivery)
                            try {
                                if (db && db.saveMessage && !(NO_PERSIST_MESSAGES === true || String(NO_PERSIST_MESSAGES) === 'true')) {
                                    // Prefer the possibly-modified transient message when saving (this will contain placeholder if key missing)
                                    const msgForSave = (typeof transientMsg.message === 'string') ? transientMsg.message : (typeof payload === 'string' ? payload : JSON.stringify(payload));
                                    const replyData = { replyToId: transientMsg.replyToId, replyToSender: transientMsg.replyToSender, replyToMessage: transientMsg.replyToMessage };
                                    // Save in background so we don't delay delivery to recipients
                                    db.saveMessage(fromId, toId, msgForSave, replyData)
                                        .then(saved => {
                                            if (!saved) return;
                                            try {
                                                // Notify sender devices that the transient message has an authoritative persisted id/timestamp
                                                const senderSockets = getClientSockets(fromId);
                                                const persistedNote = JSON.stringify({ type: 'messageSaved', data: { tempId: transientMsg.tempId || null, id: saved.id, timestamp: saved.timestamp } });
                                                for (const s of senderSockets) {
                                                    try { if (s && s.readyState === WebSocket.OPEN) s.send(persistedNote); } catch(e) { }
                                                }
                                            } catch (e) { console.error('background persist notify failed', e); }
                                        })
                                        .catch(e => { console.error('db.saveMessage failed', e); });
                                }
                            } catch (e) { console.error('persist attempt error', e); }

                            // Immediately acknowledge to the sender that the server accepted the message (single tick)
                            try {
                                ws.send(JSON.stringify({ type: 'messageConfirmation', data: { id: transientMsg.id, status: 'sent', tempId: transientMsg.tempId }, note: 'server: accepted' }));
                            } catch (e) { console.error('failed to ack sent status to sender', e); }

                            const recipientSockets = getClientSockets(toId);
                            console.log(`Attempting delivery to recipient=${String(toId)}; socketsCount=${recipientSockets.length}; socketIds=${recipientSockets.map(s=>s._instanceId).join(',')}`);
                            if (recipientSockets.length > 0) {
                                // Get pending messages and append the new one (keep bounded size)
                                let messageQueue = pendingMessages.get(toId) || [];
                                messageQueue.push(transientMsg);
                                // Trim queue to cap if necessary (drop oldest)
                                if (messageQueue.length > MAX_PENDING_PER_USER) {
                                    const dropCount = messageQueue.length - MAX_PENDING_PER_USER;
                                    messageQueue.splice(0, dropCount);
                                }
                                pendingMessages.set(toId, messageQueue);

                                // Prepare serialized payload once to reduce CPU from repeated JSON.stringify
                                const chatPayload = { type: 'chat', data: transientMsg };
                                const chatPayloadStr = JSON.stringify(chatPayload);

                                // Deliver to all active sockets for that user using pre-serialized payload
                                for (const recipientWs of recipientSockets) {
                                    try {
                                        if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
                                            recipientWs.send(chatPayloadStr);
                                        }
                                    } catch (e) { console.error('deliver error', e); }
                                }
                                console.log(`Delivered transient ${transientMsg.id} to ${String(toId)} on ${recipientSockets.length} socket(s)`);

                                // notify sender that message delivered (transient) and include tempId so clients can correlate
                                try {
                                    const confirmStr = JSON.stringify({ type: 'messageConfirmation', data: { id: transientMsg.id, status: 'delivered', tempId: transientMsg.tempId }, note: 'transient: delivered to recipient' });
                                    const statusStr = JSON.stringify({ type: 'messageStatus', data: { id: transientMsg.id, status: 'delivered' } });
                                    ws.send(confirmStr);
                                    ws.send(statusStr);
                                } catch(e){}

                                // publish to pubsub for other instances (opaque payload)
                                if (redisPub) {
                                    try { redisPub.publish('appchat:messages', JSON.stringify(transientMsg)).catch(()=>{}); } catch(e) { console.error('redis publish error', e); }
                                } else if (useHttpRelay && PUBSUB_URL) {
                                    try { const fetch = globalThis.fetch || require('node-fetch'); fetch(`${PUBSUB_URL}/publish/messages`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(transientMsg) }).catch(()=>{}); } catch(e) {}
                                }
                            } else {
                                // recipient not connected right now
                                console.warn(`Recipient ${String(toId)} not connected; queueing message ${transientMsg.id}; mappedSockets=none`);
                                // recipient offline: keep in-memory queue until delivery. Do NOT persist to DB when NO_PERSIST_MESSAGES=true
                                const q = pendingMessages.get(String(toId)) || [];
                                q.push(transientMsg);
                                if (q.length > MAX_PENDING_PER_USER) q.splice(0, q.length - MAX_PENDING_PER_USER);
                                pendingMessages.set(String(toId), q);
                                console.log(`Queued message ${transientMsg.id} for ${String(toId)}; queueLength=${q.length}`);

                                // note: we already told sender 'sent' above. Keep server-side queued copy to deliver later.

                                // also publish to pubsub so other instances may try to deliver (opaque)
                                if (redisPub) {
                                    try { redisPub.publish('appchat:messages', JSON.stringify(transientMsg)).catch(()=>{}); } catch(e) { console.error('redis publish error', e); }
                                } else if (useHttpRelay && PUBSUB_URL) {
                                    try { const fetch = globalThis.fetch || require('node-fetch'); fetch(`${PUBSUB_URL}/publish/messages`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(transientMsg) }).catch(()=>{}); } catch(e) {}
                                }

                                // Send push notification to user devices (if available)
                                (async () => {
                                    try {
                                        // Prepare title/body for notification
                                        let sender = 'New message';
                                        try { const su = await db.getUserById(fromId).catch(()=>null); if (su && su.username) sender = su.username; } catch(e) {}
                                        let bodyText = transientMsg.keyMissing ? PLACEHOLDER_KEY_LOST : '';
                                        if (!bodyText) {
                                            try {
                                                if (typeof transientMsg.message === 'string') {
                                                    try { const p = JSON.parse(transientMsg.message); bodyText = p.text || p.plain || p.message || JSON.stringify(p).slice(0,200); } catch(e) { bodyText = transientMsg.message.slice(0,200); }
                                                } else {
                                                    bodyText = String(transientMsg.message).slice(0,200);
                                                }
                                            } catch (e) { bodyText = PLACEHOLDER_KEY_LOST; }
                                        }
                                        const chatId = [String(fromId), String(toId)].sort().join('_');
                                        await sendPushToUser(toId, sender, bodyText, { type: 'chat', fromId: String(fromId), messageId: transientMsg.id, chatId });
                                    } catch (e) { console.error('push notify error', e); }
                                })();
                            }
                        } catch (e) {
                            console.error('chat handling error', e);
                        }
                        break;

                    case 'key:available':
                        try {
                            // A client notifies the server that it has received/provisioned keys.
                            // Try to deliver any pending messages we have queued for this user.
                            const targetUser = data.userId ? String(data.userId) : (ws.userId ? String(ws.userId) : null);
                            if (!targetUser) break;
                            const queue = pendingMessages.get(targetUser) || [];
                            if (!queue.length) {
                                // nothing queued
                                try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'key:available:ack', data: { userId: targetUser, delivered: 0 } })); } catch(e) {}
                                break;
                            }
                            console.log(`key:available received for ${targetUser}, delivering ${queue.length} queued message(s)`);
                            for (const m of queue) {
                                try {
                                    const sockets = getClientSockets(targetUser);
                                    for (const s of sockets) {
                                        try { if (s && s.readyState === WebSocket.OPEN) s.send(JSON.stringify({ type: 'chat', data: m })); } catch(e) { console.error('deliver on keyAvailable error', e); }
                                    }

                                    // notify original senders about delivery
                                    try {
                                        const senders = getClientSockets(m.fromId);
                                        const confirmStr = JSON.stringify({ type: 'messageConfirmation', data: { id: m.id, status: 'delivered', tempId: m.tempId || null }, note: 'delivered after keys became available' });
                                        const statusStr = JSON.stringify({ type: 'messageStatus', data: { id: m.id, status: 'delivered' } });
                                        for (const s of senders) {
                                            try { if (s && s.readyState === WebSocket.OPEN) { s.send(confirmStr); s.send(statusStr); } } catch(e) {}
                                        }
                                    } catch (e) { console.error('notify senders after keyAvailable failed', e); }
                                } catch (e) { console.error('failed to deliver queued msg on keyAvailable', e); }
                            }
                            pendingMessages.delete(targetUser);
                            try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'key:available:ack', data: { userId: targetUser, delivered: queue.length } })); } catch(e) {}
                        } catch (e) { console.error('key:available handling error', e); }
                        break;

                    case 'group_chat':
                        try {
                            const fromId = data.fromId ? String(data.fromId) : data.fromId;
                            const groupId = data.groupId ? String(data.groupId) : data.groupId;
                            const payload = data.message || data.payload || null;
                            const tempId = data.tempId || null;
                            
                            if (!fromId || !groupId || !payload) {
                                try { ws.send(JSON.stringify({ type: 'error', error: 'group_chat requires fromId, groupId and message' })); } catch(e){}
                                break;
                            }

                            console.log(`Processing group message: fromId=${fromId}, groupId=${groupId}`);

                            // Fetch members first to validate sender is member
                            let members = [];
                            try {
                                if (db && db.getGroupMembers) {
                                    members = await db.getGroupMembers(groupId);
                                    if (!members.some(m => String(m.id || m.userid || m) === fromId)) {
                                        throw new Error('Sender is not a member of this group');
                                    }
                                }
                            } catch (e) { 
                                console.error('Group message member check failed:', e);
                                try { ws.send(JSON.stringify({ type: 'error', error: String(e) })); } catch(e){} 
                                break;
                            }

                            // Generate message ID and construct message object
                            const messageId = `gmsg_${Date.now()}_${Math.random().toString(36).slice(2,6)}`;
                            const gmsg = {
                                id: messageId,
                                groupId,
                                fromId,
                                message: payload,
                                timestamp: new Date().toISOString(),
                                tempId: tempId
                            };

                            // Persist the group message (stringify objects so DB stores payload correctly)
                            try {
                                if (db && db.saveGroupMessage) {
                                    // If encrypted payload and missing key metadata, do NOT replace the payload.
                                    // Preserve the original encrypted payload and mark the group message as keyMissing
                                    // so clients can decide how to display it (placeholder or raw encrypted blob).
                                    try {
                                        let parsed = null;
                                        if (typeof payload === 'string') {
                                            try { parsed = JSON.parse(payload); } catch (e) { parsed = null; }
                                        } else if (typeof payload === 'object') {
                                            parsed = payload;
                                        }
                                        if (data.encrypted && !_hasKeyMetadata(parsed)) {
                                            // mark the group message without destroying the original payload
                                            gmsg.keyMissing = true;
                                            gmsg.keyMissingNotice = PLACEHOLDER_KEY_LOST;
                                            console.warn(`Group message ${gmsg.id} from ${fromId} missing key metadata; preserving payload and marking as keyMissing.`);
                                            // Notify the sender's devices to take action (re-share keys or prompt users)
                                            try {
                                                const senderSockets = getClientSockets(fromId);
                                                const notice = JSON.stringify({ type: 'keyRequest', data: { id: gmsg.id, groupId, note: 'one or more members missing key metadata; please provide keys or re-share' } });
                                                for (const s of senderSockets) {
                                                    try { if (s && s.readyState === WebSocket.OPEN) s.send(notice); } catch(e) {}
                                                }
                                            } catch (e) { console.error('notify sender keyRequest failed', e); }
                                        }
                                    } catch (e) { console.error('group key metadata check failed', e); }

                                    const msgForSave = (typeof payload === 'string') ? payload : JSON.stringify(payload);
                                    await db.saveGroupMessage(groupId, fromId, msgForSave);
                                }
                            } catch (e) { console.error('Failed to persist group message', e); }

                            console.log(`Broadcasting to ${members.length} group members`);
                            

                            // Send confirmation to sender first
                            try {
                                ws.send(JSON.stringify({ 
                                    type: 'messageConfirmation', 
                                    data: { 
                                        id: gmsg.id, 
                                        status: 'sent', 
                                        tempId: tempId,
                                        groupId: groupId 
                                    }
                                }));
                            } catch(e) { console.error('Failed to send confirmation to sender:', e); }

                            // Broadcast to all members including sender (for multi-device)
                            for (const m of members) {
                                const memberId = String(m.id || m.userid || m);
                                console.log(`Delivering to member: ${memberId}`);
                                
                                // Get all sockets for this member (could be multiple devices)
                                const sockets = getClientSockets(memberId);
                                
                                if (sockets.length > 0) {
                                    console.log(`Found ${sockets.length} active sockets for member ${memberId}`);
                                    // Deliver to all member's connected devices
                                    for (const s of sockets) {
                                        try { 
                                            if (s && s.readyState === WebSocket.OPEN) {
                                                console.log(`Sending to socket for member ${memberId}`);
                                                s.send(JSON.stringify({ 
                                                    type: 'chat', 
                                                    data: {
                                                        id: gmsg.id,
                                                        fromId: fromId,
                                                        toId: `group_${groupId}`,
                                                        message: payload,
                                                        timestamp: gmsg.timestamp,
                                                        groupId: groupId
                                                    }
                                                }));
                                            }
                                        } catch(e) { 
                                            console.error(`Failed to deliver to member ${memberId}:`, e); 
                                        }
                                    }
                                } else {
                                    console.log(`Member ${memberId} is offline, queueing message`);
                                    // Queue for offline member
                                    const q = pendingMessages.get(memberId) || [];
                                    q.push({
                                        id: gmsg.id,
                                        fromId: fromId,
                                        toId: `group_${groupId}`,
                                        message: payload,
                                        timestamp: gmsg.timestamp,
                                        groupId: groupId
                                    });
                                    pendingMessages.set(memberId, q);
                                    // Send push notification for offline group member (background delivery)
                                    (async () => {
                                        try {
                                            let sender = `Group ${groupId}`;
                                            try { const su = await db.getUserById(fromId).catch(()=>null); if (su && su.username) sender = su.username; } catch(e) {}
                                            let bodyText = '';
                                            if (typeof payload === 'string') {
                                                try { const p = JSON.parse(payload); bodyText = p.text || p.plain || p.message || JSON.stringify(p).slice(0,200); } catch(e) { bodyText = payload.slice(0,200); }
                                            } else {
                                                bodyText = String(payload).slice(0,200);
                                            }
                                            await sendPushToUser(memberId, sender, bodyText, { type: 'group_chat', fromId: String(fromId), groupId, messageId: gmsg.id });
                                        } catch (e) { console.error('group push error', e); }
                                    })();
                                }
                            }

                            // Acknowledge sender
                            try { ws.send(JSON.stringify({ type: 'messageConfirmation', data: { id: gmsg.id, status: 'sent', groupId }, note: 'group message accepted' })); } catch(e){}

                            // Publish to pubsub for other instances
                            if (redisPub) {
                                try { redisPub.publish('appchat:messages', JSON.stringify(Object.assign({}, gmsg, { toId: `group_${groupId}` }))).catch(()=>{}); } catch(e) { console.error('redis publish group message error', e); }
                            } else if (useHttpRelay && PUBSUB_URL) {
                                try { const fetch = globalThis.fetch || require('node-fetch'); fetch(`${PUBSUB_URL}/publish/messages`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(Object.assign({}, gmsg, { toId: `group_${groupId}` })) }).catch(()=>{}); } catch(e) {}
                            }
                        } catch (e) { console.error('group_chat handling error', e); }
                        break;

                    case 'deleteMessage':
                        try {
                            // data: { messageId, forAll, fromId, toId }
                            const { messageId, forAll } = data;
                            const from = data.fromId || data.from || null;
                            const to = data.toId || data.to || null;
                            if (!messageId) break;

                            // If forAll, verify message belongs to sender before allowing retraction
                            if (forAll) {
                                try {
                                    // Get message to verify ownership
                                    const message = await db.getMessage(messageId);
                                    if (!message || message.fromId !== from) {
                                        // Not the sender - reject the delete request
                                        try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ 
                                            type: 'error', 
                                            error: 'Only the sender can delete a message for everyone',
                                            messageId 
                                        })); } catch (e) {}
                                        break;
                                    }

                                    // Sender verified - proceed with retraction
                                    if (db && db.markMessageRetracted) {
                                        await db.markMessageRetracted(messageId);
                                    }
                                } catch (e) {
                                    console.error('DB markMessageRetracted failed', e);
                                    // Send error to client
                                    try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ 
                                        type: 'error', 
                                        error: 'Failed to delete message',
                                        messageId 
                                    })); } catch (e) {}
                                    break;
                                }

                                const retractedPayload = { id: messageId, by: from, forAll: true, replacementText: 'Pesan ini telah ditarik' };

                                // notify recipient if online (all active sockets)
                                try {
                                    const recipientSockets = getClientSockets(String(to));
                                    for (const recipientWs of recipientSockets) {
                                        try { if (recipientWs && recipientWs.readyState === WebSocket.OPEN) recipientWs.send(JSON.stringify({ type: 'messageRetracted', data: retractedPayload })); } catch(e) { console.error('notify recipient retracted error', e); }
                                    }
                                } catch (e) { console.error('notify recipient retracted error', e); }

                                // notify sender (ack)
                                try {
                                    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'messageRetracted', data: retractedPayload }));
                                } catch (e) {}
                            } else {
                                // delete for self: just acknowledge to requester; server needn't remove DB row
                                try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'messageDeleted', data: { id: messageId, by: from, forAll: false } })); } catch (e) {}
                            }
                        } catch (e) { console.error('deleteMessage handling error', e); }
                        break;

                    case 'received':
                        // Recipient confirms they've received/decrypted the message. Remove any pending copy and notify sender.
                        try {
                            const { messageId, originalFromId, originalToId } = data;
                            if (!messageId) break;
                            // remove from pending queue if present
                            try {
                                const targetKey = String(originalToId || data.toId || '');
                                const q = pendingMessages.get(targetKey) || [];
                                const filtered = q.filter(m => m.id !== messageId);
                                if (filtered.length) {
                                    pendingMessages.set(targetKey, filtered);
                                } else {
                                    pendingMessages.delete(targetKey);
                                }
                            } catch (e) {}

                            // notify original sender if connected. Include tempId when available so clients can replace optimistic messages.
                            const senderKey = String(originalFromId || data.fromId || '');
                            const senderSockets = getClientSockets(senderKey);
                            for (const senderWs of senderSockets) {
                                try { if (senderWs && senderWs.readyState === WebSocket.OPEN) { senderWs.send(JSON.stringify({ type: 'messageConfirmation', data: { id: messageId, status: 'delivered', tempId: data.tempId || null }, note: 'recipient confirmed receipt' })); senderWs.send(JSON.stringify({ type: 'messageStatus', data: { id: messageId, status: 'received' } })); } } catch(e){}
                            }
                        } catch (e) { console.error('received handling error', e); }
                        break;

                    case 'getMessages':
                        try { const opts = {}; if (data.limit) opts.limit = Number(data.limit); if (data.before) opts.before = data.before; const messages = await db.getMessages(data.userId1, data.userId2, opts); ws.send(JSON.stringify({ type: 'messageHistory', data: messages })); } catch (e) { console.error('getMessages error', e); ws.send(JSON.stringify({ type: 'messageHistory', data: [] })); }
                        break;

                    case 'read':
                        try { const { messageId } = data; if (!messageId) throw new Error('messageId required'); const updated = await db.updateMessageStatus(messageId, 'read'); if (updated) { const originalSenderSockets = getClientSockets(String(updated.fromId)); for (const originalSenderWs of originalSenderSockets) { try { if (originalSenderWs && originalSenderWs.readyState === WebSocket.OPEN) originalSenderWs.send(JSON.stringify({ type: 'messageStatus', data: { id: updated.id, status: updated.status } })); } catch(e){} } } } catch (e) { console.error('Failed to handle read receipt:', e); }
                        break;

                    case 'typing':
                        const recipientTypingSockets = getClientSockets(String(data.toId));
                        for (const recipientTypingWs of recipientTypingSockets) {
                            try { if (recipientTypingWs && recipientTypingWs.readyState === WebSocket.OPEN) recipientTypingWs.send(JSON.stringify({ type: 'typing', data: { fromId: data.fromId, isTyping: data.isTyping } })); } catch(e) {}
                        }
                        break;

                    case 'profile_update':
                        try {
                            // expect data: { userId, fields: { profileImageUrl, displayName, ... } }
                            const userId = data.userId ? String(data.userId) : (data.id ? String(data.id) : null);
                            if (!userId) break;
                            const payload = data.fields || data.payload || data.data || {};

                            // notify all sockets for the updated user (in case user has multiple devices)
                            const userSockets = getClientSockets(userId);
                            for (const recipientWs of userSockets) {
                                try { if (recipientWs && recipientWs.readyState === WebSocket.OPEN) recipientWs.send(JSON.stringify({ type: 'profile_update', data: { userId, fields: payload } })); } catch(e) { console.error('deliver profile_update to user error', e); }
                            }

                            // notify contacts of this user about the profile change (if DB provides contact list)
                            (async () => {
                                try {
                                    if (db && db.getUserContacts) {
                                        const contacts = await db.getUserContacts(userId).catch(()=>[]);
                                        for (const c of contacts || []) {
                                            const cid = String((c && (c.id || c.userId || c)) || '');
                                            if (!cid) continue;
                                            const sockets = getClientSockets(cid);
                                            for (const s of sockets) {
                                                try { if (s && s.readyState === WebSocket.OPEN) s.send(JSON.stringify({ type: 'profile_update', data: { userId, fields: payload } })); } catch(e) { console.error('deliver profile_update to contact error', e); }
                                            }
                                        }
                                    }
                                } catch (e) { console.error('profile_update contacts notify error', e); }
                            })();

                            // publish to pubsub so other instances can relay
                            if (redisPub) {
                                try { redisPub.publish('appchat:profile_update', JSON.stringify({ userId, fields: payload })).catch(()=>{}); } catch(e) { console.error('redis publish profile_update error', e); }
                            } else if (useHttpRelay && PUBSUB_URL) {
                                try { const fetch = globalThis.fetch || require('node-fetch'); fetch(`${PUBSUB_URL}/publish/profile_update`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ userId, fields: payload }) }).catch(()=>{}); } catch(e) {}
                            }
                        } catch (e) { console.error('profile_update handling error', e); }
                        break;

                    case 'status_update':
                        try {
                            // expect data: { userId, status: '... (short text)', visibility?: 'public'|'contacts'|'private' }
                            const userId = data.userId ? String(data.userId) : (data.id ? String(data.id) : null);
                            if (!userId) break;
                            const payload = { userId, status: data.status || (data.payload && data.payload.status) || '', visibility: data.visibility || 'contacts', ts: new Date().toISOString() };

                            // notify all sockets for the updated user
                            const userSockets = getClientSockets(userId);
                            for (const recipientWs of userSockets) {
                                try { if (recipientWs && recipientWs.readyState === WebSocket.OPEN) recipientWs.send(JSON.stringify({ type: 'status_update', data: payload })); } catch(e) { console.error('deliver status_update to user error', e); }
                            }

                            // notify contacts of this user about the status change (if DB provides contact list)
                            (async () => {
                                try {
                                    if (db && db.getUserContacts) {
                                        const contacts = await db.getUserContacts(userId).catch(()=>[]);
                                        for (const c of contacts || []) {
                                            const cid = String((c && (c.id || c.userId || c)) || '');
                                            if (!cid) continue;
                                            const sockets = getClientSockets(cid);
                                            for (const s of sockets) {
                                                try { if (s && s.readyState === WebSocket.OPEN) s.send(JSON.stringify({ type: 'status_update', data: payload })); } catch(e) { console.error('deliver status_update to contact error', e); }
                                            }
                                        }
                                    }
                                } catch (e) { console.error('status_update contacts notify error', e); }
                            })();

                            // publish to pubsub so other instances can relay
                            if (redisPub) {
                                try { redisPub.publish('appchat:status_update', JSON.stringify(payload)).catch(()=>{}); } catch(e) { console.error('redis publish status_update error', e); }
                            } else if (useHttpRelay && PUBSUB_URL) {
                                try { const fetch = globalThis.fetch || require('node-fetch'); fetch(`${PUBSUB_URL}/publish/status_update`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload) }).catch(()=>{}); } catch(e) {}
                            }
                        } catch (e) { console.error('status_update handling error', e); }
                        break;
                        break;

                        // WebRTC call signaling forwarder: relay call offers/answers/ice/end between users
                        case 'call:init':
                        case 'call:offer':
                        case 'call:answer':
                        case 'call:ice':
                        case 'call:end':
                            try {
                                const fromId = data.fromId || data.from || ws.userId || null;
                                const toId = data.toId || data.to || null;
                                if (!fromId || !toId) break;
                                const callId = data.callId || data.call_id || null;
                                const payload = { type: data.type, data: Object.assign({}, data, { fromId: fromId, callId: callId }) };
                                const recipientSockets = getClientSockets(String(toId));
                                const callerSockets = getClientSockets(String(fromId));
                                console.log(`Call signaling: forwarding ${data.type} from ${String(fromId)} to ${String(toId)} on ${recipientSockets.length} socket(s)`);

                                // If this is an initial offer/init, proactively inform caller that we're attempting to reach the recipient
                                if (data.type === 'call:offer' || data.type === 'call:init') {
                                    // notify caller(s) state = 'memanggil' (calling)
                                    for (const cs of callerSockets) {
                                        try { if (cs && cs.readyState === WebSocket.OPEN) cs.send(JSON.stringify({ type: 'call:state', data: { callId, toId, state: 'memanggil' } })); } catch(e) { }
                                    }
                                }

                                if (recipientSockets.length > 0) {
                                    // send an incoming-call wrapper to recipients so clients can show a consistent incoming UI
                                    const incomingMeta = { fromId: fromId, callId, fromName: data.fromName || data.fromDisplayName || null, fromAvatar: data.fromAvatar || null };
                                    for (const recipientWs of recipientSockets) {
                                        try {
                                            if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
                                                // send the actual signaling payload (offer/ice/answer/end)
                                                recipientWs.send(JSON.stringify(payload));
                                                // and a lightweight incomingCall notice for UI (only for offers/init)
                                                if (data.type === 'call:offer' || data.type === 'call:init') {
                                                    recipientWs.send(JSON.stringify({ type: 'incomingCall', data: incomingMeta }));
                                                }
                                            }
                                        } catch(e) { console.error('call signaling deliver error', e); }
                                    }

                                    // Inform caller(s) that the recipient is reachable and (presumptively) ringing
                                    for (const cs of callerSockets) {
                                        try { if (cs && cs.readyState === WebSocket.OPEN) cs.send(JSON.stringify({ type: 'call:state', data: { callId, toId, state: 'berdering' } })); } catch(e) { }
                                    }
                                } else {
                                    // recipient offline: notify caller if possible
                                    try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'call:unavailable', data: { toId, callId } })); } catch(e) {}
                                }

                                // If this is an answer -> indicate connected state to both parties
                                if (data.type === 'call:answer') {
                                    for (const cs of callerSockets) {
                                        try { if (cs && cs.readyState === WebSocket.OPEN) cs.send(JSON.stringify({ type: 'call:state', data: { callId, toId, state: 'connected' } })); } catch(e) { }
                                    }
                                    for (const rs of recipientSockets) {
                                        try { if (rs && rs.readyState === WebSocket.OPEN) rs.send(JSON.stringify({ type: 'call:state', data: { callId, fromId, state: 'connected' } })); } catch(e) { }
                                    }
                                }

                                // If this is call:end, inform both sides the call ended
                                if (data.type === 'call:end') {
                                    for (const cs of callerSockets) {
                                        try { if (cs && cs.readyState === WebSocket.OPEN) cs.send(JSON.stringify({ type: 'call:state', data: { callId, toId, state: 'ended' } })); } catch(e) { }
                                    }
                                    for (const rs of recipientSockets) {
                                        try { if (rs && rs.readyState === WebSocket.OPEN) rs.send(JSON.stringify({ type: 'call:state', data: { callId, fromId, state: 'ended' } })); } catch(e) { }
                                    }
                                }
                            } catch (e) { console.error('call handling error', e); }
                            break;

                    default:
                        break;
                }
            } catch (error) {
                console.error('Error processing message:', error);
                recentErrors.push(String(error));
            }
        });

        ws.on('close', async () => {
            connectionCount--;
            if (ws.userId) {
                try {
                    // Remove this socket from the user's socket set. If no sockets
                    // remain for the user, mark them offline and broadcast presence.
                    const userIdKey = String(ws.userId);
                    removeClient(userIdKey, ws);
                    const remaining = getClientSockets(userIdKey).length;
                    if (remaining === 0) {
                        try { await db.updateUserStatus(ws.userId, false); } catch (e) {}
                        console.log(`User ${ws.userId} disconnected (socket ${ws._instanceId}) - no remaining sockets`);
                        // announce offline presence
                        if (redisPub) {
                            try { redisPub.publish('appchat:presence', JSON.stringify({ userId: ws.userId, status: 'offline', ts: new Date().toISOString() })).catch(()=>{}); } catch(e) { console.error('redis presence publish error', e); }
                        } else if (useHttpRelay && PUBSUB_URL) {
                            try { const fetch = globalThis.fetch || require('node-fetch'); fetch(`${PUBSUB_URL}/publish/presence`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ userId: ws.userId, status: 'offline', ts: new Date().toISOString() }) }).catch(()=>{}); } catch(e) {}
                        }
                    } else {
                        console.log(`Socket ${ws._instanceId} for user ${ws.userId} closed (stale), ${remaining} socket(s) remain active`);
                    }
                } catch(e) {
                    console.error('Error during close handling for', ws.userId, e);
                }
            }

            // Remove socket from any video subscriber lists it may be part of
            try {
                if (ws._subscribedVideos && ws._subscribedVideos.size) {
                    for (const vid of Array.from(ws._subscribedVideos)) {
                        try { removeVideoSubscriber(vid, ws); } catch (e) {}
                    }
                    ws._subscribedVideos.clear();
                }
            } catch (e) {}

            console.log('Client disconnected');
            trackConnections();
        });
    });

    // Thumbnail upload endpoint: POST /thumbnails accepts multipart 'thumbnail'
    const thumbStorage = multer.diskStorage({
        destination: function (req, file, cb) { cb(null, THUMB_DIR); },
        filename: function (req, file, cb) { cb(null, `${Date.now()}_${file.originalname}`); }
    });
    const uploadThumbnail = multer({ storage: thumbStorage, limits: { fileSize: 5 * 1024 * 1024 } }); // 5MB
    app.post('/thumbnails', uploadThumbnail.single('thumbnail'), async (req, res) => {
        try {
            const file = req.file;
            if (!file) return res.status(400).json({ error: 'thumbnail file is required' });
            const url = `/uploads/thumbnails/${encodeURIComponent(file.filename)}`;
            return res.json({ success: true, url });
        } catch (e) {
            console.error('thumbnail upload failed', e);
            return res.status(500).json({ error: e.message || 'thumbnail upload failed' });
        }
    });
})();
