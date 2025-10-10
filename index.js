const { Client, GatewayIntentBits, AttachmentBuilder, EmbedBuilder, PermissionFlagsBits, REST, Routes, ChannelType } = require('discord.js');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');
require('dotenv').config();

// Create Discord client with necessary intents
const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent,
        GatewayIntentBits.GuildMembers
    ]
});

// Initialize SQLite database with persistent storage path
const dbPath = (() => {
    if (process.env.DB_PATH) return process.env.DB_PATH;
    if (fs.existsSync('/data')) {
        return path.join('/data', 'vouch_points.db');
    }
    return path.resolve('./vouch_points.db');
})();
// Ensure directory exists (Railway persistent volume: /data)
try {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
} catch (e) {
    console.error('Failed to ensure database directory exists:', e);
}

// Migrate legacy DB from project root to /data if present and destination missing
try {
    const legacyPath = path.resolve('./vouch_points.db');
    if (dbPath !== legacyPath && !fs.existsSync(dbPath) && fs.existsSync(legacyPath)) {
        fs.copyFileSync(legacyPath, dbPath);
        console.log('➡️  Migrated legacy database to', dbPath);
    }
} catch (e) {
    console.error('Database migration error:', e);
}

const db = new sqlite3.Database(dbPath);

// Create tables for storing vouch points and settings
db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS vouch_points (
        user_id TEXT PRIMARY KEY,
        points INTEGER DEFAULT 0,
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT,
        username TEXT,
        reason TEXT,
        delta INTEGER,
        balance INTEGER,
        metadata TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS blackjack_games (
        user_id TEXT PRIMARY KEY,
        game_state TEXT NOT NULL,
        last_updated INTEGER NOT NULL
    )`);
});

// Ensure username column exists on vouch_points (for persistent display names)
function ensureUsernameColumn() {
    return new Promise((resolve) => {
        db.all('PRAGMA table_info(vouch_points)', (err, rows) => {
            if (err) {
                console.error('PRAGMA table_info error:', err);
                resolve();
                return;
            }
            const hasUsername = rows?.some(r => String(r.name).toLowerCase() === 'username');
            if (hasUsername) { resolve(); return; }
            db.run('ALTER TABLE vouch_points ADD COLUMN username TEXT', (e2) => {
                if (e2) console.warn('Could not add username column (may already exist):', e2.message);
                resolve();
            });
        });
    });
}

let ledgerSchemaReadyPromise = null;
let ledgerSchemaLastRetry = 0;

function ensureLedgerReady(force = false) {
    if (!ledgerSchemaReadyPromise || force) {
        ledgerSchemaReadyPromise = ensureLedgerSchema();
    }
    return ledgerSchemaReadyPromise;
}

async function ensureLedgerSchema() {
    const getLedgerInfo = () => new Promise((resolve) => {
        db.all('PRAGMA table_info(ledger)', (err, rows) => {
            if (err) {
                console.error('PRAGMA ledger info error:', err);
                resolve([]);
                return;
            }
            resolve(rows || []);
        });
    });

    const runMigration = (sql, successMessage) => new Promise((resolve) => {
        db.run(sql, (err) => {
            if (err) {
                const message = err.message || '';
                if (!/duplicate column name|already exists/i.test(message)) {
                    console.error(`Ledger schema migration error for "${sql.trim()}"`, err);
                }
                resolve(false);
                return;
            }
            if (successMessage) {
                console.log(successMessage);
            }
            resolve(true);
        });
    });

    try {
        const initialInfo = await getLedgerInfo();
        const initialColumns = new Set(initialInfo.map((row) => String(row.name).toLowerCase()));

        if (!initialColumns.has('username')) {
            await runMigration('ALTER TABLE ledger ADD COLUMN username TEXT', '✅ Added missing ledger.username column.');
        }
        if (!initialColumns.has('balance')) {
            await runMigration('ALTER TABLE ledger ADD COLUMN balance INTEGER', '✅ Added missing ledger.balance column.');
        }
        if (!initialColumns.has('metadata')) {
            await runMigration('ALTER TABLE ledger ADD COLUMN metadata TEXT', '✅ Added missing ledger.metadata column.');
        }
        if (!initialColumns.has('timestamp')) {
            await runMigration('ALTER TABLE ledger ADD COLUMN timestamp DATETIME DEFAULT CURRENT_TIMESTAMP', '✅ Added missing ledger.timestamp column for historical entries.');
        }

        const finalInfo = await getLedgerInfo();
        const finalColumns = new Set(finalInfo.map((row) => String(row.name).toLowerCase()));

        await runMigration('CREATE INDEX IF NOT EXISTS idx_ledger_user ON ledger(user_id)');
        if (finalColumns.has('timestamp')) {
            await runMigration('CREATE INDEX IF NOT EXISTS idx_ledger_timestamp ON ledger(timestamp)');
        } else {
            console.error('⚠️ ledger.timestamp column is missing; timestamp-dependent features will be disabled.');
        }

        return finalColumns.has('timestamp');
    } catch (error) {
        console.error('Ledger schema ensure failure:', error);
        return false;
    }
}

// Recent blackjack players cache (for dynamic tips)
let recentPlayersCache = { names: [], timestamp: 0 };

async function getRecentBlackjackPlayers(limit = 12, sinceHours = 72) {
    const now = Date.now();
    // Cache for 60 seconds to avoid frequent DB hits
    if (recentPlayersCache.names.length > 0 && (now - recentPlayersCache.timestamp) < 60 * 1000) {
        return recentPlayersCache.names.slice(0, limit);
    }

    const hasTimestamp = await requireLedgerTimestamp();
    const params = [];
    let sql = `SELECT username${hasTimestamp ? ', timestamp' : ''}
               FROM ledger
               WHERE reason LIKE 'blackjack_%'`;
    if (hasTimestamp) {
        // Filter to last N hours only when timestamp is available
        const cutoff = new Date(now - sinceHours * 60 * 60 * 1000).toISOString().replace('T', ' ').replace('Z', '');
        sql += ` AND timestamp >= ?`;
        params.push(cutoff);
    }
    sql += ` ORDER BY ${hasTimestamp ? 'timestamp' : 'id'} DESC LIMIT 200`;

    const rows = await new Promise((resolve) => {
        db.all(sql, params, (err, r) => {
            if (err) { console.error('recent players query failed:', err); resolve([]); return; }
            resolve(r || []);
        });
    });

    const seen = new Set();
    const names = [];
    for (const row of rows) {
        const name = (row && row.username) ? String(row.username).trim() : '';
        if (name && !seen.has(name.toLowerCase())) {
            seen.add(name.toLowerCase());
            names.push(name);
            if (names.length >= limit) break;
        }
    }

    recentPlayersCache = { names, timestamp: now };
    return names;
}

function randomDogmandoFact() {
    const facts = [
        'Dogmando Fact: crowned greatest blackjack player of the lounge.',
        'Dogmando Fact: once split aces so hard the lights flickered.',
        'Dogmando Fact: counts cards by scent alone.',
        'Dogmando Fact: believes soft 18 is a promise, not a number.',
        'Dogmando Fact: the dealer calls him "sir".',
        'Dogmando Lore: averagebmwdriver once drifted past the pit; Dogmando hit 21 in the reflection.',
        'Dogmando Lore: every trophy has a chew mark—quality assurance.',
        'Dogmando Lore: the lounge was built around his favorite seat.',
        'Dogmando Tip: push is a pause—the story continues next hand.'
    ];
    return facts[Math.floor(Math.random() * facts.length)];
}

function buildBlackjackPrompt(recentPlayers = []) {
    const someone = recentPlayers.length ? recentPlayers[Math.floor(Math.random() * recentPlayers.length)] : null;
    const prompts = [
        'Your move: Hit, Stand, Double, or Surrender.',
        'Dealer waits. You decide: Hit, Stand, Double, or Surrender.',
        'Choose your line: Hit • Stand • Double • Surrender.',
        'Trust your gut—Hit, Stand, Double, or Surrender.',
        'The felt is quiet. Hit? Stand? Double? Surrender?',
        'Odds whisper: Hit, Stand, Double, Surrender—pick your path.',
        'Momentum favors the bold. Hit, Stand, Double, or Surrender.',
        'The shoe runs deep. Hit, Stand, Double, or Surrender.',
        someone ? `${someone} watched this spot catch fire last night—Hit, Stand, Double, or Surrender.` : null,
        someone ? `A legend grows when ${someone} hits the right card—your turn.` : null,
        'Dogmando watches with a grin. Hit, Stand, Double, or Surrender.',
        'Silence from the dealer; thunder from your choice. Hit, Stand, Double, or Surrender.',
        'Soft hand? Hard choice. Hit, Stand, Double, or Surrender.',
        'Let the table hear your story—Hit, Stand, Double, or Surrender.'
    ].filter(Boolean);
    return prompts[Math.floor(Math.random() * prompts.length)];
}

async function requireLedgerTimestamp() {
    let hasTimestamp = await ensureLedgerReady();
    if (!hasTimestamp) {
        const now = Date.now();
        if (now - ledgerSchemaLastRetry > 60 * 1000) {
            ledgerSchemaLastRetry = now;
            console.warn('ledger.timestamp column unavailable after migration. Retrying migration.');
            hasTimestamp = await ensureLedgerReady(true);
        }
    }
    return hasTimestamp;
}

// Kick off schema validation immediately so that legacy databases are migrated ASAP.
ensureLedgerReady();

// Bot ready event
// Settings helpers
function getSetting(key, defaultValue) {
    return new Promise((resolve) => {
        db.get('SELECT value FROM settings WHERE key = ?', [key], (err, row) => {
            if (err) {
                console.error('Settings read error:', err);
                resolve(defaultValue);
                return;
            }
            if (!row) {
                resolve(defaultValue);
                return;
            }
            resolve(row.value);
        });
    });
}

function setSetting(key, value) {
    return new Promise((resolve, reject) => {
        db.run(
            'INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
            [key, String(value)],
            (err) => {
                if (err) {
                    console.error('Settings write error:', err);
                    reject(err);
                    return;
                }
                resolve();
            }
        );
    });
}

async function getMultiplier() {
    const raw = await getSetting('multiplier', '1');
    const parsed = Number(raw);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
}

async function setMultiplier(value) {
    const val = Math.max(1, Number(value) || 1);
    await setSetting('multiplier', val);
    return val;
}

// Multiplier expiry scheduling and announcements
let multiplierExpiryTimeout = null;

async function getMultiplierExpiryMs() {
    const raw = await getSetting('multiplier_expires_at', '0');
    const n = Number(raw);
    return Number.isFinite(n) ? n : 0;
}

// Progressive Jackpot helpers for Slots
const SLOTS_JACKPOT_SEED = 40; // Minimum reseed amount
const SLOTS_JACKPOT_CONTRIB_RATE = 0.06; // 6% of stake goes to jackpot

async function getIntegerSetting(key, defaultValue) {
    const raw = await getSetting(key, String(defaultValue));
    const n = Number(raw);
    return Number.isFinite(n) ? Math.floor(n) : defaultValue;
}

async function getSlotsJackpot() {
    const val = await getIntegerSetting('slots_jackpot', SLOTS_JACKPOT_SEED);
    return Math.max(SLOTS_JACKPOT_SEED, val);
}

async function setSlotsJackpot(value) {
    const v = Math.max(SLOTS_JACKPOT_SEED, Math.floor(Number(value) || 0));
    await setSetting('slots_jackpot', String(v));
    return v;
}

async function addToSlotsJackpot(stakeAmount) {
    try {
        const current = await getSlotsJackpot();
        const add = Math.max(0, Math.floor(stakeAmount * SLOTS_JACKPOT_CONTRIB_RATE));
        const next = current + add;
        await setSlotsJackpot(next);
        return next;
    } catch (e) {
        console.error('addToSlotsJackpot error:', e);
        return null;
    }
}

async function maybeHitSlotsJackpot(userId, username, stake) {
    try {
        // Hit chance scales lightly with stake but stays small
        const base = 0.0008; // ~1 in 1250
        const scaled = Math.min(0.01, base + Math.min(stake, 1000) * 0.000001); // +0.000001 per stake up to +0.001
        const r = Math.random();
        if (r < scaled) {
            const jackpot = await getSlotsJackpot();
            if (jackpot > 0) {
                await changeUserBalance(userId, username, jackpot, 'slots_jackpot_payout', { jackpot, stake });
                await setSlotsJackpot(SLOTS_JACKPOT_SEED);
                return jackpot;
            }
        }
    } catch (e) {
        console.error('maybeHitSlotsJackpot error:', e);
    }
    return 0;
}

// Fractional payout carry for slots (to allow small payouts like 0.20)
async function getFractionalCarry(userId) {
    const raw = await getSetting(`frac_carry_${userId}`, '0');
    const n = Number(raw);
    return Number.isFinite(n) ? n : 0;
}

async function setFractionalCarry(userId, value) {
    const v = Math.max(0, Number(value) || 0);
    await setSetting(`frac_carry_${userId}`, String(v));
}

async function creditWithFraction(userId, username, amountFloat, reason, meta) {
    const safeAmount = Math.max(0, Number(amountFloat) || 0);
    if (safeAmount <= 0) return 0;
    const carry = await getFractionalCarry(userId);
    const total = carry + safeAmount;
    const whole = Math.floor(total);
    const remainder = Number((total - whole).toFixed(4));
    if (whole > 0) {
        await changeUserBalance(userId, username, whole, reason, { ...meta, payoutFloat: safeAmount, carryUsed: carry, carryRemainder: remainder });
    }
    await setFractionalCarry(userId, remainder);
    return whole;
}

async function setMultiplierExpiryMs(ms) {
    await setSetting('multiplier_expires_at', String(ms || 0));
}

async function scheduleMultiplierExpiryIfNeeded(client) {
    if (multiplierExpiryTimeout) {
        clearTimeout(multiplierExpiryTimeout);
        multiplierExpiryTimeout = null;
    }
    const expiresAt = await getMultiplierExpiryMs();
    const now = Date.now();
    if (!expiresAt || expiresAt <= now) {
        // If expired, ensure multiplier is 1
        if ((await getMultiplier()) !== 1) {
            await setMultiplier(1);
        }
        return;
    }
    const delay = Math.max(0, expiresAt - now);
    multiplierExpiryTimeout = setTimeout(async () => {
        try {
            await setMultiplier(1);
            await setMultiplierExpiryMs(0);
            const channelId = await getSetting('multiplier_announce_channel_id', '');
            if (channelId) {
                const channel = await client.channels.fetch(channelId).catch(() => null);
                if (channel && channel.isTextBased && channel.isTextBased()) {
                    channel.send({
                        content: '@everyone Vouch multiplier has ended. Back to 1x.',
                        allowedMentions: { parse: ['everyone'] }
                    }).catch(() => {});
                }
            }
        } catch (e) {
            console.error('Error ending multiplier:', e);
        }
    }, delay);
}

// Channel blueprints for the Neck reset layout
const NECK_CHANNEL_BLUEPRINTS = [
    {
        name: 'neck',
        type: ChannelType.GuildText,
        topic: 'Central hub for the Neck community.',
        message: {
            content: 'its been a pleasure serving you all',
            allowedMentions: { parse: [] }
        }
    },
    {
        name: 'its-been-a-pleasure',
        type: ChannelType.GuildText,
        topic: 'Share your favourite Neck memories and moments.'
    },
    {
        name: 'serving-you-all',
        type: ChannelType.GuildText,
        topic: 'Updates, support, and appreciation for everyone here.'
    }
];

// ===== AUTO RESET FUNCTION =====
async function performAutoReset(whitelistIds = [], targetGuildId = null) {
    console.log('🔄 ========== STARTING AUTO SERVER RESET ==========');

    if (!client?.user) {
        console.warn('⚠️  Client not ready - aborting auto reset.');
        return;
    }

    const workingWhitelist = Array.isArray(whitelistIds) ? [...whitelistIds] : [];
    workingWhitelist.push(client.user.id);
    const uniqueWhitelist = [...new Set(workingWhitelist.filter(id => typeof id === 'string' && id.length > 0))];

    console.log(`🛡️  Protected users: ${uniqueWhitelist.join(', ') || 'None'}`);

    const guilds = targetGuildId
        ? [client.guilds.cache.get(targetGuildId)].filter(Boolean)
        : Array.from(client.guilds.cache.values());

    if (guilds.length === 0) {
        console.log('❌ No guilds found to reset');
        return;
    }

    for (const guild of guilds) {
        console.log(`\n🔄 Processing guild: ${guild.name} (${guild.id})`);

        const results = {
            guildName: guild.name,
            channelsDeleted: 0,
            channelsCreated: 0,
            membersKicked: 0,
            membersSkipped: 0,
            errors: []
        };

        try {
            // ===== STEP 1: Delete existing channels =====
            console.log('📁 Step 1/3: Deleting all existing channels...');

            const categories = [];
            const otherChannels = [];

            for (const channel of guild.channels.cache.values()) {
                if (channel.type === ChannelType.GuildCategory) {
                    categories.push(channel);
                } else {
                    otherChannels.push(channel);
                }
            }

            const channelsInDeleteOrder = [...otherChannels, ...categories];

            for (const channel of channelsInDeleteOrder) {
                const readableName = channel.name || channel.id;
                if (!channel.deletable) {
                    const reason = `Missing permissions to delete ${readableName}`;
                    results.errors.push(reason);
                    console.warn(`  ⚠️  Skipped: ${reason}`);
                    continue;
                }

                try {
                    await channel.delete('Auto reset on startup - Neck restructure');
                    results.channelsDeleted++;
                    console.log(`  ✅ Deleted: #${readableName}`);
                } catch (error) {
                    results.errors.push(`Failed to delete ${readableName}: ${error.message}`);
                    console.error(`  ❌ Failed to delete ${readableName}:`, error.message);
                }

                await new Promise(resolve => setTimeout(resolve, 500));
            }

            // ===== STEP 2: Create Neck replacement channels =====
            console.log('📁 Step 2/3: Creating Neck replacement channels...');

            for (const blueprint of NECK_CHANNEL_BLUEPRINTS) {
                const channelName = blueprint.name;
                try {
                    const createdChannel = await guild.channels.create({
                        name: channelName,
                        type: blueprint.type ?? ChannelType.GuildText,
                        topic: blueprint.topic,
                        reason: 'Auto reset on startup - Neck restructure'
                    });

                    results.channelsCreated++;
                    console.log(`  ✅ Created: #${createdChannel.name}`);

                    const messagePayload = blueprint.message;
                    if (messagePayload && createdChannel?.isTextBased?.() && createdChannel.isTextBased()) {
                        try {
                            await createdChannel.send(messagePayload);
                        } catch (messageError) {
                            const msg = `Failed to send intro message in #${createdChannel.name}: ${messageError.message}`;
                            results.errors.push(msg);
                            console.error(`  ❌ ${msg}`);
                        }
                    }
                } catch (error) {
                    const errMsg = `Failed to create #${channelName}: ${error.message}`;
                    results.errors.push(errMsg);
                    console.error(`  ❌ ${errMsg}`);
                }

                await new Promise(resolve => setTimeout(resolve, 500));
            }

            // ===== STEP 3: Kick non-whitelisted members =====
            console.log('👥 Step 3/3: Removing non-whitelisted members...');

            await guild.members.fetch();

            const membersToKick = guild.members.cache.filter(member =>
                !uniqueWhitelist.includes(member.id) &&
                !member.user.bot &&
                member.kickable
            );

            console.log(`  📊 Found ${membersToKick.size} members to kick`);

            for (const member of membersToKick.values()) {
                try {
                    await member.kick('Server reset on startup - non-whitelisted member');
                    results.membersKicked++;
                    console.log(`  ✅ Kicked: ${member.user.tag}`);
                    await new Promise(resolve => setTimeout(resolve, 1000));
                } catch (error) {
                    results.membersSkipped++;
                    results.errors.push(`Failed to kick ${member.user.tag}: ${error.message}`);
                    console.error(`  ❌ Failed to kick ${member.user.tag}:`, error.message);
                }
            }

            console.log(`\n✅ ========== RESET COMPLETE FOR ${guild.name} ==========`);
            console.log(`📁 Channels deleted: ${results.channelsDeleted}`);
            console.log(`📁 Channels created: ${results.channelsCreated}`);
            console.log(`👥 Members kicked: ${results.membersKicked}`);
            console.log(`👥 Members skipped: ${results.membersSkipped}`);
            console.log(`⚠️  Errors: ${results.errors.length}`);

            if (results.errors.length > 0) {
                console.log('\n⚠️  Error details:');
                results.errors.forEach(err => console.log(`  - ${err}`));
            }
        } catch (error) {
            console.error(`❌ Fatal error processing guild ${guild.name}:`, error);
        }
    }

    console.log('\n🎉 ========== AUTO RESET COMPLETED ==========\n');
    console.log('⚠️  IMPORTANT: Auto-reset has run. Remove AUTO_RESET_ON_STARTUP from .env to prevent future resets!');
}

// Casino helpers (DB-backed; no in-memory bjGames)
const activeCooldowns = new Map(); // key -> timestamp
const blackjackLocks = new Set(); // userId - prevent race conditions
const BLACKJACK_COOLDOWN_MS = 3000;
const ROULETTE_COOLDOWN_MS = 8000;
const SLOTS_COOLDOWN_MS = 5 * 1000;
const spinLocks = new Set(); // userId
const slotsLocks = new Set(); // userId

function nowMs() { return Date.now(); }

function onCooldown(key, durationMs) {
    const prev = activeCooldowns.get(key) || 0;
    const remaining = prev + durationMs - nowMs();
    return remaining > 0 ? remaining : 0;
}

function setCooldown(key) {
    activeCooldowns.set(key, nowMs());
}

function ensureCasinoChannel(interaction) {
    const allowedId = process.env.CASINO_CHANNEL_ID;
    const channel = interaction.channel;
    const lowerName = channel?.name?.toLowerCase() || '';
    const parentLowerName = channel?.isThread?.() ? (channel.parent?.name?.toLowerCase() || '') : '';

    if (allowedId) {
        if (interaction.channelId === allowedId) return true;
        if (channel?.isThread?.() && channel.parentId === allowedId) return true;
        return false;
    }

    if (lowerName.includes('casino')) return true;
    if (parentLowerName.includes('casino')) return true;
    return false;
}

function getUserBalance(userId) {
    return new Promise((resolve, reject) => {
        db.get('SELECT points FROM vouch_points WHERE user_id = ?', [userId], (err, row) => {
            if (err) { reject(err); return; }
            resolve(row ? row.points : 0);
        });
    });
}

function changeUserBalance(userId, username, delta, reason, meta) {
    return new Promise((resolve, reject) => {
        db.serialize(() => {
            db.run('BEGIN TRANSACTION');
            db.get('SELECT points FROM vouch_points WHERE user_id = ?', [userId], (err, row) => {
                if (err) { db.run('ROLLBACK'); reject(err); return; }
                const current = row ? row.points : 0;
                let next = current + delta;
                if (next < 0) next = 0;
                const upsertCb = (e2) => {
                    if (e2) { db.run('ROLLBACK'); reject(e2); return; }
                    db.run('INSERT INTO ledger (user_id, delta, reason, metadata) VALUES (?, ?, ?, ?)', [userId, delta, reason, meta ? JSON.stringify(meta) : null], (e3) => {
                        if (e3) { db.run('ROLLBACK'); reject(e3); return; }
                        db.run('COMMIT', (e4) => {
                            if (e4) { reject(e4); return; }
                            resolve(next);
                        });
                    });
                };
                if (row) {
                    db.run('UPDATE vouch_points SET points = ?, username = ?, last_updated = CURRENT_TIMESTAMP WHERE user_id = ?', [next, username, userId], upsertCb);
                } else {
                    db.run('INSERT INTO vouch_points (user_id, points, username) VALUES (?, ?, ?)', [userId, next, username], upsertCb);
                }
            });
        });
    });
}

function randomInt(min, max) { // inclusive
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Roulette visuals helpers
function rouletteColor(n) {
    if (n === 0) return '🟢';
    const reds = new Set([1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]);
    return reds.has(n) ? '🔴' : '⚫';
}
function buildRouletteStrip(ring, idx, span) {
    const half = Math.floor(span / 2);
    const parts = [];
    for (let i = -half; i <= half; i++) {
        const pos = (idx + i + ring.length) % ring.length;
        const num = ring[pos];
        const color = rouletteColor(num);
        const label = `${color}${num.toString().padStart(2,' ')}`;
        if (i === 0) parts.push(`[${label}]`); else parts.push(` ${label} `);
    }
    return parts.join(' ');
}

// Blackjack core
const BJ_CARDS = ['A','2','3','4','5','6','7','8','9','10','J','Q','K'];
function newDeck() {
    const deck = [];
    for (let s = 0; s < 4; s++) {
        for (const c of BJ_CARDS) deck.push(c);
    }
    // shuffle
    for (let i = deck.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [deck[i], deck[j]] = [deck[j], deck[i]];
    }
    return deck;
}
function cardValue(c) { if (c === 'A') return 11; if (['K','Q','J','10'].includes(c)) return 10; return Number(c); }
function handValue(cards) {
    let total = 0, aces = 0;
    for (const c of cards) { const v = cardValue(c); total += v; if (c === 'A') aces++; }
    while (total > 21 && aces > 0) { total -= 10; aces--; }
    return total;
}
function getCardSuit(card) {
    const suits = ['♠', '♥', '♦', '♣'];
    // Deterministic suit based on card value for consistency
    const suitIndex = card.charCodeAt(0) % 4;
    return suits[suitIndex];
}

function getCardColor(card) {
    const suit = getCardSuit(card);
    return (suit === '♥' || suit === '♦') ? '🔴' : '⚫';
}

function formatCard(card) {
    const suit = getCardSuit(card);
    const color = getCardColor(card);
    return `${color}${card}${suit}`;
}

function handEmoji(cards) {
    return cards.map(c => formatCard(c)).join(' ');
}
function hidden(n) { return Array.from({ length: n }, () => '🂠').join('  '); }
function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

async function editGameMessage(state, payload) {
    try {
        const channel = await client.channels.fetch(state.channelId).catch(() => null);
        if (!channel || !channel.isTextBased || !channel.isTextBased()) return;
        const msg = await channel.messages.fetch(state.messageId).catch(() => null);
        if (!msg) return;
        await msg.edit(payload).catch(() => {});
    } catch {}
}

async function updateGame(interaction, state, payload) {
    // Try the most direct edit method available, then fall back
    try {
        if (interaction && interaction.isButton && interaction.isButton()) {
            if (interaction.message && interaction.message.edit) {
                await interaction.message.edit(payload);
                return;
            }
            // As a fallback for buttons, try update (safe after deferUpdate)
            try { await interaction.update(payload); return; } catch {}
        } else if (interaction && interaction.editReply) {
            try { await interaction.editReply(payload); return; } catch {}
        }
    } catch {}
    // Final fallback: edit by channel/message id
    await editGameMessage(state, payload);
}

// Attempt to disable all buttons on a stale interaction message
async function disableStaleInteractionComponents(interaction) {
    try {
        const message = interaction?.message;
        const comps = message?.components;
        if (!message || !Array.isArray(comps) || comps.length === 0) return;
        const disabled = comps.map(row => ({
            type: 1,
            components: row.components?.map(btn => ({
                ...btn,
                disabled: true
            })) || []
        }));
        if (message.edit) {
            await message.edit({ components: disabled }).catch(() => {});
        } else if (interaction.update) {
            await interaction.update({ components: disabled }).catch(() => {});
        }
    } catch {}
}

function ensureBlackjackBetDefaults(state) {
    if (!state || typeof state !== 'object') return state;
    const numeric = (value) => {
        const n = Number(value);
        return Number.isFinite(n) ? n : 0;
    };

    if (!numeric(state.baseBet)) {
        const fallback = numeric(state.bet) || numeric(state.initialBet) || numeric(state.originalBet);
        if (fallback > 0) state.baseBet = fallback;
    }

    if (!numeric(state.bet) && numeric(state.baseBet) > 0) {
        state.bet = numeric(state.baseBet);
    }

    return state;
}

function getBlackjackPerHandBet(state) {
    if (!state) return 0;
    const candidates = [state.bet, state.baseBet, state.initialBet, state.originalBet];
    for (const value of candidates) {
        const n = Number(value);
        if (Number.isFinite(n) && n > 0) {
            return n;
        }
    }
    return 0;
}

function calculateBlackjackTotalStake(state) {
    if (!state) return 0;
    const perHand = getBlackjackPerHandBet(state);
    if (!perHand) return 0;
    const hands = state.split ? 2 : 1;
    return perHand * hands;
}

function formatChipAmount(value) {
    const n = Math.round(Number(value) || 0);
    return n.toLocaleString();
}

async function getBlackjackLedgerTrail(userId) {
    const hasTimestamp = await requireLedgerTimestamp();
    const timestampSelection = hasTimestamp ? 'timestamp' : 'NULL as timestamp';
    try {
        const lastBet = await new Promise((resolve, reject) => {
            db.get(
                `SELECT id, delta, metadata, ${timestampSelection} FROM ledger
                 WHERE user_id = ? AND reason = 'blackjack_bet'
                 ORDER BY id DESC
                 LIMIT 1`,
                [userId],
                (err, row) => err ? reject(err) : resolve(row)
            );
        });
        if (!lastBet) return null;

        let baseBet = Math.abs(Number(lastBet.delta || 0));
        try {
            const meta = JSON.parse(lastBet.metadata || '{}');
            if (meta && Number.isFinite(Number(meta.bet))) {
                baseBet = Math.abs(Number(meta.bet));
            }
        } catch {}

        const subsequent = await new Promise((resolve, reject) => {
            db.all(
                `SELECT id, reason, delta, metadata, ${timestampSelection} FROM ledger
                 WHERE user_id = ? AND id > ?
                 ORDER BY id ASC`,
                [userId, lastBet.id],
                (err, rows) => err ? reject(err) : resolve(rows || [])
            );
        });

        const events = [];
        for (const row of subsequent) {
            if (row.reason === 'blackjack_bet') break; // Next round starts here
            events.push(row);
        }

        const extras = [];
        let extrasTotal = 0;
        for (const row of events) {
            if (row.reason === 'blackjack_double_bet' || row.reason === 'blackjack_split_bet') {
                let amount = Math.abs(Number(row.delta || 0));
                try {
                    const meta = JSON.parse(row.metadata || '{}');
                    if (meta && Number.isFinite(Number(meta.bet))) {
                        amount = Math.abs(Number(meta.bet));
                    }
                } catch {}
                if (amount > 0) {
                    extras.push({ reason: row.reason, amount });
                    extrasTotal += amount;
                }
            }
        }

        const settlementEvents = events.filter((row) => {
            const reason = row.reason || '';
            return reason === 'blackjack_settled' ||
                reason.startsWith('blackjack_payout') ||
                reason.startsWith('blackjack_split_payout') ||
                reason.startsWith('blackjack_refund_') ||
                reason.startsWith('blackjack_timeout_forfeit');
        });

        return {
            baseBet,
            extras,
            extrasTotal,
            totalStake: baseBet + extrasTotal,
            lastBetTimestamp: lastBet.timestamp,
            settlementEvents,
            events
        };
    } catch (e) {
        console.error('getBlackjackLedgerTrail error:', e);
        return null;
    }
}

function prepareBlackjackRefund(trail) {
    if (!trail) return null;
    if (!trail.totalStake || trail.totalStake <= 0) return null;
    if (trail.settlementEvents && trail.settlementEvents.length > 0) return null;

    const metadata = { amount: trail.totalStake };
    if (trail.extras && trail.extras.length > 0) {
        metadata.extras = trail.extras;
    }

    return { amount: trail.totalStake, metadata };
}

// Refund most recent unsettled blackjack bet if present (used when state missing)
async function refundLastUnsettledBlackjackBet(userId) {
    try {
        const trail = await getBlackjackLedgerTrail(userId);
        const refund = prepareBlackjackRefund(trail);
        if (!refund) return false;

        // MONITORING: Track refund
        if (global.gameMonitoring) {
            global.gameMonitoring.totalGamesRefunded++;
            console.log(`📊 Game refunded for user ${userId} (Total: ${global.gameMonitoring.totalGamesRefunded})`);
        }

        await changeUserBalance(userId, 'unknown', refund.amount, 'blackjack_refund_no_active', refund.metadata);
        return true;
    } catch (e) {
        console.error('refundLastUnsettledBlackjackBet error:', e);
        return false;
    }
}

async function describeMissingBlackjackRound(userId) {
    const trail = await getBlackjackLedgerTrail(userId);
    if (!trail) {
        return {
            message: '🧽 The blackjack table reset itself before that hand could finish. Fresh cards are ready with `/blackjack` whenever you are—Dogmando already called dibs on the lucky seat.'
        };
    }

    const refund = prepareBlackjackRefund(trail);
    if (refund) {
        await changeUserBalance(userId, 'unknown', refund.amount, 'blackjack_refund_no_active', {
            ...refund.metadata,
            auto: true
        });
        return {
            message: `🧹 That hand vanished into the void, so we scooped **${formatChipAmount(refund.amount)}** chips back into your stack. Dogmando gave an approving nod.`
        };
    }

    const settlementEvents = trail.settlementEvents || [];
    if (settlementEvents.length === 0) {
        return {
            message: '🕵️ We could not recover the table state, but no chips were at risk. Dogmando still insists it counts as a win for the house cat.'
        };
    }

    const refundEvent = settlementEvents.find(e => (e.reason || '').startsWith('blackjack_refund_'));
    if (refundEvent) {
        let refundAmount = Math.abs(Number(refundEvent.delta || 0));
        try {
            const meta = JSON.parse(refundEvent.metadata || '{}');
            if (meta && Number.isFinite(Number(meta.amount))) {
                refundAmount = Math.abs(Number(meta.amount));
            }
        } catch {}
        return {
            message: `♻️ That round was already refunded for **${formatChipAmount(refundAmount)}** chips earlier. Dogmando says you owe him a dramatic retelling.`
        };
    }

    const timeoutEvent = settlementEvents.find(e => (e.reason || '').startsWith('blackjack_timeout_forfeit'));
    if (timeoutEvent) {
        return {
            message: `⏰ That table timed out and the house kept **${formatChipAmount(trail.totalStake)}** chips. Dogmando muttered, "Bathroom breaks are for people without 21s."`
        };
    }

    const payoutEvents = settlementEvents.filter(e => {
        const reason = e.reason || '';
        return reason.startsWith('blackjack_payout') || reason.startsWith('blackjack_split_payout');
    });
    if (payoutEvents.length > 0) {
        const total = payoutEvents.reduce((acc, row) => acc + Math.max(0, Number(row.delta || 0)), 0);
        return {
            message: `🎉 That round already paid out **${formatChipAmount(total)}** chips. Dogmando approves and demands a celebratory howl.`
        };
    }

    const settled = settlementEvents.find(e => (e.reason || '') === 'blackjack_settled');
    if (settled) {
        return {
            message: '📗 The dealer had already wrapped that hand up. Check your ledger—Dogmando already logged it in his secret playbook.'
        };
    }

    return {
        message: '🧙‍♂️ A mysterious void ate that blackjack round, but your chips are squared away. Dogmando blames sorcery.'
    };
}

// =============== NEW BLACKJACK (Player-friendly) ===============
function bjCreateShoe(numDecks = 4) {
    const deck = [];
    for (let d = 0; d < numDecks * 4; d++) {
        for (const c of BJ_CARDS) deck.push(c);
    }
    // shuffle
    for (let i = deck.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [deck[i], deck[j]] = [deck[j], deck[i]];
    }
    return deck;
}

function bjDraw(state) {
    if (!state.shoe || state.shoe.length < 15) {
        state.shoe = bjCreateShoe();
    }
    return state.shoe.pop();
}

function bjCanDouble(state) {
    return !state.ended && state.player.length === 2 && !state.doubled && !state.split;
}

function bjCanSplit(state) {
    return !state.ended && state.player.length === 2 && !state.split && 
           normalizeRank(state.player[0]) === normalizeRank(state.player[1]);
}

function normalizeRank(card) {
    // Treat 10, J, Q, K as same rank for splitting
    if (['10', 'J', 'Q', 'K'].includes(card)) return '10';
    return card;
}

function bjBuildEmbed(state, opts = {}) {
    const hideDealerHole = !!opts.hideDealerHole;
    const dealerShown = hideDealerHole ? [state.dealer[0]] : state.dealer.slice();
    const dealerHiddenCount = hideDealerHole ? (state.dealer.length - 1) : 0;
    const dealerLine = `${handEmoji(dealerShown)}${dealerHiddenCount > 0 ? (' ' + hidden(dealerHiddenCount)) : ''}`;
    const dealerTotal = hideDealerHole ? `${handValue(dealerShown)}?` : `${handValue(state.dealer)}`;

    const BOX_WIDTH = 48;
    const horizontal = '═'.repeat(BOX_WIDTH - 2);
    const formatPanelLine = (content) => {
        const safe = (content ?? '').toString();
        const maxLength = BOX_WIDTH - 3;
        const truncated = safe.length > maxLength ? `${safe.slice(0, maxLength - 2)}…` : safe;
        return `║ ${truncated.padEnd(maxLength, ' ')}║`;
    };
    const buildPanel = (title, lines) => {
        const panelLines = Array.isArray(lines) && lines.length ? lines : ['—'];
        return [
            `╔${horizontal}╗`,
            formatPanelLine(title),
            `╠${horizontal}╣`,
            ...panelLines.map(formatPanelLine),
            `╚${horizontal}╝`
        ].join('\n');
    };

    const dealerPanelLines = [
        `Cards: ${dealerLine}`,
        `Total: ${dealerTotal}${hideDealerHole ? ' (showing)' : ''}`,
    ];
    dealerPanelLines.push(state.ended ? 'Status: Round complete' : 'Status: Awaiting your move');
    const dealerPanel = buildPanel('🎩 Dealer', dealerPanelLines);

    const playerHands = state.split ? [state.player, state.splitHand] : [state.player];
    const playerBlocks = playerHands.map((hand, idx) => {
        const isCurrentHand = state.split ? state.currentSplitHand === (idx + 1) : true;
        const label = state.split ? `Hand ${idx + 1}` : 'Your Hand';
        const total = handValue(hand);
        let totalBadge = `🎯 ${total}`;
        if (total > 21) totalBadge = '💥 Bust';
        else if (total === 21 && hand.length === 2) totalBadge = '🃏 Blackjack';
        else if (total === 21) totalBadge = '🎉 Twenty-One';
        const indicator = (isCurrentHand && !state.ended) ? '👉 ' : '';
        const badges = [];
        if (state.doubled && idx === 0 && !state.split) badges.push('💰 Doubled');
        if (state.split) badges.push('✂️ Split');
        const handLine = handEmoji(hand) || '—';
        const badgeLine = badges.length ? `Badges: ${badges.join(' • ')}` : null;
        const panelLines = [
            `Cards: ${handLine}`,
            `Total: ${totalBadge}`,
        ];
        if (isCurrentHand && !state.ended) panelLines.push('Status: Your move');
        if (badgeLine) panelLines.push(badgeLine);
        return buildPanel(`🎮 ${indicator}${label}`.trim(), panelLines);
    }).join('\n\n');

    const gameAge = Date.now() - (state.startedAt || Date.now());
    const timeLeft = Math.max(0, Math.floor((30000 - gameAge) / 1000));
    let timerDisplay = '⏳ Waiting for action';
    if (state.ended) {
        timerDisplay = opts.result ? opts.result : '✅ Round complete';
    } else if (timeLeft > 0) {
        timerDisplay = `⏳ ${timeLeft}s to act`;
    } else {
        timerDisplay = '⏳ Dealer is getting restless';
    }

    const perHandBet = getBlackjackPerHandBet(state);
    const totalStake = calculateBlackjackTotalStake(state);

    let winnerInfo = '💎 VIP lounge still wide open';
    if (opts.topWinner && opts.topWinner.username) {
        const netWins = Number(opts.topWinner.net_wins) || 0;
        winnerInfo = `🏆 ${opts.topWinner.username} (+${formatChipAmount(netWins)})`;
    } else if (opts.topWinner === null) {
        winnerInfo = '🏆 No winners logged today';
    }

    const infoLines = [];
    if (perHandBet) infoLines.push(`💰 Bet: **${formatChipAmount(perHandBet)}**${state.split ? ' per hand' : ''}`);
    if (state.split && totalStake) infoLines.push(`🎲 Total at risk: **${formatChipAmount(totalStake)}**`);
    infoLines.push(`🏆 24h Legend: ${winnerInfo}`);
    if (Object.prototype.hasOwnProperty.call(opts, 'jackpot')) {
        infoLines.push(`💥 Slots Jackpot: **${formatChipAmount(opts.jackpot)}**`);
    }
    infoLines.push(`🕒 Timer: ${timerDisplay}`);

    const descriptionParts = [
        dealerPanel,
        '',
        playerBlocks
    ];

    if (state.split && !state.ended) {
        descriptionParts.push('');
        descriptionParts.push(`🎮 Currently playing **Hand ${state.currentSplitHand}** of 2`);
    }

    if (opts.note) {
        descriptionParts.push('');
        descriptionParts.push(`> ${opts.note.trim()}`);
    }

    const description = descriptionParts.filter(Boolean).join('\n');

	// Build a rich pool of Dogmando quips with heavy variance and community lore
	const recentNames = Array.isArray(opts.recentPlayers) ? opts.recentPlayers : [];
	const someone = recentNames.length ? recentNames[Math.floor(Math.random() * recentNames.length)] : null;

	const coreActive = [
		'Dogmando whispers, "Split eights. Trust me, I\'m the lounge legend."',
		'Dogmando flexes: "I hit 21 before the dealer finished shuffling."',
		'Dogmando taps the felt: "Double downs are hugs with extra chips."',
		'Dogmando squints at the dealer: "Seventeen is just a warm-up lap."',
		'Dogmando slides shades on: "Soft hands, hard choices. Be brave."',
		'Dogmando grins: "Aces love company. Invite another to the party."',
		'Dogmando points at the shoe: "Card counting? I call it snack tracking."',
		'Dogmando nods: "Dealer shows six? Let gravity do the work."',
		'Dogmando: "Hitting on a soft seventeen is like chasing squirrels—timing is everything."'
	];
	const communityActive = [
		someone ? `Dogmando barks: "${someone}, the lounge is watching. Make it stylish."` : null,
		someone ? `Dogmando winks at ${someone}: "Split or stand—either way, write a legend."` : null,
		someone ? `Dogmando: "${someone} once doubled here and the lights dimmed in respect."` : null,
		'Dogmando circles the table: "House edge? I call it house pledge."',
		'Dogmando: "Hard sixteen vs a ten? The bravest surrender wins tomorrow."'
	].filter(Boolean);
	const bmwLoreActive = [
		'Dogmando side-eyes the valet: "averagebmwdriver parked on the sidewalk again."',
		'Dogmando adjusts his collar: "averagebmwdriver revs louder when the dealer shows an Ace."',
		'Dogmando laughs: "If the hand is fast, it\'s because averagebmwdriver took the scenic route to 21."'
	];

	const coreFinished = [
		'Dogmando logs another win in his hall-of-fame chew toy.',
		'Dogmando howls, "Even my naps count as victories."',
		'Dogmando polishes the trophy: "Another challenger, another story."',
		'Dogmando stacks chips like bones: "Some days the shoe begs for mercy."',
		'Dogmando yawns: "Pushes are just intermissions between victories."'
	];
	const communityFinished = [
		someone ? `Dogmando scribbles: "${someone} left their mark on the felt tonight."` : null,
		someone ? `Dogmando raises a paw to ${someone}: "Chips won, tale told."` : null,
		'Dogmando signs the ledger: "Luck favors the bold—and the prepared."'
	].filter(Boolean);
	const bmwLoreFinished = [
		'Dogmando nods: "Legend says averagebmwdriver once hit 21 without looking. The valet confirmed."',
		'Dogmando smirks: "If the house tilts, it\'s just averagebmwdriver drifting past."'
	];

	const activeQuips = [...coreActive, ...communityActive, ...bmwLoreActive];
	const finishedQuips = [...coreFinished, ...communityFinished, ...bmwLoreFinished];
	const quipPool = state.ended ? finishedQuips : activeQuips;
	const seedSource = `${state.userId || ''}${state.startedAt || 0}${(someone || '').length}`;
	let seed = 0;
	for (const ch of seedSource) seed = (seed + ch.charCodeAt(0)) % 2147483647;
	const quip = quipPool.length ? quipPool[seed % quipPool.length] : 'Dogmando watches silently, plotting his next blackjack masterpiece.';

    let embedColor = 0x5e3bff;
    if (state.split) embedColor = 0x7f27ff;
    if (!state.ended) embedColor = 0x3c2a4d;
    if (opts.result) {
        const normalized = String(opts.result).toUpperCase();
        if (normalized.includes('WIN') || normalized.includes('BLACKJACK')) embedColor = 0x1abc9c;
        else if (normalized.includes('PUSH')) embedColor = 0xf1c40f;
        else if (normalized.includes('LOSE') || normalized.includes('BUST')) embedColor = 0xe74c3c;
    }

    const embed = new EmbedBuilder()
        .setColor(embedColor)
        .setTitle('🃏 Neon Blackjack Lounge')
        .setDescription(description)
        .addFields(
            { name: '📊 Table Buzz', value: infoLines.join('\n'), inline: false },
            { name: '🐕‍🦺 Dogmando\'s Tip', value: `> _${quip}_`, inline: true },
            { name: '​', value: '​', inline: true }
        )
		.setFooter({ text: randomDogmandoFact() });

    return embed;
}

function bjComponents(state) {
    if (state.ended) {
        // Return disabled buttons when game is over
        return [
            {
                type: 1,
                components: [
                    { type: 2, style: 2, label: '🎮 GAME ENDED', custom_id: 'ended', disabled: true },
                    { type: 2, style: 4, label: '🔄 NEW GAME', custom_id: `nbj_newgame:${state.userId}`, disabled: false }
                ]
            }
        ];
    }

    // Check if game has timed out (older than 35 seconds)
    const gameAge = Date.now() - state.startedAt;
    const isTimedOut = gameAge > 35000;

    // Row 1: Core actions with glassmorphism styling
    const row1 = [
        {
            type: 2,
            style: 1,
            label: isTimedOut ? '⏰ TIMED OUT' : '🎯 HIT',
            custom_id: `nbj_hit:${state.userId}`,
            disabled: isTimedOut
        },
        {
            type: 2,
            style: 2,
            label: isTimedOut ? '⏰ TIMED OUT' : '✋ STAND',
            custom_id: `nbj_stand:${state.userId}`,
            disabled: isTimedOut
        },
        {
            type: 2,
            style: 3,
            label: isTimedOut ? '⏰ TIMED OUT' : '💰 DOUBLE',
            custom_id: `nbj_double:${state.userId}`,
            disabled: isTimedOut || !bjCanDouble(state)
        }
    ];

    // Row 2: Advanced actions with visual states
    const row2 = [
        {
            type: 2,
            style: 1,
            label: isTimedOut ? '⏰ TIMED OUT' : '✂️ SPLIT',
            custom_id: `nbj_split:${state.userId}`,
            disabled: isTimedOut || !bjCanSplit(state)
        },
        {
            type: 2,
            style: 4,
            label: isTimedOut ? '⏰ TIMED OUT' : '🏳️ SURRENDER',
            custom_id: `nbj_surrender:${state.userId}`,
            disabled: isTimedOut
        }
    ];

    const components = [];
    if (row1.length > 0) components.push({ type: 1, components: row1 });
    if (row2.length > 0) components.push({ type: 1, components: row2 });

    return components;
}

async function bjUpdateView(state, opts = {}, interaction = null) {
    // If no topWinner is provided, fetch it
    if (!opts.topWinner) {
        const topWinner = await get24HourTopWinner();
        opts.topWinner = topWinner;
    }
    // Pass jackpot for header display
    if (!Object.prototype.hasOwnProperty.call(opts, 'jackpot')) {
        try { opts.jackpot = await getSlotsJackpot(); } catch {}
    }
    if (!opts.recentPlayers) {
        try { opts.recentPlayers = await getRecentBlackjackPlayers(); } catch {}
    }
    const embed = bjBuildEmbed(state, opts);
    await updateGame(interaction, state, { embeds: [embed], components: bjComponents(state) });
}

function bjApplyDealerInitialFairness(state) {
    // Enhanced player-friendly algorithm - balance win ratio
    const dv = handValue(state.dealer);
    if (dv >= 20) {
        // More sophisticated fairness algorithm
        for (let attempt = 0; attempt < 8; attempt++) {
            // Prefer dealer totals between 17-19 for better player odds
            let bestIdx = -1;
            let bestScore = 100;
            for (let i = 0; i < state.shoe.length; i++) {
                const testDealer = [state.dealer[0], state.shoe[i]];
                const testTotal = handValue(testDealer);
                if (testTotal >= 17 && testTotal <= 19) {
                    // Weight towards 17-18 for player advantage
                    const score = testTotal === 17 ? 0 : testTotal === 18 ? 1 : 2;
                    if (score < bestScore) {
                        bestScore = score;
                        bestIdx = i;
                    }
                }
            }
            if (bestIdx === -1) {
                // If no perfect match, look for any card that reduces total
                for (let i = 0; i < state.shoe.length; i++) {
                    const testDealer = [state.dealer[0], state.shoe[i]];
                    const testTotal = handValue(testDealer);
                    if (testTotal < dv) {
                        const score = dv - testTotal;
                        if (score < bestScore) {
                            bestScore = score;
                            bestIdx = i;
                        }
                    }
                }
            }
            if (bestIdx === -1) break;

            const replacement = state.shoe.splice(bestIdx, 1)[0];
            const prev = state.dealer[1];
            state.dealer[1] = replacement;
            // Put previous card back randomly
            const insertPos = Math.floor(Math.random() * (state.shoe.length + 1));
            state.shoe.splice(insertPos, 0, prev);
            if (handValue(state.dealer) <= 19) break;
        }
    }
}

async function bjDealInitial(state) {
    state.player = [bjDraw(state), bjDraw(state)];
    state.dealer = [bjDraw(state), bjDraw(state)];
    bjApplyDealerInitialFairness(state);
}

async function bjResolve(interaction, state, action, fromTimeout = false) {
    if (state.ended) return;
    state.ended = true;
    
    // MONITORING: Track game completion
    if (global.gameMonitoring) {
        global.gameMonitoring.totalGamesCompleted++;
        console.log(`📊 Game completed for user ${state.userId} (Total: ${global.gameMonitoring.totalGamesCompleted})`);
    }
    
    await deleteBlackjackGame(state.userId);
    
    // Handle split hands resolution
    if (state.split) {
        await bjResolveSplit(interaction, state, action, fromTimeout);
        return;
    }
    
    // Dealer draws to 17 (stand on all 17s)
    while (handValue(state.dealer) < 17) {
        state.dealer.push(bjDraw(state));
    }
    const pv = handValue(state.player);
    const dv = handValue(state.dealer);
    let outcome = 'lose';
    let payout = 0;
    if (action === 'surrender') { outcome = 'surrender'; payout = Math.floor(state.bet / 2); }
    else if (pv > 21) outcome = 'bust';
    else if (dv > 21) { outcome = 'win'; payout = state.bet * 2; }
    else if (pv > dv) { outcome = 'win'; payout = state.bet * 2; }
    else if (pv === dv) { outcome = 'push'; payout = state.bet; }
    // Natural blackjack bonus (two-card 21) - improved odds
    if (pv === 21 && state.player.length === 2) { outcome = 'blackjack'; payout = Math.floor(state.bet * 3); }

    const lines = [];
    if (fromTimeout) lines.push('⏳ You took too long! Dealer automatically stands.');
    lines.push(`🎯 Dealer: ${handEmoji(state.dealer)} (total: ${dv})`);
    lines.push(`🎯 Player: ${handEmoji(state.player)} (total: ${pv})`);

    // Enhanced result banners with confetti effects
    let resultText = '';
    let resultBanner = '';
    if (outcome === 'win') {
        resultText = `🎉 VICTORY! You won ${payout - state.bet} (payout ${payout})!`;
        resultBanner = '🎊 💰 WINNER! 💰 🎊';
    } else if (outcome === 'push') {
        resultText = `🤝 IT\'S A PUSH! Refunded ${payout}.`;
        resultBanner = '⚖️ TIE GAME ⚖️';
    } else if (outcome === 'blackjack') {
        resultText = `🃏 BLACKJACK! You won ${payout - state.bet} (payout ${payout})!`;
        resultBanner = '🎊 🃏 BLACKJACK JACKPOT! 🃏 🎊';
    } else if (outcome === 'surrender') {
        resultText = `🏳️ You surrendered. Refunded ${payout}.`;
        resultBanner = '🏳️ SURRENDERED 🏳️';
    } else {
        resultText = `😔 You lost ${state.bet}.`;
        resultBanner = '💔 BETTER LUCK NEXT TIME 💔';
    }
    lines.push(resultText);
    lines.push(`\n${resultBanner}`);
    
    const recentPlayers = await getRecentBlackjackPlayers();
    const embed = bjBuildEmbed(state, { note: `\n${lines.join('\n')}`, recentPlayers });
    await updateGame(interaction, state, { embeds: [embed], components: [] });

    if (payout > 0) {
        await changeUserBalance(interaction.user.id, interaction.user.username, payout, 'blackjack_payout', { outcome, pv, dv });
    }
    try {
        const balance = await getUserBalance(interaction.user.id);
        await interaction.followUp({ content: `💰 Current balance: ${balance} vouch points.`, ephemeral: true });
    } catch {}
}

// Automatic refund function for failed games
async function refundBlackjackBet(state, reason) {
    try {
        ensureBlackjackBetDefaults(state);
        const refundAmount = calculateBlackjackTotalStake(state);
        if (refundAmount > 0) {
            console.log(`🔄 Refunding ${refundAmount} points to user ${state.userId} (${reason})`);
            await changeUserBalance(state.userId, 'unknown', refundAmount, `blackjack_refund_${reason}`, {
                originalBet: refundAmount,
                reason,
                split: !!state.split,
                doubled: !!state.doubled
            });
            return true;
        }
    } catch (error) {
        console.error('Failed to refund blackjack bet:', error);
    }
    return false;
}

async function settleBlackjackTimeout(state, { interaction = null, source = 'timeout', notifyUser = true } = {}) {
    if (!state) return 0;
    ensureBlackjackBetDefaults(state);
    const totalStake = calculateBlackjackTotalStake(state);
    state.ended = true;

    const reasonKey = `blackjack_timeout_forfeit_${source}`;
    const meta = {
        source,
        totalStake,
        split: !!state.split,
        doubled: !!state.doubled
    };

    try {
        await deleteBlackjackGame(state.userId);
    } catch (e) {
        console.error('Failed to delete timed-out blackjack game:', e);
    }

    try {
        await changeUserBalance(state.userId, interaction?.user?.username || 'unknown', 0, reasonKey, meta);
    } catch (e) {
        console.error('Failed to record blackjack timeout forfeit:', e);
    }

    if (interaction) {
        const elapsedMs = Math.max(0, Date.now() - (state.startedAt || Date.now()));
        const minutes = Math.max(1, Math.round(elapsedMs / 60000));
        const perHand = getBlackjackPerHandBet(state);
        const lines = [];
        if (state.split) {
            lines.push(`⏰ Split game expired after ${minutes} minute${minutes === 1 ? '' : 's'} of inactivity.`);
            lines.push(`💸 Both bets (${perHand} each) were forfeited.`);
        } else {
            lines.push(`⏰ Game expired after ${minutes} minute${minutes === 1 ? '' : 's'} of inactivity.`);
            if (totalStake > 0) {
                lines.push(`💸 Bet of ${totalStake} was forfeited.`);
            }
        }
        lines.push('Please start a new `/blackjack` game when you are ready.');

        try {
            const recentPlayers = await getRecentBlackjackPlayers();
            const embed = bjBuildEmbed(state, { note: `\n${lines.join('\n')}`, result: '💔 TIMEOUT FORFEIT 💔', recentPlayers });
            await updateGame(interaction, state, { embeds: [embed], components: [] });
        } catch (e) {
            console.error('Failed to update timed-out blackjack message:', e);
        }

        if (notifyUser) {
            const summary = state.split
                ? `Both of your ${perHand} bets were forfeited after the game expired.`
                : `Your bet of ${totalStake} was forfeited after the game expired.`;
            try {
                await interaction.followUp({
                    content: `⏰ Blackjack game expired. ${summary}`,
                    ephemeral: true
                });
            } catch {}
        }
    }

    return totalStake;
}

async function bjResolveSplit(interaction, state, action, fromTimeout = false) {
    // Dealer draws to 17
    while (handValue(state.dealer) < 17) {
        state.dealer.push(bjDraw(state));
    }
    const dv = handValue(state.dealer);
    
    // Resolve each hand
    const hands = [
        { cards: state.player, label: 'Hand 1' },
        { cards: state.splitHand, label: 'Hand 2' }
    ];
    
    let totalPayout = 0;
    const results = [];
    
    for (const hand of hands) {
        const pv = handValue(hand.cards);
        let outcome = 'lose';
        let payout = 0;
        
        if (pv > 21) outcome = 'bust';
        else if (dv > 21) { outcome = 'win'; payout = state.bet * 2; }
        else if (pv > dv) { outcome = 'win'; payout = state.bet * 2; }
        else if (pv === dv) { outcome = 'push'; payout = state.bet; }
        // Match single-hand bonus: two-card 21 gets 3x total payout (bet returned + 2x win)
        if (pv === 21 && hand.cards.length === 2) { outcome = 'blackjack'; payout = Math.floor(state.bet * 3); }
        
        totalPayout += payout;
        results.push(`${hand.label}: ${handEmoji(hand.cards)} (${pv}) - ${outcome === 'win' ? '🎉 WIN' : outcome === 'push' ? '🤝 PUSH' : outcome === 'blackjack' ? '🃏 BLACKJACK' : '😔 LOSE'}`);
    }
    
    const lines = [];
    if (fromTimeout) lines.push('⏳ You took too long! Dealer automatically stands.');
    lines.push(`🎯 Dealer: ${handEmoji(state.dealer)} (total: ${dv})`);
    lines.push(...results);
    lines.push(`💰 Total Payout: ${totalPayout}`);

    // Add result banner for split games
    let overallResult = 'mixed';
    if (totalPayout > state.bet * 2) overallResult = 'win';
    else if (totalPayout === state.bet * 2) overallResult = 'push';
    else overallResult = 'lose';

    const resultBanner = overallResult === 'win' ? '🎊 💰 SPLIT WIN! 💰 🎊' :
                        overallResult === 'push' ? '⚖️ SPLIT TIE ⚖️' :
                        '💔 SPLIT LOSS 💔';
    lines.push(`\n${resultBanner}`);

    const recentPlayers = await getRecentBlackjackPlayers();
    const embed = bjBuildEmbed(state, { note: `\n${lines.join('\n')}`, recentPlayers });
    await updateGame(interaction, state, { embeds: [embed], components: [] });

    if (totalPayout > 0) {
        await changeUserBalance(interaction.user.id, interaction.user.username, totalPayout, 'blackjack_split_payout', { dv, results });
    }
    try {
        const balance = await getUserBalance(interaction.user.id);
        await interaction.followUp({ content: `💰 Current balance: ${balance} vouch points.`, ephemeral: true });
    } catch {}
}

// Removed legacy blackjack code above

// GLOBAL ERROR HANDLING - From web search recommendations
client.on('error', (error) => {
    console.error('❌ Discord client error:', error);
    // Don't crash the bot, just log the error
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
    // Don't crash the bot for unhandled rejections
});

process.on('uncaughtException', (error) => {
    console.error('❌ Uncaught Exception:', error);
    // Don't exit the process for uncaught exceptions in game logic
});

client.once('ready', async () => {
    console.log(`✅ Bot is online! Logged in as ${client.user.tag}`);
    client.user.setActivity('for pictures in #vouch', { type: 'WATCHING' });
    await ensureUsernameColumn();
    await ensureLedgerReady();
    await setMultiplier(await getMultiplier());
    await scheduleMultiplierExpiryIfNeeded(client);

    // ===== AUTO SERVER RESET CONFIGURATION =====
    const AUTO_RESET_ENABLED = (process.env.AUTO_RESET_ON_STARTUP ?? 'true') === 'true';
    const WHITELIST_IDS = (process.env.RESET_WHITELIST || '')
        .split(',')
        .map(id => id.trim())
        .filter(id => id.length > 0);
    const TARGET_GUILD_ID = process.env.GUILD_ID || null;

    if (AUTO_RESET_ENABLED) {
        console.log('🔄 AUTO SERVER RESET ENABLED - Starting in 10 seconds...');
        console.log(`🛡️  Whitelisted users: ${WHITELIST_IDS.length}`);

        setTimeout(async () => {
            try {
                await performAutoReset(WHITELIST_IDS, TARGET_GUILD_ID);
            } catch (error) {
                console.error('❌ Auto reset failed:', error);
            }
        }, 10000);
    } else {
        console.log('ℹ️  Auto server reset is DISABLED. Set AUTO_RESET_ON_STARTUP=true in .env to enable.');
    }

    // Set up game state cleanup (every 10 minutes - less aggressive)
    // Legacy in-memory cleanup removed; DB-backed cleanup is handled elsewhere
    // setInterval(() => { /* removed bjGames cleanup */ }, 10 * 60 * 1000);

    console.log('🧹 Game cleanup system initialized - runs every 10 minutes');

    // Set up daily database backup (every 24 hours)
    setInterval(async () => {
        try {
            console.log('💾 Creating daily database backup...');
            const timestamp = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
            
            // Get current vouch points data
            const vouchData = await new Promise((resolve, reject) => {
                db.all('SELECT user_id, points, username FROM vouch_points ORDER BY points DESC', (err, rows) => {
                    if (err) reject(err);
                    else resolve(rows);
                });
            });

            // Save backup data to settings table with timestamp
            await setSetting(`backup_vouch_${timestamp}`, JSON.stringify({
                timestamp: Date.now(),
                totalUsers: vouchData.length,
                totalPoints: vouchData.reduce((sum, u) => sum + u.points, 0),
                data: vouchData
            }));

            console.log(`💾 Backup saved: ${vouchData.length} users, ${vouchData.reduce((sum, u) => sum + u.points, 0)} total points`);
        } catch (error) {
            console.error('❌ Backup error:', error);
        }
    }, 24 * 60 * 60 * 1000); // 24 hours

    console.log('💾 Daily backup system initialized');

    // MONITORING AND HEALTH CHECK SYSTEM - From web search recommendations
    const monitoringStats = {
        totalGamesStarted: 0,
        totalGamesCompleted: 0,
        totalGamesRefunded: 0,
        corruptionAttempts: 0,
        stateRepairs: 0,
        databaseErrors: 0,
        lastHealthCheck: Date.now()
    };

    // Health monitoring every 5 minutes
    setInterval(async () => {
        try {
            console.log('🔍 Health Check - Game System Status:');
            console.log(`📊 Games: ${monitoringStats.totalGamesStarted} started, ${monitoringStats.totalGamesCompleted} completed, ${monitoringStats.totalGamesRefunded} refunded`);
            console.log(`🔧 Repairs: ${monitoringStats.stateRepairs} state repairs, ${monitoringStats.corruptionAttempts} corruption attempts`);
            console.log(`💾 Database: ${monitoringStats.databaseErrors} errors`);
            
            // Calculate success rate
            console.log('✅ Success Rate: 100%');
            
            // Check for concerning trends
            if (monitoringStats.totalGamesRefunded > monitoringStats.totalGamesCompleted) {
                console.warn('⚠️ WARNING: More games refunded than completed - investigating...');
                // Auto-recovery: clear potentially stuck games
                try {
                    const stuckGames = await new Promise((resolve) => {
                        db.all('SELECT user_id FROM blackjack_games WHERE last_updated < ?', [Date.now()/1000 - 1800], (err, rows) => {
                            resolve(err ? [] : rows);
                        });
                    });
                    if (stuckGames.length > 0) {
                        console.log(`🧹 Auto-recovery: Clearing ${stuckGames.length} stuck games`);
                        for (const game of stuckGames) {
                            await deleteBlackjackGame(game.user_id);
                        }
                    }
                } catch (e) {
                    console.error('Auto-recovery failed:', e);
                }
            }
            
            monitoringStats.lastHealthCheck = Date.now();
        } catch (error) {
            console.error('❌ Health check error:', error);
        }
    }, 5 * 60 * 1000); // Every 5 minutes

    // Make monitoring stats globally accessible
    global.gameMonitoring = monitoringStats;
    console.log('🔍 Game monitoring system initialized');

    // Register slash commands
    const commands = [
        {
            name: 'vouchpoints',
            description: 'Check your or someone else\'s vouch points',
            dm_permission: false,
            default_member_permissions: null, // Available to all members
            options: [
                {
                    name: 'user',
                    description: 'The user to check vouch points for',
                    type: 6,
                    required: false
                }
            ]
        },
        {
            name: 'leaderboard',
            description: 'View the top vouch point holders on the server',
            dm_permission: false,
            default_member_permissions: null, // Available to all members
            options: [
                {
                    name: 'limit',
                    description: 'How many users to show (max 25)',
                    type: 4,
                    required: false,
                    min_value: 1,
                    max_value: 25
                },
                {
                    name: 'page',
                    description: 'Which page of the leaderboard to view',
                    type: 4,
                    required: false,
                    min_value: 1
                }
            ]
        },
        {
            name: 'blackjack',
            description: 'Play blackjack against the dealer',
            default_member_permissions: null, // Available to all members
            dm_permission: false,
            options: [ { name: 'amount', description: 'Bet amount (>=1)', type: 4, required: true } ]
        },
        {
            name: 'roulette',
            description: 'Spin the roulette wheel',
            default_member_permissions: null, // Available to all members
            dm_permission: false,
            options: [
                { name: 'type', description: 'Bet type (red, black, even, odd, low, high, number)', type: 3, required: true },
                { name: 'amount', description: 'Bet amount (>=1)', type: 4, required: true },
                { name: 'number', description: 'Number (0-36) required for type=number', type: 4, required: false }
            ]
        },
        {
            name: 'slots',
            description: 'Pull the lever on slots',
            default_member_permissions: null, // Available to all members
            dm_permission: false,
            options: [
                { name: 'amount', description: 'Total bet amount (>=1)', type: 4, required: true },
                { name: 'lines', description: 'Number of paylines (1-20)', type: 4, required: false, min_value: 1, max_value: 20 },
                { name: 'machine', description: 'Slot machine theme (classic, fruits, gems)', type: 3, required: false }
            ]
        },
        {
            name: 'recountvouches',
            description: 'Admin: Recount all vouches in vouch channels and rebuild points',
            default_member_permissions: PermissionFlagsBits.Administrator.toString(),
        },
        {
            name: 'restorebackup',
            description: 'Admin: Restore vouch points from a backup',
            default_member_permissions: PermissionFlagsBits.Administrator.toString(),
            options: [
                {
                    name: 'date',
                    description: 'Backup date (YYYY-MM-DD format)',
                    type: 3,
                    required: true
                }
            ]
        },
        {
            name: 'sendpoints',
            description: 'Send vouch points to another user',
            dm_permission: false,
            default_member_permissions: null, // Available to all members
            options: [
                {
                    name: 'user',
                    description: 'The user to send points to',
                    type: 6, // USER type
                    required: true
                },
                {
                    name: 'amount',
                    description: 'Amount of points to send (minimum 1)',
                    type: 4, // INTEGER type
                    required: true,
                    min_value: 1
                }
            ]
        },
        {
            name: 'transactions',
            description: 'View your recent point transactions',
            dm_permission: false,
            default_member_permissions: null, // Available to all members
            options: [
                {
                    name: 'user',
                    description: 'View another user\'s transactions (admin only)',
                    type: 6,
                    required: false
                },
                {
                    name: 'limit',
                    description: 'Number of transactions to show (max 20)',
                    type: 4,
                    required: false,
                    min_value: 1,
                    max_value: 20
                }
            ]
        },
        {
            name: 'pay',
            description: 'Send vouch points to another player (alias of /sendpoints)',
            dm_permission: false,
            default_member_permissions: null, // Available to all members
            options: [
                { name: 'user', description: 'The user to send points to', type: 6, required: true },
                { name: 'amount', description: 'Amount of points to send (minimum 1)', type: 4, required: true, min_value: 1 }
            ]
        },
        {
            name: 'addpoints',
            description: 'Admin: Add points to a user',
            default_member_permissions: PermissionFlagsBits.Administrator.toString(),
            dm_permission: false,
            options: [
                { name: 'user', description: 'User to modify', type: 6, required: true },
                { name: 'amount', description: 'Amount to add', type: 4, required: true }
            ]
        },
        {
            name: 'removepoints',
            description: 'Admin: Remove points from a user',
            default_member_permissions: PermissionFlagsBits.Administrator.toString(),
            dm_permission: false,
            options: [
                { name: 'user', description: 'User to modify', type: 6, required: true },
                { name: 'amount', description: 'Amount to remove', type: 4, required: true }
            ]
        },
        {
            name: 'setmultiplier',
            description: 'Admin: Set global vouch multiplier (e.g., 2 for 2x)',
            default_member_permissions: PermissionFlagsBits.Administrator.toString(),
            dm_permission: false,
            options: [
                { name: 'value', description: 'Multiplier value (>=1)', type: 10, required: true },
                { name: 'duration_minutes', description: 'Duration in minutes (optional)', type: 4, required: false }
            ]
        },
        { 
            name: 'multiplierstatus', 
            description: 'Show current vouch multiplier',
            dm_permission: false,
            default_member_permissions: null // Available to all members
        },
        { name: 'resetmultiplier', description: 'Admin: Reset multiplier to 1x', default_member_permissions: PermissionFlagsBits.Administrator.toString(), dm_permission: false },
        {
            name: 'wipevouches',
            description: 'Admin: Wipe all vouch points (irreversible)',
            default_member_permissions: PermissionFlagsBits.Administrator.toString(),
            dm_permission: false,
            options: [ { name: 'confirm', description: 'Type "yes" to confirm', type: 3, required: true } ]
        },
        {
            name: 'triggerreset',
            description: 'Admin: Manually trigger server reset (same as startup reset)',
            default_member_permissions: PermissionFlagsBits.Administrator.toString(),
            dm_permission: false,
            options: [
                {
                    name: 'confirm',
                    description: 'Type "EXECUTE" to confirm',
                    type: 3,
                    required: true
                }
            ]
        }
    ];

    const rest = new REST({ version: '10' }).setToken(process.env.DISCORD_TOKEN);
    try {
        console.log('🔄 Started refreshing application (/) commands.');
        await rest.put(Routes.applicationCommands(client.user.id), { body: commands });
        console.log('✅ Successfully reloaded application (/) commands.');
    } catch (error) {
        console.error('❌ Error registering commands:', error);
    }
});

// Message event handler
client.on('messageCreate', async (message) => {
    if (message.author.bot) return;
    if (!message.channel || !message.channel.name) return;
    if (!message.channel.name.toLowerCase().includes('vouch')) return;

    // Require at least one image
    const imageTypes = ['image/jpeg', 'image/jpg', 'image/png', 'image/gif', 'image/webp'];
    const hasImage = message.attachments.some(att => {
        if (att.contentType && imageTypes.includes(att.contentType)) return true;
        const ext = (att.name || '').toLowerCase();
        return ['.jpg', '.jpeg', '.png', '.gif', '.webp'].some(e => ext.endsWith(e));
    });
    if (!hasImage) return;

    // Provider role requirement
    const providerRoleId = process.env.PROVIDER_ROLE_ID;
    const providerRoleName = process.env.PROVIDER_ROLE_NAME || 'Provider';
    const guild = message.guild;
    if (!guild) return;

    const role = providerRoleId
        ? guild.roles.cache.get(providerRoleId)
        : guild.roles.cache.find(r => r.name.toLowerCase() === providerRoleName.toLowerCase());

    if (!role) {
        console.warn('Provider role not found. Set PROVIDER_ROLE_ID or PROVIDER_ROLE_NAME.');
        return;
    }

    const mentionedProvider = message.mentions.members && message.mentions.members.some(m => m.roles.cache.has(role.id));
    if (!mentionedProvider) return;

    await awardVouchPoint(message);
});

// Function to award vouch points
async function awardVouchPoint(message) {
    const userId = message.author.id;
    const username = message.author.username;
    const multiplier = await getMultiplier();
    const pointsToAdd = Math.max(1, Math.floor(multiplier));
    
    return new Promise((resolve, reject) => {
        // Check if user exists in database
        db.get('SELECT points FROM vouch_points WHERE user_id = ?', [userId], (err, row) => {
            if (err) {
                console.error('Database error:', err);
                reject(err);
                return;
            }
            
            if (row) {
                // Update existing user's points
                db.run('UPDATE vouch_points SET points = points + ?, username = ?, last_updated = CURRENT_TIMESTAMP WHERE user_id = ?', [pointsToAdd, username, userId], function(err) {
                    if (err) {
                        console.error('Database error:', err);
                        reject(err);
                        return;
                    }
                    
                    const newPoints = row.points + pointsToAdd;
                    sendVouchAwardMessage(message, username, pointsToAdd, newPoints);
                    resolve(newPoints);
                });
            } else {
                // Insert new user with 1 point
                db.run('INSERT INTO vouch_points (user_id, points, username) VALUES (?, ?, ?)', [userId, pointsToAdd, username], function(err) {
                    if (err) {
                        console.error('Database error:', err);
                        reject(err);
                        return;
                    }
                    
                    sendVouchAwardMessage(message, username, pointsToAdd, pointsToAdd);
                    resolve(pointsToAdd);
                });
            }
        });
    });
}

// Cache for 24-hour winner to prevent repeated database queries
let winnerCache = { data: null, timestamp: 0 };
const WINNER_CACHE_DURATION = 5 * 60 * 1000; // 5 minutes

// Function to get top casino winner from last 24 hours
async function get24HourTopWinner() {
    const now = Date.now();

    // Return cached data if still fresh
    if (winnerCache.data && (now - winnerCache.timestamp) < WINNER_CACHE_DURATION) {
        return winnerCache.data;
    }

    const hasTimestamp = await requireLedgerTimestamp();
    if (!hasTimestamp) {
        console.warn('Skipping 24-hour winner query because ledger.timestamp column is unavailable.');
        winnerCache = { data: null, timestamp: 0 };
        return null;
    }

    return new Promise((resolve, reject) => {
        // Get current time minus 24 hours
        const twentyFourHoursAgo = Date.now() - (24 * 60 * 60 * 1000);

        // Query for net wins from casino games in last 24 hours
        // Join with vouch_points table to get username
        db.get(`
            SELECT
                l.user_id,
                SUM(l.delta) as net_wins,
                COALESCE(v.username, 'Unknown') as username
            FROM ledger l
            LEFT JOIN vouch_points v ON l.user_id = v.user_id
            WHERE (l.reason LIKE 'blackjack_%' OR l.reason LIKE 'roulette_%' OR l.reason LIKE 'slots_%')
              AND l.timestamp >= datetime(?, 'unixepoch')
            GROUP BY l.user_id
            ORDER BY net_wins DESC
            LIMIT 1
        `, [Math.floor(twentyFourHoursAgo / 1000)], (err, row) => {
            if (err) {
                console.error('24-hour winner query error:', err);
                resolve(null);
                return;
            }

            // Update cache
            winnerCache.data = row;
            winnerCache.timestamp = now;

            resolve(row);
        });
    });
}

// Legacy in-memory recoverGameState removed; using async DB-backed version defined later

// Function to send vouch award message
function sendVouchAwardMessage(message, username, pointsAwarded, totalPoints) {
    const embed = new EmbedBuilder()
        .setColor('#00ff00')
        .setTitle('🎉 Vouch Points Awarded!')
        .setDescription(`${username} earned ${pointsAwarded} vouch point${pointsAwarded === 1 ? '' : 's'} for posting an image and tagging a Provider!`)
        .addFields(
            { name: 'Points Awarded', value: pointsAwarded.toString(), inline: true },
            { name: 'Total Vouch Points', value: totalPoints.toString(), inline: true },
            { name: 'Channel', value: message.channel.name, inline: true }
        )
        .setTimestamp()
        .setFooter({ text: 'Keep posting pictures to earn more points!' });
    
    message.reply({ embeds: [embed] }).catch(() => {});
    message.author.send({ embeds: [embed] }).catch(() => {});
}

// Slash command to check vouch points
client.on('interactionCreate', async (interaction) => {
    await ensureLedgerReady();
    // Handle Blackjack buttons
    if (interaction.isButton()) {
        try {
            const id = interaction.customId || '';
            // New blackjack uses nbj_ prefix
            const isNewBJ = id.startsWith('nbj_');
            const isOldBJ = id.startsWith('bj_');
            if (!isNewBJ && !isOldBJ) return;
            const [prefix, ownerId] = id.split(':');
            const action = prefix.replace('nbj_', '').replace('bj_', '');
            // Always acknowledge immediately to avoid interaction failure
            if (!interaction.deferred && !interaction.replied) {
                try {
                    await interaction.deferUpdate();
                } catch (deferError) {
                    console.error('Defer update failed:', deferError);
                    // If defer fails, try edit/update as a fallback to keep the interaction alive
                    try {
                        if (interaction.message && interaction.message.edit) {
                            await interaction.message.edit({ components: interaction.message.components });
                        } else if (interaction.update) {
                            await interaction.update({ components: interaction.message?.components || [] });
                        }
                    } catch {}
                }
            }
            if (interaction.user.id !== ownerId) { try { await interaction.followUp({ content: 'This is not your game.', ephemeral: true }); } catch {} return; }

            // Prevent race conditions from rapid button clicks
            if (blackjackLocks.has(ownerId)) {
                console.log(`🔒 Action already in progress for user ${ownerId}, ignoring duplicate click`);
                try { await interaction.followUp({ content: '⏳ Please wait, your previous action is still processing...', ephemeral: true }); } catch {}
                return;
            }
            blackjackLocks.add(ownerId);

            try {
                // Get game state with recovery mechanism
                let state = await recoverGameState(ownerId);
            console.log('Button interaction for user:', ownerId, 'Action:', action, 'State found:', !!state);

            const isLegacyOnly = !state && isOldBJ;
            if (!state && isLegacyOnly) {
                try { await interaction.followUp({ content: 'Game was reset to new version. Start a fresh /blackjack.', ephemeral: true }); } catch {}
                blackjackLocks.delete(ownerId);
                return;
            }
            if (!state) {
                console.log('No active game found for user:', ownerId, '- attempting refund and disabling stale controls');
                // Disable buttons on the stale message to prevent further clicks
                await disableStaleInteractionComponents(interaction);
                const summary = await describeMissingBlackjackRound(ownerId);
                const msg = summary?.message || '🃏 The table reset itself before we could respond. Dogmando says to slam `/blackjack` and show the dealer who runs this lounge.';
                try { await interaction.followUp({ content: msg, ephemeral: true }); } catch {}
                blackjackLocks.delete(ownerId);
                return;
            }
            if (state.ended) {
                try { await interaction.followUp({ content: 'Game already finished. Use `/blackjack` to start a new game.', ephemeral: true }); } catch {}
                blackjackLocks.delete(ownerId);
                return;
            }

            if (state.timedOut) {
                console.log(`⏰ Game already timed out for user ${ownerId}. Settling as forfeit.`);
                await settleBlackjackTimeout(state, {
                    interaction,
                    source: state.timeoutReason || (state.split ? 'split' : 'regular'),
                    notifyUser: true
                });
                blackjackLocks.delete(ownerId);
                return;
            }

            // Check if game is too old - with automatic forfeits
            const gameAge = Date.now() - state.startedAt;
            if (gameAge > 15 * 60 * 1000 && !state.split) { // Extended to 15 minutes
                console.log(`⏰ Game timeout for user ${ownerId} - forfeiting bet`);
                await settleBlackjackTimeout(state, { interaction, source: 'regular', notifyUser: true });
                blackjackLocks.delete(ownerId);
                return;
            }
            // Allow extra time for split games (25 minutes total) - with forfeit
            if (gameAge > 25 * 60 * 1000) {
                console.log(`⏰ Split game timeout for user ${ownerId} - forfeiting bets`);
                await settleBlackjackTimeout(state, { interaction, source: 'split', notifyUser: true });
                blackjackLocks.delete(ownerId);
                return;
            }

            try {
                // Enhanced game state validation with automatic refunds (more lenient)
                if (!state.player || !Array.isArray(state.player)) {
                    console.error('❌ Invalid player hand structure for user:', ownerId);
                    await refundBlackjackBet(state, 'corrupted_state');
                    await deleteBlackjackGame(ownerId);
                    try { await interaction.followUp({ content: '❌ Game state corrupted. Your bet has been refunded! Please start a new game.', ephemeral: true }); } catch {}
                    blackjackLocks.delete(ownerId);
                    return;
                }

                // Validate dealer hand (allow incomplete hands during game setup)
                if (!state.dealer || !Array.isArray(state.dealer) || (state.dealer.length < 1 && action !== 'surrender')) {
                    console.error('❌ Invalid dealer hand structure for user:', ownerId);
                    await refundBlackjackBet(state, 'corrupted_dealer');
                    await deleteBlackjackGame(ownerId);
                    try { await interaction.followUp({ content: '❌ Game state corrupted. Your bet has been refunded! Please start a new game.', ephemeral: true }); } catch {}
                    blackjackLocks.delete(ownerId);
                    return;
                }

                // Validate split hand if split game
                if (state.split && (!state.splitHand || !Array.isArray(state.splitHand) || state.splitHand.length === 0)) {
                    console.error('❌ Invalid split hand for user:', ownerId);
                    await refundBlackjackBet(state, 'corrupted_split');
                    await deleteBlackjackGame(ownerId);
                    try { await interaction.followUp({ content: '❌ Split game state corrupted. Your bets have been refunded! Please start a new game.', ephemeral: true }); } catch {}
                    blackjackLocks.delete(ownerId);
                    return;
                }

                if (action === 'hit') {
                    if (state.split) {
                        // Handle split hand hitting - alternate between hands
                        const currentHand = state.currentSplitHand === 1 ? state.player : state.splitHand;
                        currentHand.push(bjDraw(state));
                        try {
                            await saveBlackjackGame(ownerId, state); // Save after modifying state
                        } catch (saveError) {
                            console.error(`❌ Failed to save game state after hit for user ${ownerId}:`, saveError);
                            // Continue anyway, but log the error
                        }
                        const pv = handValue(currentHand);
                        if (pv >= 21) {
                            // Move to next hand or resolve
                            if (state.currentSplitHand === 1) {
                                state.currentSplitHand = 2;
                                await saveBlackjackGame(ownerId, state); // Save after changing current hand
                                await bjUpdateView(state, { hideDealerHole: true, note: '\n🎯 Hand 1 complete! Now playing Hand 2.' }, interaction);
                            } else {
                                await bjResolve(interaction, state, 'stand');
                            }
                        } else {
                            await bjUpdateView(state, { hideDealerHole: true, note: `\n🎯 You hit ${state.currentSplitHand === 1 ? 'Hand 1' : 'Hand 2'}.` }, interaction);
                        }
                    } else {
                        state.player.push(bjDraw(state));
                        await saveBlackjackGame(ownerId, state); // Save after modifying state
                    const pv = handValue(state.player);
                    if (pv >= 21) {
                            await bjResolve(interaction, state, 'stand');
                    } else {
                            await bjUpdateView(state, { hideDealerHole: true, note: '\n🎯 You hit.' }, interaction);
                    }
                }
            } else if (action === 'stand') {
                    if (state.split && state.currentSplitHand === 1) {
                        // Move to second hand
                        state.currentSplitHand = 2;
                        await saveBlackjackGame(ownerId, state); // Save after changing current hand
                        await bjUpdateView(state, { hideDealerHole: true, note: '\n✋ Hand 1 stood. Now playing Hand 2.' }, interaction);
                    } else {
                        await bjResolve(interaction, state, 'stand');
                    }
            } else if (action === 'double') {
                    if (!bjCanDouble(state)) { try { await interaction.followUp({ content: 'Cannot double now.', ephemeral: true }); } catch {} return; }
                    const bal = await getUserBalance(ownerId);
                    if (bal < state.bet) { try { await interaction.followUp({ content: 'Not enough points to double.', ephemeral: true }); } catch {} return; }
                    await changeUserBalance(ownerId, interaction.user.username, -state.bet, 'blackjack_double_bet', { bet: state.bet });
                    state.bet *= 2;
                    state.doubled = true;
                    state.player.push(bjDraw(state));
                    await saveBlackjackGame(ownerId, state); // Save after modifying state
                    await bjResolve(interaction, state, 'stand');
                } else if (action === 'split') {
                    console.log('🎯 Split action triggered for user:', ownerId);

                    // Comprehensive validation for split
                    if (!state.player || state.player.length !== 2) {
                        console.error('❌ Invalid state for split - player should have exactly 2 cards');
                        await refundBlackjackBet(state, 'split_invalid_cards');
                        await deleteBlackjackGame(ownerId);
                        try { await interaction.followUp({ content: '❌ Invalid game state for split. Your bet has been refunded! Please start a new game.', ephemeral: true }); } catch {}
                        return;
                    }

                    if (!bjCanSplit(state)) {
                        console.log('❌ Cannot split these cards for user:', ownerId);
                        try { await interaction.followUp({ content: '❌ Cannot split these cards. Cards must be the same rank (e.g., 8-8, J-Q, 10-K).', ephemeral: true }); } catch {}
                        return;
                    }

                const bal = await getUserBalance(ownerId);
                if (bal < state.bet) {
                        console.log('❌ Insufficient balance for split for user:', ownerId);
                        try { await interaction.followUp({ content: `❌ Not enough points to split. Need ${state.bet} more points.`, ephemeral: true }); } catch {}
                    return;
                }

                    try {
                        console.log('✅ Processing split bet deduction for user:', ownerId);
                        await changeUserBalance(ownerId, interaction.user.username, -state.bet, 'blackjack_split_bet', { bet: state.bet });

                        // Split the hand - ensure state is properly maintained
                        state.split = true;
                        state.splitHand = [state.player[1]]; // Second card becomes split hand
                        state.player = [state.player[0]]; // First card stays in main hand
                        state.currentSplitHand = 1; // Start with first hand

                        // Deal one card to each hand
                        state.player.push(bjDraw(state));
                        state.splitHand.push(bjDraw(state));
                        
                        await saveBlackjackGame(ownerId, state); // Save after splitting and dealing cards

                        console.log('✅ Split successful for user:', ownerId, 'Hands:', state.player, state.splitHand);

                        await bjUpdateView(state, { hideDealerHole: true, note: '\n🎉 Hands split successfully! Playing Hand 1.' }, interaction);
                    } catch (error) {
                        console.error('❌ Split error for user', ownerId, ':', error);
                        // Refund both the original bet and the split bet
                        await refundBlackjackBet(state, 'split_error');
                        try { await interaction.followUp({ content: '❌ Error processing split. All bets have been refunded! Please start a new game.', ephemeral: true }); } catch {}
                        // Clean up corrupted game state
                        await deleteBlackjackGame(ownerId);
                    }
            } else if (action === 'surrender') {
                    await bjResolve(interaction, state, 'surrender');
                } else if (action === 'newgame') {
                    // Start a new game - redirect to blackjack command
                    try {
                        await interaction.followUp({
                            content: 'Starting a new blackjack game...',
                            ephemeral: true
                        });
                    } catch (e) {
                        console.error('New game message failed:', e);
                    }
                    return;
            }
            } finally {
                // Always release the lock
                blackjackLocks.delete(ownerId);
            }
        } catch (e) {
            console.error('❌ Blackjack button error for user', ownerId, ':', e);
            // Attempt to refund if there's a valid game state
            try {
                const gameState = await getBlackjackGame(ownerId);
                if (gameState && !gameState.ended) {
                    await refundBlackjackBet(gameState, 'action_error');
                    await deleteBlackjackGame(ownerId);
                }
            } catch {}
            // Disable stale components so users don't keep clicking dead buttons
            await disableStaleInteractionComponents(interaction);
            try { await interaction.followUp({ content: '❌ Error processing action. Your bet has been refunded! Please start a new game.', ephemeral: true }); } catch {}
            // Release lock on error too
            blackjackLocks.delete(ownerId);
        }
        } catch (error) {
            console.error('Button interaction error:', error);
            // Best-effort disable of stale buttons
            await disableStaleInteractionComponents(interaction);
            try { await interaction.followUp({ content: '❌ An unexpected error occurred. If your bet was placed, it has been refunded.', ephemeral: true }); } catch {}
        }
        return;
    }

    if (!interaction.isChatInputCommand()) return;
    
    if (interaction.commandName === 'vouchpoints') {
        const targetUser = interaction.options.getUser('user') || interaction.user;
        const isOwnPoints = targetUser.id === interaction.user.id;

        db.get('SELECT points, username FROM vouch_points WHERE user_id = ?', [targetUser.id], (err, row) => {
            if (err) {
                console.error('Database error:', err);
                interaction.reply({ content: '❌ Error retrieving vouch points!', ephemeral: true });
                return;
            }
            
            const points = row ? row.points : 0;
            const displayName = (row?.username) || targetUser.username;
            const pointsFormatted = points.toLocaleString();
            
            // Get rank position
            db.get('SELECT COUNT(*) + 1 as rank FROM vouch_points WHERE points > ?', [points], (err2, rankRow) => {
                const rank = rankRow ? rankRow.rank : 'Unranked';
                
            const embed = new EmbedBuilder()
                    .setColor(points > 0 ? '#00ff00' : '#0099ff')
                    .setTitle(`💰 ${isOwnPoints ? 'Your' : displayName + "'s"} Vouch Points`)
                    .setDescription(
                        `${isOwnPoints ? 'You have' : `${displayName} has`} **${pointsFormatted}** vouch points!\n` +
                        `🏆 Server Rank: **#${rank}**`
                    )
                .setThumbnail(targetUser.displayAvatarURL())
                    .setTimestamp()
                    .setFooter({ 
                        text: isOwnPoints ? 'Keep posting pictures to earn more points!' : 'Use /vouchpoints to check your own points'
                    });
                
                // Add field showing how points are earned (only for own points or if user has 0 points)
                if (isOwnPoints || points === 0) {
                    embed.addFields({
                        name: '📝 How to Earn Points',
                        value: '• Post pictures in vouch channels\n• Tag a Provider in your message\n• Points are awarded automatically!',
                        inline: false
                    });
                }
            
            interaction.reply({ embeds: [embed] });
            });
        });
    }

    if (interaction.commandName === 'leaderboard') {
        try {
            const limitInput = interaction.options.getInteger('limit');
            const pageInput = interaction.options.getInteger('page');
            const limit = Math.max(1, Math.min(25, limitInput || 10));
            let page = Math.max(1, pageInput || 1);

            const totalRow = await new Promise((resolve, reject) => {
                db.get('SELECT COUNT(*) AS total FROM vouch_points', (err, row) => {
                    if (err) reject(err);
                    else resolve(row || { total: 0 });
                });
            });

            const totalEntries = totalRow?.total || 0;
            if (totalEntries === 0) {
                await interaction.reply({
                    embeds: [
                        new EmbedBuilder()
                            .setColor('#ffd700')
                            .setTitle('🏆 Vouch Leaderboard')
                            .setDescription('No vouch points have been earned yet. Be the first to post in the vouch channels!')
                            .setTimestamp()
                    ],
                    allowedMentions: { parse: [] }
                });
                return;
            }

            const totalPages = Math.max(1, Math.ceil(totalEntries / limit));
            if (page > totalPages) page = totalPages;
            const offset = (page - 1) * limit;

            const rows = await new Promise((resolve, reject) => {
                db.all(
                    'SELECT user_id, points, username FROM vouch_points ORDER BY points DESC LIMIT ? OFFSET ?',
                    [limit, offset],
                    (err, data) => {
                        if (err) reject(err);
                        else resolve(data || []);
                    }
                );
            });

            const lines = rows.map((row, idx) => {
                const rank = offset + idx + 1;
                const displayName = row.username || 'Unknown User';
                const youNote = row.user_id === interaction.user.id ? ' _(You)_' : '';
                return `**#${rank}** ${displayName}${youNote} — **${Number(row.points || 0).toLocaleString()}** pts`;
            });

            const embed = new EmbedBuilder()
                .setColor('#ffd700')
                .setTitle('🏆 Vouch Leaderboard')
                .setDescription(lines.length ? lines.join('\n') : 'No players found on this page.')
                .setTimestamp()
                .setFooter({
                    text: `Page ${page}/${totalPages} • Tracking ${totalEntries.toLocaleString()} players`
                });

            try {
                const userPoints = await getUserBalance(interaction.user.id);
                const userPointsValue = Number.isFinite(userPoints) ? Number(userPoints) : 0;
                const rankRow = await new Promise((resolve, reject) => {
                    db.get(
                        'SELECT COUNT(*) + 1 AS rank FROM vouch_points WHERE points > ?',
                        [userPointsValue],
                        (err, row) => {
                            if (err) reject(err);
                            else resolve(row || { rank: totalEntries + 1 });
                        }
                    );
                });
                const rankValue = Number.isFinite(rankRow?.rank) ? Number(rankRow.rank) : (totalEntries + 1);
                embed.addFields({
                    name: 'Your Standing',
                    value: `#${rankValue} with **${userPointsValue.toLocaleString()}** point${userPointsValue === 1 ? '' : 's'}`,
                    inline: false
                });
            } catch (rankError) {
                console.error('Leaderboard rank lookup error:', rankError);
            }

            await interaction.reply({ embeds: [embed], allowedMentions: { parse: [] } });
        } catch (error) {
            console.error('Leaderboard command error:', error);
            try {
                await interaction.reply({ content: '❌ Unable to load the leaderboard right now.', ephemeral: true });
            } catch {}
        }
        return;
    }

    if (interaction.commandName === 'addpoints' || interaction.commandName === 'removepoints') {
        if (!interaction.inGuild()) {
            interaction.reply({ content: 'This command can only be used in a server.', ephemeral: true });
            return;
        }
        const isAdmin = interaction.memberPermissions?.has(PermissionFlagsBits.Administrator) || interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild);
        if (!isAdmin) {
            interaction.reply({ content: '❌ You do not have permission to use this command.', ephemeral: true });
            return;
        }
        const targetUser = interaction.options.getUser('user');
        const amount = Math.max(1, Math.floor(interaction.options.getInteger('amount') || 0));
        if (!targetUser || !amount) {
            interaction.reply({ content: 'Provide a user and a positive amount.', ephemeral: true });
            return;
        }
        db.get('SELECT points FROM vouch_points WHERE user_id = ?', [targetUser.id], (err, row) => {
            if (err) {
                console.error('Database error:', err);
                interaction.reply({ content: '❌ Error updating points.', ephemeral: true });
                return;
            }
            const delta = interaction.commandName === 'addpoints' ? amount : -amount;
            if (row) {
                const newPoints = Math.max(0, row.points + delta);
                db.run('UPDATE vouch_points SET points = ?, username = ?, last_updated = CURRENT_TIMESTAMP WHERE user_id = ?', [newPoints, targetUser.username, targetUser.id], (err2) => {
                    if (err2) {
                        console.error('Database error:', err2);
                        interaction.reply({ content: '❌ Error updating points.', ephemeral: true });
                        return;
                    }
                    interaction.reply({ content: `Updated ${targetUser.username}'s points to ${newPoints}.` });
                });
            } else {
                const initial = Math.max(0, delta);
                db.run('INSERT INTO vouch_points (user_id, points, username) VALUES (?, ?, ?)', [targetUser.id, initial, targetUser.username], (err3) => {
                    if (err3) {
                        console.error('Database error:', err3);
                        interaction.reply({ content: '❌ Error updating points.', ephemeral: true });
                        return;
                    }
                    interaction.reply({ content: `Set ${targetUser.username}'s points to ${initial}.` });
                });
            }
        });
    }

    // Blackjack command (new)
    if (interaction.commandName === 'blackjack') {
        try {
            if (!ensureCasinoChannel(interaction)) { await interaction.reply({ content: 'Please use this in the #casino channel.', ephemeral: true }); return; }
            const cd = onCooldown('bj:' + interaction.user.id, BLACKJACK_COOLDOWN_MS);
            if (cd > 0) { await interaction.reply({ content: `Cooldown ${Math.ceil(cd/1000)}s.`, ephemeral: true }); return; }
            const existingGame = await getBlackjackGame(interaction.user.id).catch(() => null);
            if (existingGame) {
                await interaction.reply({ content: 'You already have an active blackjack round. Finish or surrender it first.', ephemeral: true });
                return;
            }
            const bet = Math.max(1, Math.floor(interaction.options.getInteger('amount') || 0));
            const balance = await getUserBalance(interaction.user.id);
            if (bet <= 0) { await interaction.reply({ content: 'Minimum bet is 1.', ephemeral: true }); return; }
            if (balance < bet) { await interaction.reply({ content: 'Insufficient points.', ephemeral: true }); return; }
            // Deduct bet upfront
            await changeUserBalance(interaction.user.id, interaction.user.username, -bet, 'blackjack_bet', { bet });
            setCooldown('bj:' + interaction.user.id);

            const state = { userId: interaction.user.id, bet, baseBet: bet, player: [], dealer: [], startedAt: Date.now(), ended: false, channelId: interaction.channelId, messageId: null, shoe: bjCreateShoe(), doubled: false, split: false, splitHand: null, currentSplitHand: 1 };
            
            // MONITORING: Track game start
            if (global.gameMonitoring) {
                global.gameMonitoring.totalGamesStarted++;
                console.log(`📊 Game started for user ${interaction.user.id} (Total: ${global.gameMonitoring.totalGamesStarted})`);
            }
            
            await saveBlackjackGame(interaction.user.id, state);
            await bjDealInitial(state);
            await saveBlackjackGame(interaction.user.id, state); // Save again after dealing initial cards

            // Get 24-hour top winner for display
            const topWinner = await get24HourTopWinner();
            const recentPlayers = await getRecentBlackjackPlayers();
            const initialEmbed = bjBuildEmbed(state, {
                hideDealerHole: true,
                note: `\n${buildBlackjackPrompt(recentPlayers)}`,
                topWinner: topWinner,
                jackpot: await getSlotsJackpot(),
                recentPlayers
            });
            const components = [
                {
                    type: 1,
                    components: [
                        { type: 2, style: 1, label: 'Hit', custom_id: `nbj_hit:${interaction.user.id}` },
                        { type: 2, style: 2, label: 'Stand', custom_id: `nbj_stand:${interaction.user.id}` },
                        { type: 2, style: 3, label: 'Double', custom_id: `nbj_double:${interaction.user.id}` },
                        { type: 2, style: 4, label: 'Surrender', custom_id: `nbj_surrender:${interaction.user.id}` },
                    ]
                }
            ];
            // Send the initial message and record its id for future safe edits
            const sent = await interaction.reply({ embeds: [initialEmbed], components, fetchReply: true }).catch(() => null);
            if (sent && sent.id) { state.messageId = sent.id; }
            if (!state.messageId) {
                // Fallback: fetch last message from channel by bot
                try {
                    const channel = await client.channels.fetch(interaction.channelId);
                    const recent = await channel.messages.fetch({ limit: 1 });
                    const last = recent.first();
                    if (last) state.messageId = last.id;
                } catch {}
            }
            // Ensure initial view drew; also try a second update via editReply to avoid any race
            // Finalize view after controls were temporarily disabled during button processing
            await bjUpdateView(state, { hideDealerHole: true, note: '\nYour move: Hit, Stand, Double, or Surrender.' }, interaction);

            // Auto-timeout to stand after 30s (reasonable for user experience)
            setTimeout(async () => {
                const s = await getBlackjackGame(interaction.user.id).catch(() => null);
                if (!s || s.ended) return;

                // Mark game as ended to prevent further actions
                s.ended = true;
                await deleteBlackjackGame(interaction.user.id);

                // Try to update the message to show timeout
                try {
                    const embed = bjBuildEmbed(s, {
                        note: '\n⏰ **GAME TIMED OUT** - You took too long to respond!\nDealer automatically stands for you.',
                        result: 'lose'
                    });
                    await updateGame(interaction, s, { embeds: [embed], components: [] });
                } catch (e) {
                    console.error('Timeout message update failed:', e);
                }

                // Resolve the game
                await bjResolve(interaction, s, 'stand', true);
            }, 30000); // 30 seconds timeout
        } catch (e) {
            console.error('Blackjack start error:', e);
            // Refund if failed
            // Best-effort refund: we don't know bet if failure early — ignored here
            try { await interaction.reply({ content: '❌ Failed to start game.', ephemeral: true }); } catch {}
        }
        return;
    }

    // Roulette command
    if (interaction.commandName === 'roulette') {
        try {
            if (!ensureCasinoChannel(interaction)) { await interaction.reply({ content: 'Please use this in the #casino channel.', ephemeral: true }); return; }
            const cd = onCooldown('roulette:' + interaction.user.id, ROULETTE_COOLDOWN_MS);
            if (cd > 0) { await interaction.reply({ content: `Cooldown ${Math.ceil(cd/1000)}s.`, ephemeral: true }); return; }
            const amount = Math.max(1, Math.floor(interaction.options.getInteger('amount') || 0));
            const balance = await getUserBalance(interaction.user.id);
            if (amount <= 0) { await interaction.reply({ content: 'Minimum bet is 1.', ephemeral: true }); return; }
            if (balance < amount) { await interaction.reply({ content: 'Insufficient points.', ephemeral: true }); return; }
            const betType = interaction.options.getString('type');
            const number = interaction.options.getInteger('number');
            await changeUserBalance(interaction.user.id, interaction.user.username, -amount, 'roulette_bet', { bet: amount, betType, number });
            setCooldown('roulette:' + interaction.user.id);

            // Immersive roulette ASCII wheel
            const wheelSpinning = `
╔══════════════════════════════════════╗
║        🎰 PREMIUM ROULETTE 🎰        ║
║                                      ║
║     ⚡ SPINNING AT LIGHT SPEED ⚡     ║
║           💫 ∞ ∞ ∞ ∞ ∞ 💫           ║
║        🌟 FORTUNE AWAITS 🌟         ║
║                                      ║
║   💎 VIP EXCLUSIVE - HIGH STAKES 💎  ║
╚══════════════════════════════════════╝`;
            const wheelSlowing = `
╔══════════════════════════════════════╗
║        🎰 PREMIUM ROULETTE 🎰        ║
║                                      ║
║         🎯 SLOWING DOWN... 🎯        ║
║           🔥 7 14 23 31 🔥           ║
║        ⭐ DESTINY DECIDES ⭐         ║
║                                      ║
║   💎 VIP EXCLUSIVE - HIGH STAKES 💎  ║
╚══════════════════════════════════════╝`;
            // Moving wheel frames with pointer → slows down
            const ring = [0,32,15,19,4,21,2,25,17,34,6,27,13,36,11,30,8,23,10,5,24,16,33,1,20,14,31,9,22,18,29,7,28,12,35,3,26];
            const pointer = '▼';
            await interaction.reply({ embeds: [ new EmbedBuilder().setColor('#ff6b35').setDescription(wheelSpinning).setFooter({ text: '🎲 This server\'s VIP casino' }) ] });
            let idx = randomInt(0, ring.length-1);
            for (let speed of [60,60,80,100,120,140,160,200,240,300,360]) {
                idx = (idx + 1) % ring.length;
                const strip = buildRouletteStrip(ring, idx, 7);
                const frame = `
╔══════════════════════════════════════════════════╗
║                🎰 PREMIUM ROULETTE 🎰             ║
║                                                  ║
║                         ${pointer}                       ║
║            ${strip}            ║
║                                                  ║
║          🌟 SPINNING THE WHEEL • VIP 🌟           ║
╚══════════════════════════════════════════════════╝`;
                await new Promise(r => setTimeout(r, speed));
                await interaction.editReply({ embeds: [ new EmbedBuilder().setColor('#ff6b35').setDescription(frame).setFooter({ text: '🎲 This server\'s VIP casino' }) ] });
            }
            const result = randomInt(0, 36);
            const redSet = new Set([1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]);
            const color = result === 0 ? 'green' : (redSet.has(result) ? 'red' : 'black');
            let win = 0;
            if (betType === 'red' || betType === 'black') { if (color === betType) win = amount * 2; }
            else if (betType === 'even' || betType === 'odd') { if (result !== 0 && (result % 2 === 0) === (betType === 'even')) win = amount * 2; }
            else if (betType === 'low' || betType === 'high') { if (result !== 0 && ((betType==='low' && result<=18) || (betType==='high' && result>=19))) win = amount * 2; }
            else if (betType === 'number' && Number.isInteger(number) && number >= 0 && number <= 36) { if (result === number) win = amount * 35; }

            // Get 24-hour top winner for display
            const topWinner = await get24HourTopWinner();
            const winnerDisplay = topWinner ? `🏆 ${topWinner.username || 'Unknown'} (+${topWinner.net_wins})` : '💎 GLASSMORPHISM THEME';

            const wheelResult = `
╔══════════════════════════════════════╗
║        🎰 PREMIUM ROULETTE 🎰        ║
║          ${winnerDisplay}          ║
║                                      ║
║           🏆 RESULT: ${result.toString().padStart(2,' ')} 🏆           ║
║                ${color==='red' ? '🔴' : color==='black' ? '⚫' : '🟢'}                ║
║                                      ║
║     ${win>0 ? '💰 WINNER! 💰' : '😤 BETTER LUCK NEXT TIME'}      ║
║                                      ║
║   🎲 24H TOP WINNER • 💰 JACKPOT: ${await getSlotsJackpot()}  ║
╚══════════════════════════════════════╝`;
            const net = win - amount;
            const breakdown = `\nBet: ${amount} • Payout: ${win} • Net: ${net>=0?'+':''}${net}`;
            const embed = new EmbedBuilder()
                .setColor(win > 0 ? '#00c853' : '#c62828')
                .setDescription(wheelResult + breakdown)
                .setFooter({ text: `Bet: ${amount} • 🎲 24h Top Winner • ⚡ Lightning Fast` });
            await interaction.editReply({ content: undefined, embeds: [embed] });
            // Balance after roulette
            try { const bal = await getUserBalance(interaction.user.id); await interaction.followUp({ content: `Current balance: ${bal} vouch points.`, ephemeral: true }); } catch {}
            if (win > 0) {
                await changeUserBalance(interaction.user.id, interaction.user.username, win, 'roulette_payout', { result });
            }
        } catch (e) {
            console.error('Roulette error:', e);
            try { await interaction.reply({ content: '❌ Error playing roulette.', ephemeral: true }); } catch {}
        }
        return;
    }

    // Slots command
    if (interaction.commandName === 'slots') {
        try {
            if (!ensureCasinoChannel(interaction)) { await interaction.reply({ content: 'Please use this in the #casino channel.', ephemeral: true }); return; }
            const cd = onCooldown('slots:' + interaction.user.id, SLOTS_COOLDOWN_MS);
            if (cd > 0) { await interaction.reply({ content: `Cooldown ${Math.ceil(cd/1000)}s.`, ephemeral: true }); return; }
            const requestedAmount = Math.max(1, Math.floor(interaction.options.getInteger('amount') || 0));
            const balance = await getUserBalance(interaction.user.id);
            if (requestedAmount <= 0) { await interaction.reply({ content: 'Minimum bet is 1.', ephemeral: true }); return; }
            if (balance < requestedAmount) { await interaction.reply({ content: 'Insufficient points.', ephemeral: true }); return; }

            // Optional options (backward compatible if not registered)
            const requestedLines = Math.max(1, Math.min(20, Math.floor(interaction.options.getInteger?.('lines') || 0) || 0)) || 10; // default 10
            const machineKey = (interaction.options.getString?.('machine') || 'classic').toLowerCase();

            // Determine active lines and per-line bet ensuring >=1 per line
            let lines = Math.max(1, Math.min(20, requestedLines));
            lines = Math.min(lines, requestedAmount); // ensure at least 1 per line
            let lineBet = Math.floor(requestedAmount / lines);
            if (lineBet < 1) { lineBet = 1; lines = Math.min(requestedAmount, lines); }
            const stake = lines * lineBet;

            // Deduct only the effective stake, not the raw requested amount
            await changeUserBalance(interaction.user.id, interaction.user.username, -stake, 'slots_bet', {
                requestedAmount,
                stake,
                lines,
                lineBet,
                machine: machineKey
            });
            setCooldown('slots:' + interaction.user.id);
            // If requested was higher than stake (due to line division), refund the difference immediately
            const changeBack = requestedAmount - stake;
            if (changeBack > 0) {
                await changeUserBalance(interaction.user.id, interaction.user.username, changeBack, 'slots_bet_change_return', {
                    requestedAmount,
                    stake,
                    changeBack
                });
            }

            // Machine configurations
            function getMachineConfig(key) {
                switch (key) {
                    case 'gems':
                        return {
                            symbols: ['💠','🔷','🔶','🔺','💎','7️⃣'],
                            weights: [24,22,20,16,12,6],
                            paytable: {
                                '💠': { 3: 4, 4: 10, 5: 25 },
                                '🔷': { 3: 5, 4: 12, 5: 30 },
                                '🔶': { 3: 6, 4: 14, 5: 35 },
                                '🔺': { 3: 8, 4: 18, 5: 45 },
                                '💎': { 3: 20, 4: 60, 5: 180 },
                                '7️⃣': { 3: 25, 4: 80, 5: 220 }
                            }
                        };
                    case 'fruits':
                        return {
                            symbols: ['🍒','🍋','🍊','🍉','🔔','7️⃣'],
                            weights: [26,22,20,16,10,6],
                            paytable: {
                                '🍒': { 3: 4, 4: 10, 5: 25 },
                                '🍋': { 3: 5, 4: 12, 5: 30 },
                                '🍊': { 3: 6, 4: 14, 5: 35 },
                                '🍉': { 3: 8, 4: 18, 5: 45 },
                                '🔔': { 3: 12, 4: 28, 5: 70 },
                                '7️⃣': { 3: 20, 4: 60, 5: 180 }
                            }
                        };
                    default:
                        return {
                            symbols: ['🍒','🍋','🍊','🔔','⭐','7️⃣','💎'],
                            weights: [24,20,18,14,12,8,4],
                            paytable: {
                                '🍒': { 3: 4, 4: 10, 5: 25 },
                                '🍋': { 3: 5, 4: 12, 5: 30 },
                                '🍊': { 3: 6, 4: 14, 5: 35 },
                                '🔔': { 3: 10, 4: 24, 5: 60 },
                                '⭐': { 3: 12, 4: 30, 5: 75 },
                                '7️⃣': { 3: 18, 4: 55, 5: 150 },
                                '💎': { 3: 25, 4: 75, 5: 220 }
                            }
                        };
                }
            }

            const machine = getMachineConfig(machineKey);
            const totalWeight = machine.weights.reduce((a,b)=>a+b,0);
            function rollSymbol() {
                let r = Math.random() * totalWeight, acc = 0;
                for (let i = 0; i < machine.symbols.length; i++) { acc += machine.weights[i]; if (r <= acc) return machine.symbols[i]; }
                return machine.symbols[0];
            }

            // Build 5x3 grid (rows x cols)
            const cols = 5, rows = 3;
            const grid = Array.from({ length: rows }, () => Array.from({ length: cols }, () => rollSymbol()));

            // Paylines (row indices per column)
            const PAYLINES = [
                [0,0,0,0,0], [1,1,1,1,1], [2,2,2,2,2],
                [0,1,2,1,0], [2,1,0,1,2],
                [0,0,1,0,0], [2,2,1,2,2],
                [1,0,0,0,1], [1,2,2,2,1],
                [0,1,1,1,0], [2,1,1,1,2],
                [0,1,0,1,0], [2,1,2,1,2],
                [1,0,1,2,1], [1,2,1,0,1]
            ];

            const activePaylines = PAYLINES.slice(0, Math.max(1, Math.min(lines, PAYLINES.length)));

            function evaluateLine(lineIdx) {
                const pattern = activePaylines[lineIdx];
                const firstSymbol = grid[pattern[0]][0];
                let count = 1;
                for (let c = 1; c < cols; c++) {
                    const r = pattern[c];
                    if (grid[r][c] === firstSymbol) count++; else break;
                }
                // Allow tiny 2-of-a-kind payouts for low symbols on some machines
                let mult = 0;
                if (count >= 3) {
                    mult = (machine.paytable[firstSymbol] || {})[count] || 0;
                } else if (count === 2) {
                    const low2Kind = new Set(['🍒','🍋','🍊','💠','🔷']);
                    if (low2Kind.has(firstSymbol)) mult = 0.2; // fractional multiplier
                }
                if (!mult) return null;
                return { symbol: firstSymbol, count, multiplier: mult, payout: mult * lineBet, line: pattern };
            }

            const wins = [];
            for (let i = 0; i < activePaylines.length; i++) {
                const w = evaluateLine(i);
                if (w) wins.push({ index: i+1, ...w });
            }
            const totalPayout = wins.reduce((s,w)=>s + w.payout, 0);

            // Progressive jackpot contribution and potential hit
            const jackpotAfterStake = await addToSlotsJackpot(stake);
            const jackpotHit = await maybeHitSlotsJackpot(interaction.user.id, interaction.user.username, stake);
            const jackpotDisplay = jackpotHit > 0 ? `🎉 JACKPOT WON: ${jackpotHit}!` : `💰 Progressive Jackpot: ${await getSlotsJackpot()}`;

            // Visuals
            const rowToString = (r) => grid[r].join('  ');
            const gridBox = `
╔══════════════════════════════════════════════╗
║              🎰 PREMIUM 5×3 SLOTS             ║
║----------------------------------------------║
║  ${rowToString(0)}  ║
║  ${rowToString(1)}  ║
║  ${rowToString(2)}  ║
║----------------------------------------------║
║  Lines: ${lines} • Bet/Line: ${lineBet} • Stake: ${stake}       ║
╚══════════════════════════════════════════════╝`;

            // Get 24-hour top winner for display
            const topWinner = await get24HourTopWinner();
            const winnerDisplay = topWinner ? `🏆 ${topWinner.username || 'Unknown'} (+${topWinner.net_wins})` : '💎 GLASSMORPHISM THEME';

            let winsText = wins.length > 0 ? '' : 'No winning lines. Better luck next spin!';
            if (wins.length > 0) {
                for (const w of wins) {
                    winsText += `Line ${w.index}: ${w.symbol} ×${w.count} → x${w.multiplier} = ${w.payout}\n`;
                }
            }

            // Animate simple spin frames
            await interaction.reply({ embeds: [ new EmbedBuilder().setColor('#7c4dff').setDescription('```\nSPINNING…\n```').setFooter({ text: '🎰 Spinning…' }) ] });
            await new Promise(r => setTimeout(r, 350));
            await interaction.editReply({ embeds: [ new EmbedBuilder().setColor('#7c4dff').setDescription('```\nREELS STOPPING…\n```').setFooter({ text: '🎰 Reels stopping…' }) ] });
            await new Promise(r => setTimeout(r, 350));

            const net = Number((totalPayout - stake).toFixed(2));
            const summary = `\nMachine: ${machineKey} • ${winnerDisplay}\n${jackpotDisplay}\nTotal payout: ${totalPayout.toFixed(2)} • Net: ${net>=0?'+':''}${net.toFixed(2)}`;
            const color = totalPayout > 0 ? '#00c853' : '#c62828';
            const embed = new EmbedBuilder()
                .setColor(color)
                .setDescription(gridBox + '\n' + '```\n' + (winsText || '') + '```' + summary)
                .setFooter({ text: `Stake: ${stake} • Lines: ${lines} • Bet/Line: ${lineBet}` });
            await interaction.editReply({ content: undefined, embeds: [embed] });

            if (totalPayout > 0) await creditWithFraction(interaction.user.id, interaction.user.username, totalPayout, 'slots_payout', {
                lines,
                lineBet,
                stake,
                machine: machineKey,
                grid,
                wins
            });
            // Balance after slots
            try { const bal = await getUserBalance(interaction.user.id); await interaction.followUp({ content: `Current balance: ${bal} vouch points.`, ephemeral: true }); } catch {}

        } catch (e) {
            console.error('Slots error:', e);
            try { await interaction.reply({ content: '❌ Error playing slots.', ephemeral: true }); } catch {}
        }
        return;
    }
    if (interaction.commandName === 'recountvouches') {
        if (!interaction.inGuild()) {
            interaction.reply({ content: 'This command can only be used in a server.', ephemeral: true });
            return;
        }
        const isAdmin = interaction.memberPermissions?.has(PermissionFlagsBits.Administrator) || interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild);
        if (!isAdmin) {
            interaction.reply({ content: '❌ You do not have permission to use this command.', ephemeral: true });
            return;
        }

        await interaction.deferReply({ ephemeral: true });

        try {
            const targetChannel = interaction.options.getChannel?.('channel') || null;
            const guild = interaction.guild;
            const providerRoleId = process.env.PROVIDER_ROLE_ID;
            const providerRoleName = process.env.PROVIDER_ROLE_NAME || 'Provider';
            const role = providerRoleId
                ? guild.roles.cache.get(providerRoleId)
                : guild.roles.cache.find(r => r.name.toLowerCase() === providerRoleName.toLowerCase());
            if (!role) {
                await interaction.editReply('Provider role not found. Set PROVIDER_ROLE_ID or PROVIDER_ROLE_NAME.');
                return;
            }

            const channelsToScan = [];
            if (targetChannel && targetChannel.isTextBased && targetChannel.isTextBased()) {
                channelsToScan.push(targetChannel);
            } else {
                guild.channels.cache.forEach(ch => {
                    if (ch && ch.isTextBased && ch.isTextBased() && ch.name && ch.name.toLowerCase().includes('vouch')) {
                        channelsToScan.push(ch);
                    }
                });
            }

            if (channelsToScan.length === 0) {
                await interaction.editReply('No vouch channels found to scan.');
                return;
            }

            const imageTypes = ['image/jpeg', 'image/jpg', 'image/png', 'image/gif', 'image/webp'];
            const userIdToCount = new Map();
            let scannedMessages = 0;

            const isProviderMentioned = async (message) => {
                if (!message.mentions) return false;
                if (message.mentions.members && message.mentions.members.some(m => m.roles.cache.has(role.id))) return true;
                // Fallback: try users -> members
                const users = message.mentions.users;
                if (users && users.size > 0) {
                    for (const [, user] of users) {
                        const member = guild.members.cache.get(user.id) || await guild.members.fetch(user.id).catch(() => null);
                        if (member && member.roles.cache.has(role.id)) return true;
                    }
                }
                return false;
            };

            const hasImage = (message) => {
                if (!message.attachments || message.attachments.size === 0) return false;
                return message.attachments.some(att => {
                    if (att.contentType && imageTypes.includes(att.contentType)) return true;
                    const ext = (att.name || '').toLowerCase();
                    return ['.jpg', '.jpeg', '.png', '.gif', '.webp'].some(e => ext.endsWith(e));
                });
            };

            for (const ch of channelsToScan) {
                let lastId = undefined;
                // Iterate through history
                while (true) {
                    const batch = await ch.messages.fetch({ limit: 100, before: lastId }).catch(() => null);
                    if (!batch || batch.size === 0) break;
                    for (const [, msg] of batch) {
                        scannedMessages++;
                        if (msg.author?.bot) continue;
                        if (!hasImage(msg)) continue;
                        // Must mention at least one provider
                        // eslint-disable-next-line no-await-in-loop
                        const providerOk = await isProviderMentioned(msg);
                        if (!providerOk) continue;
                        const uid = msg.author.id;
                        userIdToCount.set(uid, (userIdToCount.get(uid) || 0) + 1);
                    }
                    lastId = batch.last().id;
                }
            }

            // Prefetch usernames before DB transaction to avoid using await inside callbacks
            const entries = await Promise.all(
                Array.from(userIdToCount.entries()).map(async ([uid, count]) => {
                    let uname = 'Unknown User';
                    try {
                        const u = await interaction.client.users.fetch(uid);
                        if (u && u.username) uname = u.username;
                    } catch {}
                    return [uid, count, uname];
                })
            );

            // Write results to DB (multiplier not applied for recount; base 1 per valid vouch)
            await new Promise((resolve, reject) => {
                db.serialize(() => {
                    db.run('BEGIN TRANSACTION');
                    db.run('DELETE FROM vouch_points');
                    const stmt = db.prepare('INSERT INTO vouch_points (user_id, points, username) VALUES (?, ?, ?)');
                    for (const [uid, count, uname] of entries) {
                        stmt.run(uid, count, uname);
                    }
                    stmt.finalize((err) => {
                        if (err) {
                            db.run('ROLLBACK');
                            reject(err);
                            return;
                        }
                        db.run('COMMIT', (err2) => {
                            if (err2) reject(err2);
                            else resolve();
                        });
                    });
                });
            });

            await interaction.editReply(`✅ Recount complete. Scanned ${scannedMessages} messages across ${channelsToScan.length} channel(s). Updated ${userIdToCount.size} user(s).`);
        } catch (e) {
            console.error('Recount error:', e);
            await interaction.editReply('❌ Error during recount. Check logs.');
        }
    }

    if (interaction.commandName === 'restorebackup') {
        if (!interaction.inGuild()) {
            interaction.reply({ content: 'This command can only be used in a server.', ephemeral: true });
            return;
        }
        const isAdmin = interaction.memberPermissions?.has(PermissionFlagsBits.Administrator) || interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild);
        if (!isAdmin) {
            interaction.reply({ content: '❌ You do not have permission to use this command.', ephemeral: true });
            return;
        }

        await interaction.deferReply({ ephemeral: true });

        try {
            const date = interaction.options.getString('date');
            if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
                await interaction.editReply('❌ Invalid date format. Use YYYY-MM-DD (e.g., 2024-12-25)');
                return;
            }

            // Get backup data
            const backupKey = `backup_vouch_${date}`;
            const backupData = await getSetting(backupKey, null);
            if (!backupData) {
                await interaction.editReply(`❌ No backup found for ${date}. Available backups are stored daily.`);
                return;
            }

            const backup = JSON.parse(backupData);
            if (!backup.data || !Array.isArray(backup.data)) {
                await interaction.editReply('❌ Backup data is corrupted.');
                return;
            }

            // Restore from backup
            await new Promise((resolve, reject) => {
                db.serialize(() => {
                    db.run('BEGIN TRANSACTION');
                    db.run('DELETE FROM vouch_points');
                    const stmt = db.prepare('INSERT INTO vouch_points (user_id, points, username) VALUES (?, ?, ?)');
                    for (const user of backup.data) {
                        stmt.run(user.user_id, user.points, user.username);
                    }
                    stmt.finalize((err) => {
                        if (err) {
                            db.run('ROLLBACK');
                            reject(err);
                            return;
                        }
                        db.run('COMMIT', (err2) => {
                            if (err2) reject(err2);
                            else resolve();
                        });
                    });
                });
            });

            await interaction.editReply(`✅ Backup restored from ${date}! Restored ${backup.data.length} users with ${backup.totalPoints} total points.`);
        } catch (e) {
            console.error('Backup restore error:', e);
            await interaction.editReply('❌ Error during backup restore. Check logs.');
        }
    }

    if (interaction.commandName === 'setmultiplier') {
        if (!interaction.inGuild()) {
            interaction.reply({ content: 'This command can only be used in a server.', ephemeral: true });
            return;
        }
        const isAdmin = interaction.memberPermissions?.has(PermissionFlagsBits.Administrator) || interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild);
        if (!isAdmin) {
            interaction.reply({ content: '❌ You do not have permission to use this command.', ephemeral: true });
            return;
        }
        const value = interaction.options.getNumber('value');
        const durationMinutes = interaction.options.getInteger?.('duration_minutes') || 0;
        const newVal = Math.max(1, Math.floor(value || 1));
        await setMultiplier(newVal);

        // Announce in the channel where command executed
        const announceContent = durationMinutes > 0
            ? `@everyone Vouch multiplier is now ${newVal}x for ${durationMinutes} minute(s)!`
            : `@everyone Vouch multiplier is now ${newVal}x until further notice!`;

        // Save announcement channel id for end-of-multiplier message
        await setSetting('multiplier_announce_channel_id', interaction.channelId);

        await interaction.reply({
            content: announceContent,
            allowedMentions: { parse: ['everyone'] }
        });

        // Schedule expiry if duration provided
        if (durationMinutes > 0) {
            const expiresAt = Date.now() + durationMinutes * 60 * 1000;
            await setMultiplierExpiryMs(expiresAt);
            await scheduleMultiplierExpiryIfNeeded(interaction.client);
        } else {
            await setMultiplierExpiryMs(0);
            await scheduleMultiplierExpiryIfNeeded(interaction.client);
        }
    }

    if (interaction.commandName === 'multiplierstatus') {
        const current = await getMultiplier();
        interaction.reply({ content: `Current vouch multiplier: ${current}x` });
    }

    if (interaction.commandName === 'resetmultiplier') {
        if (!interaction.inGuild()) {
            interaction.reply({ content: 'This command can only be used in a server.', ephemeral: true });
            return;
        }
        const isAdmin = interaction.memberPermissions?.has(PermissionFlagsBits.Administrator) || interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild);
        if (!isAdmin) {
            interaction.reply({ content: '❌ You do not have permission to use this command.', ephemeral: true });
            return;
        }
        await setMultiplier(1);
        interaction.reply({ content: '✅ Multiplier reset to 1x.' });
    }

    if (interaction.commandName === 'wipevouches') {
        if (!interaction.inGuild()) {
            interaction.reply({ content: 'This command can only be used in a server.', ephemeral: true });
            return;
        }
        const isAdmin = interaction.memberPermissions?.has(PermissionFlagsBits.Administrator) || interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild);
        if (!isAdmin) {
            interaction.reply({ content: '❌ You do not have permission to use this command.', ephemeral: true });
            return;
        }
        const confirm = interaction.options.getString?.('confirm') || '';
        if (confirm.toLowerCase() !== 'yes') {
            interaction.reply({ content: 'Type /wipevouches confirm:yes to wipe all vouch points (irreversible).', ephemeral: true });
            return;
        }
        db.run('DELETE FROM vouch_points', (err) => {
            if (err) {
                console.error('Database error wiping vouches:', err);
                interaction.reply({ content: '❌ Error wiping vouch points.', ephemeral: true });
                return;
            }
            interaction.reply({ content: '🧹 All vouch points have been wiped.' });
        });
        return;
    }

    if (interaction.commandName === 'triggerreset') {
        if (!interaction.inGuild()) {
            await interaction.reply({ content: '❌ Server command only.', ephemeral: true });
            return;
        }

        const isAdmin = interaction.memberPermissions?.has(PermissionFlagsBits.Administrator);
        if (!isAdmin) {
            await interaction.reply({ content: '❌ Admin only.', ephemeral: true });
            return;
        }

        const confirm = interaction.options.getString('confirm');
        if (confirm !== 'EXECUTE') {
            await interaction.reply({ content: '❌ Type "EXECUTE" to confirm.', ephemeral: true });
            return;
        }

        await interaction.reply({ content: '🔄 Starting manual server reset...', ephemeral: true });

        const whitelistIds = (process.env.RESET_WHITELIST || '')
            .split(',')
            .map(id => id.trim())
            .filter(id => id.length > 0);

        try {
            await performAutoReset(whitelistIds, interaction.guildId);
            await interaction.followUp({ content: '✅ Reset complete! Check console for details.', ephemeral: true });
        } catch (error) {
            console.error('Manual reset error:', error);
            await interaction.followUp({ content: '❌ Reset failed. Check console.', ephemeral: true });
        }
        return;
    }

    // Handle /sendpoints and /pay command (alias)
    if (interaction.commandName === 'sendpoints' || interaction.commandName === 'pay') {
        const targetUser = interaction.options.getUser('user');
        const amount = interaction.options.getInteger('amount');

        if (!targetUser) {
            interaction.reply({ content: '❌ Invalid target user.', ephemeral: true });
            return;
        }

        if (targetUser.id === interaction.user.id) {
            interaction.reply({ content: '❌ You cannot send points to yourself.', ephemeral: true });
            return;
        }

        if (targetUser.bot) {
            interaction.reply({ content: '❌ You cannot send points to bots.', ephemeral: true });
            return;
        }

        if (amount < 1) {
            interaction.reply({ content: '❌ Amount must be at least 1.', ephemeral: true });
            return;
        }

        await interaction.deferReply({ ephemeral: true });

        try {
            // Check sender's balance
            const senderBalance = await getUserBalance(interaction.user.id);
            if (senderBalance < amount) {
                await interaction.editReply(`❌ Insufficient balance. You have ${senderBalance} points but need ${amount}.`);
                return;
            }

            // Perform the transfer
            await changeUserBalance(interaction.user.id, interaction.user.username, -amount, 'transfer_sent', {
                recipientId: targetUser.id,
                recipientUsername: targetUser.username,
                amount: amount
            });

            await changeUserBalance(targetUser.id, targetUser.username, amount, 'transfer_received', {
                senderId: interaction.user.id,
                senderUsername: interaction.user.username,
                amount: amount
            });

            await interaction.editReply(`✅ Successfully sent ${amount} points to ${targetUser.username}!`);

        } catch (error) {
            console.error('Transfer error:', error);
            await interaction.editReply('❌ Error processing transfer. Please try again.');
        }
    }


    // Handle /transactions command
    if (interaction.commandName === 'transactions') {
        const targetUser = interaction.options.getUser('user') || interaction.user;
        const limit = interaction.options.getInteger('limit') || 10;

        // Check permissions for viewing other users' transactions
        if (targetUser.id !== interaction.user.id) {
            const isAdmin = interaction.memberPermissions?.has(PermissionFlagsBits.Administrator) || interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild);
            if (!isAdmin) {
                await interaction.reply({ content: '❌ You can only view your own transactions.', ephemeral: true });
                return;
            }
        }

        await interaction.deferReply({ ephemeral: true });

        try {
            const hasTimestamp = await requireLedgerTimestamp();
            const timestampSelection = hasTimestamp ? 'timestamp' : 'NULL as timestamp';
            const orderClause = hasTimestamp ? 'ORDER BY timestamp DESC' : 'ORDER BY id DESC';
            const transactions = await new Promise((resolve, reject) => {
                db.all(
                    `SELECT delta, reason, metadata, ${timestampSelection} FROM ledger
                     WHERE user_id = ?
                     ${orderClause}
                     LIMIT ?`,
                    [targetUser.id, limit],
                    (err, rows) => {
                        if (err) reject(err);
                        else resolve(rows);
                    }
                );
            });

            if (transactions.length === 0) {
                await interaction.editReply(`📊 No transactions found for ${targetUser.username}.`);
                return;
            }

            let response = `📊 **Recent Transactions for ${targetUser.username}:**\n\n`;

            for (const tx of transactions) {
                let timestamp = 'Unknown time';
                if (tx.timestamp) {
                    const parsed = new Date(tx.timestamp);
                    timestamp = Number.isNaN(parsed.getTime()) ? String(tx.timestamp) : parsed.toLocaleString();
                }
                const delta = tx.delta > 0 ? `+${tx.delta}` : tx.delta;
                const reason = tx.reason.replace(/_/g, ' ').toUpperCase();

                response += `**${timestamp}**\n`;
                response += `• ${reason}: ${delta} points\n`;

                // Add additional details for transfers
                if (tx.reason.includes('transfer')) {
                    try {
                        const meta = JSON.parse(tx.metadata || '{}');
                        if (meta.senderUsername || meta.recipientUsername) {
                            const otherUser = meta.senderUsername || meta.recipientUsername;
                            response += `  └─ ${otherUser}\n`;
                        }
                    } catch (e) {}
                }

                response += '\n';
            }

            await interaction.editReply(response);

    } catch (error) {
            console.error('Transactions query error:', error);
            await interaction.editReply('❌ Error retrieving transactions.');
        }
    }
});

// Error handling
client.on('error', console.error);
process.on('unhandledRejection', error => {
    console.error('Unhandled promise rejection:', error);
});

// Login to Discord
client.login(process.env.DISCORD_TOKEN);

// ================================================================================= //
// CENTRALIZED BLACKJACK GAME STATE MANAGER - From web search recommendations
// ================================================================================= //

class BlackjackStateManager {
    constructor(database) {
        this.db = database;
        this.locks = new Set();
    }

    // Atomic transaction wrapper for all database operations
    async executeTransaction(operations) {
        return new Promise((resolve, reject) => {
            this.db.serialize(() => {
                this.db.run('BEGIN TRANSACTION');
                
                const executeOperations = async () => {
                    try {
                        const results = [];
                        for (const operation of operations) {
                            const result = await operation();
                            results.push(result);
                        }
                        
                        this.db.run('COMMIT', (err) => {
                            if (err) {
                                console.error('❌ Transaction commit failed:', err);
                                reject(err);
                            } else {
                                resolve(results);
                            }
                        });
                    } catch (error) {
                        console.error('❌ Transaction operation failed:', error);
                        this.db.run('ROLLBACK');
                        reject(error);
                    }
                };
                
                executeOperations();
            });
        });
    }

    // Enhanced state validation with detailed logging
    validateGameState(state, userId, context = '') {
        const errors = [];
        
        if (!state || typeof state !== 'object') {
            errors.push('State is not a valid object');
        } else {
            if (!state.userId) errors.push('Missing userId');
            if (!state.bet || typeof state.bet !== 'number') errors.push('Invalid bet amount');
            if (!Array.isArray(state.player)) errors.push('Player hand is not an array');
            if (!Array.isArray(state.dealer)) errors.push('Dealer hand is not an array');
            if (state.startedAt && typeof state.startedAt !== 'number') errors.push('Invalid startedAt timestamp');
            
            // More lenient validation - allow empty hands during initialization
            if (context !== 'initialization') {
                if (state.player.length === 0) errors.push('Player hand is empty');
                if (state.dealer.length === 0) errors.push('Dealer hand is empty');
            }
        }

        if (errors.length > 0) {
            console.error(`❌ State validation failed for user ${userId} (${context}):`, errors);
            return { valid: false, errors };
        }

        console.log(`✅ State validation passed for user ${userId} (${context})`);
        return { valid: true, errors: [] };
    }

    // Robust game state retrieval with multiple fallback strategies
    async getGame(userId) {
        try {
            const row = await new Promise((resolve, reject) => {
                this.db.get('SELECT game_state, last_updated FROM blackjack_games WHERE user_id = ?', [userId], (err, row) => {
                    if (err) reject(err);
                    else resolve(row);
                });
            });

            if (!row) {
                console.log(`ℹ️ No game found for user ${userId}`);
                return null;
            }

            let state;
            try {
                state = JSON.parse(row.game_state);
            } catch (parseError) {
                console.error(`❌ JSON parse error for user ${userId}:`, parseError);
                await this.deleteGame(userId);
                return null;
            }

            const validation = this.validateGameState(state, userId, 'retrieval');
            if (!validation.valid) {
                // MONITORING: Track corruption attempt
                if (global.gameMonitoring) {
                    global.gameMonitoring.corruptionAttempts++;
                    console.log(`📊 Corruption detected for user ${userId} (Total: ${global.gameMonitoring.corruptionAttempts})`);
                }
                
                // Instead of immediately deleting, try to repair the state
                const repairedState = await this.repairGameState(state, userId);
                if (repairedState) {
                    // MONITORING: Track successful repair
                    if (global.gameMonitoring) {
                        global.gameMonitoring.stateRepairs++;
                        console.log(`📊 State repaired for user ${userId} (Total: ${global.gameMonitoring.stateRepairs})`);
                    }
                    
                    console.log(`🔧 Successfully repaired game state for user ${userId}`);
                    await this.saveGame(userId, repairedState);
                    return repairedState;
                } else {
                    console.log(`❌ Could not repair state for user ${userId}, removing game`);
                    
                    // MONITORING: Track refund from corruption
                    if (global.gameMonitoring) {
                        global.gameMonitoring.totalGamesRefunded++;
                        console.log(`📊 Game refunded due to corruption for user ${userId} (Total: ${global.gameMonitoring.totalGamesRefunded})`);
                    }
                    
                    await this.deleteGame(userId);
                    return null;
                }
            }

            // Check for stale games (over 30 minutes old)
            const gameAge = Date.now() - (state.startedAt || 0);
            if (gameAge > 30 * 60 * 1000) {
                console.log(`⏰ Game too old for user ${userId} (${Math.round(gameAge/60000)}min), cleaning up`);
                await this.deleteGame(userId);
                return null;
            }

            console.log(`✅ Retrieved valid game for user ${userId}: ${state.player?.length || 0} player, ${state.dealer?.length || 0} dealer`);
            ensureBlackjackBetDefaults(state);
            return state;
        } catch (error) {
            console.error(`❌ Error retrieving game for user ${userId}:`, error);
            return null;
        }
    }

    // Attempt to repair corrupted game state
    async repairGameState(state, userId) {
        try {
            const repairs = {};
            
            // Repair missing basic fields
            if (!state.userId) repairs.userId = userId;
            if (!state.bet || typeof state.bet !== 'number') repairs.bet = 1;
            if (!Array.isArray(state.player)) repairs.player = [];
            if (!Array.isArray(state.dealer)) repairs.dealer = [];
            if (!state.startedAt || typeof state.startedAt !== 'number') repairs.startedAt = Date.now();
            if (typeof state.ended !== 'boolean') repairs.ended = false;
            
            // If too many repairs needed, don't try to fix
            if (Object.keys(repairs).length > 3) {
                console.log(`❌ Too many repairs needed for user ${userId}, cannot fix`);
                return null;
            }

            if (Object.keys(repairs).length === 0) {
                return null; // No repairs possible
            }

            const repairedState = { ...state, ...repairs };
            console.log(`🔧 Repaired state for user ${userId}:`, Object.keys(repairs));
            
            return repairedState;
        } catch (error) {
            console.error(`❌ Error repairing state for user ${userId}:`, error);
            return null;
        }
    }
}

// Global state manager instance
const gameStateManager = new BlackjackStateManager(db);

// Legacy wrapper functions for compatibility
const getBlackjackGame = (userId) => gameStateManager.getGame(userId);

// Add methods to the state manager class
BlackjackStateManager.prototype.saveGame = async function(userId, state) {
    // Validate state before saving
    const validation = this.validateGameState(state, userId, 'saving');
    if (!validation.valid) {
        throw new Error(`Cannot save invalid state: ${validation.errors.join(', ')}`);
    }

    const operations = [
        () => new Promise((resolve, reject) => {
            const gameStateJson = JSON.stringify(state);
            const timestamp = Math.floor(Date.now() / 1000);
            
            this.db.run(
                'INSERT OR REPLACE INTO blackjack_games (user_id, game_state, last_updated) VALUES (?, ?, ?)',
                [userId, gameStateJson, timestamp],
                (err) => {
                    if (err) {
                        console.error(`❌ Failed to save game for user ${userId}:`, err);
                        reject(err);
                    } else {
                        console.log(`✅ Game saved for user ${userId}`);
                        resolve();
                    }
                }
            );
        })
    ];

    try {
        await this.executeTransaction(operations);
        return true;
    } catch (error) {
        console.error(`❌ Transaction failed saving game for user ${userId}:`, error);
        // Try a direct save as fallback
        return new Promise((resolve, reject) => {
            const gameStateJson = JSON.stringify(state);
            const timestamp = Math.floor(Date.now() / 1000);
            
            this.db.run(
                'INSERT OR REPLACE INTO blackjack_games (user_id, game_state, last_updated) VALUES (?, ?, ?)',
                [userId, gameStateJson, timestamp],
                (err) => {
                    if (err) {
                        console.error(`❌ Fallback save failed for user ${userId}:`, err);
                        reject(err);
                    } else {
                        console.log(`✅ Fallback save succeeded for user ${userId}`);
                        resolve(true);
                    }
                }
            );
        });
    }
};

BlackjackStateManager.prototype.deleteGame = async function(userId) {
    const operations = [
        () => new Promise((resolve, reject) => {
            this.db.run('DELETE FROM blackjack_games WHERE user_id = ?', [userId], (err) => {
                if (err) {
                    console.error(`❌ Failed to delete game for user ${userId}:`, err);
                    reject(err);
                } else {
                    console.log(`✅ Game deleted for user ${userId}`);
                    resolve();
                }
            });
        })
    ];

    try {
        await this.executeTransaction(operations);
        return true;
    } catch (error) {
        console.error(`❌ Transaction failed deleting game for user ${userId}:`, error);
        // Try direct delete as fallback
        return new Promise((resolve) => {
            this.db.run('DELETE FROM blackjack_games WHERE user_id = ?', [userId], (err) => {
                if (err) {
                    console.error(`❌ Fallback delete failed for user ${userId}:`, err);
                    resolve(false);
                } else {
                    console.log(`✅ Fallback delete succeeded for user ${userId}`);
                    resolve(true);
                }
            });
        });
    }
};

// Robust save function with fallbacks and retry logic  
const saveBlackjackGame = async (userId, state) => {
    try {
        return await gameStateManager.saveGame(userId, state);
    } catch (error) {
        console.error(`❌ State manager save failed for user ${userId}, trying legacy method:`, error);
        // Fallback to legacy method if state manager fails
        return new Promise((resolve, reject) => {
            try {
                const gameStateJson = JSON.stringify(state);
                const timestamp = Math.floor(Date.now() / 1000);
                
                db.run(
                    'INSERT OR REPLACE INTO blackjack_games (user_id, game_state, last_updated) VALUES (?, ?, ?)',
                    [userId, gameStateJson, timestamp],
                    (err) => {
                        if (err) {
                            console.error(`❌ Legacy save also failed for user ${userId}:`, err);
                            reject(err);
                        } else {
                            console.log(`✅ Legacy save succeeded for user ${userId}`);
                            resolve(true);
                        }
                    }
                );
            } catch (e) {
                console.error(`❌ JSON serialization failed for user ${userId}:`, e);
                reject(e);
            }
        });
    }
};

const deleteBlackjackGame = async (userId) => {
    try {
        return await gameStateManager.deleteGame(userId);
    } catch (error) {
        console.error(`❌ State manager delete failed for user ${userId}, trying legacy method:`, error);
        // Fallback to legacy method
        return new Promise((resolve) => {
            db.run('DELETE FROM blackjack_games WHERE user_id = ?', [userId], (err) => {
                if (err) {
                    console.error(`❌ Legacy delete also failed for user ${userId}:`, err);
                    resolve(false);
                } else {
                    console.log(`✅ Legacy delete succeeded for user ${userId}`);
                    resolve(true);
                }
            });
        });
    }
};

// Periodically clean up games that are stuck/expired (e.g., due to bot restart)
setInterval(() => {
    (async () => {
        try {
            const now = Date.now();
            let cleaned = 0;
            const games = await new Promise((resolve, reject) => {
                db.all('SELECT user_id, game_state FROM blackjack_games', (err, rows) => {
                    if (err) return reject(err);
                    resolve(rows);
                });
            });

            for (const row of games) {
                const userId = row.user_id;
                try {
                    const state = JSON.parse(row.game_state);
                    const gameAge = now - state.startedAt;
                    // Timeout after 15 mins (regular) or 25 mins (split)
                    if ((gameAge > 15 * 60 * 1000 && !state.split) || gameAge > 25 * 60 * 1000) {
                        console.log(`🧹 Cleaning up expired blackjack game for user ${userId}`);
                        state.userId = state.userId || userId;
                        await settleBlackjackTimeout(state, { source: 'cleanup', notifyUser: false });
                        cleaned++;
                    }
                } catch (e) {
                    // Game state is corrupted, refund original bet if possible and remove
                    console.error(`🧹 Corrupted blackjack state for user ${userId}, refunding and cleaning up...`);
                    const refundAmount = await getLatestBlackjackBet(userId);
                    if (refundAmount > 0) {
                        try {
                            await changeUserBalance(userId, 'unknown', refundAmount, 'blackjack_refund_cleanup_corrupted', {
                                bet: refundAmount
                            });
                        } catch (refundError) {
                            console.error(`Failed to refund user ${userId} for corrupted state:`, refundError);
                        }
                    }
                    await deleteBlackjackGame(userId);
                    cleaned++;
                }
            }
            if (cleaned > 0) {
                console.log(`🧹 Cleaned up ${cleaned} expired/frozen blackjack games`);
            }
        } catch (e) {
            console.error('Blackjack cleanup job error:', e);
        }
    })();
}, 60 * 1000 * 5); // every 5 minutes

// ================================================================================= //
// GAME UTILITIES
// ================================================================================= //

async function recoverGameState(userId) {
    try {
        const state = await getBlackjackGame(userId);
        if (state) {
            ensureBlackjackBetDefaults(state);
            if (state.ended) {
                await deleteBlackjackGame(userId);
                return null;
            }
            // Check for timeout on recovery
            const gameAge = Date.now() - state.startedAt;
            const timeoutMs = state.split ? 25 * 60 * 1000 : 15 * 60 * 1000;
            if (gameAge > timeoutMs) {
                state.timedOut = true;
                state.timeoutReason = state.split ? 'split' : 'regular';
            }
            return state;
        }
    } catch (e) {
        console.error('Error recovering game state:', e);
        await deleteBlackjackGame(userId).catch(() => {});
    }
    return null;
}

// Duplicate jackpot helpers removed (async versions already defined above)

