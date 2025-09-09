const { Client, GatewayIntentBits, AttachmentBuilder, EmbedBuilder, PermissionFlagsBits, REST, Routes } = require('discord.js');
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
const dbPath = process.env.DATABASE_PATH || '/data/vouch_points.db';
// Ensure directory exists (Railway persistent volume: /data)
try {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
} catch (e) {
    console.error('Failed to ensure database directory exists:', e);
}

// Migrate legacy DB from project root to /data if present and destination missing
try {
    const legacyPath = path.resolve('./vouch_points.db');
    if (!fs.existsSync(dbPath) && fs.existsSync(legacyPath)) {
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
        user_id TEXT NOT NULL,
        delta INTEGER NOT NULL,
        reason TEXT NOT NULL,
        meta TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);
    db.run('CREATE INDEX IF NOT EXISTS idx_ledger_user ON ledger(user_id)');
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

// Casino helpers
const bjGames = new Map(); // userId -> state
const activeCooldowns = new Map(); // key -> timestamp
const BLACKJACK_COOLDOWN_MS = 10 * 1000;
const ROULETTE_COOLDOWN_MS = 10 * 1000;
const SLOTS_COOLDOWN_MS = 5 * 1000;

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
    const nameOk = interaction.channel?.name?.toLowerCase().includes('casino');
    if (allowedId) return interaction.channelId === allowedId;
    return Boolean(nameOk);
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
                    db.run('INSERT INTO ledger (user_id, delta, reason, meta) VALUES (?, ?, ?, ?)', [userId, delta, reason, meta ? JSON.stringify(meta) : null], (e3) => {
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
function handEmoji(cards) { return cards.map(c => `🃏${c}`).join(' '); }
function hidden(n) { return Array.from({ length: n }, () => '🂠').join(' '); }
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
    return !state.ended && state.player.length === 2 && !state.doubled;
}

function bjBuildEmbed(state, opts = {}) {
    const hideDealerHole = !!opts.hideDealerHole;
    const dealerShown = hideDealerHole ? [state.dealer[0]] : state.dealer.slice();
    const dealerHiddenCount = hideDealerHole ? (state.dealer.length - 1) : 0;
    const dealerLine = `${handEmoji(dealerShown)}${dealerHiddenCount > 0 ? (' ' + hidden(dealerHiddenCount)) : ''}`;
    const dealerTotal = hideDealerHole ? `${handValue(dealerShown)}?` : `${handValue(state.dealer)}`;
    const playerLine = handEmoji(state.player);
    const playerTotal = handValue(state.player);

    const table = [
        '╔════════════════════════════════════════════════════════════╗',
        '║                        BLACKJACK                          ║',
        '╠════════════════════════════════════════════════════════════╣',
        `║ Dealer: ${dealerLine.padEnd(52,' ')}║`,
        `║ Total : ${dealerTotal.padEnd(52,' ')}║`,
        '╟────────────────────────────────────────────────────────────╢',
        `║ Player: ${playerLine.padEnd(52,' ')}║`,
        `║ Total : ${String(playerTotal).padEnd(52,' ')}║`,
        '╚════════════════════════════════════════════════════════════╝'
    ].join('\n');

    const note = opts.note ? `\n${opts.note}` : '';
    return new EmbedBuilder()
        .setColor('#2b2d31')
        .setTitle('')
        .setDescription(`${table}${note}`)
        .setFooter({ text: `Bet: ${state.bet}` });
}

function bjComponents(state) {
    if (state.ended) return [];
    const comps = [
        { type: 2, style: 1, label: 'Hit', custom_id: `nbj_hit:${state.userId}` },
        { type: 2, style: 2, label: 'Stand', custom_id: `nbj_stand:${state.userId}` }
    ];
    if (bjCanDouble(state)) comps.push({ type: 2, style: 3, label: 'Double', custom_id: `nbj_double:${state.userId}` });
    comps.push({ type: 2, style: 4, label: 'Surrender', custom_id: `nbj_surrender:${state.userId}` });
    return [{ type: 1, components: comps }];
}

async function bjUpdateView(state, opts = {}) {
    const embed = bjBuildEmbed(state, opts);
    await editGameMessage(state, { embeds: [embed], components: bjComponents(state) });
}

function bjApplyDealerInitialFairness(state) {
    // If dealer starts with 20/21 too often, soften it by attempting to swap dealer's second card.
    const dv = handValue(state.dealer);
    if (dv >= 20) {
        for (let attempt = 0; attempt < 3; attempt++) {
            // Prefer lower card (<=6) if available in shoe
            let idx = -1;
            for (let i = 0; i < state.shoe.length; i++) {
                const v = cardValue(state.shoe[i]);
                if (v <= 6) { idx = i; break; }
            }
            if (idx === -1) break;
            const replacement = state.shoe.splice(idx, 1)[0];
            // Put previous second card back into shoe randomly
            const prev = state.dealer[1];
            state.dealer[1] = replacement;
            // return prev back randomly into shoe
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
    bjGames.delete(state.userId);
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
    // Natural blackjack bonus (two-card 21)
    if (pv === 21 && state.player.length === 2) { outcome = 'blackjack'; payout = Math.floor(state.bet * 2.5); }

    const lines = [];
    if (fromTimeout) lines.push('⏳ You took too long! Dealer automatically stands.');
    lines.push(`Dealer: ${handEmoji(state.dealer)} (total: ${dv})`);
    lines.push(`You: ${handEmoji(state.player)} (total: ${pv})`);
    const resultText = outcome === 'win' ? `You won ${payout - state.bet} (payout ${payout}).` :
                      outcome === 'push' ? `It's a push. Refunded ${payout}.` :
                      outcome === 'blackjack' ? `Blackjack! You won ${payout - state.bet} (payout ${payout}).` :
                      outcome === 'surrender' ? `You surrendered. Refunded ${payout}.` :
                      `You lost ${state.bet}.`;
    const embed = bjBuildEmbed(state, { note: `\n${lines.join('\n')}` });
    await editGameMessage(state, { embeds: [embed], components: [] });

    if (payout > 0) {
        await changeUserBalance(interaction.user.id, interaction.user.username, payout, 'blackjack_payout', { outcome, pv, dv });
    }
    try {
        const balance = await getUserBalance(interaction.user.id);
        await interaction.followUp({ content: `Current balance: ${balance} vouch points.`, ephemeral: true });
    } catch {}
}

// Removed legacy blackjack code above

client.once('ready', async () => {
    console.log(`✅ Bot is online! Logged in as ${client.user.tag}`);
    client.user.setActivity('for pictures in #vouch', { type: 'WATCHING' });
    await ensureUsernameColumn();
    await setMultiplier(await getMultiplier());
    await scheduleMultiplierExpiryIfNeeded(client);
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
                await interaction.deferUpdate().catch(async () => { try { await interaction.reply({ content: 'Working…', ephemeral: true }); } catch {} });
            }
            if (interaction.user.id !== ownerId) { try { await interaction.followUp({ content: 'This is not your game.', ephemeral: true }); } catch {} return; }

            // Prefer new blackjack state if exists
            let state = bjGames.get(ownerId);
            const isLegacyOnly = !state && isOldBJ;
            if (!state && isLegacyOnly) {
                try { await interaction.followUp({ content: 'Game was reset to new version. Start a fresh /blackjack.', ephemeral: true }); } catch {}
                return;
            }
            if (!state) { try { await interaction.followUp({ content: 'No active game found.', ephemeral: true }); } catch {} return; }
            if (state.ended) { try { await interaction.followUp({ content: 'Game already finished.', ephemeral: true }); } catch {} return; }

            if (action === 'hit') {
                state.player.push(bjDraw(state));
                const pv = handValue(state.player);
                if (pv >= 21) {
                    await bjResolve(interaction, state, 'stand');
                } else {
                    await bjUpdateView(state, { hideDealerHole: true, note: '\nYou hit.' });
                }
            } else if (action === 'stand') {
                await bjResolve(interaction, state, 'stand');
            } else if (action === 'double') {
                if (!bjCanDouble(state)) { try { await interaction.followUp({ content: 'Cannot double now.', ephemeral: true }); } catch {} return; }
                const bal = await getUserBalance(ownerId);
                if (bal < state.bet) { try { await interaction.followUp({ content: 'Not enough points to double.', ephemeral: true }); } catch {} return; }
                await changeUserBalance(ownerId, interaction.user.username, -state.bet, 'blackjack_double_bet', { bet: state.bet });
                state.bet *= 2;
                state.doubled = true;
                state.player.push(bjDraw(state));
                await bjResolve(interaction, state, 'stand');
            } else if (action === 'surrender') {
                await bjResolve(interaction, state, 'surrender');
            }
        } catch (e) {
            console.error('Blackjack button error:', e);
            try { await interaction.followUp({ content: '❌ Error processing action.', ephemeral: true }); } catch {}
        }
        return;
    }

    if (!interaction.isChatInputCommand()) return;
    
    if (interaction.commandName === 'vouchpoints') {
        const targetUser = interaction.options.getUser('user') || interaction.user;
        
        db.get('SELECT points, username FROM vouch_points WHERE user_id = ?', [targetUser.id], (err, row) => {
            if (err) {
                console.error('Database error:', err);
                interaction.reply({ content: '❌ Error retrieving vouch points!', ephemeral: true });
                return;
            }
            
            const points = row ? row.points : 0;
            const embed = new EmbedBuilder()
                .setColor('#0099ff')
                .setTitle('📊 Vouch Points')
                .setDescription(`${(row?.username) || targetUser.username} has **${points}** vouch points!`)
                .setThumbnail(targetUser.displayAvatarURL())
                .setTimestamp();
            
            interaction.reply({ embeds: [embed] });
        });
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
            if (bjGames.has(interaction.user.id)) { await interaction.reply({ content: 'You already have an active blackjack round.', ephemeral: true }); return; }
            const bet = Math.max(1, Math.floor(interaction.options.getInteger('amount') || 0));
            const balance = await getUserBalance(interaction.user.id);
            if (bet <= 0) { await interaction.reply({ content: 'Minimum bet is 1.', ephemeral: true }); return; }
            if (balance < bet) { await interaction.reply({ content: 'Insufficient points.', ephemeral: true }); return; }
            // Deduct bet upfront
            await changeUserBalance(interaction.user.id, interaction.user.username, -bet, 'blackjack_bet', { bet });
            setCooldown('bj:' + interaction.user.id);

            const state = { userId: interaction.user.id, bet, player: [], dealer: [], startedAt: Date.now(), ended: false, channelId: interaction.channelId, messageId: null, shoe: bjCreateShoe(), doubled: false };
            bjGames.set(interaction.user.id, state);
            await bjDealInitial(state);

            const embed = new EmbedBuilder()
                .setColor('#2b2d31')
                .setTitle('🃏 Blackjack — Dealer')
                .setDescription('Preparing the table…')
                .setFooter({ text: `Bet: ${bet}` });
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
            const sent = await interaction.reply({ embeds: [embed], components, fetchReply: true }).catch(() => null);
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
            // Show initial view
            await bjUpdateView(state, { hideDealerHole: true, note: '\nYour move: Hit, Stand, Double, or Surrender.' });

            // Auto-timeout to stand after 30s
            setTimeout(async () => {
                const s = bjGames.get(interaction.user.id);
                if (!s || s.ended) return;
                await bjResolve(interaction, s, 'stand', true);
            }, 30000);
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
            await interaction.reply({ embeds: [ new EmbedBuilder().setColor('#ff6b35').setDescription(wheelSpinning).setFooter({ text: '🎲 This server’s VIP casino' }) ] });
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
                await interaction.editReply({ embeds: [ new EmbedBuilder().setColor('#ff6b35').setDescription(frame).setFooter({ text: '🎲 This server’s VIP casino' }) ] });
            }
            const result = randomInt(0, 36);
            const redSet = new Set([1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]);
            const color = result === 0 ? 'green' : (redSet.has(result) ? 'red' : 'black');
            let win = 0;
            if (betType === 'red' || betType === 'black') { if (color === betType) win = amount * 2; }
            else if (betType === 'even' || betType === 'odd') { if (result !== 0 && (result % 2 === 0) === (betType === 'even')) win = amount * 2; }
            else if (betType === 'low' || betType === 'high') { if (result !== 0 && ((betType==='low' && result<=18) || (betType==='high' && result>=19))) win = amount * 2; }
            else if (betType === 'number' && Number.isInteger(number) && number >= 0 && number <= 36) { if (result === number) win = amount * 35; }

            const wheelResult = `
╔══════════════════════════════════════╗
║        🎰 PREMIUM ROULETTE 🎰        ║
║                                      ║
║           🏆 RESULT: ${result.toString().padStart(2,' ')} 🏆           ║
║                ${color==='red' ? '🔴' : color==='black' ? '⚫' : '🟢'}                ║
║                                      ║
║     ${win>0 ? '💰 WINNER! 💰' : '😤 BETTER LUCK NEXT TIME'}      ║
║                                      ║
║   💎 ONLY IN OUR VIP CASINO 💎      ║
╚══════════════════════════════════════╝`;
            const net = win - amount;
            const breakdown = `\nBet: ${amount} • Payout: ${win} • Net: ${net>=0?'+':''}${net}`;
            const embed = new EmbedBuilder()
                .setColor(win > 0 ? '#00c853' : '#c62828')
                .setDescription(wheelResult + breakdown)
                .setFooter({ text: `Bet: ${amount} • VIP rewards available` });
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
            const amount = Math.max(1, Math.floor(interaction.options.getInteger('amount') || 0));
            const balance = await getUserBalance(interaction.user.id);
            if (amount <= 0) { await interaction.reply({ content: 'Minimum bet is 1.', ephemeral: true }); return; }
            if (balance < amount) { await interaction.reply({ content: 'Insufficient points.', ephemeral: true }); return; }
            await changeUserBalance(interaction.user.id, interaction.user.username, -amount, 'slots_bet', { bet: amount });
            setCooldown('slots:' + interaction.user.id);

            const symbols = ['🍒','🍋','💎','🔔','7️⃣','🃏'];
            const weights = [30, 25, 15, 15, 10, 5]; // sum=100; 🃏 is rare
            function spin() {
                const roll = () => {
                    let r = Math.random() * 100, acc = 0;
                    for (let i = 0; i < symbols.length; i++) { acc += weights[i]; if (r <= acc) return symbols[i]; }
                    return symbols[0];
                };
                return [roll(), roll(), roll()];
            }

            // Slots immersive frames
            const frameBase = (line) => `
╔══════════════════════════════════════╗
║         💎 DIAMOND SLOT™ 💎          ║
║       VIP-ONLY — HIGH LIMITS         ║
║--------------------------------------║
║           ${line}           ║
║--------------------------------------║
║  🎉 Exclusive jackpots in this server only! 🎉  ║
╚══════════════════════════════════════╝`;
            await interaction.reply({ embeds: [ new EmbedBuilder().setColor('#7c4dff').setDescription(frameBase('🌀 🌀 🌀')).setFooter({ text: '🎰 Spinning…' }) ] });
            await new Promise(r => setTimeout(r, 500));
            await interaction.editReply({ embeds: [ new EmbedBuilder().setColor('#7c4dff').setDescription(frameBase('🍒 🌀 🌀')).setFooter({ text: '🎰 Reels stopping…' }) ] });
            await new Promise(r => setTimeout(r, 500));
            const [a,b,c] = spin();
            let payout = 0;
            if (a === b && b === c) payout = amount * 10;
            else if (a === b || b === c || a === c) payout = amount * 2;
            const line = `${a} ${b} ${c}`;
            const resultBox = `
╔══════════════════════════════════════╗
║         💎 DIAMOND SLOT™ 💎          ║
║       VIP-ONLY — HIGH LIMITS         ║
║--------------------------------------║
║           ${line}           ║
║--------------------------------------║
║ ${payout>0 ? '💰 JACKPOT! CLAIM YOUR VIP REWARDS 💰' : '😤 MISS! TRY THE VIP LUCK AGAIN'} ║
╚══════════════════════════════════════╝`;
            const net = payout - amount;
            const breakdown = `\nBet: ${amount} • Payout: ${payout} • Net: ${net>=0?'+':''}${net}`;
            const embed = new EmbedBuilder().setColor(payout>0?'#00c853':'#c62828').setDescription(resultBox + breakdown).setFooter({ text: `Bet: ${amount}` });
            await interaction.editReply({ content: undefined, embeds: [embed] });
            if (payout > 0) await changeUserBalance(interaction.user.id, interaction.user.username, payout, 'slots_payout', { a,b,c });
            // Balance after slots
            try { const bal = await getUserBalance(interaction.user.id); await interaction.followUp({ content: `Current balance: ${bal} vouch points.`, ephemeral: true }); } catch {}

        } catch (e) {
            console.error('Slots error:', e);
            try { await interaction.reply({ content: '❌ Error playing slots.', ephemeral: true }); } catch {}
        }
        return;
    }

    // Casino leaderboard
    if (interaction.commandName === 'casinoleaderboard') {
        try {
            db.all("SELECT user_id, SUM(delta) AS net FROM ledger WHERE reason LIKE 'blackjack_%' OR reason LIKE 'roulette_%' OR reason LIKE 'slots_%' GROUP BY user_id ORDER BY net DESC LIMIT 10", [], async (err, rows) => {
                if (err) { console.error('Ledger query error:', err); await interaction.reply({ content: '❌ Error.', ephemeral: true }); return; }
                if (!rows || rows.length === 0) { await interaction.reply({ content: 'No casino activity yet.', ephemeral: true }); return; }
                let text = '';
                for (let i = 0; i < rows.length; i++) {
                    const { user_id, net } = rows[i];
                    const medal = i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : '🔸';
                    text += `${medal} <@${user_id}> — ${net >= 0 ? '+' : ''}${net}\n`;
                }
                const embed = new EmbedBuilder().setColor('#ffd700').setTitle('🏆 Casino Leaderboard').setDescription(text);
                await interaction.reply({ embeds: [embed] });
            });
        } catch (e) {
            console.error('Casino leaderboard error:', e);
            try { await interaction.reply({ content: '❌ Error.', ephemeral: true }); } catch {}
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
    }
    if (interaction.commandName === 'vouchleaderboard' || interaction.commandName === 'leaderboard') {
        db.all('SELECT user_id, points, username FROM vouch_points ORDER BY points DESC LIMIT 10', [], (err, rows) => {
            if (err) {
                console.error('Database error:', err);
                interaction.reply({ content: '❌ Error retrieving leaderboard!', ephemeral: true });
                return;
            }
            
            if (rows.length === 0) {
                interaction.reply({ content: '📊 No vouch points have been awarded yet!', ephemeral: true });
                return;
            }
            
            let leaderboardText = '';
            for (let i = 0; i < rows.length; i++) {
                const { user_id, points, username } = rows[i];
                const medal = i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : '🔸';
                // Use mention to always resolve current display name; still show points
                leaderboardText += `${medal} **${i + 1}.** <@${user_id}> - ${points} points\n`;
                // Backfill username in DB if missing
                if (!username) {
                    client.users.fetch(user_id).then(u => {
                        if (u && u.username) {
                            db.run('UPDATE vouch_points SET username = ? WHERE user_id = ?', [u.username, user_id], () => {});
                        }
                    }).catch(() => {});
                }
            }
            
            const embed = new EmbedBuilder()
                .setColor('#ffd700')
                .setTitle('🏆 Vouch Points Leaderboard')
                .setDescription(leaderboardText)
                .setTimestamp();
            
            interaction.reply({ embeds: [embed] });
        });
    }
});

// Register slash commands when bot starts
client.once('ready', async () => {
    const commands = [
        {
            name: 'vouchpoints',
            description: 'Check your or someone else\'s vouch points',
            options: [
                {
                    name: 'user',
                    description: 'The user to check vouch points for',
                    type: 6,
                    required: false
                }
            ]
        },
        { name: 'leaderboard', description: 'View the vouch points leaderboard' },
        { name: 'casinoleaderboard', description: 'View casino net winners leaderboard' },
        {
            name: 'blackjack',
            description: 'Play blackjack against the dealer',
            options: [ { name: 'amount', description: 'Bet amount (>=1)', type: 4, required: true } ]
        },
        {
            name: 'roulette',
            description: 'Spin the roulette wheel',
            options: [
                { name: 'type', description: 'Bet type (red, black, even, odd, low, high, number)', type: 3, required: true },
                { name: 'amount', description: 'Bet amount (>=1)', type: 4, required: true },
                { name: 'number', description: 'Number (0-36) required for type=number', type: 4, required: false }
            ]
        },
        {
            name: 'slots',
            description: 'Pull the lever on slots',
            options: [ { name: 'amount', description: 'Bet amount (>=1)', type: 4, required: true } ]
        },
        {
            name: 'recountvouches',
            description: 'Admin: Recount all vouches in vouch channels and rebuild points',
            default_member_permissions: PermissionFlagsBits.Administrator.toString(),
            dm_permission: false,
            options: [
                { name: 'channel', description: 'Specific vouch channel to scan (optional)', type: 7, required: false }
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
        { name: 'multiplierstatus', description: 'Show current vouch multiplier' },
        { name: 'resetmultiplier', description: 'Admin: Reset multiplier to 1x', default_member_permissions: PermissionFlagsBits.Administrator.toString(), dm_permission: false },
        {
            name: 'wipevouches',
            description: 'Admin: Wipe all vouch points (irreversible)',
            default_member_permissions: PermissionFlagsBits.Administrator.toString(),
            dm_permission: false,
            options: [ { name: 'confirm', description: 'Type "yes" to confirm', type: 3, required: true } ]
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

// Error handling
client.on('error', console.error);
process.on('unhandledRejection', error => {
    console.error('Unhandled promise rejection:', error);
});

// Login to Discord
client.login(process.env.DISCORD_TOKEN);


