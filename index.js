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

    // Handle split hands with glassmorphism pill badges
    const playerHands = state.split ? [state.player, state.splitHand] : [state.player];
    const playerLines = playerHands.map((hand, idx) => {
        const isCurrentHand = state.split && state.currentSplitHand === (idx + 1);
        const handLabel = state.split ? (idx === 0 ? 'Hand 1' : 'Hand 2') : 'Player';
        const handCards = handEmoji(hand);
        const handTotal = handValue(hand);
        const activeIndicator = isCurrentHand ? '✨ ' : '  ';

        // Pill badge styling for totals (glassmorphism effect)
        const totalBadge = handTotal > 21 ? '💥 BUST' :
                          handTotal === 21 ? '🎉 BLACKJACK' :
                          `🎯 ${handTotal}`;

        // Enhanced card container with glassmorphism borders
        const cardContainer = `\n║ ${activeIndicator}${handLabel}: ${handCards}║`;
        const totalContainer = `\n║     ${totalBadge.padEnd(25,' ')}║`;

        return cardContainer + totalContainer;
    }).join('\n╟────────────────────────────────────────────────────────────────────────────╢');

    // Glassmorphism table design with enhanced borders
    const table = [
        '╔════════════════════════════════════════════════════════════════════════════════╗',
        '║                        🎰 PREMIUM BLACKJACK CASINO 🎰                         ║',
        '║                           💎 GLASSMORPHISM THEME 💎                           ║',
        '╠════════════════════════════════════════════════════════════════════════════════╣',
        '║                                                                                ║',
        `║ 🎯 DEALER: ${dealerLine.padEnd(65,' ')}║`,
        `║ 🎯 TOTAL : ${dealerTotal.padEnd(20,' ')}║`,
        '║                                                                                ║',
        '╟────────────────────────────────────────────────────────────────────────────╢',
        '║                                                                                ║',
        playerLines,
        '║                                                                                ║',
        '╚════════════════════════════════════════════════════════════════════════════════╝'
    ].join('\n');

    const note = opts.note ? `\n\n${opts.note}` : '';
    const gameStatus = state.split ? `\n🎮 Playing ${state.currentSplitHand === 1 ? 'Hand 1' : 'Hand 2'} of 2` : '';

    // Dynamic colors for glassmorphism effect
    let embedColor = '#0f0f23'; // Dark casino glass base
    if (state.split) embedColor = '#1a0d2e'; // Purple glow for splits
    if (opts.result) {
        if (opts.result.includes('WIN') || opts.result.includes('BLACKJACK')) embedColor = '#0d4f3c'; // Green success
        if (opts.result.includes('LOSE') || opts.result.includes('BUST')) embedColor = '#4f0d0d'; // Red failure
        if (opts.result.includes('PUSH')) embedColor = '#4a4a0d'; // Yellow push
    }

    return new EmbedBuilder()
        .setColor(embedColor)
        .setTitle('🎰 PREMIUM BLACKJACK CASINO')
        .setDescription(`${table}${note}${gameStatus}`)
        .setFooter({ text: `💰 Bet: ${state.bet}${state.split ? ' per hand' : ''} • 🎲 Glassmorphism Theme • ⚡ Lightning Fast` });
}

function bjComponents(state) {
    if (state.ended) return [];

    // Row 1: Core actions with glassmorphism styling
    const row1 = [
        { type: 2, style: 1, label: '🎯 HIT', custom_id: `nbj_hit:${state.userId}` },
        { type: 2, style: 2, label: '✋ STAND', custom_id: `nbj_stand:${state.userId}` },
        { type: 2, style: 3, label: '💰 DOUBLE', custom_id: `nbj_double:${state.userId}`, disabled: !bjCanDouble(state) }
    ];

    // Row 2: Advanced actions with visual states
    const row2 = [
        { type: 2, style: 1, label: '✂️ SPLIT', custom_id: `nbj_split:${state.userId}`, disabled: !bjCanSplit(state) },
        { type: 2, style: 4, label: '🏳️ SURRENDER', custom_id: `nbj_surrender:${state.userId}` }
    ];

    const components = [];
    if (row1.length > 0) components.push({ type: 1, components: row1 });
    if (row2.length > 0) components.push({ type: 1, components: row2 });

    return components;
}

async function bjUpdateView(state, opts = {}, interaction = null) {
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
    bjGames.delete(state.userId);
    
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
    
    const embed = bjBuildEmbed(state, { note: `\n${lines.join('\n')}` });
    await updateGame(interaction, state, { embeds: [embed], components: [] });

    if (payout > 0) {
        await changeUserBalance(interaction.user.id, interaction.user.username, payout, 'blackjack_payout', { outcome, pv, dv });
    }
    try {
        const balance = await getUserBalance(interaction.user.id);
        await interaction.followUp({ content: `💰 Current balance: ${balance} vouch points.`, ephemeral: true });
    } catch {}
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
        else if (dv > 21) { outcome = 'win'; payout = state.bet; }
        else if (pv > dv) { outcome = 'win'; payout = state.bet; }
        else if (pv === dv) { outcome = 'push'; payout = state.bet; }
        if (pv === 21 && hand.cards.length === 2) { outcome = 'blackjack'; payout = Math.floor(state.bet * 1.5); }
        
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

    const embed = bjBuildEmbed(state, { note: `\n${lines.join('\n')}` });
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

client.once('ready', async () => {
    console.log(`✅ Bot is online! Logged in as ${client.user.tag}`);
    client.user.setActivity('for pictures in #vouch', { type: 'WATCHING' });
    await ensureUsernameColumn();
    await setMultiplier(await getMultiplier());
    await scheduleMultiplierExpiryIfNeeded(client);

    // Set up periodic leaderboard updates (every 30 minutes)
    setInterval(async () => {
        try {
            await updateLiveLeaderboard(client);
        } catch (error) {
            console.error('Periodic leaderboard update error:', error);
        }
    }, 30 * 60 * 1000); // 30 minutes

    console.log('📊 Live leaderboard system initialized - updates every 30 minutes');
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
                    // Update live leaderboard
                    if (client) {
                        updateLiveLeaderboard(client).catch(err => console.error('Leaderboard update error:', err));
                    }
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
                    // Update live leaderboard
                    if (client) {
                        updateLiveLeaderboard(client).catch(err => console.error('Leaderboard update error:', err));
                    }
                    resolve(pointsToAdd);
                });
            }
        });
    });
}

// Function to update live leaderboard in #leaderboard channel
async function updateLiveLeaderboard(client) {
    try {
        // Find the leaderboard channel
        const leaderboardChannel = client.channels.cache.find(ch =>
            ch.name.toLowerCase() === 'leaderboard' && ch.type === 0
        );

        if (!leaderboardChannel) {
            console.log('Leaderboard channel not found. Skipping leaderboard update.');
            return;
        }

        // Get leaderboard data
        const leaderboardData = await new Promise((resolve, reject) => {
            db.all('SELECT user_id, points, username FROM vouch_points ORDER BY points DESC LIMIT 20', [], (err, rows) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(rows || []);
            });
        });

        if (leaderboardData.length === 0) {
            console.log('No leaderboard data available.');
            return;
        }

        // Create leaderboard embed
        const embed = new EmbedBuilder()
            .setColor('#ffd700')
            .setTitle('🏆 Live Vouch Points Leaderboard')
            .setDescription('Real-time rankings updated automatically!')
            .setTimestamp()
            .setFooter({ text: 'Updated when points are awarded' });

        let leaderboardText = '';
        for (let i = 0; i < leaderboardData.length; i++) {
            const { user_id, points, username } = leaderboardData[i];
            const medal = i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : '🔸';
            const displayName = username || 'Unknown User';
            leaderboardText += `${medal} **${i + 1}.** <@${user_id}> - **${points}** points\n`;
        }

        embed.addFields({
            name: '🏅 Rankings',
            value: leaderboardText || 'No points awarded yet!',
            inline: false
        });

        // Try to find existing leaderboard message
        let existingMessage = null;
        try {
            const messages = await leaderboardChannel.messages.fetch({ limit: 50 });
            existingMessage = messages.find(msg =>
                msg.author.id === client.user.id &&
                msg.embeds.length > 0 &&
                msg.embeds[0].title === '🏆 Live Vouch Points Leaderboard'
            );
        } catch (error) {
            console.log('Error fetching messages:', error.message);
        }

        // Update or create the leaderboard message
        if (existingMessage) {
            await existingMessage.edit({ embeds: [embed] });
            console.log('✅ Updated existing leaderboard message');
        } else {
            await leaderboardChannel.send({ embeds: [embed] });
            console.log('✅ Created new leaderboard message');
        }

    } catch (error) {
        console.error('Error updating live leaderboard:', error);
    }
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

            // Get game state with better error handling
            let state = bjGames.get(ownerId);
            const isLegacyOnly = !state && isOldBJ;
            if (!state && isLegacyOnly) {
                try { await interaction.followUp({ content: 'Game was reset to new version. Start a fresh /blackjack.', ephemeral: true }); } catch {}
                return;
            }
            if (!state) { 
                try { await interaction.followUp({ content: 'No active game found. Use `/blackjack` to start a new game.', ephemeral: true }); } catch {} 
                return; 
            }
            if (state.ended) { 
                try { await interaction.followUp({ content: 'Game already finished. Use `/blackjack` to start a new game.', ephemeral: true }); } catch {} 
                return; 
            }
            
            // Check if game is too old (5 minutes timeout)
            const gameAge = Date.now() - state.startedAt;
            if (gameAge > 5 * 60 * 1000) {
                bjGames.delete(ownerId);
                try { await interaction.followUp({ content: 'Game timed out after 5 minutes. Use `/blackjack` to start a new game.', ephemeral: true }); } catch {}
                return;
            }

            try {
                if (action === 'hit') {
                    if (state.split) {
                        // Handle split hand hitting - alternate between hands
                        const currentHand = state.currentSplitHand === 1 ? state.player : state.splitHand;
                        currentHand.push(bjDraw(state));
                        const pv = handValue(currentHand);
                        if (pv >= 21) {
                            // Move to next hand or resolve
                            if (state.currentSplitHand === 1) {
                                state.currentSplitHand = 2;
                                await bjUpdateView(state, { hideDealerHole: true, note: '\n🎯 Hand 1 complete! Now playing Hand 2.' }, interaction);
                            } else {
                                await bjResolve(interaction, state, 'stand');
                            }
                        } else {
                            await bjUpdateView(state, { hideDealerHole: true, note: `\n🎯 You hit ${state.currentSplitHand === 1 ? 'Hand 1' : 'Hand 2'}.` }, interaction);
                        }
                    } else {
                        state.player.push(bjDraw(state));
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
                    await bjResolve(interaction, state, 'stand');
                } else if (action === 'split') {
                    if (!bjCanSplit(state)) {
                        try { await interaction.followUp({ content: 'Cannot split these cards. Cards must be the same rank (e.g., 8-8, J-Q, 10-K).', ephemeral: true }); } catch {}
                        return;
                    }
                    const bal = await getUserBalance(ownerId);
                    if (bal < state.bet) {
                        try { await interaction.followUp({ content: `Not enough points to split. Need ${state.bet} more points.`, ephemeral: true }); } catch {}
                        return;
                    }

                    try {
                        await changeUserBalance(ownerId, interaction.user.username, -state.bet, 'blackjack_split_bet', { bet: state.bet });

                        // Split the hand
                        state.split = true;
                        state.splitHand = [state.player[1]]; // Second card becomes split hand
                        state.player = [state.player[0]]; // First card stays in main hand
                        state.currentSplitHand = 1; // Start with first hand

                        // Deal one card to each hand
                        state.player.push(bjDraw(state));
                        state.splitHand.push(bjDraw(state));

                        await bjUpdateView(state, { hideDealerHole: true, note: '\n🎉 Hands split successfully! Playing Hand 1.' }, interaction);
                    } catch (error) {
                        console.error('Split error:', error);
                        try { await interaction.followUp({ content: 'Error processing split. Please try again.', ephemeral: true }); } catch {}
                    }
                } else if (action === 'surrender') {
                    await bjResolve(interaction, state, 'surrender');
                }
            } catch (e) {
                console.error('Blackjack button error:', e);
                try { await interaction.followUp({ content: '❌ Error processing action.', ephemeral: true }); } catch {}
            }
        return;
    } catch (error) {
        console.error('Button interaction error:', error);
        try { await interaction.followUp({ content: '❌ An unexpected error occurred. Please try again.', ephemeral: true }); } catch {}
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
                    // Update live leaderboard
                    updateLiveLeaderboard(interaction.client).catch(err => console.error('Leaderboard update error:', err));
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
                        // Update live leaderboard
                        updateLiveLeaderboard(interaction.client).catch(err => console.error('Leaderboard update error:', err));
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

            const state = { userId: interaction.user.id, bet, player: [], dealer: [], startedAt: Date.now(), ended: false, channelId: interaction.channelId, messageId: null, shoe: bjCreateShoe(), doubled: false, split: false, splitHand: null, currentSplitHand: 1 };
            bjGames.set(interaction.user.id, state);
            await bjDealInitial(state);

            const initialEmbed = bjBuildEmbed(state, { hideDealerHole: true, note: '\nYour move: Hit, Stand, Double, or Surrender.' });
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
            await bjUpdateView(state, { hideDealerHole: true, note: '\nYour move: Hit, Stand, Double, or Surrender.' }, interaction);

            // Auto-timeout to stand after 60s (increased from 30s)
            setTimeout(async () => {
                const s = bjGames.get(interaction.user.id);
                if (!s || s.ended) return;
                await bjResolve(interaction, s, 'stand', true);
            }, 60000);
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

    if (interaction.commandName === 'updateleaderboard') {
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
            await updateLiveLeaderboard(interaction.client);
            await interaction.editReply('✅ Live leaderboard has been updated in the #leaderboard channel!');
        } catch (error) {
            console.error('Manual leaderboard update error:', error);
            await interaction.editReply('❌ Error updating leaderboard. Check the console for details.');
        }
    }

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
            name: 'updateleaderboard',
            description: 'Admin: Force update the live leaderboard in #leaderboard channel',
            default_member_permissions: PermissionFlagsBits.Administrator.toString(),
            dm_permission: false
        },
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

});

// Login to Discord
client.login(process.env.DISCORD_TOKEN);


