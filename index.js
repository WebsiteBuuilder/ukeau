const { Client, GatewayIntentBits, Partials, EmbedBuilder, REST, Routes, PermissionFlagsBits, Events } = require('discord.js');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');
require('dotenv').config();

const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
if (!DISCORD_TOKEN) {
    console.error('Missing DISCORD_TOKEN environment variable.');
    process.exit(1);
}

const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent,
        GatewayIntentBits.GuildMembers
    ],
    partials: [Partials.Channel]
});

const dbPath = (() => {
    if (process.env.DB_PATH) return process.env.DB_PATH;
    if (fs.existsSync('/data')) {
        return path.join('/data', 'vouch_points.db');
    }
    return path.resolve('./vouch_points.db');
})();

try {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
} catch (error) {
    console.error('Failed to ensure database directory exists:', error);
}

const db = new sqlite3.Database(dbPath);

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS vouch_points (
        user_id TEXT PRIMARY KEY,
        username TEXT,
        points INTEGER DEFAULT 0,
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )`);
});

function ensureVouchUsernameColumn() {
    db.all('PRAGMA table_info(vouch_points)', (err, rows) => {
        if (err) {
            console.error('Failed to inspect vouch_points schema:', err);
            return;
        }
        const hasUsername = rows.some((row) => String(row.name).toLowerCase() === 'username');
        if (!hasUsername) {
            db.run('ALTER TABLE vouch_points ADD COLUMN username TEXT', (alterErr) => {
                if (alterErr) {
                    console.error('Failed to add username column to vouch_points:', alterErr);
                }
            });
        }
    });
}

ensureVouchUsernameColumn();

function getSetting(key, fallback = null) {
    return new Promise((resolve, reject) => {
        db.get('SELECT value FROM settings WHERE key = ?', [key], (err, row) => {
            if (err) { reject(err); return; }
            if (!row) { resolve(fallback); return; }
            resolve(row.value);
        });
    });
}

function setSetting(key, value) {
    return new Promise((resolve, reject) => {
        db.run(
            `INSERT INTO settings (key, value) VALUES (?, ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
            [key, value],
            (err) => {
                if (err) { reject(err); return; }
                resolve();
            }
        );
    });
}

async function getMultiplier() {
    const raw = await getSetting('multiplier', '1');
    const numeric = Number(raw);
    if (!Number.isFinite(numeric) || numeric < 1) {
        await setMultiplier(1);
        return 1;
    }
    return Math.floor(numeric);
}

async function setMultiplier(value) {
    const safe = Math.max(1, Math.floor(Number(value) || 1));
    await setSetting('multiplier', String(safe));
    return safe;
}

function getUserPoints(userId) {
    return new Promise((resolve, reject) => {
        db.get('SELECT points FROM vouch_points WHERE user_id = ?', [userId], (err, row) => {
            if (err) { reject(err); return; }
            resolve(row ? row.points : 0);
        });
    });
}

function changeUserBalance(userId, username, delta) {
    return new Promise((resolve, reject) => {
        db.serialize(() => {
            db.get('SELECT points FROM vouch_points WHERE user_id = ?', [userId], (err, row) => {
                if (err) { reject(err); return; }
                const current = row ? row.points : 0;
                const next = Math.max(0, current + delta);
                db.run(
                    `INSERT INTO vouch_points (user_id, username, points, last_updated)
                     VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                     ON CONFLICT(user_id) DO UPDATE SET
                        username = excluded.username,
                        points = excluded.points,
                        last_updated = CURRENT_TIMESTAMP`,
                    [userId, username, next],
                    (updateErr) => {
                        if (updateErr) { reject(updateErr); return; }
                        resolve(next);
                    }
                );
            });
        });
    });
}

function formatPoints(points) {
    return `${points} point${points === 1 ? '' : 's'}`;
}

function isImageAttachment(attachment) {
    if (!attachment) return false;
    const contentType = attachment.contentType || '';
    if (contentType.startsWith('image/')) return true;
    const name = attachment.name ? attachment.name.toLowerCase() : '';
    return ['.png', '.jpg', '.jpeg', '.gif', '.webp'].some((ext) => name.endsWith(ext));
}

const providerRoleCache = new Map();

async function resolveProviderRole(guild) {
    if (!guild) return null;
    const cached = providerRoleCache.get(guild.id);
    if (cached && guild.roles.cache.has(cached.id)) {
        return guild.roles.cache.get(cached.id);
    }

    try {
        await guild.roles.fetch();
    } catch (error) {
        console.error(`Failed to fetch roles for guild ${guild.id}:`, error);
    }

    const roleId = process.env.PROVIDER_ROLE_ID;
    const roleName = process.env.PROVIDER_ROLE_NAME;

    let role = null;
    if (roleId) {
        role = guild.roles.cache.get(roleId);
    }
    if (!role && roleName) {
        const lower = roleName.toLowerCase();
        role = guild.roles.cache.find((r) => r.name.toLowerCase() === lower);
    }

    if (role) {
        providerRoleCache.set(guild.id, { id: role.id, fetchedAt: Date.now() });
    }

    return role || null;
}

function channelMatchesVouchName(channel) {
    if (!channel) return false;
    const name = channel.name ? channel.name.toLowerCase() : '';
    if (name.includes('vouch')) return true;
    if (channel.parent && channel.parent.name && channel.parent.name.toLowerCase().includes('vouch')) {
        return true;
    }
    return false;
}

async function handleVouchMessage(message) {
    if (message.author.bot) return;
    if (!message.guild) return;
    if (!channelMatchesVouchName(message.channel)) return;

    const providerRole = await resolveProviderRole(message.guild);
    if (!providerRole) return;

    const mentionedMembers = message.mentions?.members;
    if (!mentionedMembers || mentionedMembers.size === 0) return;

    const providerMember = mentionedMembers.find((member) => member.roles.cache.has(providerRole.id));
    if (!providerMember) return;

    const attachments = [...message.attachments.values()].filter((att) => isImageAttachment(att));
    if (attachments.length === 0) return;

    try {
        const multiplier = await getMultiplier();
        const awarded = multiplier;
        const displayName = providerMember.displayName || providerMember.user.username;
        const total = await changeUserBalance(providerMember.id, displayName, awarded);

        const embed = new EmbedBuilder()
            .setColor(0x2ecc71)
            .setTitle('Vouch recorded!')
            .setDescription(`${providerMember} now has **${total}** vouch points.`)
            .addFields(
                { name: 'Provider', value: `${providerMember}`, inline: true },
                { name: 'Voucher', value: `${message.author}`, inline: true },
                { name: 'Awarded', value: `${formatPoints(awarded)} (x${multiplier})`, inline: true }
            )
            .setTimestamp();

        await message.reply({ embeds: [embed] }).catch(() => {});

        const dmEmbed = new EmbedBuilder()
            .setColor(0x2ecc71)
            .setTitle('You received a vouch point!')
            .setDescription(`You were vouched by ${message.author} in **${message.guild.name}**.`)
            .addFields(
                { name: 'New total', value: `${total} vouch points`, inline: true },
                { name: 'Multiplier', value: `x${multiplier}`, inline: true }
            )
            .setTimestamp();

        await providerMember.send({ embeds: [dmEmbed] }).catch(() => {});
    } catch (error) {
        console.error('Failed to process vouch message:', error);
    }
}

const slashCommands = [
    {
        name: 'vouchpoints',
        description: 'Check vouch points for yourself or another user.',
        dm_permission: false,
        options: [
            {
                name: 'user',
                description: 'User to check',
                type: 6,
                required: false
            }
        ]
    },
    {
        name: 'addpoints',
        description: 'Admin: Add vouch points to a user.',
        default_member_permissions: PermissionFlagsBits.Administrator.toString(),
        dm_permission: false,
        options: [
            {
                name: 'user',
                description: 'User to modify',
                type: 6,
                required: true
            },
            {
                name: 'amount',
                description: 'Number of points to add',
                type: 4,
                required: true,
                min_value: 1
            }
        ]
    },
    {
        name: 'removepoints',
        description: 'Admin: Remove vouch points from a user.',
        default_member_permissions: PermissionFlagsBits.Administrator.toString(),
        dm_permission: false,
        options: [
            {
                name: 'user',
                description: 'User to modify',
                type: 6,
                required: true
            },
            {
                name: 'amount',
                description: 'Number of points to remove',
                type: 4,
                required: true,
                min_value: 1
            }
        ]
    },
    {
        name: 'setmultiplier',
        description: 'Admin: Set the global vouch multiplier (minimum 1).',
        default_member_permissions: PermissionFlagsBits.Administrator.toString(),
        dm_permission: false,
        options: [
            {
                name: 'value',
                description: 'New multiplier value',
                type: 4,
                required: true,
                min_value: 1
            }
        ]
    },
    {
        name: 'multiplierstatus',
        description: 'Show the current global vouch multiplier.',
        dm_permission: false
    },
    {
        name: 'resetmultiplier',
        description: 'Admin: Reset the vouch multiplier back to 1x.',
        default_member_permissions: PermissionFlagsBits.Administrator.toString(),
        dm_permission: false
    }
];

async function registerGuildCommands(guild) {
    if (!client.user || !guild) return;
    const rest = new REST({ version: '10' }).setToken(DISCORD_TOKEN);
    try {
        await rest.put(
            Routes.applicationGuildCommands(client.user.id, guild.id),
            { body: slashCommands }
        );
        console.log(`Registered slash commands for guild ${guild.name} (${guild.id})`);
    } catch (error) {
        console.error(`Failed to register commands for guild ${guild?.id}:`, error);
    }
}

client.once(Events.ClientReady, async () => {
    console.log(`Logged in as ${client.user.tag}`);
    try {
        const guilds = await client.guilds.fetch();
        for (const guildData of guilds.values()) {
            const guild = await client.guilds.fetch(guildData.id);
            await registerGuildCommands(guild);
        }
    } catch (error) {
        console.error('Failed to register commands on startup:', error);
    }
});

client.on('guildCreate', async (guild) => {
    console.log(`Joined new guild: ${guild.name} (${guild.id})`);
    await registerGuildCommands(guild);
});

client.on('interactionCreate', async (interaction) => {
    if (!interaction.isChatInputCommand()) return;

    try {
        switch (interaction.commandName) {
            case 'vouchpoints': {
                const user = interaction.options.getUser('user') || interaction.user;
                const points = await getUserPoints(user.id);
                await interaction.reply({
                    content: `${user} has ${formatPoints(points)}.`,
                    ephemeral: false
                });
                break;
            }
            case 'addpoints': {
                if (!interaction.memberPermissions?.has(PermissionFlagsBits.Administrator)) {
                    await interaction.reply({ content: 'You need administrator permissions to use this command.', ephemeral: true });
                    return;
                }
                const user = interaction.options.getUser('user', true);
                const amount = interaction.options.getInteger('amount', true);
                const member = await interaction.guild.members.fetch(user.id).catch(() => null);
                const name = member?.displayName || user.username;
                const total = await changeUserBalance(user.id, name, amount);
                await interaction.reply({ content: `Added ${formatPoints(amount)} to ${user}. New total: ${formatPoints(total)}.` });
                break;
            }
            case 'removepoints': {
                if (!interaction.memberPermissions?.has(PermissionFlagsBits.Administrator)) {
                    await interaction.reply({ content: 'You need administrator permissions to use this command.', ephemeral: true });
                    return;
                }
                const user = interaction.options.getUser('user', true);
                const amount = interaction.options.getInteger('amount', true);
                const member = await interaction.guild.members.fetch(user.id).catch(() => null);
                const name = member?.displayName || user.username;
                const total = await changeUserBalance(user.id, name, -amount);
                await interaction.reply({ content: `Removed ${formatPoints(amount)} from ${user}. New total: ${formatPoints(total)}.` });
                break;
            }
            case 'setmultiplier': {
                if (!interaction.memberPermissions?.has(PermissionFlagsBits.Administrator)) {
                    await interaction.reply({ content: 'You need administrator permissions to use this command.', ephemeral: true });
                    return;
                }
                const value = interaction.options.getInteger('value', true);
                const multiplier = await setMultiplier(value);
                await interaction.reply({ content: `Multiplier updated to x${multiplier}.` });
                break;
            }
            case 'multiplierstatus': {
                const multiplier = await getMultiplier();
                await interaction.reply({ content: `The current multiplier is x${multiplier}.` });
                break;
            }
            case 'resetmultiplier': {
                if (!interaction.memberPermissions?.has(PermissionFlagsBits.Administrator)) {
                    await interaction.reply({ content: 'You need administrator permissions to use this command.', ephemeral: true });
                    return;
                }
                await setMultiplier(1);
                await interaction.reply({ content: 'Multiplier reset to x1.' });
                break;
            }
            default:
                break;
        }
    } catch (error) {
        console.error(`Command handler error for ${interaction.commandName}:`, error);
        if (interaction.deferred || interaction.replied) {
            await interaction.followUp({ content: 'An error occurred while processing that command.', ephemeral: true }).catch(() => {});
        } else {
            await interaction.reply({ content: 'An error occurred while processing that command.', ephemeral: true }).catch(() => {});
        }
    }
});

client.on('messageCreate', handleVouchMessage);

client.login(DISCORD_TOKEN).catch((error) => {
    console.error('Failed to login to Discord:', error);
    process.exit(1);
});
