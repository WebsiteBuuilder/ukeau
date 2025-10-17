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
    db.run(`CREATE TABLE IF NOT EXISTS pending_vouches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id TEXT,
        channel_id TEXT,
        message_id TEXT,
        voucher_id TEXT,
        voucher_tag TEXT,
        message_content TEXT,
        attachment_urls TEXT,
        provider_id TEXT,
        provider_tag TEXT,
        approved INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        approved_at DATETIME
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

function getSafeUserTag(user) {
    if (!user) return 'Unknown user';
    if (user.tag) return user.tag;
    if (user.username && user.discriminator && user.discriminator !== '0') {
        return `${user.username}#${user.discriminator}`;
    }
    return user.username || 'Unknown user';
}

function serializeAttachments(attachments) {
    try {
        return JSON.stringify(
            attachments.map((att) => ({
                url: att.url,
                name: att.name || 'attachment'
            }))
        );
    } catch (error) {
        console.error('Failed to serialize attachments for pending vouch:', error);
        return '[]';
    }
}

function deserializeAttachments(raw) {
    if (!raw) return [];
    try {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) return parsed;
        return [];
    } catch (error) {
        return [];
    }
}

function truncateContent(text, limit = 200) {
    if (!text) return '*No message content provided.*';
    const cleaned = text.trim();
    if (!cleaned) return '*No message content provided.*';
    if (cleaned.length <= limit) return cleaned;
    return `${cleaned.slice(0, limit - 3)}...`;
}

function createPendingVouch(entry) {
    return new Promise((resolve, reject) => {
        db.run(
            `INSERT INTO pending_vouches (
                guild_id, channel_id, message_id, voucher_id, voucher_tag,
                message_content, attachment_urls
            ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
            [
                entry.guildId,
                entry.channelId,
                entry.messageId,
                entry.voucherId,
                entry.voucherTag,
                entry.messageContent,
                serializeAttachments(entry.attachments)
            ],
            function (err) {
                if (err) { reject(err); return; }
                resolve(this.lastID);
            }
        );
    });
}

function listPendingVouches(limit = 5) {
    const safeLimit = Math.max(1, Math.min(25, Number(limit) || 5));
    return new Promise((resolve, reject) => {
        db.all(
            `SELECT id, guild_id, channel_id, message_id, voucher_id, voucher_tag,
                    message_content, attachment_urls, created_at
             FROM pending_vouches
             WHERE approved = 0
             ORDER BY datetime(created_at) ASC
             LIMIT ?`,
            [safeLimit],
            (err, rows) => {
                if (err) { reject(err); return; }
                resolve(
                    rows.map((row) => ({
                        ...row,
                        attachments: deserializeAttachments(row.attachment_urls)
                    }))
                );
            }
        );
    });
}

function getPendingVouchById(id) {
    return new Promise((resolve, reject) => {
        db.get(
            `SELECT id, guild_id, channel_id, message_id, voucher_id, voucher_tag,
                    message_content, attachment_urls, approved
             FROM pending_vouches
             WHERE id = ?`,
            [id],
            (err, row) => {
                if (err) { reject(err); return; }
                if (!row) { resolve(null); return; }
                resolve({
                    ...row,
                    attachments: deserializeAttachments(row.attachment_urls)
                });
            }
        );
    });
}

function markPendingVouchApproved(id, providerId, providerTag) {
    return new Promise((resolve, reject) => {
        db.run(
            `UPDATE pending_vouches
             SET approved = 1,
                 provider_id = ?,
                 provider_tag = ?,
                 approved_at = CURRENT_TIMESTAMP
             WHERE id = ? AND approved = 0`,
            [providerId, providerTag, id],
            function (err) {
                if (err) { reject(err); return; }
                resolve(this.changes > 0);
            }
        );
    });
}

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

    const attachments = [...message.attachments.values()].filter((att) => isImageAttachment(att));
    if (attachments.length === 0) return;

    const mentionedMembers = message.mentions?.members;
    const providerMember = mentionedMembers?.find((member) => member.roles.cache.has(providerRole.id)) || null;

    if (!providerMember) {
        try {
            const pendingId = await createPendingVouch({
                guildId: message.guild.id,
                channelId: message.channel.id,
                messageId: message.id,
                voucherId: message.author.id,
                voucherTag: getSafeUserTag(message.author),
                messageContent: message.content || '',
                attachments
            });

            const embed = new EmbedBuilder()
                .setColor(0xf1c40f)
                .setTitle('Vouch pending approval')
                .setDescription('No provider was mentioned in this vouch. The provider can approve it using `/approvevouch`.')
                .addFields(
                    { name: 'Voucher', value: `${message.author}`, inline: true },
                    { name: 'Pending ID', value: `#${pendingId}`, inline: true }
                )
                .setFooter({ text: 'Use /approvevouch id:<pending id> to claim this vouch.' })
                .setTimestamp();

            const firstAttachment = attachments[0];
            if (firstAttachment?.url) {
                embed.setImage(firstAttachment.url);
            }

            await message.reply({ embeds: [embed] }).catch(() => {});
        } catch (error) {
            console.error('Failed to create pending vouch entry:', error);
        }
        return;
    }

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
        name: 'approvevouch',
        description: 'Claim a pending vouch when you were not mentioned.',
        dm_permission: false,
        options: [
            {
                name: 'id',
                description: 'Pending vouch ID to approve',
                type: 4,
                required: false,
                min_value: 1
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
            case 'approvevouch': {
                const pendingId = interaction.options.getInteger('id');

                if (!pendingId) {
                    const pending = await listPendingVouches(5);
                    if (pending.length === 0) {
                        await interaction.reply({
                            content: 'There are no pending vouches awaiting approval right now.',
                            ephemeral: true
                        });
                        break;
                    }

                    const embed = new EmbedBuilder()
                        .setColor(0xf1c40f)
                        .setTitle('Pending vouches needing approval')
                        .setDescription('Use `/approvevouch id:<pending id>` to claim a vouch that belongs to you.')
                        .setTimestamp();

                    for (const entry of pending) {
                        const voucherLabel = entry.voucher_id ? `<@${entry.voucher_id}>` : (entry.voucher_tag || 'Unknown user');
                        const attachmentDisplay = entry.attachments.length
                            ? entry.attachments
                                  .slice(0, 3)
                                  .map((att) => `[${att.name}](${att.url})`)
                                  .join(' • ')
                            : '_No attachments stored._';
                        const messageLink = `https://discord.com/channels/${entry.guild_id}/${entry.channel_id}/${entry.message_id}`;

                        embed.addFields({
                            name: `ID #${entry.id}`,
                            value: [
                                `Voucher: ${voucherLabel}`,
                                `Content: ${truncateContent(entry.message_content, 180)}`,
                                `Attachments: ${attachmentDisplay}`,
                                `[Open message](${messageLink})`
                            ].join('\n')
                        });
                    }

                    const firstWithImage = pending.find((entry) => entry.attachments[0]?.url);
                    if (firstWithImage?.attachments[0]?.url) {
                        embed.setImage(firstWithImage.attachments[0].url);
                    }

                    await interaction.reply({ embeds: [embed], ephemeral: true });
                    break;
                }

                const pending = await getPendingVouchById(pendingId);
                if (!pending) {
                    await interaction.reply({ content: 'No pending vouch was found with that ID.', ephemeral: true });
                    break;
                }
                if (pending.approved) {
                    await interaction.reply({ content: 'That vouch has already been approved.', ephemeral: true });
                    break;
                }

                const approved = await markPendingVouchApproved(
                    pendingId,
                    interaction.user.id,
                    getSafeUserTag(interaction.user)
                );

                if (!approved) {
                    await interaction.reply({ content: 'That vouch has already been approved.', ephemeral: true });
                    break;
                }

                const multiplier = await getMultiplier();
                const member = interaction.member || (interaction.guild ? await interaction.guild.members.fetch(interaction.user.id).catch(() => null) : null);
                const displayName = member?.displayName || interaction.user.username;
                const total = await changeUserBalance(interaction.user.id, displayName, multiplier);

                const voucherMention = pending.voucher_id ? `<@${pending.voucher_id}>` : (pending.voucher_tag || 'Unknown user');
                const messageLink = `https://discord.com/channels/${pending.guild_id}/${pending.channel_id}/${pending.message_id}`;

                const embed = new EmbedBuilder()
                    .setColor(0x2ecc71)
                    .setTitle('Pending vouch approved!')
                    .setDescription(`You were awarded ${formatPoints(multiplier)}.`)
                    .addFields(
                        { name: 'Voucher', value: voucherMention, inline: true },
                        { name: 'New total', value: `${total} vouch points`, inline: true },
                        { name: 'Original message', value: `[View message](${messageLink})` }
                    )
                    .setTimestamp();

                if (pending.attachments[0]?.url) {
                    embed.setImage(pending.attachments[0].url);
                }

                await interaction.reply({ embeds: [embed], ephemeral: true });

                const dmEmbed = new EmbedBuilder()
                    .setColor(0x2ecc71)
                    .setTitle('You approved a vouch!')
                    .setDescription(`You claimed a pending vouch from ${voucherMention} in **${interaction.guild?.name || 'this server'}**.`)
                    .addFields(
                        { name: 'Awarded', value: `${formatPoints(multiplier)} (x${multiplier})`, inline: true },
                        { name: 'New total', value: `${total} vouch points`, inline: true }
                    )
                    .setTimestamp();

                await interaction.user.send({ embeds: [dmEmbed] }).catch(() => {});

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
