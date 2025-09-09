# Ukeau Discord Bot

A Discord bot that rewards users with vouch points for posting pictures in channels with "vouch" in the name.

## Features

- 🖼️ **Automatic Picture Detection**: Awards vouch points when users post images in channels containing "vouch"
- 📊 **Point Tracking**: SQLite database to store and track user vouch points
- 🏆 **Leaderboard**: View top 10 users with most vouch points
- 📈 **Individual Stats**: Check your own or others' vouch points
- 🎉 **Celebration Messages**: Fun embed messages when points are awarded

## Commands

- `/vouchpoints [user]` - Check vouch points for yourself or another user
- `/vouchleaderboard` - View the top 10 users with most vouch points

## Setup

### 1. Create Discord Application

1. Go to [Discord Developer Portal](https://discord.com/developers/applications)
2. Click "New Application" and give it a name
3. Go to "Bot" section and click "Add Bot"
4. Copy the bot token (you'll need this later)
5. Under "Privileged Gateway Intents", enable:
   - Message Content Intent
   - Server Members Intent

### 2. Invite Bot to Server

1. Go to "OAuth2" > "URL Generator"
2. Select scopes: `bot` and `applications.commands`
3. Select bot permissions:
   - Send Messages
   - Use Slash Commands
   - Embed Links
   - Read Message History
4. Copy the generated URL and open it to invite the bot

### 3. Local Development

1. Clone this repository
2. Install dependencies:
   ```bash
   npm install
   ```
3. Copy `env.example` to `.env` and add your Discord bot token:
   ```
   DISCORD_TOKEN=your_bot_token_here
   ```
4. Run the bot:
   ```bash
   npm start
   ```

### 4. Deploy to Railway

1. Push your code to GitHub
2. Go to [Railway](https://railway.app)
3. Connect your GitHub account
4. Create a new project and select your repository
5. Add environment variables:
   - `DISCORD_TOKEN`: Your Discord bot token
   - `DATABASE_PATH`: `/data/vouch_points.db` (for persistent storage)
6. Deploy!

## How It Works

1. **Picture Detection**: The bot monitors all channels with "vouch" in the name
2. **Image Validation**: Checks if uploaded files are images (JPEG, PNG, GIF, WebP)
3. **Point Award**: Automatically awards 1 vouch point per image posted
4. **Database Storage**: Uses SQLite to persistently store user points
5. **Commands**: Users can check their points and view leaderboards

## File Structure

```
ukeau/
├── index.js          # Main bot file
├── package.json      # Dependencies and scripts
├── railway.json      # Railway deployment config
├── Procfile         # Railway process file
├── env.example      # Environment variables template
└── README.md        # This file
```

## Environment Variables

- `DISCORD_TOKEN` (required): Your Discord bot token
- `DATABASE_PATH` (optional): Path to SQLite database file (defaults to `/data/vouch_points.db` for Railway persistent storage)

## Database Schema

The bot uses SQLite with the following table:

```sql
CREATE TABLE vouch_points (
    user_id TEXT PRIMARY KEY,
    points INTEGER DEFAULT 0,
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

MIT License - feel free to use this bot for your own servers!

## Support

If you encounter any issues:
1. Check that your bot has the correct permissions
2. Ensure the bot token is valid
3. Verify the bot is in the correct channels
4. Check the console logs for error messages

---

Made with ❤️ for the Discord community


