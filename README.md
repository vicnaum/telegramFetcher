# tgx - Personal Telegram Exporter

A CLI tool to archive and export Telegram chat history from channels and groups you are a member of.

## Features

- **Login as yourself** via MTProto (Telethon) - no bot required
- **QR code login** - scan with your phone, no phone number typing needed
- **Incremental sync** - only fetches new messages on subsequent runs
- **Local SQLite database** - messages cached locally, no re-fetching
- **Export formats**: TXT (with message IDs and reply threading) and JSONL
- **Date/ID range filtering** - export specific time periods or message ranges
- **Rate limit handling** - automatic sleep on FloodWait errors
- **Graceful shutdown** - Ctrl+C saves state safely

## Installation

Requires Python 3.10+.

```bash
# Using uv (recommended)
cd telegramFetcher
uv sync

# Or using pip
pip install -e .
```

## Setup

### 1. Get Telegram API Credentials

You need API credentials to use Telegram's MTProto protocol. This is a **one-time setup** that takes ~2 minutes:

1. **Go to** https://my.telegram.org/auth
2. **Log in** with your phone number (you'll receive a code in Telegram)
3. **Click** "API development tools"
4. **Fill the form** (all fields can be anything):
   - App title: `My Exporter` (or whatever you want)
   - Short name: `myexporter` (lowercase, no spaces)
   - Platform: `Desktop`
   - Description: `Personal use`
5. **Click** "Create application"
6. **Copy** your `api_id` (a number) and `api_hash` (a long hex string)

> ⚠️ **Keep these secret!** Don't share them or commit to git.

### 2. Configure Environment

**Option A: Use a `.env` file (recommended)**

```bash
cp .env.example .env
# Edit .env with your actual credentials
```

Your `.env` file should look like:
```bash
TGX_API_ID=12345678
TGX_API_HASH=abcdef1234567890abcdef1234567890
```

**Option B: Export in terminal**

```bash
export TGX_API_ID=12345678
export TGX_API_HASH=abcdef1234567890abcdef1234567890
```

**Optional settings:**
```bash
TGX_SESSION=./my_session.session  # Custom session file path
TGX_DB=./my_data.sqlite           # Custom database path
```

## Usage

### First run - Authenticate

```bash
uv run python -m tgx.main auth-test
```

**QR Code Login (default):**
1. A QR code will be displayed in your terminal
2. Open Telegram on your phone → Settings → Devices → Link Desktop Device
3. Scan the QR code
4. If you have 2FA enabled, enter your password

**Phone Login (alternative):**
```bash
uv run python -m tgx.main auth-test --phone
```

On success, you'll see "AUTHORIZED" and your session is saved to `tgx.session`.

### Session Caching

**Your login is cached!** After the first successful authentication:
- The session is stored in `tgx.session` file
- Subsequent runs skip login entirely
- To re-authenticate, delete the session file

### Find a chat to export

```bash
# List your dialogs
uv run python -m tgx.main dialogs

# Search for specific chats
uv run python -m tgx.main dialogs --search "crypto"
```

### Test fetching

```bash
# Fetch 5 messages to verify access
uv run python -m tgx.main fetch-test --peer @channelname --limit 5
```

### Export messages

```bash
# Export last 100 messages to TXT
uv run python -m tgx.main export --peer @channelname --last 100 --txt out.txt

# Export to both formats
uv run python -m tgx.main export --peer @channelname --last 1000 --txt out.txt --jsonl out.jsonl

# Export by date range
uv run python -m tgx.main export --peer @channelname --start "2025-01-01" --end "2025-01-31" --txt january.txt

# Export by message ID range
uv run python -m tgx.main export --peer @channelname --since-id 5000 --until-id 6000 --jsonl range.jsonl

# Include raw JSON data in JSONL
uv run python -m tgx.main export --peer @channelname --last 100 --jsonl out.jsonl --include-raw
```

### Sync only (without export)

```bash
# Sync last 100 messages to local DB
uv run python -m tgx.main sync --peer @channelname --last 100
```

## Output Formats

### TXT Format

One message per line, optimized for AI/LLM consumption:

```
[12345] 2025-01-15 10:30:45 | Alice | Hey everyone, check out this news article!
[12346] 2025-01-15 10:31:02 | Bob | [reply to #12345 @Alice] Interesting, thanks for sharing
[12347] 2025-01-15 10:32:15 | channel | [photo]
```

- **Message ID** in brackets at the start `[12345]`
- **Reply info** shows which message is being replied to and who wrote it `[reply to #12345 @Alice]`
- Timestamps are in local timezone
- Newlines in messages are flattened to spaces
- Media messages show `[media_type]`

### JSONL Format

One JSON object per line:

```json
{"id": 12345, "peer_id": -1001234567890, "date": "2025-01-15T09:30:45+00:00", "date_utc_ms": 1736934645000, "sender_id": 111, "sender_name": "Alice", "text": "Hello", "reply_to_msg_id": null, "has_media": false, "media_type": null}
```

With `--include-raw`, the `raw_data` field contains the full Telethon message object (parsed JSON, not a string). Use `--raw-as-string` to emit it as a JSON string instead.

## Peer Input Formats

You can specify peers in multiple ways:

- `@username` - Public username
- `https://t.me/username` - Telegram link
- `-1001234567890` - Peer ID (channels start with -100)

> **Note:** Title-based lookup is not supported. Use `dialogs --search "keyword"` to find peer IDs.

## Telegram Chats Packer (`tg_packer.py`)

A companion tool to compress exported chat logs for efficient AI/LLM processing.

### Features

- **Message concatenation** - Consecutive messages from the same user merged with `. `
- **Media grouping** - `[photo] [photo] [photo]` → `[3 photos]`
- **Link shortening** - Full URLs → `[x.com link]`, `[@levelsio tweet]`, `[org/repo repo]`
- **Date headers** - Removes timestamps, shows `# 2025-01-15` for new days
- **User renaming** - Interactive mode to shorten names (e.g., "Victor Naumik" → "V")
- **Sensitive data redaction** - API keys, tokens, passwords, mnemonics
- **Token counting** - Shows estimated AI token count (using tiktoken)

### Usage

```bash
# Basic usage (interactive user renaming)
uv run python tg_packer.py pack export.txt

# Non-interactive with all defaults
uv run python tg_packer.py pack export.txt -n

# With sensitive data redaction
uv run python tg_packer.py pack export.txt -n --redact

# Custom output file
uv run python tg_packer.py pack export.txt -n -o compressed.txt

# Analyze without packing (statistics only)
uv run python tg_packer.py analyze export.txt
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--output` | `-o` | Output file path (default: `input_packed.txt`) |
| `--no-interactive` | `-n` | Skip interactive user renaming |
| `--links` | `-l` | Link handling: `full`, `short` (default), `remove` |
| `--redact` | `-s` | Redact sensitive data (API keys, tokens, etc.) |
| `--keep-replies` | `-r` | Keep full reply metadata |

### Link Modes

| Mode | Example Input | Example Output |
|------|--------------|----------------|
| `full` | `https://x.com/user/status/123` | (unchanged) |
| `short` | `https://x.com/levelsio/status/123` | `[@levelsio tweet]` |
| `short` | `https://github.com/org/repo` | `[org/repo repo]` |
| `remove` | Any URL | `[link]` or `[3 links]` |

### Redaction (`--redact`)

Automatically redacts:
- JWT tokens, API keys (OpenAI, Google, etc.)
- Ethereum private keys and long hex strings
- Mnemonic seed phrases
- Environment variable secrets (PASSWORD, TOKEN, SECRET, etc.)
- Database URLs, Supabase/ngrok URLs
- Any alphanumeric string 16+ chars after `=`

### Example Output

**Before:**
```
[12345] 2025-01-01 10:30:00 | Alice | [video]
[12346] 2025-01-01 10:31:00 | Bob | [reply to #12345 @Alice] Nice video!
[12347] 2025-01-01 10:31:30 | Bob | Check out this link
[12348] 2025-01-01 10:31:45 | Bob | https://x.com/someone/status/123456
```

**After (with user renames A/B):**
```
# 2025-01-01
A: [video]
B: Nice video!. Check out this link. [@someone tweet]
```

Typical compression: **55-60% token reduction**

## Security Notes

⚠️ **Important:**

- The `.session` file contains your Telegram login. **Keep it private!**
- Add `*.session` to your `.gitignore`
- The session file allows full access to your Telegram account
- If compromised, revoke all sessions at https://my.telegram.org

## Limitations (MVP)

- **No forum topics/threads** - Only main chat history
- **No media downloads** - Only `has_media` flag and `media_type` stored
- **No reactions** - Not captured
- **No edit history** - Only original message stored (append-only)
- **No deletions** - Deleted messages remain in local DB

## Files

- `tgx.session` - Telegram session (encrypted credentials)
- `tgx.sqlite` - Local message database (WAL mode)

## Troubleshooting

### "Could not find entity"

- Make sure you've joined the channel/group
- Try using the peer_id instead of username

### "ChannelPrivateError"

- The channel is private or you were kicked/banned

### "FloodWaitError"

- The tool handles this automatically
- For large syncs (>5000 messages), expect occasional waits

### Session expired

- Delete the `.session` file and re-authenticate

## Development

```bash
# Install dev dependencies
uv sync

# Run directly
uv run python -m tgx.main --help
```

## License

MIT

