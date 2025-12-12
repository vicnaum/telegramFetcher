This document analyzes the technical implementations of six competitor repositories (`tg-archive`, `Telegram-Archive`, `telegram-messages-dump`, `telegram-download-chat`, `telegram-export`, `telegram-scraper`).

It extracts specific Telethon API usage, synchronization logic, database schemas, and formatting strategies relevant to building **`tgx`**.

---

# Competition Codebase Notes for `tgx`

## 1. Authentication & Session Management
All repos use `Telethon`. The handling of the `.session` file and login flows varies.

### Best Pattern: Interactive CLI + QR Code
**Source:** `unnohwn/telegram-scraper` & `GeiserX/Telegram-Archive`

Instead of just asking for phone/code, `telegram-scraper` offers QR code login, which is faster and safer (avoids typing codes).

**Code Snippet (`telegram-scraper.py`):**
```python
# QR Code Login Logic
qr_login = await client.qr_login()
# Display QR in terminal (requires 'qrcode' lib)
qr.print_ascii(out=f) 
try:
    await qr_login.wait() # Waits for user to scan
except SessionPasswordNeededError:
    # Handle 2FA
    password = input("Enter 2FA: ")
    await client.sign_in(password=password)
```

**`tgx` Takeaway:** Implement standard phone auth first, but structure the auth module to allow `client.qr_login()` later. Always handle `SessionPasswordNeededError` (2FA).

### Session File Location
**Source:** `GeiserX/Telegram-Archive` (`config.py`)
Separates the session file from the database.
*   `SESSION_NAME`: Defaults to `telegram_backup`.
*   `SESSION_DIR`: Stores it in a specific config folder, not CWD.

---

## 2. Sync Logic (The "Cursor" Strategy)
This is the most critical part for `tgx`. How do they fetch only *new* messages?

### The "Reverse Iterator" Pattern (Recommended)
**Source:** `GeiserX/Telegram-Archive` (`src/telegram_backup.py`) & `tg-archive` (`sync.py`)

They use `iter_messages` with `reverse=True`. This fetches messages in chronological order (Oldest -> Newest). By passing `min_id` (the highest ID currently in DB), Telethon automatically fetches only new messages.

**Code Snippet (`GeiserX` style):**
```python
# last_message_id = SELECT MAX(id) FROM messages WHERE peer_id = ...
async for message in client.iter_messages(
    entity,
    min_id=last_message_id, # The cursor
    reverse=True            # Fetch Oldest -> Newest
):
    save_to_db(message)
```

### The "GetHistoryRequest" Pattern (Manual Pagination)
**Source:** `knadh/tg-archive` (`sync.py`) & `expectocode/telegram-export` (`dumper.py`)

They use the raw API call `GetHistoryRequest`. This is lower-level but offers granular control over limits and offsets.

**Code Snippet (`tg-archive`):**
```python
messages = client.get_messages(
    group,
    offset_id=last_id, # Start from here
    limit=batch_size,  # e.g., 100
    reverse=True       # Chronological order
)
```
**`tgx` Takeaway:** Use `client.iter_messages(min_id=..., reverse=True)`. It handles pagination and flood waits internally, reducing code complexity significantly compared to `GetHistoryRequest`.

### Handling "Backfill" (History Holes)
**Source:** `popstas/telegram-download-chat` (`core/download.py`)

This repo handles the `--until` logic (fetching older messages). It iterates normally but breaks the loop if `msg.date < until_date`.

**`tgx` Takeaway:**
1.  **Tail Sync:** `min_id=db.max_id` (Fetching forwards).
2.  **Backfill:** `max_id=db.min_id` (Fetching backwards).

---

## 3. Database Schema & Storage
Different approaches to storing Telegram data.

### The "Raw Dump" Approach (Future-Proofing)
**Source:** `GeiserX/Telegram-Archive` (`src/database.py`)

Instead of creating columns for every single Telegram property (which changes often), they store core columns and a JSON blob.

**Schema Note:**
```sql
CREATE TABLE messages (
    id INTEGER,
    chat_id INTEGER,
    date TIMESTAMP,
    text TEXT,
    sender_id INTEGER,
    reply_to_msg_id INTEGER,
    media_type TEXT,
    raw_data TEXT,  -- <--- The "Saving Grace" column
    PRIMARY KEY (id, chat_id)
);
```
**`tgx` Takeaway:** Adopt this. Store `id`, `date`, `sender`, `text` as columns for SQL querying, but dump `message.to_json()` into `raw_data`. This satisfies the "Data Fidelity" requirement.

### The "Normalized" Approach (Complex)
**Source:** `expectocode/telegram-export` (`dumper.py`)

Creates tables for `User`, `Chat`, `Message`, `Media`, `Forward`.
*   *Pros:* Very efficient storage (users stored once).
*   *Cons:* Complex insertion logic (must insert User before Message).

**`tgx` Takeaway:** For a local CLI tool, strict normalization is overkill. Use a denormalized `messages` table. If `sender_name` changes, just record the name *at the time of the message* (or update a simple `peers` table).

---

## 4. FloodWait & Robustness
How to handle Telegram telling you to stop.

### Explicit Sleep
**Source:** `knadh/tg-archive` (`sync.py`)

```python
try:
    # fetch...
except errors.FloodWaitError as e:
    logging.info(f"Flood wait: sleeping {e.seconds} seconds")
    time.sleep(e.seconds)
```

### Telethon's Auto-Sleep
**Source:** `telegram-download-chat` (`core/downloader.py`)

They don't explicitly catch `FloodWait` everywhere because Telethon's `TelegramClient` has a `flood_sleep_threshold`.

**`tgx` Takeaway:**
Set `client.flood_sleep_threshold = 60` (auto-sleep for waits < 60s). Catch larger errors manually to save state (commit DB) before sleeping or exiting.

---

## 5. Text Formatting (AI/LLM Friendly)
How to convert the complex object into a flat line.

### The "One-Liner"
**Source:** `Kosat/telegram-messages-dump` (`exporters/text.py`)

```python
# Format: [YYYY-MM-DD HH:MM] ID=123 Name: Message
timestamp = msg.date.strftime('%Y-%m-%d %H:%M')
display_name = get_display_name(msg.sender)
reply_info = f"RE_ID={msg.reply_to_msg_id} " if msg.reply_to_msg_id else ""
return f"[{timestamp}] ID={msg.id} {reply_info}{display_name}: {msg.message}"
```

### The "Readable" Block
**Source:** `popstas/telegram-download-chat` (`core/messages.py`)

Uses a more chat-log style format.
```text
2025-07-01 10:00 Alice -> Bob:
Hello world
```

**`tgx` Takeaway:** The one-liner is better for AI ingestion (CSV/JSONL style).
*   Flatten newlines: `text.replace('\n', ' ')`.
*   Include Sender ID if name is missing.
*   Convert timestamps to Local Time (as requested in spec).

---

## 6. Media Handling (Lazy)
The spec requests "note has_media" but download on demand.

### Unique Filename Generation
**Source:** `GeiserX/Telegram-Archive` (`src/telegram_backup.py`)

To avoid collisions and re-downloads:
`filename = f"{chat_id}_{message.id}_{media_type}.{ext}"`

### Mime-Type Detection
**Source:** `expectocode/telegram-export` (`utils.py`)

Telegram `document` media is generic. You must check `document.mime_type` or `document.attributes` (like `DocumentAttributeFilename`) to know if it's a Voice Note, Sticker, or Video.

**`tgx` Takeaway:**
In the DB `messages` table, store:
*   `has_media`: boolean
*   `media_type`: 'photo', 'document', 'web_page', etc.
*   `media_id`: The Telethon ID (for potential future download).

---

## 7. Useful Telethon Snippets found in Repos

### Resolving Peer (User/Channel/Group)
**Source:** `telegram-download-chat`
Handling `@username`, `https://t.me/...`, and integer IDs.

```python
# Checks if input is numeric ID, invite link, or username
if str(identifier).lstrip("-").isdigit():
    entity = await client.get_entity(int(identifier))
else:
    entity = await client.get_entity(identifier)
```

### Getting a clean display name
**Source:** `telegram-messages-dump` (`exporters/common.py`)

```python
sender = msg.sender
if sender:
    name = getattr(sender, 'username', None)
    if not name:
        name = getattr(sender, 'title', None) # For groups
    if not name:
        # Fallback to First + Last
        name = (sender.first_name or "") + " " + (sender.last_name or "")
else:
    name = "Unknown"
```

### Checking for "Service Messages"
**Source:** `tg-archive` (`sync.py`)
Service messages (Pin message, User joined) have `msg.action`.

```python
if m.action:
    if isinstance(m.action, MessageActionChatAddUser):
        type = "user_joined"
    # ...
```
**`tgx` Takeaway:** Filter these out for the AI export unless explicitly requested, or format them distinctively (e.g., `[System]: User joined`).

---

## Summary of Architecture for `tgx`

Based on these repos, here is the optimal path:

1.  **DB:** Copy `GeiserX`'s strategy: Simple `messages` table + `raw_json` column.
2.  **Sync:** Copy `GeiserX` / `tg-archive` strategy: `client.iter_messages(min_id=db_max_id, reverse=True)`.
3.  **Auth:** Use standard Telethon interactive auth (phone + code).
4.  **Export:** Copy `Kosat`'s logic for flattening messages, but output to JSONL/TXT directly from SQLite, not memory.
5.  **Robustness:** Set `flood_sleep_threshold` on the client, and wrap the main loop in a `try/except` to commit the DB cursor even if a crash occurs.