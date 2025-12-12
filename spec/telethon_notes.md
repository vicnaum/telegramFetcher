This is a comprehensive developer's cheatsheet for **Telethon (v1.x)**, tailored specifically for building **`tgx`**.

It filters out bot-specific or irrelevant features (like sending messages or making calls) and focuses strictly on **archiving, history retrieval, entity resolution, and serialization**.

---

# `tgx` Dev Reference: Telethon V1

## 1. Initialization & Configuration
Telethon is `asyncio` based. You must run it within an `async def` function using `client.loop.run_until_complete()` or `asyncio.run()`.

### The Client
**Location:** `telethon/client/telegramclient.py`

```python
from telethon import TelegramClient

# Session path can be a string (filename) or a Path object.
# Telethon creates a .session file (SQLite) here automatically.
session_path = "./tgx.session" 

client = TelegramClient(
    session_path, 
    api_id=12345,       # From .env
    api_hash="abcdef",  # From .env
    # Recommendations for archivers:
    flood_sleep_threshold=60,  # Auto-sleep if wait time < 60s
    request_retries=5,         # Retry internal errors 5 times
    receive_updates=False      # IMPORTANT: Set False. We are scraping, not listening to live events.
)
```

---

## 2. Authentication Flow
**Location:** `telethon/client/auth.py`

For a CLI tool, avoid `client.start()` (which uses `input()` internally) and build a custom flow to handle the CLI args properly.

### Standard Login (Phone + Code + 2FA)
```python
await client.connect()

if not await client.is_user_authorized():
    phone = "+1234567890"
    
    # 1. Send Code
    sent = await client.send_code_request(phone)
    # sent.phone_code_hash must be passed to sign_in if used explicitly
    
    # 2. User inputs code
    code = input("Enter Code: ")
    
    try:
        await client.sign_in(phone, code)
    except SessionPasswordNeededError:
        # 3. Handle 2FA
        pw = getpass("Enter 2FA Password: ")
        await client.sign_in(password=pw)

# Save session immediately after login
# Telethon autosaves, but good to be sure before exit.
```

### QR Code Login (Optional but cool)
**Location:** `telethon/client/auth.py` -> `qr_login()`
```python
qr_login = await client.qr_login()
print(qr_login.url) # Render this as a QR code in terminal using `qrcode` lib
try:
    # Waits for the user to scan via mobile app
    await qr_login.wait(timeout=120) 
except SessionPasswordNeededError:
    # Handle 2FA password
```

---

## 3. Entity Resolution (The "Peer")
**Location:** `telethon/client/users.py`

You need to convert user inputs (`@channel`, `t.me/join/...`, `-100xyz`) into a generic entity object that Telethon understands.

### The Magic Method: `get_input_entity`
Use this for **everything**. It handles usernames, phone numbers, invite links, and IDs. It leverages the local `.session` cache to avoid API calls if possible.

```python
try:
    # Input: str | int | Peer
    entity = await client.get_input_entity(user_input) 
except ValueError:
    print("Could not find entity. If it's a private group, join it first.")
```

### Getting Metadata (Title, ID)
If you need the chat title or the resolved ID for your DB:

```python
from telethon import utils

# get_entity does a network call if not cached to get Full info (title, etc)
full_entity = await client.get_entity(entity)

chat_title = utils.get_display_name(full_entity)
chat_id = utils.get_peer_id(full_entity) # Returns the integer ID (e.g. -100123...)
```

---

## 4. Message Retrieval (The Core Logic)
**Location:** `telethon/client/messages.py`

This is the engine of `tgx`. We use `iter_messages` (which yields an async iterator).

### Strategy A: Tail Sync (Newest Messages)
Fetching messages *newer* than what we have in DB.
*   **Logic:** Start from the `max_msg_id` we have, iterate in **reverse** (Oldest -> Newest).
*   **Key Param:** `reverse=True`. This flips the meaning of offsets.

```python
# Fetches messages chronologically starting AFTER min_id
async for msg in client.iter_messages(
    entity,
    min_id=last_synced_id, # Exclusive (fetches IDs > min_id)
    limit=None,            # Fetch all new ones
    reverse=True,          # Essential for chronological sync
    wait_time=1            # Sleep 1s between chunks to be nice
):
    process(msg)
```

### Strategy B: Backfill (Older Messages)
Fetching messages *older* than what we have (filling history gaps).
*   **Logic:** Start from `min_msg_id` we have, iterate normally (Newest -> Oldest).

```python
async for msg in client.iter_messages(
    entity,
    max_id=oldest_synced_id, # Exclusive (fetches IDs < max_id)
    limit=target_limit       # e.g., 5000 messages chunk
):
    process(msg)
```

### Strategy C: Window Export (Date Range)
Fetching specific dates.
*   **Note:** Telethon's `offset_date` fetches messages *older* than that date.

```python
async for msg in client.iter_messages(
    entity,
    offset_date=end_date_utc, # Start looking from here (going backwards)
    limit=None
):
    if msg.date < start_date_utc:
        break # We passed the window
    process(msg)
```

---

## 5. The Message Object
**Location:** `telethon/tl/custom/message.py`

The iterator yields `Message` objects. Here is the mapping to your `tgx` spec.

### Essential Fields
| `tgx` Field | Telethon Attribute | Type | Notes |
| :--- | :--- | :--- | :--- |
| `msg_id` | `msg.id` | `int` | |
| `date` | `msg.date` | `datetime` | Always UTC (timezone-aware). |
| `sender_id` | `msg.sender_id` | `int` | Can be `None` (channels) or a Peer ID. |
| `text` | `msg.raw_text` | `str` | Text without markdown formatting. Use `msg.text` if you want markdown entities applied. |
| `reply_to` | `msg.reply_to_msg_id` | `int` | ID of message replied to. |
| `has_media` | `bool(msg.media)` | `bool` | |
| `views` | `msg.views` | `int` | For channels. |
| `forwards` | `msg.forwards` | `int` | |
| `edit_date` | `msg.edit_date` | `datetime` | `None` if never edited. |

### JSON Serialization (Future Proofing)
Your spec requires storing `raw_json`. Telethon objects have a built-in method for this.

```python
# Returns a JSON string of the TLObject
raw_json_str = msg.to_json() 

# Alternatively, if you want a dict first:
raw_dict = msg.to_dict()
```
*Note: `to_json` handles datetime serialization (ISO format) and bytes (base64) automatically.*

---

## 6. Rate Limits & Errors
**Location:** `telethon/errors`

You **must** wrap your fetch loops in try/except blocks to handle Telegram's strict limits.

### Key Errors to Catch
1.  **`FloodWaitError`**: You are requesting too fast.
    *   **Property:** `.seconds` (int, how long to wait).
    *   **Action:** If seconds < 60, sleep. If large, stop sync and save state.
2.  **`RpcCallFailError`** / **`ServerError`**: Temporary Telegram internal issues.
    *   **Action:** Retry after 1-2 seconds.
3.  **`ChannelPrivateError`**: You were kicked or the channel went private.
    *   **Action:** Abort sync for this peer.

```python
from telethon import errors

try:
    async for msg in client.iter_messages(...):
        # ... logic
except errors.FloodWaitError as e:
    print(f"Rate limited. Sleeping for {e.seconds} seconds.")
    await asyncio.sleep(e.seconds)
```

---

## 7. Useful Utilities

### Text Formatting
**Location:** `telethon/utils.py`
If you need to generate the "Display Name" for the TXT export:
```python
from telethon import utils

# Resolves User/Chat/Channel object to "First Last" or "Title"
sender_name = utils.get_display_name(msg.sender) 
```

### Media (For future use)
**Location:** `telethon/client/downloads.py`
Even though you aren't downloading media in MVP, knowing how to identify it is useful for the `media_type` column.

```python
if msg.photo:
    m_type = "photo"
elif msg.document:
    m_type = "document" # Could be file, video, audio, sticker
    if msg.video: m_type = "video"
    if msg.voice: m_type = "voice"
elif msg.web_preview:
    m_type = "web_link"
```

---

## 8. Development Quirks (Gotchas)

1.  **Iterating vs Getting:**
    *   `client.get_messages(limit=100)` -> Returns a **list** (loads all into RAM).
    *   `client.iter_messages(limit=100)` -> Returns an **async generator** (streamed).
    *   **Use `iter_messages`** for archiving to keep memory usage low.

2.  **Total Count:**
    *   `iter_messages` does not return the total count of messages in the chat immediately.
    *   If you need the total count (for a progress bar), you have to fetch one message first:
        ```python
        total = (await client.get_messages(entity, limit=0)).total
        ```

3.  **IDs:**
    *   Telethon normalizes IDs.
    *   User IDs are positive integers.
    *   Chat IDs are negative integers.
    *   Channel IDs are `-100` + ID.
    *   `utils.get_peer_id(entity)` ensures you get the correct ID format for the DB.

4.  **Service Messages:**
    *   Events like "User joined", "Channel renamed" appear as messages.
    *   They have `msg.action` set (e.g., `MessageActionChatAddUser`).
    *   Check `isinstance(msg, types.MessageService)` to filter them out of TXT exports if desired.