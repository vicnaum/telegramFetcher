# Developer Specification: `tgx` (Telegram Exporter)

## 1. Project Architecture

### 1.1 High-Level Design
**`tgx`** is a synchronous, stateful CLI tool. It follows a **Load -> Sync -> Export** pipeline.
1.  **Init**: Load configuration, connect to SQLite, authenticate with Telegram.
2.  **Resolution**: Convert user input (e.g., `@chatname`) into a valid Entity ID.
3.  **Sync Engine**: Compare local DB state vs. Telegram state. Fetch missing messages (Tail + Backfill).
4.  **Export Engine**: Query local DB (not API) and generate files.

### 1.2 Directory Structure
```text
tgx/
├── __init__.py
├── main.py              # CLI Entry point & Arg parsing
├── config.py            # Env vars & Constants
├── client.py            # Telethon Wrapper (Network Layer)
├── db.py                # SQLite Wrapper (Storage Layer)
├── sync.py              # Logic: Tail Sync & Backfill strategies
├── exporter.py          # Logic: JSONL/TXT formatting
└── utils.py             # Date parsing, text sanitization, logging
```

---

## 2. Database Schema (SQLite)
**File:** `tgx.sqlite`
**Design Philosophy:** Denormalized enough for fast exports, but normalized enough to save space on repetitive user data. Uses `WAL` mode for concurrency safety.

### Table: `peers`
Tracks metadata and sync progress for every channel/group encountered.
```sql
CREATE TABLE IF NOT EXISTS peers (
    id INTEGER PRIMARY KEY,          -- Real Telegram ID (User > 0, Chat < 0, Channel starts -100)
    username TEXT,
    title TEXT,
    type TEXT,                       -- 'user', 'group', 'channel'
    min_msg_id INTEGER DEFAULT 0,    -- Oldest message ID we have stored
    max_msg_id INTEGER DEFAULT 0,    -- Newest message ID we have stored
    last_sync_ts TIMESTAMP,          -- UTC timestamp of last successful sync
    raw_data TEXT                    -- JSON dump of the full Entity object (future proofing)
);
CREATE INDEX IF NOT EXISTS idx_peers_username ON peers(username);
```

### Table: `users`
Lookup table to resolve `sender_id` to human names for TXT exports. Updated whenever we fetch messages.
```sql
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    updated_at TIMESTAMP
);
```

### Table: `messages`
The core archive. Stores core columns for SQL querying and a JSON blob for full fidelity.
```sql
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER,                      -- Message ID (Local to the peer)
    peer_id INTEGER,                 -- Foreign Key -> peers.id
    date TIMESTAMP NOT NULL,         -- UTC ISO8601
    sender_id INTEGER,               -- ID of who sent it
    text TEXT,                       -- Plain text content (cleaned)
    reply_to_msg_id INTEGER,         -- ID of parent message
    has_media INTEGER DEFAULT 0,     -- Boolean (0/1)
    media_type TEXT,                 -- 'photo', 'document', 'web_page', etc.
    raw_data TEXT,                   -- Full JSON serialization of the Telethon Message object
    PRIMARY KEY (id, peer_id)
);
CREATE INDEX IF NOT EXISTS idx_msg_date ON messages(peer_id, date);
```

---

## 3. Component Specifications

### 3.1 `db.py` (Storage Layer)
Handles all SQLite interactions.
*   **Init:** Run `PRAGMA journal_mode=WAL;` on connection.
*   **`update_peer(entity)`**: Upsert peer metadata.
*   **`upsert_users(users_list)`**: Batch upsert sender info found in message metadata.
*   **`insert_messages(messages_list)`**: Batch insert. Uses `INSERT OR IGNORE` to handle overlaps safely.
*   **`get_sync_boundaries(peer_id)`**: Returns `(min_msg_id, max_msg_id)` to helper the Sync Engine.
*   **`get_messages_cursor(...)`**: Returns a generator/cursor for the Export Engine based on filters (date range, limit).

### 3.2 `client.py` (Network Layer)
Wrapper around `Telethon.TelegramClient`.
*   **Auth:** Checks `client.is_user_authorized()`. If False, runs interactive phone/code flow.
*   **`get_entity(input_str)`**: Uses `client.get_input_entity` (cached) or `client.get_entity` (network) to resolve inputs like `@user`, `https://t.me/...`. Returns the `InputPeer`.
*   **`iter_history(peer, min_id, max_id, reverse, limit)`**: The core fetcher.
    *   *Critical:* Must define `flood_sleep_threshold=60`.
    *   *Critical:* Must catch `FloodWaitError` and `RpcCallFailError` (internal server error) and retry.

### 3.3 `sync.py` (Business Logic)
This is the "Brain" of the tool. It orchestrates the fetch loop.

**Function: `sync_peer(client, db, peer_id, target_limit, target_start_date)`**

1.  **Resolve State**: Query `db` for `current_max_id` and `current_min_id` for this `peer_id`.
2.  **Phase 1: Tail Sync (Forward)**
    *   Fetch messages *newer* than `current_max_id`.
    *   Use Telethon: `client.iter_messages(peer, min_id=current_max_id, reverse=True)`.
    *   *Why reverse?* Telethon iterates newest->oldest by default. `reverse=True` iterates oldest->newest. This fills the gap from our last sync up to "now".
    *   Batch insert into DB. Update `peers.max_msg_id`.
3.  **Phase 2: Backfill (Backward) - Optional**
    *   Only runs if user requested a specific range (e.g., `--last 5000`) and our `db.count` < 5000.
    *   Fetch messages *older* than `current_min_id`.
    *   Use Telethon: `client.iter_messages(peer, max_id=current_min_id, limit=needed_amount)`.
    *   Batch insert. Update `peers.min_msg_id`.

### 3.4 `exporter.py` (Format Layer)
Reads from DB, writes to disk. **Does not touch Network.**

**Function: `export(db, peer_id, output_path, format, **filters)`**
1.  Query DB using `filters` (date range, limit).
2.  **Format: JSONL**
    *   Dump the `raw_data` column (if it exists) or construct a dict from columns.
    *   Write line by line.
3.  **Format: TXT**
    *   Requires `sender_id` resolution. Query `users` table to map `sender_id` -> `first_name`.
    *   Sanitize text: Replace `\n` with ` ` (space). Remove weird control chars.
    *   Convert `date` (UTC) to `Local TZ`.
    *   Write: `[YYYY-MM-DD HH:MM] Name: Message content`

---

## 4. Implementation Details (Code Snippets)

### 4.1 CLI Entry Point (`main.py`)
Using `argparse` for zero-dependencies.

```python
import argparse
import asyncio
from tgx.sync import SyncEngine
from tgx.exporter import export

def main():
    parser = argparse.ArgumentParser(prog="tgx")
    parser.add_argument("--peer", required=True, help="@username or link")
    parser.add_argument("--last", type=int, help="Fetch last N messages")
    parser.add_argument("--txt", help="Output path for TXT")
    parser.add_argument("--jsonl", help="Output path for JSONL")
    # ... auth args ...
    args = parser.parse_args()

    asyncio.run(run_job(args))

async def run_job(args):
    # 1. Setup DB & Client
    # 2. Sync
    engine = SyncEngine(client, db)
    await engine.sync(args.peer, limit=args.last)
    # 3. Export
    if args.txt:
        export(db, args.peer, args.txt, format="txt", limit=args.last)
```

### 4.2 The Sync Engine Logic (`sync.py`)

```python
class SyncEngine:
    async def sync(self, peer_input, limit=None):
        # 1. Resolve
        entity = await self.client.get_input_entity(peer_input)
        peer_id = utils.get_peer_id(entity) 
        
        # 2. Get DB boundaries
        db_min, db_max = self.db.get_boundaries(peer_id)
        
        # 3. Tail Sync (Get everything new since last run)
        # reverse=True means we fetch Oldest -> Newest starting AFTER db_max
        async for msg in self.client.iter_messages(entity, min_id=db_max, reverse=True):
            self.db.insert_message(msg)
            # Update user cache periodically
            if msg.sender: self.db.upsert_user(msg.sender)
            
        # 4. Backfill (If limit requires older messages we don't have)
        current_count = self.db.count_messages(peer_id)
        if limit and current_count < limit:
            needed = limit - current_count
            # Fetch normal order (Newest -> Oldest) starting OLDER than db_min
            async for msg in self.client.iter_messages(entity, max_id=db_min, limit=needed):
                self.db.insert_message(msg)
```

### 4.3 The DB Wrapper (`db.py`)

```python
def insert_message(self, msg):
    # Telethon objects have a .to_json() method.
    # We save that for full fidelity.
    raw_json = msg.to_json()
    
    # We extract fields for the normalized columns
    media_type = None
    if msg.photo: media_type = 'photo'
    elif msg.document: media_type = 'document'
    
    sql = """
    INSERT OR IGNORE INTO messages 
    (id, peer_id, date, sender_id, text, reply_to_msg_id, has_media, media_type, raw_data)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    self.cursor.execute(sql, (
        msg.id, msg.peer_id.channel_id, msg.date, msg.sender_id, 
        msg.raw_text, msg.reply_to_msg_id, 1 if msg.media else 0, media_type, raw_json
    ))
```

---

## 5. Deployment & Usage
**Environment Variables (.env):**
```bash
TGX_API_ID=12345
TGX_API_HASH=abcdef...
TGX_SESSION=mysession
```

**Commands:**
```bash
# First run (asks for auth)
python -m tgx.main export --peer @tech_news --last 100 --txt out.txt

# Second run (incremental sync, no re-login)
python -m tgx.main export --peer @tech_news --last 200 --txt out.txt
```

## 6. Key differentiator from competitors
Unlike `telegram-export` (which dumps everything blindly) or `telegram-messages-dump` (which is often memory-bound), **`tgx` uses the DB as the source of truth**.
If you ask for "Last 1000 messages", `tgx` checks: "Do I have messages 5000-6000? Yes. Do I have 6000+? No." It only fetches 6000+, then exports 5000-6000 from disk. This makes it instant for repeated runs.