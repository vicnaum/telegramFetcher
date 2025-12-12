Here is the consolidated design document for **`tgx`**.

It incorporates your requirements: **SQLite-first**, **Python/Telethon** (chosen for maturity/robustness), **single-command sync+export**, **no media downloads**, and **AI-optimized text output**.

---

# Project Spec: `tgx` (Personal Telegram Exporter)

## 1. Overview
**`tgx`** is a command-line tool that archives Telegram chat history from channels and groups you are a member of. It uses your personal Telegram account (MTProto), stores messages in a local **SQLite database** to prevent re-fetching, and exports them into **LLM-friendly flat text** or **JSONL**.

**Philosophy:**
*   **Sporadic Use:** Designed for "I haven't read this group in a week, dump the last 7 days so I can ask an LLM to summarize it."
*   **Local First:** No cloud, no bots, just a local Python script and a `.sqlite` file.
*   **Robust:** Handles Telegram rate limits (`FloodWait`) automatically.
*   **Simple:** No edit tracking, no forum topics, no complex media management.

---

## 2. Technical Stack
*   **Language:** Python 3.10+
*   **Library:** [Telethon](https://docs.telethon.dev/) (Best-in-class support for MTProto, rate limit handling, and updates).
*   **Storage:** SQLite (Native Python `sqlite3`).
*   **Format:** JSONL (Line-delimited JSON) and Flattened TXT.

---

## 3. User Experience (CLI)

There is primarily **one command**. When you run it, the tool automatically **syncs** (fetches missing messages) and then **exports** the requested range.

### The Command: `tgx export`

```bash
tgx export --peer <target> [selectors] [outputs]
```

#### Examples

**1. "Just give me the last 20k messages from this channel"**
```bash
tgx export --peer @technews --last 20000 --txt out.txt
```
*   *Action:* Fetches the newest 20k messages. If some are already in DB, it only fetches the new ones. Exports to `out.txt`.

**2. "Give me last week's messages from a specific group"**
```bash
tgx export --peer "Crypto Traders Group" --start "2025-12-01" --end "2025-12-08" --txt summary_input.txt
```
*   *Action:* Converts dates (Warsaw/Local) to UTC. Checks DB. Fetches any missing messages in that window. Exports.

**3. "Deterministic backup by ID"**
```bash
tgx export --peer -100123456789 --since-id 5000 --until-id 6000 --jsonl backup.jsonl
```

### Configuration
Credentials (`api_id`, `api_hash`) are passed via environment variables or a `.env` file to keep the CLI clean.
*   `TGX_API_ID=...`
*   `TGX_API_HASH=...`
*   `TGX_SESSION=./tgx.session`

---

## 4. Data Logic & Sync Strategy

### Database Schema (SQLite)
We use a **Single DB** approach (`tgx.sqlite`).

**Table: `peers`**
*   `id`: INTEGER PK (Telegram Peer ID, e.g., `-100...`)
*   `username`: TEXT
*   `title`: TEXT
*   `last_msg_id`: INTEGER (The newest message ID we have stored)
*   `last_sync_ts`: TEXT (ISO timestamp of last run)

**Table: `messages`**
*   `peer_id`: INTEGER
*   `id`: INTEGER (Message ID)
*   `date`: TEXT (ISO UTC)
*   `sender_id`: INTEGER
*   `sender_name`: TEXT (Cached display name, e.g., "Alice")
*   `text`: TEXT (Plain text content)
*   `has_media`: BOOLEAN (0 or 1)
*   `raw_json`: TEXT (Full JSON dump of the Telethon object for future-proofing)
*   **PK:** `(peer_id, id)`

### The Sync Algorithm
Since we ignore edits and deletions, the logic is **Append-Only + Gap Filling**.

1.  **Resolve Peer:** Convert `@channel` or `"Title"` to an ID.
2.  **Tail Sync (Always runs):**
    *   Check DB for `last_msg_id` for this peer.
    *   Fetch from Telegram: `min_id = last_msg_id`.
    *   Insert new messages.
3.  **Window Check (If specific range requested):**
    *   If the user asks for `--start 2024-01-01`, and our DB only goes back to `2024-06-01`, we trigger a **Backfill**.
    *   Fetch messages *older* than our oldest stored message until we hit the requested start date.
4.  **Save:** Commit to SQLite.

### Rate Limiting (Robustness)
*   **FloodWaitError:** If Telegram says "Wait 30 seconds", the script will catch the exception, print `[!] Rate limit. Sleeping 30s...`, sleep, and retry automatically.
*   **Batching:** Commit to SQLite every ~100 messages. If the script crashes (Ctrl+C), next run resumes near where it left off.

---

## 5. Output Formats

### A. Flat Text (AI / LLM Friendly)
Optimized for copy-pasting into ChatGPT/Claude.
*   **Filename:** `*.txt`
*   **Timezone:** Converted from stored UTC to **Local (Europe/Warsaw)**.
*   **Formatting:** One line per message. Newlines in message text are replaced by spaces.

**Example:**
```text
2025-12-01 10:00:05 | Alice (@alice) | Hey guys, did you see the news?
2025-12-01 10:01:20 | Bob | Yes, I saw it. It's crazy. [Media]
2025-12-01 10:02:00 | Admin | Please keep discussion on topic.
```

### B. JSONL (Backup / programmatic)
One valid JSON object per line.
```json
{"id": 101, "date": "2025-12-01T09:00:05Z", "sender": "Alice", "text": "Hey guys...", "has_media": false, "peer_id": -100123...}
{"id": 102, "date": "2025-12-01T09:01:20Z", "sender": "Bob", "text": "Yes...", "has_media": true, "peer_id": -100123...}
```

---

## 6. Development Plan

### Milestone 1: Setup & Auth
*   Initialize Poetry/Pip environment with `telethon`.
*   Create `db.py` to initialize SQLite schema.
*   Create `client.py` to handle `TelegramClient` login and session file creation.
*   **Goal:** Run script, login, verify `tgx.session` and `tgx.sqlite` are created.

### Milestone 2: The Syncer (Tail)
*   Implement `resolve_peer`.
*   Implement `sync_tail(peer)`: Fetch messages newer than DB's `max_id`.
*   Store `id`, `date`, `text`, `raw_json`.
*   **Goal:** `python main.py sync --peer @test` populates DB. Running it twice fetches 0 new messages the second time.

### Milestone 3: The Exporter (TXT + JSONL)
*   Implement `export(peer, format='txt')`.
*   Add logic to query DB by date range (UTC conversion).
*   Format output lines (replace `\n`, format timestamp).
*   **Goal:** Generate a readable `out.txt` from the DB.

### Milestone 4: Integration (The "One Command")
*   Combine Sync and Export.
*   Implement `backfill` logic (if requesting older data than in DB).
*   Add CLI argument parsing (`argparse` or `click`).
*   **Goal:** `tgx export --last 1000` works end-to-end.

### Milestone 5: Robustness
*   Wrap fetch loops in `try/except FloodWaitError`.
*   Add visual progress bar (`tqdm`).

---

## 7. Research & References (Actionable)

When building, look at these specific parts of existing repos:

1.  **Telethon Docs (Iterators):**
    *   Look at `client.iter_messages()`. This is the core API.
    *   *Note:* It supports `min_id` (for tail sync) and `offset_date` (for backfill).

2.  **`knadh/tg-archive`:**
    *   Inspect `sync.go` (conceptually) to see how they track the "cursor" (last message ID).
    *   **Ignore:** Their HTML generation code.

3.  **`popstas/telegram-download-chat`:**
    *   Look at their output format logic. They have good heuristics for making text "readable".

4.  **`Kosat/telegram-messages-dump`:**
    *   Look at `dump.py` for how they handle `resolve_peer` for various inputs (username vs join link).

---

## 8. Summary of Scope (MVP)

| Feature | Status | Note |
| :--- | :--- | :--- |
| **Peer Types** | Channels + Groups | No Forum/Topics support. |
| **Sync** | Incremental | Fetches new; backfills old if requested. |
| **Edits** | Ignored | Stored version is final. |
| **Media** | `has_media` flag | No file downloads. |
| **Output** | TXT + JSONL | TXT flattened for AI ingestion. |
| **Comments** | Ignored | Main thread only. |
| **Timezone** | User Local | Inputs and TXT output in Local TZ. |