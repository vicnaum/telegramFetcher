"""Sync logic for fetching messages from Telegram."""

import asyncio
import logging
from datetime import datetime

from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FloodWaitError, RPCError
from telethon.tl.types import Channel, Chat, User

from tgx.db import Database
from tgx.utils import get_display_name, get_peer_id

logger = logging.getLogger(__name__)


def classify_peer_type(entity) -> str:
    """Classify the peer type based on Telethon entity class.

    Args:
        entity: Telethon User/Chat/Channel object

    Returns:
        Peer type string: 'user', 'group', 'channel', or 'unknown'
    """
    if isinstance(entity, User):
        return "user"
    elif isinstance(entity, Chat):
        # Basic group (not supergroup/megagroup)
        return "group"
    elif isinstance(entity, Channel):
        # Channel can be broadcast channel, megagroup, or gigagroup
        if getattr(entity, "megagroup", False) or getattr(entity, "gigagroup", False):
            return "group"  # Supergroups are groups
        else:
            return "channel"  # Broadcast channel
    else:
        # Fallback for unknown types
        logger.warning(f"Unknown entity type: {type(entity).__name__}")
        return "unknown"


def get_media_type(msg) -> str | None:
    """Determine media type from a message.

    Args:
        msg: Telethon Message object

    Returns:
        Media type string or None
    """
    if not msg.media:
        return None

    if msg.photo:
        return "photo"
    if msg.video:
        return "video"
    if msg.voice:
        return "voice"
    if msg.audio:
        return "audio"
    if msg.sticker:
        return "sticker"
    if msg.gif:
        return "gif"
    if msg.document:
        return "document"
    if msg.web_preview:
        return "web_page"

    return "other"


async def get_sender_name(msg, sender_cache: dict[int, str | None]) -> str | None:
    """Get sender name, resolving if needed and using cache.

    Args:
        msg: Telethon Message object
        sender_cache: Cache mapping sender_id -> sender_name

    Returns:
        Sender name or None
    """
    if msg.sender_id is None:
        return None

    # Check cache first
    if msg.sender_id in sender_cache:
        return sender_cache[msg.sender_id]

    # Try to get from message's sender attribute
    sender = msg.sender
    if sender is None:
        # Resolve sender via API call
        try:
            sender = await msg.get_sender()
        except Exception:
            # Failed to resolve, cache as None
            sender_cache[msg.sender_id] = None
            return None

    # Get display name and cache
    name = get_display_name(sender) if sender else None
    sender_cache[msg.sender_id] = name
    return name


async def message_to_dict(msg, peer_id: int, sender_cache: dict[int, str | None]) -> dict:
    """Convert a Telethon Message to a dict for database insertion.

    Args:
        msg: Telethon Message object
        peer_id: Peer ID
        sender_cache: Cache mapping sender_id -> sender_name

    Returns:
        Dict ready for db.insert_messages_batch
    """
    sender_name = await get_sender_name(msg, sender_cache)

    reply_to_msg_id = None
    if msg.reply_to:
        reply_to_msg_id = msg.reply_to.reply_to_msg_id

    return {
        "msg_id": msg.id,
        "peer_id": peer_id,
        "date": msg.date,
        "sender_id": msg.sender_id,
        "sender_name": sender_name,
        "text": msg.raw_text,
        "reply_to_msg_id": reply_to_msg_id,
        "has_media": bool(msg.media),
        "media_type": get_media_type(msg),
        "raw_data": msg.to_json(),
    }


async def sync_peer(
    client: TelegramClient,
    db: Database,
    peer_input: str | int | None = None,
    target_count: int = 100,
    batch_size: int = 100,
    min_date: datetime | None = None,
    min_id: int | None = None,
    *,
    entity=None,
    peer_id: int | None = None,
) -> dict:
    """Sync messages from a peer to the database.

    Strategy:
    1. Tail sync: Fetch messages newer than db.max_msg_id (using reverse=True)
    2. Backfill: If count < target_count OR min_date/min_id not reached, fetch older messages

    Args:
        client: Authenticated TelegramClient
        db: Database instance
        peer_input: Peer identifier (@username, link, or ID) - not needed if entity/peer_id provided
        target_count: Target number of messages to have in DB
        batch_size: Commit every N messages
        min_date: If set, backfill until messages older than this date are in DB
        min_id: If set, backfill until messages with ID <= this are in DB
        entity: Pre-resolved Telethon entity (optional, avoids extra API call)
        peer_id: Pre-resolved peer ID (optional, used with entity)

    Returns:
        Dict with sync stats: inserted, peer_id, min_id, max_id
    """
    # Resolve peer if not already provided
    if entity is None:
        if peer_input is None:
            raise ValueError("Either peer_input or entity must be provided")
        try:
            input_entity = await client.get_input_entity(peer_input)
            entity = await client.get_entity(input_entity)
        except ValueError as e:
            raise ValueError(f"Could not find entity '{peer_input}'. Make sure you have joined the group/channel first.") from e
        except ChannelPrivateError:
            raise ValueError(f"Channel '{peer_input}' is private or you were kicked/banned.")
        peer_id = get_peer_id(entity)
    elif peer_id is None:
        peer_id = get_peer_id(entity)

    title = get_display_name(entity)
    username = getattr(entity, "username", None)
    peer_type = classify_peer_type(entity)

    # Update peer metadata
    db.update_peer(peer_id, username, title, peer_type)
    db.commit()

    print(f"Syncing: {title} (peer_id: {peer_id})")

    # Get current boundaries
    db_min_id, db_max_id = db.get_sync_boundaries(peer_id)
    print(f"  DB boundaries: min_id={db_min_id}, max_id={db_max_id}")

    total_inserted = 0
    batch = []
    new_min_id = db_min_id
    new_max_id = db_max_id

    # Sender cache for efficient sender name resolution
    sender_cache: dict[int, str | None] = {}

    # Phase 1: Tail sync (fetch newer messages)
    if db_max_id > 0:
        logger.info(f"Phase 1: Tail sync (messages newer than {db_max_id})...")
        print(f"  Phase 1: Tail sync (messages newer than {db_max_id})...")
        tail_count = 0
        last_processed_id = db_max_id

        while True:
            try:
                async for msg in client.iter_messages(
                    entity,
                    min_id=last_processed_id,
                    reverse=True,  # Fetch oldest -> newest
                    wait_time=1,   # Be nice to Telegram
                ):
                    batch.append(await message_to_dict(msg, peer_id, sender_cache))
                    last_processed_id = max(last_processed_id, msg.id)

                    if msg.id > new_max_id:
                        new_max_id = msg.id

                    if len(batch) >= batch_size:
                        inserted, errors = db.insert_messages_batch(batch)
                        total_inserted += inserted
                        tail_count += inserted
                        db.commit()
                        if errors:
                            for err in errors:
                                logger.warning(f"Insert error: {err}")
                        print(f"    Committed batch: {inserted} messages")
                        batch = []

                # Completed iteration successfully
                break

            except FloodWaitError as e:
                logger.warning(f"Rate limited! Sleeping {e.seconds}s...")
                print(f"  Rate limited! Sleeping {e.seconds}s...")
                await asyncio.sleep(e.seconds)
                # Continue from where we left off
                continue
            except ChannelPrivateError:
                raise ValueError("Channel became private or you were kicked/banned.")
            except RPCError as e:
                logger.warning(f"RPC error: {e}. Retrying after 5s...")
                print(f"  RPC error: {e}. Retrying after 5s...")
                await asyncio.sleep(5)
                # Continue from where we left off
                continue

        # Commit remaining
        if batch:
            inserted, errors = db.insert_messages_batch(batch)
            total_inserted += inserted
            tail_count += inserted
            db.commit()
            if errors:
                for err in errors:
                    logger.warning(f"Insert error: {err}")
            batch = []

        print(f"  Tail sync complete: {tail_count} new messages")

    # Phase 2: Backfill (if we need more messages OR haven't reached boundary)
    # Recompute needed from actual DB count (not from fetched count)
    current_count = db.count_messages(peer_id)
    needed = target_count - current_count

    # Check if we need to backfill for boundary requirements
    boundary_reached = True
    if min_date is not None:
        boundary_reached = db.has_message_at_or_before_date(peer_id, min_date)
        if not boundary_reached:
            logger.info(f"Need to backfill to reach date boundary: {min_date}")
            print(f"  Need to backfill to reach date boundary: {min_date}")
    if min_id is not None and boundary_reached:
        boundary_reached = db.has_message_at_or_before_id(peer_id, min_id)
        if not boundary_reached:
            logger.info(f"Need to backfill to reach ID boundary: {min_id}")
            print(f"  Need to backfill to reach ID boundary: {min_id}")

    if needed > 0 or not boundary_reached:
        if needed > 0 and not boundary_reached:
            logger.info(f"Phase 2: Backfill (need {needed} more messages + reach boundary)...")
            print(f"  Phase 2: Backfill (need {needed} more messages + reach boundary)...")
        elif needed > 0:
            logger.info(f"Phase 2: Backfill (need {needed} more messages)...")
            print(f"  Phase 2: Backfill (need {needed} more messages)...")
        else:
            logger.info("Phase 2: Backfill (syncing until date/ID boundary)...")
            print("  Phase 2: Backfill (syncing until date/ID boundary)...")

        backfill_count = 0
        # Track the lowest ID we've seen to resume from
        resume_max_id = new_min_id if new_min_id > 0 else (db_min_id if db_min_id > 0 else None)

        # Loop until we have enough messages in DB AND reached boundary
        while True:
            # Recompute needed from actual DB count after each iteration
            current_count = db.count_messages(peer_id)
            needed = target_count - current_count

            # Check boundary conditions
            boundary_reached = True
            if min_date is not None:
                boundary_reached = db.has_message_at_or_before_date(peer_id, min_date)
            if min_id is not None and boundary_reached:
                boundary_reached = db.has_message_at_or_before_id(peer_id, min_id)

            # Stop if we have enough messages AND reached boundary
            if needed <= 0 and boundary_reached:
                break

            # Determine fetch limit:
            # - If syncing for count (needed > 0): fetch min(needed, batch_size)
            # - If syncing for boundary only (needed <= 0): fetch batch_size
            fetch_limit = min(needed, batch_size) if needed > 0 else batch_size

            iter_kwargs: dict = {
                "entity": entity,
                "limit": fetch_limit,
                "wait_time": 1,
            }

            # If we have a resume point, fetch older than that
            if resume_max_id is not None and resume_max_id > 0:
                iter_kwargs["max_id"] = resume_max_id

            try:
                batch_fetched = 0
                async for msg in client.iter_messages(**iter_kwargs):
                    batch.append(await message_to_dict(msg, peer_id, sender_cache))
                    batch_fetched += 1

                    # Track lowest ID for resume
                    if resume_max_id is None or msg.id < resume_max_id:
                        resume_max_id = msg.id

                    if new_min_id == 0 or msg.id < new_min_id:
                        new_min_id = msg.id
                    if msg.id > new_max_id:
                        new_max_id = msg.id

                    if len(batch) >= batch_size:
                        inserted, errors = db.insert_messages_batch(batch)
                        total_inserted += inserted
                        backfill_count += inserted
                        db.commit()
                        if errors:
                            for err in errors:
                                logger.warning(f"Insert error: {err}")
                        print(f"    Committed batch: {inserted} messages")
                        batch = []

                # Commit any remaining messages in batch
                if batch:
                    inserted, errors = db.insert_messages_batch(batch)
                    total_inserted += inserted
                    backfill_count += inserted
                    db.commit()
                    if errors:
                        for err in errors:
                            logger.warning(f"Insert error: {err}")
                    print(f"    Committed batch: {inserted} messages")
                    batch = []

                # If we got no messages, we've reached the beginning of the chat
                if batch_fetched == 0:
                    print("  Reached beginning of chat history")
                    break

            except FloodWaitError as e:
                logger.warning(f"Rate limited! Sleeping {e.seconds}s...")
                print(f"  Rate limited! Sleeping {e.seconds}s...")
                await asyncio.sleep(e.seconds)
                # Continue from where we left off
                continue
            except ChannelPrivateError:
                raise ValueError("Channel became private or you were kicked/banned.")
            except RPCError as e:
                logger.warning(f"RPC error: {e}. Retrying after 5s...")
                print(f"  RPC error: {e}. Retrying after 5s...")
                await asyncio.sleep(5)
                # Continue from where we left off
                continue

        print(f"  Backfill complete: {backfill_count} messages")

    # Update boundaries
    if new_min_id > 0 or new_max_id > 0:
        db.update_peer_sync_boundaries(
            peer_id,
            min_msg_id=new_min_id if new_min_id > 0 else None,
            max_msg_id=new_max_id if new_max_id > 0 else None,
        )
        db.commit()

    final_min, final_max = db.get_sync_boundaries(peer_id)
    final_count = db.count_messages(peer_id)

    print("\nSync complete:")
    print(f"  Total inserted: {total_inserted}")
    print(f"  DB message count: {final_count}")
    print(f"  DB boundaries: min_id={final_min}, max_id={final_max}")

    return {
        "inserted": total_inserted,
        "peer_id": peer_id,
        "min_id": final_min,
        "max_id": final_max,
        "count": final_count,
    }

