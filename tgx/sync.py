"""Sync logic for fetching messages from Telegram."""

import asyncio
import logging

from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FloodWaitError, RPCError
from telethon.tl.types import Channel, Chat, User

from tgx.db import Database
from tgx.utils import get_display_name, get_peer_id

logger = logging.getLogger(__name__)


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


def message_to_dict(msg, peer_id: int) -> dict:
    """Convert a Telethon Message to a dict for database insertion.

    Args:
        msg: Telethon Message object
        peer_id: Peer ID

    Returns:
        Dict ready for db.insert_messages_batch
    """
    sender_name = None
    if msg.sender:
        sender_name = get_display_name(msg.sender)

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
    peer_input: str,
    target_count: int = 100,
    batch_size: int = 100,
) -> dict:
    """Sync messages from a peer to the database.

    Strategy:
    1. Tail sync: Fetch messages newer than db.max_msg_id (using reverse=True)
    2. Backfill: If count < target_count, fetch older messages

    Args:
        client: Authenticated TelegramClient
        db: Database instance
        peer_input: Peer identifier (@username, link, or ID)
        target_count: Target number of messages to have in DB
        batch_size: Commit every N messages

    Returns:
        Dict with sync stats: inserted, peer_id, min_id, max_id
    """
    # Resolve peer
    try:
        input_entity = await client.get_input_entity(peer_input)
        entity = await client.get_entity(input_entity)
    except ValueError as e:
        raise ValueError(f"Could not find entity '{peer_input}'. Make sure you have joined the group/channel first.") from e
    except ChannelPrivateError:
        raise ValueError(f"Channel '{peer_input}' is private or you were kicked/banned.")

    peer_id = get_peer_id(entity)
    title = get_display_name(entity)
    username = getattr(entity, "username", None)

    # Determine peer type based on Telethon entity class
    if isinstance(entity, User):
        peer_type = "user"
    elif isinstance(entity, Chat):
        # Basic group (not supergroup/megagroup)
        peer_type = "group"
    elif isinstance(entity, Channel):
        # Channel can be broadcast channel, megagroup, or gigagroup
        if getattr(entity, "megagroup", False) or getattr(entity, "gigagroup", False):
            peer_type = "group"  # Supergroups are groups
        else:
            peer_type = "channel"  # Broadcast channel
    else:
        # Fallback for unknown types
        peer_type = "unknown"
        logger.warning(f"Unknown entity type: {type(entity).__name__}")

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
                    batch.append(message_to_dict(msg, peer_id))
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

    # Phase 2: Backfill (if we need more messages)
    current_count = db.count_messages(peer_id)
    needed = target_count - current_count

    if needed > 0:
        logger.info(f"Phase 2: Backfill (need {needed} more messages)...")
        print(f"  Phase 2: Backfill (need {needed} more messages)...")

        backfill_count = 0
        fetched_this_phase = 0
        # Track the lowest ID we've seen to resume from
        resume_max_id = new_min_id if new_min_id > 0 else (db_min_id if db_min_id > 0 else None)

        while fetched_this_phase < needed:
            remaining = needed - fetched_this_phase

            iter_kwargs: dict = {
                "entity": entity,
                "limit": remaining,
                "wait_time": 1,
            }

            # If we have a resume point, fetch older than that
            if resume_max_id is not None and resume_max_id > 0:
                iter_kwargs["max_id"] = resume_max_id

            try:
                batch_fetched = 0
                async for msg in client.iter_messages(**iter_kwargs):
                    batch.append(message_to_dict(msg, peer_id))
                    batch_fetched += 1
                    fetched_this_phase += 1

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

                # If we got no messages, we've reached the beginning
                if batch_fetched == 0:
                    break

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
            backfill_count += inserted
            db.commit()
            if errors:
                for err in errors:
                    logger.warning(f"Insert error: {err}")
            batch = []

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

