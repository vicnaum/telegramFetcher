"""Sync logic for fetching messages from Telegram."""

import asyncio
from datetime import datetime

from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError, RPCError

from tgx.db import Database
from tgx.utils import get_display_name, get_peer_id


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
    
    # Determine peer type
    peer_type = "channel"
    if hasattr(entity, "megagroup") and entity.megagroup:
        peer_type = "group"
    elif hasattr(entity, "broadcast") and entity.broadcast:
        peer_type = "channel"
    elif hasattr(entity, "gigagroup") and entity.gigagroup:
        peer_type = "channel"
    
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
        print(f"  Phase 1: Tail sync (messages newer than {db_max_id})...")
        tail_count = 0
        
        try:
            async for msg in client.iter_messages(
                entity,
                min_id=db_max_id,
                reverse=True,  # Fetch oldest -> newest
                wait_time=1,   # Be nice to Telegram
            ):
                batch.append(message_to_dict(msg, peer_id))
                
                if msg.id > new_max_id:
                    new_max_id = msg.id
                
                if len(batch) >= batch_size:
                    inserted = db.insert_messages_batch(batch)
                    total_inserted += inserted
                    tail_count += inserted
                    db.commit()
                    print(f"    Committed batch: {inserted} messages")
                    batch = []
        except FloodWaitError as e:
            print(f"  Rate limited! Sleeping {e.seconds}s...")
            await asyncio.sleep(e.seconds)
        except ChannelPrivateError:
            raise ValueError(f"Channel became private or you were kicked/banned.")
        except RPCError as e:
            print(f"  RPC error: {e}. Retrying after 5s...")
            await asyncio.sleep(5)
        
        # Commit remaining
        if batch:
            inserted = db.insert_messages_batch(batch)
            total_inserted += inserted
            tail_count += inserted
            db.commit()
            batch = []
        
        print(f"  Tail sync complete: {tail_count} new messages")
    
    # Phase 2: Backfill (if we need more messages)
    current_count = db.count_messages(peer_id)
    needed = target_count - current_count
    
    if needed > 0:
        print(f"  Phase 2: Backfill (need {needed} more messages)...")
        
        backfill_count = 0
        
        # Build iter_messages kwargs - only include max_id if we have existing messages
        iter_kwargs = {
            "entity": entity,
            "limit": needed,
            "wait_time": 1,
        }
        
        # If we have messages, fetch older than our oldest (max_id = db_min_id)
        # Otherwise, just fetch from newest (don't pass max_id)
        if db_min_id > 0:
            iter_kwargs["max_id"] = db_min_id
        
        try:
            async for msg in client.iter_messages(**iter_kwargs):
                batch.append(message_to_dict(msg, peer_id))
                
                if new_min_id == 0 or msg.id < new_min_id:
                    new_min_id = msg.id
                if msg.id > new_max_id:
                    new_max_id = msg.id
                
                if len(batch) >= batch_size:
                    inserted = db.insert_messages_batch(batch)
                    total_inserted += inserted
                    backfill_count += inserted
                    db.commit()
                    print(f"    Committed batch: {inserted} messages")
                    batch = []
        except FloodWaitError as e:
            print(f"  Rate limited! Sleeping {e.seconds}s...")
            await asyncio.sleep(e.seconds)
        except ChannelPrivateError:
            raise ValueError(f"Channel became private or you were kicked/banned.")
        except RPCError as e:
            print(f"  RPC error: {e}. Retrying after 5s...")
            await asyncio.sleep(5)
        
        # Commit remaining
        if batch:
            inserted = db.insert_messages_batch(batch)
            total_inserted += inserted
            backfill_count += inserted
            db.commit()
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
    
    print(f"\nSync complete:")
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

