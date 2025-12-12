"""Sync logic for fetching messages from Telegram."""

import asyncio
import logging
from datetime import datetime

from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FloodWaitError, RPCError
from telethon.tl.types import Channel, Chat, User

from tgx.db import Database, datetime_to_epoch_ms
from tgx.utils import get_display_name, get_peer_id

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 5
INITIAL_BACKOFF = 5  # seconds
MAX_BACKOFF = 300  # 5 minutes cap


def _calculate_backoff(retry_count: int) -> float:
    """Calculate exponential backoff delay.

    Args:
        retry_count: Number of retries so far (0-indexed)

    Returns:
        Delay in seconds, capped at MAX_BACKOFF
    """
    return min(INITIAL_BACKOFF * (2 ** retry_count), MAX_BACKOFF)


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


async def get_sender_name(
    msg,
    sender_cache: dict[int, str | None],
    peer_title: str | None = None,
) -> str | None:
    """Get sender name, resolving if needed and using cache.

    Handles multiple sender attribution sources:
    1. sender_id -> resolve to user/chat name (cached)
    2. post_author -> channel post signature
    3. sender_chat -> anonymous admin or linked channel
    4. peer_title -> fallback for channel posts with no attribution

    Args:
        msg: Telethon Message object
        sender_cache: Cache mapping sender_id -> sender_name
        peer_title: Fallback title for channel posts (optional)

    Returns:
        Sender name or None

    Note:
        Only caches successful lookups and permanent errors.
        Transient errors (FloodWait, network issues) are not cached
        to allow retry on subsequent messages.
    """
    # Case 1: Message has a sender_id (most common)
    if msg.sender_id is not None:
        # Check cache first
        if msg.sender_id in sender_cache:
            return sender_cache[msg.sender_id]

        # Try to get from message's sender attribute
        sender = msg.sender
        if sender is None:
            # Resolve sender via API call
            try:
                sender = await msg.get_sender()
            except FloodWaitError:
                # Transient error - don't cache, let caller handle
                logger.debug(f"FloodWait while resolving sender {msg.sender_id}")
                return None
            except ChannelPrivateError:
                # Permanent error - sender is in a private channel we can't access
                sender_cache[msg.sender_id] = None
                return None
            except RPCError as e:
                # Check if it's a permanent error (user not found, etc.)
                error_msg = str(e).lower()
                if "user" in error_msg and ("invalid" in error_msg or "not found" in error_msg):
                    # Permanent - user doesn't exist
                    sender_cache[msg.sender_id] = None
                    return None
                # Other RPC errors might be transient - don't cache
                logger.debug(f"RPC error while resolving sender {msg.sender_id}: {e}")
                return None
            except (OSError, ConnectionError):
                # Network errors are transient - don't cache
                logger.debug(f"Network error while resolving sender {msg.sender_id}")
                return None

        # Get display name and cache
        name = get_display_name(sender) if sender else None
        sender_cache[msg.sender_id] = name
        return name

    # Case 2: Channel post with signature (post_author)
    if hasattr(msg, "post_author") and msg.post_author:
        return msg.post_author

    # Case 3: Anonymous admin or linked channel (sender_chat)
    if hasattr(msg, "sender_chat") and msg.sender_chat:
        return get_display_name(msg.sender_chat)

    # Case 4: Fallback to peer title for channel posts
    if peer_title:
        return peer_title

    return None


async def message_to_dict(
    msg,
    peer_id: int,
    sender_cache: dict[int, str | None],
    peer_title: str | None = None,
    store_raw: bool = True,
) -> dict:
    """Convert a Telethon Message to a dict for database insertion.

    Args:
        msg: Telethon Message object
        peer_id: Peer ID
        sender_cache: Cache mapping sender_id -> sender_name
        peer_title: Peer title for fallback sender attribution
        store_raw: Whether to store raw JSON (default True)

    Returns:
        Dict ready for db.insert_messages_batch
    """
    sender_name = await get_sender_name(msg, sender_cache, peer_title)

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
        "raw_data": msg.to_json() if store_raw else None,
    }


def _flush_batch(
    db: Database,
    batch: list[dict],
    stats: dict,
) -> int:
    """Flush a batch of messages to the database.

    Also updates stats with range metadata for sync range tracking.

    Args:
        db: Database instance
        batch: List of message dicts to insert
        stats: Dict to update with inserted count and range info

    Returns:
        Number of messages inserted
    """
    if not batch:
        return 0

    inserted = db.insert_messages_batch(batch)
    db.commit()
    stats["total_inserted"] += inserted

    # Track range metadata for this sync session
    if inserted > 0:
        batch_ids = [m["msg_id"] for m in batch]
        batch_dates = [datetime_to_epoch_ms(m["date"]) for m in batch if m["date"]]

        if batch_ids:
            batch_min_id = min(batch_ids)
            batch_max_id = max(batch_ids)

            # Update session-wide range tracking
            if stats.get("session_min_id") is None or batch_min_id < stats["session_min_id"]:
                stats["session_min_id"] = batch_min_id
            if stats.get("session_max_id") is None or batch_max_id > stats["session_max_id"]:
                stats["session_max_id"] = batch_max_id

        if batch_dates:
            batch_min_date = min(batch_dates)
            batch_max_date = max(batch_dates)

            if stats.get("session_min_date_ms") is None or batch_min_date < stats["session_min_date_ms"]:
                stats["session_min_date_ms"] = batch_min_date
            if stats.get("session_max_date_ms") is None or batch_max_date > stats["session_max_date_ms"]:
                stats["session_max_date_ms"] = batch_max_date

    return inserted


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
    shutdown_event: asyncio.Event | None = None,
    store_raw: bool = True,
) -> dict:
    """Sync messages from a peer to the database.

    Strategy:
    1. Tail sync: Fetch messages newer than db.max_msg_id (using reverse=True)
    2. Backfill: If count < target_count OR min_date/min_id not reached, fetch older messages

    Data integrity guarantees:
    - Cursors are only advanced after successful commit
    - Partial batches are flushed before retry/sleep
    - Boundaries are derived from actual DB data after all commits

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
        shutdown_event: Optional event to signal graceful shutdown
        store_raw: Whether to store raw JSON for each message (default True)

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
            raise ValueError(f"Channel '{peer_input}' is private or you were kicked/banned.") from None
        peer_id = get_peer_id(entity)
    elif peer_id is None:
        peer_id = get_peer_id(entity)

    title = get_display_name(entity)
    username = getattr(entity, "username", None)
    peer_type = classify_peer_type(entity)

    # Update peer metadata
    db.update_peer(peer_id, username, title, peer_type)
    db.commit()

    logger.info(f"Syncing: {title} (peer_id: {peer_id})")

    # Get current boundaries - reconcile peers table with actual message data
    # This ensures crash-safe resume: if interrupted after commit but before
    # peers table update, we use the actual message boundaries
    db_min_id, db_max_id = db.get_sync_boundaries(peer_id)
    actual_min, actual_max = db.get_actual_message_boundaries(peer_id)

    # Use the better of the two sources
    if actual_min > 0:
        db_min_id = min(db_min_id, actual_min) if db_min_id > 0 else actual_min
    if actual_max > 0:
        db_max_id = max(db_max_id, actual_max)

    logger.info(f"DB boundaries: min_id={db_min_id}, max_id={db_max_id}")

    # Stats tracking - use a dict so _flush_batch can update it
    stats = {"total_inserted": 0}
    batch: list[dict] = []

    # Sender cache for efficient sender name resolution
    sender_cache: dict[int, str | None] = {}

    def _should_shutdown() -> bool:
        """Check if shutdown was requested."""
        return shutdown_event is not None and shutdown_event.is_set()

    # Phase 1: Tail sync (fetch newer messages)
    if db_max_id > 0:
        logger.info(f"Phase 1: Tail sync (messages newer than {db_max_id})...")
        tail_count = 0
        retry_count = 0

        # last_committed_id: only advance after successful commit
        # This is the resume point if we get interrupted
        last_committed_id = db_max_id

        while True:
            if _should_shutdown():
                # Flush any pending batch before exiting
                if batch:
                    inserted = _flush_batch(db, batch, stats)
                    tail_count += inserted
                    logger.info(f"Flushed {inserted} messages before shutdown")
                    batch = []
                logger.info("Shutdown requested, stopping tail sync")
                break

            try:
                async for msg in client.iter_messages(
                    entity,
                    min_id=last_committed_id,
                    reverse=True,  # Fetch oldest -> newest
                    wait_time=1,   # Be nice to Telegram
                ):
                    batch.append(await message_to_dict(msg, peer_id, sender_cache, title, store_raw))

                    if len(batch) >= batch_size:
                        inserted = _flush_batch(db, batch, stats)
                        tail_count += inserted
                        # Only advance cursor after successful commit
                        last_committed_id = max(m["msg_id"] for m in batch)
                        # Show progress with date range
                        batch_dates = [m["date"] for m in batch if m["date"]]
                        if batch_dates:
                            oldest = min(batch_dates).strftime("%Y-%m-%d %H:%M")
                            newest = max(batch_dates).strftime("%Y-%m-%d %H:%M")
                            logger.info(f"  +{inserted} msgs [{oldest} → {newest}] (total: {stats['total_inserted']})")
                        else:
                            logger.info(f"  +{inserted} msgs (total: {stats['total_inserted']})")
                        batch = []

                    # Check shutdown between messages
                    if _should_shutdown():
                        break

                # Commit remaining batch after successful iteration
                if batch:
                    inserted = _flush_batch(db, batch, stats)
                    tail_count += inserted
                    batch_dates = [m["date"] for m in batch if m["date"]]
                    if batch_dates:
                        oldest = min(batch_dates).strftime("%Y-%m-%d %H:%M")
                        newest = max(batch_dates).strftime("%Y-%m-%d %H:%M")
                        logger.info(f"  +{inserted} msgs [{oldest} → {newest}] (total: {stats['total_inserted']})")
                    else:
                        logger.info(f"  +{inserted} msgs (total: {stats['total_inserted']})")
                    batch = []

                # Completed iteration successfully - reset retry count
                retry_count = 0
                break

            except FloodWaitError as e:
                # Flush partial batch before sleeping
                if batch:
                    inserted = _flush_batch(db, batch, stats)
                    tail_count += inserted
                    last_committed_id = max(m["msg_id"] for m in batch)
                    logger.info(f"Flushed {inserted} messages before rate limit sleep")
                    batch = []

                # FloodWait is handled by Telegram's specified wait time
                logger.warning(f"Rate limited! Sleeping {e.seconds}s...")
                await asyncio.sleep(e.seconds)
                # Continue from last_committed_id (don't count as retry)
                continue
            except ChannelPrivateError:
                raise ValueError("Channel became private or you were kicked/banned.") from None
            except RPCError as e:
                # Flush partial batch before retry
                if batch:
                    inserted = _flush_batch(db, batch, stats)
                    tail_count += inserted
                    last_committed_id = max(m["msg_id"] for m in batch)
                    logger.info(f"Flushed {inserted} messages before retry")
                    batch = []

                retry_count += 1
                if retry_count > MAX_RETRIES:
                    logger.error(f"Max retries ({MAX_RETRIES}) exceeded in tail sync. Last error: {e}")
                    break  # Exit phase with partial results

                backoff = _calculate_backoff(retry_count - 1)
                logger.warning(f"RPC error: {e}. Retry {retry_count}/{MAX_RETRIES} after {backoff}s...")
                await asyncio.sleep(backoff)
                continue

        logger.info(f"Tail sync complete: {tail_count} new messages")

    # Check for shutdown before Phase 2
    if _should_shutdown():
        logger.info("Shutdown requested, skipping backfill")
    else:
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
        if min_id is not None and boundary_reached:
            boundary_reached = db.has_message_at_or_before_id(peer_id, min_id)
            if not boundary_reached:
                logger.info(f"Need to backfill to reach ID boundary: {min_id}")

        if needed > 0 or not boundary_reached:
            if needed > 0 and not boundary_reached:
                logger.info(f"Phase 2: Backfill (need {needed} more messages + reach boundary)...")
            elif needed > 0:
                logger.info(f"Phase 2: Backfill (need {needed} more messages)...")
            else:
                logger.info("Phase 2: Backfill (syncing until date/ID boundary)...")

            backfill_count = 0
            retry_count = 0

            # Get the actual lowest message ID from DB to use as resume point
            # This is more reliable than tracking in-memory
            actual_min, _ = db.get_actual_message_boundaries(peer_id)
            last_committed_max_id = actual_min if actual_min > 0 else None

            # Loop until we have enough messages in DB AND reached boundary
            while True:
                if _should_shutdown():
                    if batch:
                        inserted = _flush_batch(db, batch, stats)
                        backfill_count += inserted
                        logger.info(f"Flushed {inserted} messages before shutdown")
                        batch = []
                    logger.info("Shutdown requested, stopping backfill")
                    break

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
                if last_committed_max_id is not None and last_committed_max_id > 0:
                    iter_kwargs["max_id"] = last_committed_max_id

                try:
                    batch_fetched = 0
                    lowest_id_in_batch: int | None = None

                    async for msg in client.iter_messages(**iter_kwargs):
                        batch.append(await message_to_dict(msg, peer_id, sender_cache, title, store_raw))
                        batch_fetched += 1

                        # Track lowest ID in current batch for resume
                        if lowest_id_in_batch is None or msg.id < lowest_id_in_batch:
                            lowest_id_in_batch = msg.id

                        if len(batch) >= batch_size:
                            inserted = _flush_batch(db, batch, stats)
                            backfill_count += inserted
                            # Update resume point to lowest ID we've committed
                            if lowest_id_in_batch is not None:
                                last_committed_max_id = lowest_id_in_batch
                            # Show progress with date range and target
                            batch_dates = [m["date"] for m in batch if m["date"]]
                            if batch_dates:
                                oldest = min(batch_dates).strftime("%Y-%m-%d %H:%M")
                                newest = max(batch_dates).strftime("%Y-%m-%d %H:%M")
                                target_info = ""
                                if min_date:
                                    target_info = f" → target: {min_date.strftime('%Y-%m-%d %H:%M')}"
                                logger.info(f"  +{inserted} msgs [{oldest} → {newest}]{target_info} (total: {stats['total_inserted']})")
                            else:
                                logger.info(f"  +{inserted} msgs (total: {stats['total_inserted']})")
                            batch = []
                            lowest_id_in_batch = None

                        # Check shutdown between messages
                        if _should_shutdown():
                            break

                    # Commit any remaining messages in batch
                    if batch:
                        inserted = _flush_batch(db, batch, stats)
                        backfill_count += inserted
                        if lowest_id_in_batch is not None:
                            last_committed_max_id = lowest_id_in_batch
                        batch_dates = [m["date"] for m in batch if m["date"]]
                        if batch_dates:
                            oldest = min(batch_dates).strftime("%Y-%m-%d %H:%M")
                            newest = max(batch_dates).strftime("%Y-%m-%d %H:%M")
                            target_info = ""
                            if min_date:
                                target_info = f" → target: {min_date.strftime('%Y-%m-%d %H:%M')}"
                            logger.info(f"  +{inserted} msgs [{oldest} → {newest}]{target_info} (total: {stats['total_inserted']})")
                        else:
                            logger.info(f"  +{inserted} msgs (total: {stats['total_inserted']})")
                        batch = []

                    # If we got no messages, we've reached the beginning of the chat
                    if batch_fetched == 0:
                        logger.info("Reached beginning of chat history")
                        break

                    # Successful iteration - reset retry count
                    retry_count = 0

                except FloodWaitError as e:
                    # Flush partial batch before sleeping
                    if batch:
                        inserted = _flush_batch(db, batch, stats)
                        backfill_count += inserted
                        # Get actual min from DB after commit for accurate resume
                        actual_min, _ = db.get_actual_message_boundaries(peer_id)
                        if actual_min > 0:
                            last_committed_max_id = actual_min
                        logger.info(f"Flushed {inserted} messages before rate limit sleep")
                        batch = []

                    # FloodWait is handled by Telegram's specified wait time
                    logger.warning(f"Rate limited! Sleeping {e.seconds}s...")
                    await asyncio.sleep(e.seconds)
                    continue
                except ChannelPrivateError:
                    raise ValueError("Channel became private or you were kicked/banned.") from None
                except RPCError as e:
                    # Flush partial batch before retry
                    if batch:
                        inserted = _flush_batch(db, batch, stats)
                        backfill_count += inserted
                        actual_min, _ = db.get_actual_message_boundaries(peer_id)
                        if actual_min > 0:
                            last_committed_max_id = actual_min
                        logger.info(f"Flushed {inserted} messages before retry")
                        batch = []

                    retry_count += 1
                    if retry_count > MAX_RETRIES:
                        logger.error(f"Max retries ({MAX_RETRIES}) exceeded in backfill. Last error: {e}")
                        break  # Exit phase with partial results

                    backoff = _calculate_backoff(retry_count - 1)
                    logger.warning(f"RPC error: {e}. Retry {retry_count}/{MAX_RETRIES} after {backoff}s...")
                    await asyncio.sleep(backoff)
                    continue

            logger.info(f"Backfill complete: {backfill_count} messages")

    # Derive final boundaries from actual DB data (not in-memory tracking)
    # This ensures boundaries are always accurate regardless of retry paths
    actual_min_id, actual_max_id = db.get_actual_message_boundaries(peer_id)

    # Update peer boundaries from actual data
    if actual_min_id > 0 or actual_max_id > 0:
        db.update_peer_sync_boundaries(
            peer_id,
            min_msg_id=actual_min_id if actual_min_id > 0 else None,
            max_msg_id=actual_max_id if actual_max_id > 0 else None,
        )
        db.commit()

    # Register this sync session as a range (if we inserted anything)
    if stats["total_inserted"] > 0 and stats.get("session_min_id") is not None:
        db.add_sync_range(
            peer_id=peer_id,
            min_msg_id=stats["session_min_id"],
            max_msg_id=stats["session_max_id"],
            min_date_utc_ms=stats["session_min_date_ms"],
            max_date_utc_ms=stats["session_max_date_ms"],
            message_count=stats["total_inserted"],
        )
        db.commit()
        logger.debug(f"Registered sync range: [{stats['session_min_id']}, {stats['session_max_id']}]")

    final_min, final_max = db.get_sync_boundaries(peer_id)
    final_count = db.count_messages(peer_id)

    logger.info(f"Sync complete: inserted={stats['total_inserted']}, count={final_count}, "
                f"boundaries=[{final_min}, {final_max}]")

    return {
        "inserted": stats["total_inserted"],
        "peer_id": peer_id,
        "min_id": final_min,
        "max_id": final_max,
        "count": final_count,
    }
