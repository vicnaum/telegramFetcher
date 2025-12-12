"""Utility functions for tgx."""

import re

from telethon import utils as tl_utils


def normalize_peer_input(peer_input: str) -> str | int:
    """Normalize peer input to username or int ID.

    Handles various input formats:
    - https://t.me/username -> @username
    - t.me/username -> @username
    - @username -> @username (unchanged)
    - username -> @username
    - -1001234567890 -> -1001234567890 (int)
    - 1234567890 -> 1234567890 (int)

    Args:
        peer_input: Raw peer identifier from user

    Returns:
        Normalized username (str with @) or peer ID (int)
    """
    peer_input = peer_input.strip()

    # Handle t.me links (various formats)
    if "t.me/" in peer_input:
        # Match: t.me/username, t.me/+invite, t.me/joinchat/hash
        match = re.search(r't\.me/(?:\+|joinchat/)?([a-zA-Z][\w]+)', peer_input)
        if match:
            return f"@{match.group(1)}"
        # If we can't parse it, return as-is and let Telethon try
        return peer_input

    # Already has @ prefix
    if peer_input.startswith("@"):
        return peer_input

    # Try parsing as integer (peer ID)
    try:
        return int(peer_input)
    except ValueError:
        pass

    # Assume it's a username without @ prefix
    return f"@{peer_input}"


def get_display_name(entity) -> str:
    """Get a human-readable display name for an entity.

    Args:
        entity: Telethon User/Chat/Channel object

    Returns:
        Display name string
    """
    if entity is None:
        return "Unknown"

    return tl_utils.get_display_name(entity)


def get_peer_id(entity) -> int:
    """Get the normalized peer ID for an entity.

    Args:
        entity: Telethon entity object

    Returns:
        Integer peer ID (negative for chats/channels)
    """
    return tl_utils.get_peer_id(entity)


def truncate_text(text: str | None, max_len: int = 50) -> str:
    """Truncate text for preview display.

    Args:
        text: Text to truncate
        max_len: Maximum length

    Returns:
        Truncated text with ellipsis if needed
    """
    if not text:
        return ""

    # Flatten newlines for preview
    text = text.replace("\n", " ").replace("\r", "")

    if len(text) <= max_len:
        return text

    return text[:max_len - 3] + "..."


def flatten_text(text: str | None) -> str:
    """Flatten text by replacing newlines with spaces and collapsing whitespace.

    Args:
        text: Text to flatten

    Returns:
        Flattened text with normalized whitespace
    """
    if not text:
        return ""

    # split() handles all whitespace (spaces, tabs, newlines) and collapses multiple
    return " ".join(text.split())

