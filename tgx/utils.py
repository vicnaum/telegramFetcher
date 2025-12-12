"""Utility functions for tgx."""

import re
from urllib.parse import urlparse

from telethon import utils as tl_utils


def normalize_peer_input(peer_input: str) -> str | int:
    """Normalize peer input to username, int ID, or passthrough for special links.

    Handles various input formats:
    - https://t.me/username -> @username
    - https://t.me/username/123 -> @username (message link)
    - t.me/username -> @username
    - @username -> @username (unchanged)
    - username -> @username
    - -1001234567890 -> -1001234567890 (int)
    - 1234567890 -> 1234567890 (int)

    Special cases (passed through to Telethon):
    - t.me/c/123456789/123 -> passthrough (private channel link with numeric ID)
    - t.me/+abcdef -> passthrough (invite link)
    - t.me/joinchat/abcdef -> passthrough (invite link)

    Args:
        peer_input: Raw peer identifier from user

    Returns:
        Normalized username (str with @), peer ID (int), or original string for
        special links that Telethon should handle directly
    """
    peer_input = peer_input.strip()

    # Handle t.me links (various formats)
    if "t.me/" in peer_input or "telegram.me/" in peer_input:
        # Parse URL properly
        url = peer_input
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        parsed = urlparse(url)
        path = parsed.path.strip("/")

        if not path:
            # Just "t.me" with no path
            return peer_input

        # Split path into segments
        segments = path.split("/")
        first_segment = segments[0]

        # Invite links: t.me/+xxx or t.me/joinchat/xxx
        # Pass these through to Telethon to handle
        if first_segment.startswith("+") or first_segment == "joinchat":
            return peer_input

        # Private channel links: t.me/c/123456789/123
        # The "c" indicates a private channel with numeric ID
        # Convert to peer_id format: -100 prefix for channels
        if first_segment == "c" and len(segments) >= 2:
            try:
                internal_id = int(segments[1])
                # Telegram channel IDs use -100 prefix
                return -1000000000000 - internal_id
            except ValueError:
                # Not a valid numeric ID, pass through
                return peer_input

        # Public username links: t.me/username or t.me/username/123
        # Validate that it looks like a username (starts with letter, alphanumeric + underscore)
        if re.match(r"^[a-zA-Z][a-zA-Z0-9_]{3,}$", first_segment):
            return f"@{first_segment}"

        # Unknown format, pass through
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

    result: str = tl_utils.get_display_name(entity)
    return result


def get_peer_id(entity) -> int:
    """Get the normalized peer ID for an entity.

    Args:
        entity: Telethon entity object

    Returns:
        Integer peer ID (negative for chats/channels)
    """
    result: int = tl_utils.get_peer_id(entity)
    return result


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

