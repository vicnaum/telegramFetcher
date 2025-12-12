"""Utility functions for tgx."""

from telethon import utils as tl_utils


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
    """Flatten text by replacing newlines with spaces.
    
    Args:
        text: Text to flatten
    
    Returns:
        Flattened text
    """
    if not text:
        return ""
    
    # Replace CR/LF with spaces, collapse multiple spaces
    text = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    
    # Collapse multiple spaces
    while "  " in text:
        text = text.replace("  ", " ")
    
    return text.strip()

