"""Tests for sync functionality.

Focus on:
- Peer type classification (User vs Chat vs Channel)
- Media type detection
"""

from unittest.mock import MagicMock

from telethon.tl.types import Channel, Chat, User

from tgx.sync import classify_peer_type, get_media_type


class TestClassifyPeerType:
    """Test peer type classification logic."""

    def test_user_classified_as_user(self):
        """User entities should be classified as 'user'."""
        user = MagicMock(spec=User)
        assert classify_peer_type(user) == "user"

    def test_chat_classified_as_group(self):
        """Basic Chat entities should be classified as 'group'."""
        chat = MagicMock(spec=Chat)
        assert classify_peer_type(chat) == "group"

    def test_broadcast_channel_classified_as_channel(self):
        """Broadcast channels should be classified as 'channel'."""
        channel = MagicMock(spec=Channel)
        channel.megagroup = False
        channel.gigagroup = False
        assert classify_peer_type(channel) == "channel"

    def test_megagroup_classified_as_group(self):
        """Megagroups (supergroups) should be classified as 'group'."""
        megagroup = MagicMock(spec=Channel)
        megagroup.megagroup = True
        megagroup.gigagroup = False
        assert classify_peer_type(megagroup) == "group"

    def test_gigagroup_classified_as_group(self):
        """Gigagroups should be classified as 'group'."""
        gigagroup = MagicMock(spec=Channel)
        gigagroup.megagroup = False
        gigagroup.gigagroup = True
        assert classify_peer_type(gigagroup) == "group"

    def test_unknown_entity_classified_as_unknown(self):
        """Unknown entity types should be classified as 'unknown'."""
        # MagicMock without spec is not an instance of User/Chat/Channel
        unknown = MagicMock()
        assert classify_peer_type(unknown) == "unknown"


class TestGetMediaType:
    """Test media type detection."""

    def test_no_media(self):
        """No media should return None."""
        msg = MagicMock()
        msg.media = None

        assert get_media_type(msg) is None

    def test_photo(self):
        """Photo should be detected."""
        msg = MagicMock()
        msg.media = MagicMock()
        msg.photo = MagicMock()
        msg.video = None
        msg.voice = None
        msg.audio = None
        msg.sticker = None
        msg.gif = None
        msg.document = None
        msg.web_preview = None

        assert get_media_type(msg) == "photo"

    def test_video(self):
        """Video should be detected."""
        msg = MagicMock()
        msg.media = MagicMock()
        msg.photo = None
        msg.video = MagicMock()
        msg.voice = None
        msg.audio = None
        msg.sticker = None
        msg.gif = None
        msg.document = None
        msg.web_preview = None

        assert get_media_type(msg) == "video"

    def test_voice(self):
        """Voice should be detected."""
        msg = MagicMock()
        msg.media = MagicMock()
        msg.photo = None
        msg.video = None
        msg.voice = MagicMock()
        msg.audio = None
        msg.sticker = None
        msg.gif = None
        msg.document = None
        msg.web_preview = None

        assert get_media_type(msg) == "voice"

    def test_sticker(self):
        """Sticker should be detected."""
        msg = MagicMock()
        msg.media = MagicMock()
        msg.photo = None
        msg.video = None
        msg.voice = None
        msg.audio = None
        msg.sticker = MagicMock()
        msg.gif = None
        msg.document = None
        msg.web_preview = None

        assert get_media_type(msg) == "sticker"

    def test_document(self):
        """Document should be detected."""
        msg = MagicMock()
        msg.media = MagicMock()
        msg.photo = None
        msg.video = None
        msg.voice = None
        msg.audio = None
        msg.sticker = None
        msg.gif = None
        msg.document = MagicMock()
        msg.web_preview = None

        assert get_media_type(msg) == "document"

    def test_web_page(self):
        """Web preview should be detected as web_page."""
        msg = MagicMock()
        msg.media = MagicMock()
        msg.photo = None
        msg.video = None
        msg.voice = None
        msg.audio = None
        msg.sticker = None
        msg.gif = None
        msg.document = None
        msg.web_preview = MagicMock()

        assert get_media_type(msg) == "web_page"

    def test_other_media(self):
        """Unknown media type should return 'other'."""
        msg = MagicMock()
        msg.media = MagicMock()
        msg.photo = None
        msg.video = None
        msg.voice = None
        msg.audio = None
        msg.sticker = None
        msg.gif = None
        msg.document = None
        msg.web_preview = None

        assert get_media_type(msg) == "other"
