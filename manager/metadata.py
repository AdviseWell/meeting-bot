from __future__ import annotations

import os
from typing import Dict


def load_meeting_metadata(
    *,
    meeting_id: str | None,
    gcs_path: str | None,
) -> Dict:
    """Build the metadata dict used for meeting-bot API calls.

    Keep this module dependency-free so it can be unit-tested without pulling
    in requests/OpenSSL (which may be unavailable in some minimal
    environments).
    """

    metadata: Dict[str, str | None] = {}

    if meeting_id:
        metadata["meeting_id"] = meeting_id
    if gcs_path:
        metadata["gcs_path"] = gcs_path

    bearer_token = (
        os.environ.get("BEARERTOKEN")
        or os.environ.get("BEARER_TOKEN")
        or os.environ.get("bearer_token")
    )
    user_id = (
        os.environ.get("USERID")
        or os.environ.get("USER_ID")
        or os.environ.get("user_id")
    )
    bot_id = (
        os.environ.get("BOTID") or os.environ.get("BOT_ID") or os.environ.get("bot_id")
    )
    event_id = (
        os.environ.get("EVENTID")
        or os.environ.get("EVENT_ID")
        or os.environ.get("event_id")
    )

    metadata["bearerToken"] = bearer_token
    metadata["userId"] = user_id
    metadata["botId"] = bot_id or event_id or meeting_id

    metadata["timezone"] = (
        os.environ.get("TIMEZONE") or os.environ.get("timezone") or "UTC"
    )

    metadata["teamId"] = (
        os.environ.get("TEAMID")
        or os.environ.get("TEAM_ID")
        or os.environ.get("team_id")
        or meeting_id
    )
    metadata["name"] = (
        os.environ.get("NAME")
        or os.environ.get("name")
        or os.environ.get("BOT_NAME")
        or "Meeting Bot"
    )

    metadata["bearer_token"] = bearer_token
    metadata["user_id"] = user_id
    return metadata
