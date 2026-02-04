from __future__ import annotations

import logging
from typing import Dict

logger = logging.getLogger(__name__)


def build_join_payload(*, meeting_url: str, metadata: Dict) -> Dict:
    """Build meeting-bot join payload.

    Some meeting-bot deployments validate that these keys exist:
    url, name, teamId, timezone, bearerToken, userId.

    To stay compatible, always include all keys.
    """

    bearer_token = (
        metadata.get("bearerToken") or metadata.get("bearer_token") or "AUTO-GENERATED"
    )

    if bearer_token == "AUTO-GENERATED":
        logger.warning(
            "⚠️ Using AUTO-GENERATED bearerToken. "
            "This may cause the bot to fail if the backend requires authentication."
        )

    payload = {
        "url": meeting_url,
        "name": metadata.get("name") or "Meeting Bot",
        "teamId": (
            metadata.get("teamId")
            or metadata.get("team_id")
            or metadata.get("meeting_id")
            or "default-team"
        ),
        "timezone": metadata.get("timezone") or "UTC",
        "bearerToken": bearer_token,
        "userId": (
            metadata.get("userId") or metadata.get("user_id") or "AUTO-GENERATED"
        ),
        "botId": (metadata.get("botId") or metadata.get("bot_id") or "AUTO-GENERATED"),
        "occurrenceStartUtc": (
            metadata.get("occurrenceStartUtc")
            or metadata.get("occurrence_start_utc")
            or ""
        ),
    }

    return payload
