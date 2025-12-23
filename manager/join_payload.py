from __future__ import annotations

from typing import Dict


def build_join_payload(*, meeting_url: str, metadata: Dict) -> Dict:
    """Build meeting-bot join payload.

    Some meeting-bot deployments validate that these keys exist:
    url, name, teamId, timezone, bearerToken, userId.

    To stay compatible, always include all keys.
    """

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
        "bearerToken": (
            metadata.get("bearerToken")
            or metadata.get("bearer_token")
            or "AUTO-GENERATED"
        ),
        "userId": (
            metadata.get("userId") or metadata.get("user_id") or "AUTO-GENERATED"
        ),
        "botId": (metadata.get("botId") or metadata.get("bot_id") or "AUTO-GENERATED"),
    }

    return payload
