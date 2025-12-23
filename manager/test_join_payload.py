from __future__ import annotations

from join_payload import build_join_payload


def test_join_payload_includes_required_keys() -> None:
    payload = build_join_payload(meeting_url="https://x", metadata={})

    for key in [
        "url",
        "name",
        "teamId",
        "timezone",
        "bearerToken",
        "userId",
    ]:
        assert key in payload


def test_join_payload_defaults_auto_generated_fields() -> None:
    payload = build_join_payload(meeting_url="https://x", metadata={})
    assert payload["bearerToken"] == "AUTO-GENERATED"
    assert payload["userId"] == "AUTO-GENERATED"


def test_join_payload_prefers_metadata_values() -> None:
    payload = build_join_payload(
        meeting_url="https://x",
        metadata={
            "name": "Test Bot",
            "teamId": "advisewell",
            "timezone": "UTC",
            "bearerToken": "tok",
            "userId": "u",
        },
    )

    assert payload["name"] == "Test Bot"
    assert payload["teamId"] == "advisewell"
    assert payload["bearerToken"] == "tok"
    assert payload["userId"] == "u"
