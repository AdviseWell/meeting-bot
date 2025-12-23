from __future__ import annotations

from metadata import load_meeting_metadata


def test_load_metadata_includes_required_join_fields(monkeypatch) -> None:
    meeting_id = "m1"
    gcs_path = "recordings/x"

    # These are the fields meeting-bot expects on join.
    monkeypatch.delenv("NAME", raising=False)
    monkeypatch.delenv("TEAMID", raising=False)
    monkeypatch.delenv("TIMEZONE", raising=False)

    md = load_meeting_metadata(meeting_id=meeting_id, gcs_path=gcs_path)

    assert md["name"]
    assert md["teamId"]
    assert md["timezone"]
