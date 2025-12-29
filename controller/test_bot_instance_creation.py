class _FakeRef:
    def __init__(self, doc_id: str):
        self.id = doc_id


class _FakeDoc:
    def __init__(self, doc_id: str, data: dict):
        self.id = doc_id
        self._data = data
        self.reference = _FakeRef(doc_id)

    def to_dict(self):
        return self._data


def test_try_create_bot_instance_skips_when_meeting_has_bot(monkeypatch):
    from main import MeetingController  # type: ignore

    monkeypatch.setenv("GCP_PROJECT_ID", "demo")
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("MANAGER_IMAGE", "manager")
    monkeypatch.setenv("MEETING_BOT_IMAGE", "bot")

    c = MeetingController()

    meeting = _FakeDoc(
        "meet1",
        {
            "meeting_url": "https://teams.microsoft.com/l/meeting-join/...",
            "status": "scheduled",
            "bot_instance_id": "existing",
        },
    )

    # Method should return existing bot id without trying to touch Firestore.
    out = c._try_create_bot_instance_for_meeting(meeting)  # noqa: SLF001
    assert out == "existing"


def test_try_create_bot_instance_skips_when_no_meeting_url(monkeypatch):
    from main import MeetingController  # type: ignore

    monkeypatch.setenv("GCP_PROJECT_ID", "demo")
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("MANAGER_IMAGE", "manager")
    monkeypatch.setenv("MEETING_BOT_IMAGE", "bot")

    c = MeetingController()

    meeting = _FakeDoc(
        "meet2",
        {
            "status": "scheduled",
            # No meeting_url
        },
    )

    out = c._try_create_bot_instance_for_meeting(meeting)  # noqa: SLF001
    assert out is None

