class _FakeDoc:
    def __init__(self, doc_id: str, data: dict):
        self.id = doc_id
        self._data = data
        self.reference = None

    def to_dict(self):
        return self._data


def _import_controller():
    # Local import so tests can run without executing controller startup.
    from main import MeetingController  # type: ignore

    return MeetingController


def test_build_job_payload_minimal_fields(monkeypatch):
    MeetingController = _import_controller()

    # Minimal required env
    monkeypatch.setenv("GCP_PROJECT_ID", "demo")
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("MANAGER_IMAGE", "manager")
    monkeypatch.setenv("MEETING_BOT_IMAGE", "bot")

    doc = _FakeDoc(
        "bot123",
        {
            "meeting_url": "https://teams.microsoft.com/l/meeting-join/...",
            "status": "queued",
        },
    )

    c = MeetingController()

    # Monkeypatch datetime usage by providing explicit gcs_path on doc.
    doc._data["gcs_path"] = "recordings/ad-hoc/x/2025/01/02/teams-meet42"

    p = c._build_job_payload_from_firestore(doc)  # noqa: SLF001

    assert p["meeting_url"].startswith("https://teams.microsoft.com")
    assert p["meeting_id"] == "bot123"
    assert p["gcs_path"] == "recordings/ad-hoc/x/2025/01/02/teams-meet42"
    assert p["bot_instance_id"] == "bot123"


def test_build_job_payload_prefers_initial_linked_meeting(monkeypatch):
    MeetingController = _import_controller()

    monkeypatch.setenv("GCP_PROJECT_ID", "demo")
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("MANAGER_IMAGE", "manager")
    monkeypatch.setenv("MEETING_BOT_IMAGE", "bot")

    doc = _FakeDoc(
        "bot123",
        {
            "meeting_url": "https://teams.microsoft.com/l/meeting-join/...",
            "status": "queued",
            "creator_organization_id": "org1",
            "creator_user_id": "user1",
            "bot_name": "AdviseWell",
            "initial_linked_meeting": {
                "meeting_id": "meet999",
                "organization_id": "org1",
                "user_id": "user1",
            },
            "gcs_path": "recordings/ad-hoc/org1/2025/01/02/teams-meet999",
        },
    )

    c = MeetingController()
    p = c._build_job_payload_from_firestore(doc)  # noqa: SLF001

    assert p["meeting_id"] == "meet999"
    assert p["teamId"] == "org1"
    assert p["name"] == "AdviseWell"
    assert p["user_id"] == "user1"
