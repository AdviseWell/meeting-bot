class _FakeDoc:
    def __init__(self, doc_id: str, data: dict):
        self.id = doc_id
        self._data = data
        self.reference = None

    def to_dict(self):
        return self._data


def _import_controller():
    # Tests validate our payload mapping logic; they shouldn't require heavy
    # external deps (google-cloud, kubernetes) or a working OpenSSL stack in
    # the local environment.
    import sys
    import types

    for mod in [
        "google",
        "google.cloud",
        "google.cloud.firestore",
        "google.cloud.pubsub_v1",
        "google.cloud.pubsub_v1.subscriber",
        "google.cloud.pubsub_v1.subscriber.message",
        "kubernetes",
        "kubernetes.client",
        "kubernetes.config",
        "kubernetes.client.rest",
    ]:
        sys.modules.setdefault(mod, types.ModuleType(mod))

    # Provide the attribute imported in controller/main.py
    sys.modules["kubernetes.client.rest"].ApiException = Exception  # type: ignore[attr-defined]  # noqa: E501

    # Provide types referenced in annotations.
    sys.modules["google.cloud.firestore"].DocumentSnapshot = object  # type: ignore[attr-defined]  # noqa: E501
    sys.modules["google.cloud.firestore"].DocumentReference = object  # type: ignore[attr-defined]  # noqa: E501
    sys.modules["google.cloud.firestore"].Transaction = object  # type: ignore[attr-defined]  # noqa: E501

    # Provide decorator used in controller code.
    sys.modules["google.cloud.firestore"].transactional = lambda f: f  # type: ignore[attr-defined]  # noqa: E501

    # Pub/Sub payload annotation referenced by controller code.
    sys.modules["google.cloud.pubsub_v1"].subscriber = sys.modules[
        "google.cloud.pubsub_v1.subscriber"
    ]  # type: ignore[attr-defined]
    sys.modules["google.cloud.pubsub_v1.subscriber"].message = sys.modules[
        "google.cloud.pubsub_v1.subscriber.message"
    ]  # type: ignore[attr-defined]
    sys.modules["google.cloud.pubsub_v1.subscriber.message"].Message = object  # type: ignore[attr-defined]  # noqa: E501

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

    # Avoid constructing the controller (requires Firestore/K8s clients).
    c = MeetingController.__new__(MeetingController)

    # Even if a document carries an old-style gcs_path, controller should now
    # enforce the canonical layout: recordings/<firestore_doc_id>/...
    doc._data["gcs_path"] = "recordings/ad-hoc/x/2025/01/02/teams-meet42"

    p = c._build_job_payload_from_firestore(doc)  # noqa: SLF001

    assert p["meeting_url"].startswith("https://teams.microsoft.com")
    assert p["meeting_id"] == "bot123"
    assert p["fs_meeting_id"] == "bot123"
    assert p["gcs_path"] == "recordings/bot123"
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

    # Avoid constructing the controller (requires Firestore/K8s clients).
    c = MeetingController.__new__(MeetingController)
    p = c._build_job_payload_from_firestore(doc)  # noqa: SLF001

    assert p["meeting_id"] == "meet999"
    assert p["teamId"] == "org1"
    assert p["name"] == "AdviseWell"
    assert p["user_id"] == "user1"
    # Storage is keyed by Firestore document id, not meeting_id.
    assert p["fs_meeting_id"] == "bot123"
    assert p["gcs_path"] == "recordings/bot123"
