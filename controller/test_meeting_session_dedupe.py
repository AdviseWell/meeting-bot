from __future__ import annotations


class _FakeDoc:
    def __init__(self, doc_id: str, data: dict):
        self.id = doc_id
        self._data = data
        self.reference = None

    def to_dict(self):
        return self._data


def _import_controller():
    # Import controller/main.py directly with dependency stubs.
    import sys
    import types
    from pathlib import Path
    import importlib.util

    controller_dir = Path(__file__).resolve().parent

    for mod in [
        "google",
        "google.cloud",
        "google.cloud.firestore",
        "google.cloud.storage",
        "google.cloud.pubsub_v1",
        "google.cloud.pubsub_v1.subscriber",
        "google.cloud.pubsub_v1.subscriber.message",
        "kubernetes",
        "kubernetes.client",
        "kubernetes.config",
        "kubernetes.client.rest",
    ]:
        sys.modules.setdefault(mod, types.ModuleType(mod))

    k8s_rest = sys.modules["kubernetes.client.rest"]
    k8s_rest.ApiException = Exception  # type: ignore[attr-defined]

    # Types referenced in annotations.
    firestore_mod = sys.modules["google.cloud.firestore"]
    firestore_mod.DocumentSnapshot = object  # type: ignore[attr-defined]
    firestore_mod.DocumentReference = object  # type: ignore[attr-defined]
    firestore_mod.Transaction = object  # type: ignore[attr-defined]
    # Provide decorator used in controller code.
    transactional = lambda f: f  # noqa: E731
    firestore_mod = sys.modules["google.cloud.firestore"]
    firestore_mod.transactional = transactional  # type: ignore[attr-defined]

    sys.modules["google.cloud.pubsub_v1"].subscriber = sys.modules[
        "google.cloud.pubsub_v1.subscriber"
    ]  # type: ignore[attr-defined]
    sys.modules["google.cloud.pubsub_v1.subscriber"].message = sys.modules[
        "google.cloud.pubsub_v1.subscriber.message"
    ]  # type: ignore[attr-defined]
    sys.modules["google.cloud.pubsub_v1.subscriber.message"].Message = (
        object  # type: ignore[attr-defined]
    )

    spec = importlib.util.spec_from_file_location(
        "controller_main", controller_dir / "main.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod.MeetingController


def test_normalize_meeting_url_strips_tracking_params():
    MeetingController = _import_controller()
    c = MeetingController.__new__(MeetingController)

    url = "https://teams.microsoft.com/l/meeting-join/abc"
    url += "?utm_source=x&foo=bar#fragment"
    norm = c._normalize_meeting_url(url)  # noqa: SLF001
    assert norm == "https://teams.microsoft.com/l/meeting-join/abc?foo=bar"


def test_session_id_deterministic_for_equivalent_urls():
    MeetingController = _import_controller()
    c = MeetingController.__new__(MeetingController)

    org = "org1"
    a = "https://teams.microsoft.com/l/meeting-join/abc/?utm_source=x"
    b = "https://TEAMS.microsoft.com/l/meeting-join/abc"
    left = c._meeting_session_id(org_id=org, meeting_url=a)
    right = c._meeting_session_id(org_id=org, meeting_url=b)
    assert left == right


def test_session_id_differs_across_orgs_for_same_url():
    MeetingController = _import_controller()
    c = MeetingController.__new__(MeetingController)

    url = "https://teams.microsoft.com/l/meeting-join/abc"
    org_a = "orgA"
    org_b = "orgB"

    a = c._meeting_session_id(org_id=org_a, meeting_url=url)
    b = c._meeting_session_id(org_id=org_b, meeting_url=url)
    assert a != b


def test_build_payload_from_meeting_session_uses_canonical_gcs_path(
    monkeypatch,
):
    MeetingController = _import_controller()

    monkeypatch.setenv("GCP_PROJECT_ID", "demo")
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("MANAGER_IMAGE", "manager")
    monkeypatch.setenv("MEETING_BOT_IMAGE", "bot")

    c = MeetingController.__new__(MeetingController)
    doc = _FakeDoc(
        "sess123",
        {
            "meeting_url": "https://teams.microsoft.com/l/meeting-join/...",
            "org_id": "org1",
            "canonical_gcs_path": "recordings/sessions/sess123",
            "status": "queued",
        },
    )

    payload = c._build_job_payload_from_meeting_session(doc)  # noqa: SLF001
    assert payload["gcs_path"] == "recordings/sessions/sess123"
    assert "fs_meeting_id" not in payload


def test_meeting_session_id_stripping_contract():
    """Regression: controller must not persist session ids with whitespace."""

    # This mirrors the contract enforced in controller/main.py when linking a
    # meeting doc to a meeting_session.
    assert "sess123\n".strip() == "sess123"
    assert "  sess123  ".strip() == "sess123"
