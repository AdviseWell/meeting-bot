from __future__ import annotations


def _import_meeting_manager():
    """Import MeetingManager with google cloud deps stubbed.

    The test suite environment may not have compatible OpenSSL bindings for
    requests/pyopenssl used transitively by google libs.
    """

    import sys
    import types
    from pathlib import Path
    import importlib.util

    sys.modules.setdefault("google", types.ModuleType("google"))
    sys.modules.setdefault("google.genai", types.ModuleType("google.genai"))
    sys.modules.setdefault("google.cloud", types.ModuleType("google.cloud"))
    sys.modules.setdefault(
        "google.cloud.firestore",
        types.ModuleType("google.cloud.firestore"),
    )
    sys.modules.setdefault(
        "google.cloud.storage",
        types.ModuleType("google.cloud.storage"),
    )

    # Ensure `from google.cloud import storage, firestore` works.
    cloud = sys.modules["google.cloud"]
    firestore_mod = sys.modules["google.cloud.firestore"]
    storage_mod = sys.modules["google.cloud.storage"]
    cloud.firestore = firestore_mod  # type: ignore[attr-defined]
    cloud.storage = storage_mod  # type: ignore[attr-defined]

    # Ensure `from google import genai` works.
    google_root = sys.modules["google"]
    genai_mod = sys.modules["google.genai"]
    google_root.genai = genai_mod  # type: ignore[attr-defined]

    manager_dir = Path(__file__).resolve().parent
    spec = importlib.util.spec_from_file_location(
        "manager_main", manager_dir / "main.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod.MeetingManager


def test_manager_refuses_blank_user_id(monkeypatch):
    MeetingManager = _import_meeting_manager()

    monkeypatch.setenv("MEETING_URL", "https://meet.google.com/abc-defg-hij")
    monkeypatch.setenv("MEETING_ID", "m1")
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("GCS_PATH", "recordings/someone/someMeeting")
    monkeypatch.setenv("USER_ID", "   ")

    # Prevent google.cloud import side effects in this unit test.
    monkeypatch.setenv("FIRESTORE_DATABASE", "(default)")
    monkeypatch.setenv("MEETING_BOT_API_URL", "http://example")

    try:
        MeetingManager()
        assert False, "Expected ValueError for blank USER_ID"
    except ValueError as e:
        assert "Missing USER_ID" in str(e)


def test_manager_allows_session_canonical_path_without_user_id(monkeypatch):
    MeetingManager = _import_meeting_manager()

    monkeypatch.setenv("MEETING_URL", "https://meet.google.com/abc-defg-hij")
    monkeypatch.setenv("MEETING_ID", "m1")
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("GCS_PATH", "recordings/sessions/sess123")

    # USER_ID can be absent for canonical session runs.
    monkeypatch.delenv("USER_ID", raising=False)

    # Firestore / API envs needed for validation.
    monkeypatch.setenv("FIRESTORE_DATABASE", "(default)")
    monkeypatch.setenv("MEETING_BOT_API_URL", "http://example")

    # We don't want to fully initialize cloud clients in this unit test.
    # Instead, validate we get past the USER_ID check by short-circuiting
    # client initialization.
    orig_validate = MeetingManager._validate_config
    try:
        MeetingManager._validate_config = lambda self: None
        m = MeetingManager()
        assert m.gcs_path.startswith("recordings/sessions/")
    finally:
        MeetingManager._validate_config = orig_validate
