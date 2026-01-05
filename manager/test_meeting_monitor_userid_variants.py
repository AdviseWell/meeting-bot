from __future__ import annotations

import os


def test_monitor_until_complete_accepts_userid_variants(monkeypatch):
    """Regression test: user id may be passed as USER_ID/user_id.

    Not all callers consistently use the `userId` casing.
    """

    # Import inside the test so monkeypatching env/fs is easier.
    from meeting_monitor import MeetingMonitor

    mm = MeetingMonitor(api_base_url="http://example")

    # Force job to be immediately completed.
    monkeypatch.setattr(
        mm, "get_job_status", lambda job_id: {"status": "completed"}
    )

    # Pretend meeting-bot already produced a recording.
    monkeypatch.setattr(os.path, "exists", lambda p: True)

    def fake_glob(pattern):
        # Should be called with a path that includes our user id.
        assert (
            "/recordings/u123" in pattern
            or "/scratch/tempvideo/u123" in pattern
        )
        return ["/recordings/u123/recording.webm"]

    import glob as _glob

    monkeypatch.setattr(_glob, "glob", fake_glob)
    monkeypatch.setattr(os.path, "isfile", lambda p: True)
    monkeypatch.setattr(os.path, "getsize", lambda p: 1024)

    # Use USER_ID variant rather than userId.
    path = mm.monitor_until_complete(
        job_id="job",
        metadata={"USER_ID": "u123"},
        check_interval=0,
        max_wait_time=1,
    )
    assert path == "/recordings/u123/recording.webm"


def test_monitor_until_complete_rejects_blank_userid(monkeypatch):
    from meeting_monitor import MeetingMonitor

    mm = MeetingMonitor(api_base_url="http://example")

    # Force job to be immediately completed.
    monkeypatch.setattr(
        mm, "get_job_status", lambda job_id: {"status": "completed"}
    )

    # If the code mistakenly proceeds, we'd see os.path.exists/glob called.
    called = {"exists": 0, "glob": 0}

    monkeypatch.setattr(
        os.path,
        "exists",
        lambda p: called.__setitem__("exists", called["exists"] + 1) or True,
    )

    import glob as _glob

    monkeypatch.setattr(
        _glob,
        "glob",
        lambda pattern: called.__setitem__("glob", called["glob"] + 1) or [],
    )

    path = mm.monitor_until_complete(
        job_id="job",
        metadata={"userId": "   ", "user_id": ""},
        check_interval=0,
        max_wait_time=1,
    )
    assert path is None
    assert called["exists"] == 0
    assert called["glob"] == 0
