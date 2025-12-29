"""Tests for MediaConverter timeout configuration.

These tests do not run ffmpeg. They only verify what timeout values are passed
into subprocess.run based on environment configuration.
"""

from __future__ import annotations

import subprocess

import pytest

from media_converter import MediaConverter


def _fake_completed_process(
    returncode: int = 0,
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["ffmpeg"],
        returncode=returncode,
        stdout="",
        stderr="",
    )


def test_mp4_timeout_is_disabled_by_default(monkeypatch: pytest.MonkeyPatch):
    # Ensure env not set
    monkeypatch.delenv("MEDIA_CONVERTER_MP4_TIMEOUT_SECONDS", raising=False)

    calls = []

    def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        calls.append((cmd, kwargs))
        # First call is `which ffmpeg`
        if cmd[:2] == ["which", "ffmpeg"]:
            return _fake_completed_process(0)
        return _fake_completed_process(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    c = MediaConverter()
    assert c._mp4_timeout_seconds is None  # noqa: SLF001 - asserting config

    assert c._convert_to_mp4("in.webm", "out.mp4") is True  # noqa: SLF001

    convert_call = [c for c in calls if c[0] and c[0][0] == "ffmpeg"][0]
    assert "timeout" in convert_call[1]
    assert convert_call[1]["timeout"] is None


def test_mp4_timeout_can_be_configured(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MEDIA_CONVERTER_MP4_TIMEOUT_SECONDS", "123")

    calls = []

    def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        calls.append((cmd, kwargs))
        if cmd[:2] == ["which", "ffmpeg"]:
            return _fake_completed_process(0)
        return _fake_completed_process(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    c = MediaConverter()
    assert c._mp4_timeout_seconds == 123  # noqa: SLF001

    assert c._convert_to_mp4("in.webm", "out.mp4") is True  # noqa: SLF001

    convert_call = [c for c in calls if c[0] and c[0][0] == "ffmpeg"][0]
    assert convert_call[1]["timeout"] == 123


def test_mp4_timeout_zero_disables_timeout(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MEDIA_CONVERTER_MP4_TIMEOUT_SECONDS", "0")

    calls = []

    def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        calls.append((cmd, kwargs))
        if cmd[:2] == ["which", "ffmpeg"]:
            return _fake_completed_process(0)
        return _fake_completed_process(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    c = MediaConverter()
    assert c._mp4_timeout_seconds is None  # noqa: SLF001


def test_invalid_timeout_uses_default(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MEDIA_CONVERTER_MP4_TIMEOUT_SECONDS", "not-an-int")

    def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[:2] == ["which", "ffmpeg"]:
            return _fake_completed_process(0)
        return _fake_completed_process(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    c = MediaConverter()
    assert c._mp4_timeout_seconds is None  # noqa: SLF001


def test_m4a_timeout_is_disabled_by_default(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("MEDIA_CONVERTER_M4A_TIMEOUT_SECONDS", raising=False)

    calls = []

    def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        calls.append((cmd, kwargs))
        if cmd[:2] == ["which", "ffmpeg"]:
            return _fake_completed_process(0)
        return _fake_completed_process(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    c = MediaConverter()
    assert c._m4a_timeout_seconds is None  # noqa: SLF001

    assert c._extract_m4a("in.webm", "out.m4a") is True  # noqa: SLF001

    extract_call = [c for c in calls if c[0] and c[0][0] == "ffmpeg"][0]
    assert extract_call[1]["timeout"] is None


def test_m4a_timeout_can_be_configured(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MEDIA_CONVERTER_M4A_TIMEOUT_SECONDS", "777")

    calls = []

    def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        calls.append((cmd, kwargs))
        if cmd[:2] == ["which", "ffmpeg"]:
            return _fake_completed_process(0)
        return _fake_completed_process(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    c = MediaConverter()
    assert c._m4a_timeout_seconds == 777  # noqa: SLF001

    assert c._extract_m4a("in.webm", "out.m4a") is True  # noqa: SLF001

    extract_call = [c for c in calls if c[0] and c[0][0] == "ffmpeg"][0]
    assert extract_call[1]["timeout"] == 777
