#!/usr/bin/env python3
"""One-off smoke test to ensure SpeechBrain imports under torchaudio 2.9+.

We patch torchaudio's removed backend APIs BEFORE importing speechbrain.
"""

from __future__ import annotations


def _ensure_torchaudio_compat() -> None:
    try:
        import torchaudio  # type: ignore

        if not hasattr(torchaudio, "list_audio_backends"):
            torchaudio.list_audio_backends = lambda: []  # type: ignore[attr-defined]
        if not hasattr(torchaudio, "get_audio_backend"):
            torchaudio.get_audio_backend = lambda: None  # type: ignore[attr-defined]
        if not hasattr(torchaudio, "set_audio_backend"):
            torchaudio.set_audio_backend = (  # type: ignore[attr-defined]
                lambda _backend=None: None
            )
    except Exception:
        pass


def main() -> int:
    _ensure_torchaudio_compat()

    import torchaudio  # type: ignore

    assert hasattr(torchaudio, "list_audio_backends")

    import speechbrain  # noqa: F401

    print("SpeechBrain import OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
