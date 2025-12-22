"""Container-only Python startup hook.

Python automatically imports `sitecustomize` on startup (if present on
`sys.path`). We use this to patch torchaudio API removals that some versions of
SpeechBrain still rely on at import time.

This keeps offline diarization working inside the manager Docker image.
"""

from __future__ import annotations


def _patch_torchaudio() -> None:
    try:
        import torchaudio  # type: ignore

        # Torchaudio 2.9 wheels may not expose backend helpers at all.
        if not hasattr(torchaudio, "list_audio_backends"):
            torchaudio.list_audio_backends = lambda: []  # type: ignore[attr-defined]
        if not hasattr(torchaudio, "get_audio_backend"):
            torchaudio.get_audio_backend = lambda: None  # type: ignore[attr-defined]
        if not hasattr(torchaudio, "set_audio_backend"):
            torchaudio.set_audio_backend = (  # type: ignore[attr-defined]
                lambda _backend=None: None
            )
    except Exception:
        # If torchaudio isn't installed, do nothing.
        pass


_patch_torchaudio()
