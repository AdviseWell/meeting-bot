from __future__ import annotations

import stat
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, List

import numpy as np
import pytest  # type: ignore


def _install_fake_speechbrain(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install a minimal fake `speechbrain.inference.speaker` module.

    This avoids depending on the real SpeechBrain stack for this unit test.
    The production code only needs
    `EncoderClassifier.from_hparams(...).encode_batch(...)`.
    """

    class _FakeTensor:
        def __init__(self, arr: np.ndarray):
            self._arr = arr

        def squeeze(self, _dim: int) -> "_FakeTensor":
            return self

        def detach(self) -> "_FakeTensor":
            return self

        def cpu(self) -> "_FakeTensor":
            return self

        def numpy(self) -> np.ndarray:
            return self._arr

    class _FakeEncoderClassifier:
        @staticmethod
        def from_hparams(
            source: str, savedir: str, run_opts: Any
        ) -> "_FakeEncoderClassifier":
            # Assert our code is loading from a writable temp copy, not the
            # original.
            assert source == savedir
            assert "/app/tools/" not in source
            assert Path(source).exists()
            return _FakeEncoderClassifier()

        def encode_batch(self, wav_tensor: Any) -> _FakeTensor:
            # Return a stable embedding vector.
            emb = np.zeros((1, 1, 192), dtype=np.float32)
            return _FakeTensor(emb)

    speechbrain = ModuleType("speechbrain")
    inference = ModuleType("speechbrain.inference")
    speaker = ModuleType("speechbrain.inference.speaker")
    setattr(
        speaker,
        "EncoderClassifier",
        _FakeEncoderClassifier,
    )  # type: ignore[attr-defined]

    setattr(inference, "speaker", speaker)  # type: ignore[attr-defined]
    setattr(speechbrain, "inference", inference)  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "speechbrain", speechbrain)
    monkeypatch.setitem(sys.modules, "speechbrain.inference", inference)
    monkeypatch.setitem(sys.modules, "speechbrain.inference.speaker", speaker)


def _install_fake_torch(monkeypatch: pytest.MonkeyPatch) -> None:
    class _InferenceMode:
        def __enter__(self):
            return None

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeTorch(ModuleType):
        def __init__(self):
            super().__init__("torch")

        def from_numpy(self, arr: np.ndarray):
            return arr

        def inference_mode(self):
            return _InferenceMode()

    monkeypatch.setitem(sys.modules, "torch", _FakeTorch())


def test_diarization_does_not_write_to_baked_model_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Regression: diarization must work when baked model dir is read-only.

    Previously, `_patch_speechbrain_hyperparams_for_local_model()` tried to
    write to `<model_dir>/hyperparams.yaml` directly, which fails in non-root
    containers when the image is read-only.
    """

    _install_fake_speechbrain(monkeypatch)
    _install_fake_torch(monkeypatch)

    # Import after fakes are installed.
    from offline_pipeline import Segment, diarize_segments_offline

    # Create a fake model directory with required files.
    model_dir = tmp_path / "spkrec-ecapa-voxceleb"
    model_dir.mkdir(parents=True)

    hp = model_dir / "hyperparams.yaml"
    hp.write_text(
        "pretrained_path: speechbrain/spkrec-ecapa-voxceleb\n",
        encoding="utf-8",
    )
    for name in [
        "embedding_model.ckpt",
        "mean_var_norm_emb.ckpt",
        "classifier.ckpt",
        "label_encoder.txt",
    ]:
        (model_dir / name).write_text("x", encoding="utf-8")

    # Make the hyperparams file read-only to simulate baked assets in a
    # non-root container.
    hp.chmod(stat.S_IREAD)

    # Avoid running ffmpeg to extract windows.
    monkeypatch.setattr(
        "offline_pipeline._extract_wav_window",
        lambda src_wav, start, end: tmp_path / "dummy.wav",
    )
    monkeypatch.setattr(
        "offline_pipeline.subprocess.check_output",
        lambda cmd: (np.zeros(16000, dtype=np.int16)).tobytes(),
    )

    # Force deterministic clustering.
    monkeypatch.setattr(
        "offline_pipeline._cluster_speakers",
        lambda embeddings, max_speakers: (2, [0, 1]),
    )

    # Two windows required, so create segments that will create >=2 windows.
    segments: List[Segment] = [
        Segment(start=0.0, end=3.0, text="hello"),
        Segment(start=3.1, end=6.2, text="world"),
    ]

    # Wav path isn't actually read due to mocked check_output.
    wav_path = tmp_path / "audio.wav"
    wav_path.write_bytes(b"RIFF")

    detected, out_segments = diarize_segments_offline(
        wav_path=wav_path,
        segments=segments,
        max_speakers=6,
        model_dir=model_dir,
    )

    # We mainly care that diarization ran and produced non-null speaker labels
    # without attempting to write into the baked model directory.
    assert detected >= 1
    assert out_segments[0].speaker is not None
    assert out_segments[1].speaker is not None

    # Ensure we didn't manage to modify the original baked hyperparams file.
    assert "speechbrain/spkrec-ecapa-voxceleb" in hp.read_text(encoding="utf-8")
