import os
from pathlib import Path


def test_speechbrain_assets_present_or_skip() -> None:
    """Ensure offline diarization assets are fully present if provided.

    This repository deliberately does not commit model weights. In production
    images, these files should be prebaked into:
      /app/tools/speechbrain/spkrec-ecapa-voxceleb/

    If classifier.ckpt is missing, diarization should be disabled or the
    image build should provide it.
    """

    model_dir = Path(
        os.getenv(
            "SPEECHBRAIN_MODEL_DIR",
            "/app/tools/speechbrain/spkrec-ecapa-voxceleb",
        )
    )

    # These files are expected by our packaged hyperparams.yaml.
    expected = [
        model_dir / "hyperparams.yaml",
        model_dir / "embedding_model.ckpt",
        model_dir / "mean_var_norm_emb.ckpt",
        model_dir / "classifier.ckpt",
        # Required by the SpeechBrain pretrained interface for this model.
        model_dir / "label_encoder.txt",
    ]

    missing = [p.name for p in expected if not p.exists()]

    # If nothing is present, that's fine for dev builds. If some are present
    # but classifier.ckpt is missing, diarization will likely attempt a HF
    # download; we treat that as a configuration error.
    if len(missing) == len(expected):
        return

    assert not missing, f"Missing diarization assets: {missing}"
