"""Offline transcription + diarization pipeline used by the manager.

This module is the production-friendly counterpart to
`scripts/whisper_diarize.py`.

Contract
- Input: local media file path (webm/mp4/m4a/wav/etc)
- Output: ``(txt_path, json_path)`` containing speaker-labelled segments when
    diarization is enabled.

Notes
- whisper.cpp binary and model are expected to exist in the container.
- SpeechBrain model directory is expected to be available locally to keep this
    fully offline.
"""

# pyright: reportMissingTypeStubs=false

# type: ignore

from __future__ import annotations

import datetime as _datetime
import json
import logging
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

UTC = getattr(_datetime, "UTC", timezone.utc)
SPEAKER_LABELS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


# When running in-repo, this file lives at "manager/offline_pipeline.py" and
# repo root is parents[1]. In the Docker image, we COPY manager/ into /app, so
# this file becomes "/app/offline_pipeline.py" and repo root is the file's
# parent directory.
_this_file = Path(__file__).resolve()
REPO_ROOT = (
    _this_file.parents[1] if _this_file.parent.name == "manager" else _this_file.parent
)
DEFAULT_WHISPER_DIR = REPO_ROOT / "tools" / "whisper.cpp"
DEFAULT_WHISPER_BIN = DEFAULT_WHISPER_DIR / "build" / "bin" / "whisper-cli"
DEFAULT_WHISPER_MODEL = DEFAULT_WHISPER_DIR / "models" / "ggml-base.en.bin"
DEFAULT_SPEECHBRAIN_MODEL_DIR = (
    REPO_ROOT / "tools" / "speechbrain" / "spkrec-ecapa-voxceleb"
)


def _ensure_torchaudio_compat() -> None:
    """Ensure torchaudio exposes legacy backend APIs SpeechBrain expects."""

    try:  # pragma: no cover
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


@dataclass
class Segment:
    start: float
    end: float
    text: str
    speaker: Optional[str] = None


def _run(cmd: List[str]) -> None:
    logger.debug("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def _which(binary: str) -> Optional[str]:
    return shutil.which(binary)


def ffmpeg_to_wav16k_mono(src: Path, dst: Path) -> None:
    if not _which("ffmpeg"):
        raise RuntimeError("ffmpeg is required but not found on PATH")

    dst.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(src),
        "-ac",
        "1",
        "-ar",
        "16000",
        "-vn",
        str(dst),
    ]
    try:
        _run(cmd)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "ffmpeg failed converting input to WAV (is "
            f"'{src}' an audio/video file with an audio stream?)"
        ) from e


def _parse_whisper_srt(path: Path) -> List[Segment]:
    def _ts_to_seconds(ts: str) -> float:
        hh, mm, rest = ts.split(":")
        ss, ms = rest.split(",")
        return int(hh) * 3600 + int(mm) * 60 + int(ss) + int(ms) / 1000.0

    segments: List[Segment] = []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    i = 0
    while i < len(lines):
        while i < len(lines) and not lines[i].strip():
            i += 1
        if i >= len(lines):
            break
        if lines[i].strip().isdigit():
            i += 1
        if i >= len(lines):
            break
        if "-->" not in lines[i]:
            i += 1
            continue

        start_s, end_s = [p.strip() for p in lines[i].split("-->")]
        i += 1
        text_lines = []
        while i < len(lines) and lines[i].strip():
            text_lines.append(lines[i].strip())
            i += 1

        text = " ".join(text_lines).strip()
        if text:
            segments.append(
                Segment(
                    start=_ts_to_seconds(start_s),
                    end=_ts_to_seconds(end_s),
                    text=text,
                )
            )

    return segments


def run_whisper_cpp(
    *,
    whisper_bin: Path,
    model_path: Path,
    wav_path: Path,
    out_dir: Path,
    language: str,
) -> Tuple[Path, List[Segment]]:
    out_dir.mkdir(parents=True, exist_ok=True)
    base = wav_path.stem
    out_prefix = out_dir / base

    cmd = [
        str(whisper_bin),
        "-m",
        str(model_path),
        "-f",
        str(wav_path),
        "-l",
        language,
        "-osrt",
        "-of",
        str(out_prefix),
    ]
    _run(cmd)

    srt_path = out_dir / f"{base}.srt"
    if not srt_path.exists():
        raise RuntimeError(f"Expected whisper.cpp SRT not found: {srt_path}")

    return srt_path, _parse_whisper_srt(srt_path)


def _merge_segments_for_diarization(
    segments: List[Segment],
    min_seconds: float = 2.5,
    max_seconds: float = 12.0,
    max_gap_seconds: float = 0.8,
) -> List[Tuple[float, float, List[int]]]:
    if not segments:
        return []

    windows: List[Tuple[float, float, List[int]]] = []
    cur_start = segments[0].start
    cur_end = segments[0].end
    cur_idxs: List[int] = [0]

    for idx in range(1, len(segments)):
        s = segments[idx]
        gap = max(0.0, s.start - cur_end)
        proposed_end = max(cur_end, s.end)
        proposed_dur = proposed_end - cur_start

        should_break = False
        if gap > max_gap_seconds:
            should_break = True
        elif proposed_dur > max_seconds and ((cur_end - cur_start) >= min_seconds):
            should_break = True

        if should_break:
            windows.append((cur_start, cur_end, cur_idxs))
            cur_start, cur_end, cur_idxs = s.start, s.end, [idx]
        else:
            cur_end = proposed_end
            cur_idxs.append(idx)

    windows.append((cur_start, cur_end, cur_idxs))

    merged: List[Tuple[float, float, List[int]]] = []
    for w in windows:
        if merged and (w[1] - w[0]) < min_seconds:
            ps, _, pidxs = merged[-1]
            merged[-1] = (ps, w[1], pidxs + w[2])
        else:
            merged.append(w)

    return merged


def _extract_wav_window(src_wav: Path, start: float, end: float) -> Path:
    if end <= start:
        raise ValueError("Invalid window")

    out_path = Path(tempfile.mkstemp(prefix="spkwin_", suffix=".wav")[1])
    cmd = [
        "ffmpeg",
        "-y",
        "-ss",
        f"{start:.3f}",
        "-to",
        f"{end:.3f}",
        "-i",
        str(src_wav),
        "-ac",
        "1",
        "-ar",
        "16000",
        str(out_path),
    ]
    _run(cmd)
    return out_path


def _cluster_speakers(embeddings, max_speakers: int) -> Tuple[int, List[int]]:
    # scikit-learn has no typing stubs.
    from sklearn.cluster import AgglomerativeClustering  # type: ignore
    from sklearn.metrics import silhouette_score  # type: ignore
    import numpy as np

    X = np.asarray(embeddings, dtype=np.float32)
    if X.ndim != 2 or X.shape[0] < 2:
        return 1, [0] * int(X.shape[0])

    best_k = 1
    best_score = -1.0
    best_labels = [0] * X.shape[0]

    upper = min(max_speakers, X.shape[0])
    for k in range(2, upper + 1):
        try:
            clustering = AgglomerativeClustering(n_clusters=k)
            labels = clustering.fit_predict(X)
            score = silhouette_score(X, labels)
            if score > best_score:
                best_score = float(score)
                best_k = k
                best_labels = labels.tolist()
        except Exception:
            continue

    if best_k == 1:
        return 1, [0] * X.shape[0]

    return best_k, best_labels


def diarize_segments_offline(
    *,
    wav_path: Path,
    segments: List[Segment],
    max_speakers: int,
    model_dir: Path,
) -> Tuple[int, List[Segment]]:
    import numpy as np
    import torch

    device = "cpu"

    _ensure_torchaudio_compat()

    try:
        from speechbrain.inference.speaker import (  # type: ignore
            EncoderClassifier,
        )
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Offline diarization requires SpeechBrain + PyTorch. "
            "Install offline diarization deps."
        ) from e

    if not segments:
        return 0, segments

    if not model_dir.exists():
        raise RuntimeError(
            "Missing local diarization modeldir. Expected: "
            f"{model_dir}.\n"
            "Bake the SpeechBrain ECAPA model artifacts into the image at "
            "that path."
        )

    windows = _merge_segments_for_diarization(segments)
    if len(windows) < 2:
        for s in segments:
            s.speaker = "Speaker A"
        return 1, segments

    logger.info(
        "Diarization: computing embeddings for %d windows",
        len(windows),
    )

    classifier: Any = EncoderClassifier.from_hparams(
        source=str(model_dir),
        savedir=str(model_dir),
        run_opts={"device": device},
    )

    embeddings: List[List[float]] = []
    win_tmp_files: List[Path] = []
    try:
        for ws, we, _ in windows:
            wpath = _extract_wav_window(wav_path, ws, we)
            win_tmp_files.append(wpath)

            cmd = [
                "ffmpeg",
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                str(wpath),
                "-f",
                "s16le",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-",
            ]
            raw = subprocess.check_output(cmd)
            audio_i16 = np.frombuffer(raw, dtype=np.int16)
            audio_f32 = (audio_i16.astype(np.float32) / 32768.0).reshape(1, -1)
            wav = torch.from_numpy(audio_f32)
            with torch.inference_mode():
                emb_tensor = classifier.encode_batch(wav)
            emb_np = (
                (emb_tensor.squeeze(0).squeeze(0).detach().cpu().numpy())
                .astype(np.float32)
                .reshape(-1)
            )
            embeddings.append(emb_np.tolist())
    finally:
        for p in win_tmp_files:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass

    k, labels = _cluster_speakers(embeddings, max_speakers=max_speakers)
    logger.info("Diarization: selected %d speakers", k)

    label_map: Dict[int, str] = {}
    next_label_idx = 0
    for lab in labels:
        if lab not in label_map:
            label_map[lab] = f"Speaker {SPEAKER_LABELS[next_label_idx]}"
            next_label_idx += 1

    for w, lab in zip(windows, labels):
        speaker = label_map.get(lab, "Speaker A")
        for idx in w[2]:
            segments[idx].speaker = speaker

    return k, segments


def resolve_whisper_paths(
    *,
    whisper_bin: Optional[str] = None,
    model_path: Optional[str] = None,
) -> Tuple[Path, Path]:
    # IMPORTANT: Path("") becomes Path(".") which is truthy as a string.
    # Treat empty / unset env the same as "not provided".
    env_bin = os.getenv("WHISPER_CPP_BIN")
    bin_path = Path(whisper_bin) if whisper_bin else Path(env_bin or "")
    if bin_path in (Path("."), Path("")):
        bin_path = DEFAULT_WHISPER_BIN

    if not bin_path.is_file():
        resolved = _which(str(bin_path)) if str(bin_path) else None
        if resolved:
            bin_path = Path(resolved)

    env_model = os.getenv("WHISPER_CPP_MODEL")
    model = Path(model_path) if model_path else Path(env_model or "")
    if model in (Path("."), Path("")):
        model = DEFAULT_WHISPER_MODEL

    if not bin_path.is_file():
        raise RuntimeError(
            "whisper.cpp binary not found. Provide WHISPER_CPP_BIN or bake it "
            f"into the image. Tried: {bin_path}"
        )

    if not model.exists():
        raise RuntimeError(
            "whisper.cpp model not found. Provide WHISPER_CPP_MODEL or bake "
            f"it into the image. Tried: {model}"
        )

    return bin_path, model


def transcribe_and_diarize_local_media(
    *,
    input_path: Path,
    out_dir: Path,
    meeting_id: str,
    language: str = "en",
    diarize: bool = True,
    max_speakers: int = 6,
    whisper_bin: Optional[str] = None,
    whisper_model: Optional[str] = None,
    speechbrain_model_dir: Optional[str] = None,
) -> Tuple[Path, Path]:
    """Run whisper.cpp + (optional) diarization on a local file."""

    if not input_path.exists():
        raise FileNotFoundError(str(input_path))

    out_dir.mkdir(parents=True, exist_ok=True)

    whisper_bin_path, whisper_model_path = resolve_whisper_paths(
        whisper_bin=whisper_bin, model_path=whisper_model
    )

    sb_model_dir_str: str = (
        speechbrain_model_dir
        or os.getenv("SPEECHBRAIN_MODEL_DIR")
        or str(DEFAULT_SPEECHBRAIN_MODEL_DIR)
    )
    sb_model_dir = Path(sb_model_dir_str)

    tmp_root = Path(tempfile.mkdtemp(prefix="offline_transcribe_"))
    try:
        wav_path = tmp_root / "audio_16k_mono.wav"
        ffmpeg_to_wav16k_mono(input_path, wav_path)

        whisper_out_dir = tmp_root / "whisper"
        _, segments = run_whisper_cpp(
            whisper_bin=whisper_bin_path,
            model_path=whisper_model_path,
            wav_path=wav_path,
            out_dir=whisper_out_dir,
            language=language,
        )

        diarization_info: Dict[str, Any] = {
            "enabled": bool(diarize),
            "max_speakers": max_speakers,
            "detected_speakers": None,
        }

        if diarize:
            detected, segments = diarize_segments_offline(
                wav_path=wav_path,
                segments=segments,
                max_speakers=max_speakers,
                model_dir=sb_model_dir,
            )
            diarization_info["detected_speakers"] = detected

        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        base_name = f"{meeting_id}_{timestamp}_whispercpp"

        txt_path = out_dir / f"{base_name}.txt"
        json_path = out_dir / f"{base_name}.json"

        transcript_lines: List[str] = []
        for s in segments:
            speaker_prefix = f"{s.speaker}: " if s.speaker else ""
            transcript_lines.append(
                f"[{s.start:0.2f}-{s.end:0.2f}] {speaker_prefix}{s.text}"
            )
        txt_path.write_text("\n".join(transcript_lines), encoding="utf-8")

        payload: Dict[str, Any] = {
            "engine": "whisper.cpp",
            "model": str(whisper_model_path),
            "language": language,
            "diarization": diarization_info,
            "segments": [
                {
                    "start": s.start,
                    "end": s.end,
                    "speaker": s.speaker,
                    "text": s.text,
                }
                for s in segments
            ],
            "transcript": " ".join([s.text for s in segments]).strip(),
        }
        json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        return txt_path, json_path
    finally:
        # Keep temp dir for debugging when needed.
        if os.getenv("OFFLINE_PIPELINE_KEEP_TMP"):
            logger.info("Keeping temp dir: %s", tmp_root)
        else:
            shutil.rmtree(tmp_root, ignore_errors=True)
