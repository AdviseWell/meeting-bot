#!/usr/bin/env bash
set -euo pipefail

# Repo-local whisper.cpp setup for offline CPU transcription.
# This script clones whisper.cpp into ./tools/whisper.cpp, builds it,
# and downloads a ggml model into ./tools/whisper.cpp/models.
#
# Requirements:
# - git
# - build tools (make, clang/gcc)
# - curl

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WHISPER_DIR="$ROOT_DIR/tools/whisper.cpp"
MODEL_NAME="${1:-ggml-base.en.bin}"
MODEL_DIR="$WHISPER_DIR/models"
MODEL_PATH="$MODEL_DIR/$MODEL_NAME"

mkdir -p "$ROOT_DIR/tools"

if [[ ! -d "$WHISPER_DIR/.git" ]]; then
  echo "Cloning whisper.cpp into $WHISPER_DIR"
  git clone https://github.com/ggerganov/whisper.cpp.git "$WHISPER_DIR"
else
  echo "whisper.cpp already cloned at $WHISPER_DIR"
fi

echo "Building whisper.cpp..."
cd "$WHISPER_DIR"
make -j"$(nproc)"

mkdir -p "$MODEL_DIR"
if [[ ! -f "$MODEL_PATH" ]]; then
  echo "Downloading model ${MODEL_NAME}..."
  # Newer whisper.cpp uses model names like "base.en" (not "ggml-base.en").
  ./models/download-ggml-model.sh base.en || true
fi

if [[ ! -f "$MODEL_PATH" ]]; then
  echo "Model download didn't produce $MODEL_PATH"
  echo "Available models in $MODEL_DIR:"
  ls -la "$MODEL_DIR" || true
  echo "You can also run:"
  echo "  cd $WHISPER_DIR && ./models/download-ggml-model.sh base.en"
  exit 1
fi

echo "Done."
echo "whisper binary: $WHISPER_DIR/main"
echo "model:          $MODEL_PATH"
