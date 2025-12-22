# SpeechBrain diarization model cache

This folder is intentionally **not** populated in git.

`scripts/whisper_diarize.py` can run fully offline diarization if the SpeechBrain
ECAPA model files are present here:

- `hyperparams.yaml`
- `embedding_model.ckpt`
- (optional) `mean_var_norm_emb.ckpt`

## How this is used

The Docker build for `Dockerfile.manager` copies this directory into the image at:

- `/app/tools/speechbrain/spkrec-ecapa-voxceleb/`

so you can prebake the model into the image by providing these files in the build
context (for example via CI artifacts or a private repo layer).

## Notes

- Do **not** commit model weights to this repo unless you are sure licensing and
  repo policy allow it.
- If you don't provide the model files, diarization should be run with
  `--no-diarize`.
