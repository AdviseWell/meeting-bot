#!/bin/bash
# Run controller locally against REAL GCP Firestore (with DRY_RUN enabled)
# This gives you fast iteration on meeting discovery logic without needing the emulator

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CONTROLLER_DIR="$PROJECT_ROOT/controller"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# GCP project configuration (REAL project, not emulator)
export GCP_PROJECT_ID="${GCP_PROJECT_ID:-aw-development-7226}"
export FIRESTORE_DATABASE="${FIRESTORE_DATABASE:-(default)}"
export GCS_BUCKET="${GCS_BUCKET:-advisewell-firebase-development}"

# Meeting discovery - use collection_group to query across all orgs
export MEETINGS_COLLECTION_PATH="${MEETINGS_COLLECTION_PATH:-meetings}"
export MEETINGS_QUERY_MODE="${MEETINGS_QUERY_MODE:-collection_group}"

# Controller settings
export K8S_NAMESPACE="${K8S_NAMESPACE:-default}"
export POLL_INTERVAL="${POLL_INTERVAL:-10}"
export PYTHONUNBUFFERED="1"

# Disable Pub/Sub subscription (polling only for local testing)
export PUBSUB_SUBSCRIPTION=""

# Enable dry-run mode (CRITICAL - prevents K8s job creation)
export DRY_RUN="${DRY_RUN:-true}"

# Skip leader election for local testing (so we can run while GKE controller is active)
export SKIP_LEADER_ELECTION="${SKIP_LEADER_ELECTION:-true}"

# Use a different health port to avoid conflicts
export HEALTH_PORT="${HEALTH_PORT:-8088}"

# Manager/bot images (for job spec generation, not actually used in dry-run)
export MANAGER_IMAGE="${MANAGER_IMAGE:-meeting-bot-manager:local}"
export MEETING_BOT_IMAGE="${MEETING_BOT_IMAGE:-meeting-bot:local}"

log_info "Starting controller against REAL Firestore..."
log_warn "⚠️  Using real GCP project: $GCP_PROJECT_ID"
log_info "  Firestore DB: $FIRESTORE_DATABASE"
log_info "  GCS Bucket: $GCS_BUCKET"
log_info ""
log_info "  Dry Run:   $DRY_RUN (no K8s jobs will be created)"
log_info "  Poll Int:  ${POLL_INTERVAL}s"
log_info ""

cd "$CONTROLLER_DIR"

# Activate venv if available
if [ -d "$PROJECT_ROOT/.venv" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
elif [ -d "$CONTROLLER_DIR/.venv" ]; then
    source "$CONTROLLER_DIR/.venv/bin/activate"
fi

# Run controller
exec python -u main.py "$@"
