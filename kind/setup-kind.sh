#!/bin/bash
# Setup KinD cluster for meeting-bot local development
# Connects to Firebase emulator running in Docker

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CLUSTER_NAME="meeting-bot-dev"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v kind &> /dev/null; then
        log_error "kind is not installed. Install with: brew install kind"
        exit 1
    fi
    
    if ! command -v docker &> /dev/null; then
        log_error "docker is not installed"
        exit 1
    fi
    
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed"
        exit 1
    fi
    
    # Check if Firebase emulator is running
    if ! docker ps | grep -q "advisewell-firebase-emulator"; then
        log_warn "Firebase emulator container not found. Make sure it's running!"
        log_warn "Expected container: advisewell-firebase-emulator-1"
    else
        log_info "Firebase emulator is running ✓"
    fi
}

# Get host IP that's reachable from Docker containers
get_host_ip() {
    # On Linux, use the docker0 bridge IP or host.docker.internal
    if [[ "$(uname)" == "Linux" ]]; then
        # Try to get docker bridge IP
        DOCKER_HOST_IP=$(ip -4 addr show docker0 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -1)
        if [[ -z "$DOCKER_HOST_IP" ]]; then
            # Fallback to default gateway
            DOCKER_HOST_IP=$(ip route | grep default | awk '{print $3}')
        fi
    else
        # On Mac/Windows, host.docker.internal works
        DOCKER_HOST_IP="host.docker.internal"
    fi
    echo "$DOCKER_HOST_IP"
}

# Create or recreate the KinD cluster
create_cluster() {
    log_info "Creating KinD cluster: $CLUSTER_NAME"
    
    # Delete existing cluster if it exists
    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        log_warn "Cluster $CLUSTER_NAME already exists. Deleting..."
        kind delete cluster --name "$CLUSTER_NAME"
    fi
    
    # Create new cluster
    kind create cluster --config "$SCRIPT_DIR/kind-config.yaml"
    
    # Wait for cluster to be ready
    log_info "Waiting for cluster to be ready..."
    kubectl wait --for=condition=Ready nodes --all --timeout=120s
    
    log_info "KinD cluster created successfully ✓"
}

# Build and load Docker images into KinD
build_and_load_images() {
    log_info "Building Docker images..."
    
    cd "$PROJECT_ROOT"
    
    # Build controller image
    log_info "Building controller image..."
    docker build -t meeting-bot-controller:local -f Dockerfile.controller .
    
    # Build manager image
    log_info "Building manager image..."
    docker build -t meeting-bot-manager:local -f Dockerfile.manager .
    
    # Build meeting-bot (NodeJS) image
    log_info "Building meeting-bot image..."
    docker build -t meeting-bot:local -f Dockerfile.development .
    
    # Load images into KinD
    log_info "Loading images into KinD cluster..."
    kind load docker-image meeting-bot-controller:local --name "$CLUSTER_NAME"
    kind load docker-image meeting-bot-manager:local --name "$CLUSTER_NAME"
    kind load docker-image meeting-bot:local --name "$CLUSTER_NAME"
    
    log_info "Images loaded into KinD ✓"
}

# Update ConfigMap with correct host IP
update_configmap() {
    HOST_IP=$(get_host_ip)
    log_info "Host IP for Firebase emulator: $HOST_IP"
    
    # Create a temporary file with updated ConfigMap
    sed "s/host.docker.internal/$HOST_IP/g" "$SCRIPT_DIR/controller-deployment.yaml" > /tmp/controller-deployment-updated.yaml
    
    # On Linux, we might need extra_hosts or hostAliases approach
    if [[ "$(uname)" == "Linux" ]]; then
        log_info "Linux detected - configuring host access..."
        # Update hostAliases to use actual IP
        sed -i "s/host-gateway/$HOST_IP/g" /tmp/controller-deployment-updated.yaml
    fi
}

# Deploy controller to KinD
deploy_controller() {
    log_info "Deploying controller to KinD..."
    
    update_configmap
    
    kubectl apply -f /tmp/controller-deployment-updated.yaml
    
    # Wait for deployment to be ready
    log_info "Waiting for controller deployment..."
    kubectl wait --for=condition=Available deployment/controller -n meeting-bot --timeout=120s || true
    
    log_info "Controller deployed ✓"
}

# Show status and next steps
show_status() {
    echo ""
    echo "========================================"
    echo "  KinD Meeting Bot Dev Environment"
    echo "========================================"
    echo ""
    log_info "Cluster: $CLUSTER_NAME"
    log_info "Namespace: meeting-bot"
    echo ""
    
    echo "Firebase Emulator Endpoints (from inside cluster):"
    HOST_IP=$(get_host_ip)
    echo "  Firestore:  $HOST_IP:8080"
    echo "  Pub/Sub:    $HOST_IP:8085"
    echo "  Auth:       $HOST_IP:9099"
    echo "  UI:         http://localhost:4000"
    echo ""
    
    echo "Useful commands:"
    echo "  # View controller logs"
    echo "  kubectl logs -f deployment/controller -n meeting-bot"
    echo ""
    echo "  # Restart controller after code changes"
    echo "  $SCRIPT_DIR/reload-controller.sh"
    echo ""
    echo "  # Delete the cluster"
    echo "  kind delete cluster --name $CLUSTER_NAME"
    echo ""
    echo "  # Port forward to access controller directly"
    echo "  kubectl port-forward deployment/controller -n meeting-bot 8000:8000"
    echo ""
}

# Main
main() {
    echo "========================================"
    echo "  Setting up KinD for Meeting Bot"
    echo "========================================"
    
    check_prerequisites
    create_cluster
    build_and_load_images
    deploy_controller
    show_status
}

# Handle arguments
case "${1:-}" in
    --images-only)
        build_and_load_images
        ;;
    --deploy-only)
        deploy_controller
        ;;
    --status)
        show_status
        ;;
    *)
        main
        ;;
esac
