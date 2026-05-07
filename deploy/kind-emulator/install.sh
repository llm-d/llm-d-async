#!/usr/bin/env bash
#
# Async-Processor KIND Emulator Deployment Script
# Automated deployment of AP with llm-d infrastructure on Kind cluster with llm-d-inference-sim simulator
#
# Prerequisites:
# - kubectl installed and configured
# - helm installed
# - kind installed (for cluster creation)
# - Docker installed and running
#

set -e  # Exit on error
set -o pipefail  # Exit on pipe failure

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
AP_PROJECT=${AP_PROJECT:-$PWD}
WELL_LIT_PATH_NAME="simulated-accelerators"
NAMESPACE_SUFFIX="sim"
EXAMPLE_DIR="$AP_PROJECT/$LLM_D_PROJECT/guides/$WELL_LIT_PATH_NAME"
DEPLOY_LLM_D_INFERENCE_SIM=true

# Namespaces
LLMD_NS="llm-d-$NAMESPACE_SUFFIX"
MONITORING_NAMESPACE="async-processor-monitoring"
AP_NS=${AP_NS:-"async-processor-system"}

# AP Configuration
SKIP_TLS_VERIFY=true  # Skip TLS verification in emulated environments
AP_LOG_LEVEL="debug" # AP log level set to debug for emulated environments

# llm-d Configuration
LLM_D_INFERENCE_SIM_IMG_REPO=${LLM_D_INFERENCE_SIM_IMG_REPO:-"ghcr.io/llm-d/llm-d-inference-sim"}
LLM_D_INFERENCE_SIM_IMG_TAG=${LLM_D_INFERENCE_SIM_IMG_TAG:-"latest"}
LLM_D_MODELSERVICE_NAME="ms-$NAMESPACE_SUFFIX-llm-d-modelservice"
LLM_D_MODELSERVICE_VALUES="ms-$NAMESPACE_SUFFIX/values.yaml"

# Model and SLO Configuration
MODEL_ID=${MODEL_ID:-"unsloth/Meta-Llama-3.1-8B"}
DEFAULT_MODEL_ID="random"
ACCELERATOR_TYPE="A100"
SLO_TPOT=24     # Target time-per-output-token SLO (in ms)
SLO_TTFT=500  # Target time-to-first-token SLO (in ms)

# Gateway Configuration
BENCHMARK_MODE="false"  # if true, deploys gateway in benchmark mode - unavailable for simulated deployments
INSTALL_GATEWAY_CTRLPLANE="true" # if true, installs gateway control plane providers - defaults to true for emulated clusters

# Prometheus Configuration
PROMETHEUS_SVC_NAME="kube-prometheus-stack-prometheus"
PROMETHEUS_BASE_URL="https://$PROMETHEUS_SVC_NAME.$MONITORING_NAMESPACE.svc.cluster.local"
PROMETHEUS_PORT="9090"
PROMETHEUS_URL="$PROMETHEUS_BASE_URL:$PROMETHEUS_PORT"
PROMETHEUS_SECRET_NAME="prometheus-web-tls"

# KIND cluster configuration
CLUSTER_NAME=${CLUSTER_NAME:-"kind-ap-gpu-cluster"}
CLUSTER_NODES=${CLUSTER_NODES:-"3"}
CLUSTER_GPUS=${CLUSTER_GPUS:-"4"}
CLUSTER_GPU_TYPE=${CLUSTER_GPU_TYPE:-"mix"}

# Flags for deployment steps
CREATE_CLUSTER=${CREATE_CLUSTER:-false}
DEPLOY_LLM_D_INFERENCE_SIM=${DEPLOY_LLM_D_INFERENCE_SIM:-true}
E2E_TESTS_ENABLED=${E2E_TESTS_ENABLED:-false}

# Undeployment flags
DELETE_CLUSTER=${DELETE_CLUSTER:-false}

# Kind-specific prerequisites
REQUIRED_TOOLS=("kind")

# Function to check Kind emulator-specific prerequisites
# - checks for kind, kubectl and helm
# - creates Kind cluster if CREATE_CLUSTER=true, otherwise tries to use an existing cluster
# - loads AP image into Kind cluster
check_specific_prerequisites() {
    log_info "Checking Kubernetes-specific prerequisites..."

    local missing_tools=()

    # Check for required tools (including Kubernetes-specific ones)
    for tool in "${REQUIRED_TOOLS[@]}"; do
        if ! command -v $tool &> /dev/null; then
            missing_tools+=($tool)
        fi
    done

    if [ ${#missing_tools[@]} -ne 0 ]; then
        log_error "Missing required tools: ${missing_tools[*]}"
    fi

    # Create or use existing KIND cluster
    if [ "$CREATE_CLUSTER" = "true" ]; then
        # Check if the specific cluster exists - if so, delete and recreate
        if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
            log_info "KIND cluster '${CLUSTER_NAME}' already exists, tearing it down and recreating..."
            kind delete cluster --name "${CLUSTER_NAME}"
        else
            log_info "KIND cluster '${CLUSTER_NAME}' not found, creating it..."
        fi
        create_kind_cluster

    else
        log_info "Cluster creation skipped (CREATE_CLUSTER=false)"
        # Verify the Kind cluster exists
        if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
            log_error "KIND cluster '${CLUSTER_NAME}' not found and CREATE_CLUSTER=false"
        fi
        # Set kubectl context to the Kind cluster
        kubectl config use-context "kind-${CLUSTER_NAME}" &> /dev/null
    fi
    # Verify kubectl can connect to the cluster
    if ! kubectl cluster-info &> /dev/null; then
        log_error "Failed to connect to KIND cluster '${CLUSTER_NAME}'"
    fi
    log_success "Using KIND cluster '${CLUSTER_NAME}'"

    # Load AP image into KIND cluster
    load_image

    log_success "All Kind emulated deployment prerequisites met"
}

# Creates Kind cluster using `setup.sh` script for GPU emulation
create_kind_cluster() {
    log_info "Creating KIND cluster with GPU emulation..."

    # Check if cluster already exists
    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        log_warning "KIND cluster '${CLUSTER_NAME}' already exists"
        log_info "Deleting existing cluster to create a fresh one..."
        kind delete cluster --name "${CLUSTER_NAME}"
    fi

    # Run setup.sh to create the cluster
    local SETUP_SCRIPT="${AP_PROJECT}/deploy/kind-emulator/setup.sh"

    if [ ! -f "$SETUP_SCRIPT" ]; then
        log_error "Setup script not found at: $SETUP_SCRIPT"
        exit 1
    fi

    log_info "Running setup script with: cluster=$CLUSTER_NAME, nodes=$CLUSTER_NODES, gpus=$CLUSTER_GPUS, type=$CLUSTER_GPU_TYPE"
    bash "$SETUP_SCRIPT" -c "${CLUSTER_NAME}" -n "$CLUSTER_NODES" -g "$CLUSTER_GPUS" -t "$CLUSTER_GPU_TYPE"

    # Ensure kubectl context is set to the new cluster
    kubectl config use-context "kind-${CLUSTER_NAME}" &> /dev/null

    log_success "KIND cluster '${CLUSTER_NAME}' created successfully"
}

# Loads AP image into the Kind cluster
load_image() {
    log_info "Loading AP image '$AP_IMAGE_REPO:$AP_IMAGE_TAG' into KIND cluster..."

    # Try to pull the image, or use local image if pull fails
    if ! docker pull "$AP_IMAGE_REPO:$AP_IMAGE_TAG"; then
        log_warning "Failed to pull image '$AP_IMAGE_REPO:$AP_IMAGE_TAG' from registry"
        log_info "Attempting to use local image..."

        # Check if the image exists locally
        if ! docker image inspect "$AP_IMAGE_REPO:$AP_IMAGE_TAG" >/dev/null 2>&1; then
            log_error "Image '$AP_IMAGE_REPO:$AP_IMAGE_TAG' not found locally either - Please build the image or check the registry"
        else
            log_info "Using local image '$AP_IMAGE_REPO:$AP_IMAGE_TAG'"
        fi
    else
        log_success "Successfully pulled image '$AP_IMAGE_REPO:$AP_IMAGE_TAG' from registry"
    fi

    # Load the image into the KIND cluster
    kind load docker-image "$AP_IMAGE_REPO:$AP_IMAGE_TAG" --name "$CLUSTER_NAME"

    log_success "Image '$AP_IMAGE_REPO:$AP_IMAGE_TAG' loaded into KIND cluster '$CLUSTER_NAME'"
}

#### REQUIRED FUNCTION used by deploy/install.sh ####
create_namespaces() {
    log_info "Creating namespaces..."

    for ns in $REDIS_NS $AP_NS $MONITORING_NAMESPACE $LLMD_NS; do
        if kubectl get namespace $ns &> /dev/null; then
            log_warning "Namespace $ns already exists"
        else
            kubectl create namespace $ns
            log_success "Namespace $ns created"
        fi
    done
}

#### REQUIRED FUNCTION used by deploy/install.sh ####
# Deploy Prometheus stack with TLS for Kubernetes
deploy_prometheus_stack() {
    log_info "Deploying kube-prometheus-stack with TLS..."

    # Add helm repo
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts || true
    helm repo update

    # Create self-signed TLS certificate for Prometheus
    log_info "Creating self-signed TLS certificate for Prometheus"
    openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout /tmp/prometheus-tls.key \
        -out /tmp/prometheus-tls.crt \
        -days 365 \
        -subj "/CN=prometheus" \
        -addext "subjectAltName=DNS:kube-prometheus-stack-prometheus.${MONITORING_NAMESPACE}.svc.cluster.local,DNS:kube-prometheus-stack-prometheus.${MONITORING_NAMESPACE}.svc,DNS:prometheus,DNS:localhost" \
        &> /dev/null

    # Create Kubernetes secret with TLS certificate
    log_info "Creating Kubernetes secret for Prometheus TLS"
    kubectl create secret tls $PROMETHEUS_SECRET_NAME \
        --cert=/tmp/prometheus-tls.crt \
        --key=/tmp/prometheus-tls.key \
        -n $MONITORING_NAMESPACE \
        --dry-run=client -o yaml | kubectl apply -f - &> /dev/null

    # Clean up temp files
    rm -f /tmp/prometheus-tls.{key,crt}

    # Install kube-prometheus-stack with TLS enabled
    log_info "Installing kube-prometheus-stack with TLS configuration"
    helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
        -n $MONITORING_NAMESPACE \
        --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
        --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false \
        --set prometheus.service.type=ClusterIP \
        --set prometheus.service.port=$PROMETHEUS_PORT \
        --set prometheus.prometheusSpec.web.tlsConfig.cert.secret.name=$PROMETHEUS_SECRET_NAME \
        --set prometheus.prometheusSpec.web.tlsConfig.cert.secret.key=tls.crt \
        --set prometheus.prometheusSpec.web.tlsConfig.keySecret.name=$PROMETHEUS_SECRET_NAME \
        --set prometheus.prometheusSpec.web.tlsConfig.keySecret.key=tls.key \
        --timeout=5m \
        --wait

    log_success "kube-prometheus-stack deployed with TLS"
    log_info "Prometheus URL: $PROMETHEUS_URL"
}

# Kubernetes-specific Undeployment functions
undeploy_prometheus_stack() {
    log_info "Uninstalling kube-prometheus-stack..."

    helm uninstall kube-prometheus-stack -n $MONITORING_NAMESPACE 2>/dev/null || \
        log_warning "Prometheus stack not found or already uninstalled"

    kubectl delete secret $PROMETHEUS_SECRET_NAME -n $MONITORING_NAMESPACE --ignore-not-found

    log_success "Prometheus stack uninstalled"
}

#### REQUIRED FUNCTION used by deploy/install.sh ####
delete_namespaces() {
    log_info "Deleting namespaces..."

    for ns in $REDIS_NS $LLMD_NS $AP_NS $MONITORING_NAMESPACE; do
        if kubectl get namespace $ns &> /dev/null; then
            if [[ "$ns" == "$LLMD_NS" && "$DEPLOY_LLM_D" == "false" ]] || [[ "$ns" == "$AP_NS" && "$DEPLOY_AP" == "false" ]] || [[ "$ns" == "$MONITORING_NAMESPACE" && "$DEPLOY_PROMETHEUS" == "false" ]] ; then
                log_info "Skipping deletion of namespace $ns as it was not deployed"
            else
                log_info "Deleting namespace $ns..."
                kubectl delete namespace $ns 2>/dev/null || \
                    log_warning "Failed to delete namespace $ns"
            fi
        fi
    done

    log_success "Namespaces deleted"

    if [ "$DELETE_CLUSTER" = true ]; then
        delete_kind_cluster
    fi
}

# Deletes the Kind cluster
# Used when DELETE_CLUSTER=true by delete_namespaces()
delete_kind_cluster() {
    log_info "Deleting KIND cluster '${CLUSTER_NAME}'..."

    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        kind delete cluster --name "${CLUSTER_NAME}"
        log_success "KIND cluster '${CLUSTER_NAME}' deleted"
    else
        log_warning "KIND cluster '${CLUSTER_NAME}' not found"
    fi
}
