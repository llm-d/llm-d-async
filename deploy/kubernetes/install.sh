#!/bin/bash
#
# Workload-Variant-Autoscaler Kubernetes Environment-Specific Configuration
# This script provides Kubernetes-specific functions and variable overrides
# It is sourced by the main install.sh script
# Note: it is NOT meant to be executed directly
#

set -e  # Exit on error
set -o pipefail  # Exit on pipe failure

check_specific_prerequisites() {
    log_info "No special pre-reqs"
}




create_namespaces() {
    log_info "Creating namespaces..."

    for ns in $AP_NS $MONITORING_NAMESPACE $LLMD_NS; do
        local ns_exists=false
        local ns_terminating=false

        # Check namespace state
        if kubectl get namespace $ns &> /dev/null; then
            ns_exists=true
            local ns_status=$(kubectl get namespace $ns -o jsonpath='{.status.phase}' 2>/dev/null)
            if [ "$ns_status" = "Terminating" ]; then
                ns_terminating=true
            fi
        fi

        # Handle each case explicitly
        if [ "$ns_exists" = true ] && [ "$ns_terminating" = false ]; then
            # Namespace exists and is active - skip
            log_info "Namespace $ns already exists"
            continue
        elif [ "$ns_terminating" = true ]; then
            # Namespace is terminating - force delete and recreate
            log_info "Namespace $ns is terminating, forcing deletion..."
            kubectl get namespace $ns -o json | \
                jq '.spec.finalizers = []' | \
                kubectl replace --raw "/api/v1/namespaces/$ns/finalize" -f - 2>/dev/null || true
            kubectl wait --for=delete namespace/$ns --timeout=120s 2>/dev/null || true
        fi
        # At this point: namespace doesn't exist OR was terminating and is now deleted
        kubectl create namespace $ns
        log_success "Namespace $ns created"
    done
}



delete_namespaces() {
    log_info "Deleting namespaces..."

    for ns in $LLMD_NS $AP_NS $MONITORING_NAMESPACE; do
        if kubectl get namespace $ns &> /dev/null; then
            if [[ "$ns" == "$LLMD_NS" && "$DEPLOY_LLM_D" == "false" ]] || [[ "$ns" == "$WVA_NS" && "$DEPLOY_WVA" == "false" ]] || [[ "$ns" == "$MONITORING_NAMESPACE" && "$DEPLOY_PROMETHEUS" == "false" ]] ; then
                log_info "Skipping deletion of namespace $ns as it was not deployed"
            else
                log_info "Deleting namespace $ns..."
                kubectl delete namespace $ns 2>/dev/null || \
                    log_warning "Failed to delete namespace $ns"
            fi
        fi
    done

    log_success "Namespaces deleted"
}

# Environment-specific functions are now sourced by the main install.sh script
# Do not call functions directly when sourced
