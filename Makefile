# Local env overrides (if exist)
-include .env
export

.DELETE_ON_ERROR:



# Image URL to use all building/pushing image targets
IMAGE_TAG_BASE ?= ghcr.io/llm-d-incubation
IMG_TAG ?= latest
IMG ?= $(IMAGE_TAG_BASE)/async-processor:$(IMG_TAG)

# Versioning information
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
VERSION := $(VERSION)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
COMMIT := $(COMMIT)
DATE ?= $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
DATE := $(DATE)

# Build flags
LDFLAGS := -s -w \
	-X github.com/llm-d-incubation/llm-d-async/pkg/version.Version=$(VERSION) \
	-X github.com/llm-d-incubation/llm-d-async/pkg/version.Commit=$(COMMIT) \
	-X github.com/llm-d-incubation/llm-d-async/pkg/version.BuildDate=$(DATE)

# KIND_ARGS etc.
KIND_ARGS ?= -t mix -n 3 -g 2   # Default: 3 nodes, 2 GPUs per node, mixed vendors
CLUSTER_GPU_TYPE ?= mix
CLUSTER_NODES ?= 3
CLUSTER_GPUS ?= 4
KUBECONFIG ?= $(HOME)/.kube/config
K8S_VERSION ?= v1.32.0

CONTROLLER_NAMESPACE ?= async-processor-system
LLMD_NAMESPACE       ?= llm-d-inference-scheduler
GATEWAY_NAME         ?= infra-inference-scheduling-inference-gateway-istio
MODEL_ID             ?= unsloth/Meta-Llama-3.1-8B
DEPLOYMENT           ?= ms-inference-scheduling-llm-d-modelservice-decode
REQUEST_RATE         ?= 20
NUM_PROMPTS          ?= 3000

# Flags for deploy/install.sh installation script
CREATE_CLUSTER ?= false
DEPLOY_LLM_D ?= true
DEPLOY_REDIS ?= true
DELETE_CLUSTER ?= false
DELETE_NAMESPACES ?= false

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN := $(shell go env GOPATH)/bin
else
GOBIN := $(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Auto-detects docker (if daemon running) or podman; override with CONTAINER_TOOL=podman.
CONTAINER_TOOL ?= $(shell (command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 && echo docker) || (command -v podman >/dev/null 2>&1 && echo podman) || echo docker)

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: fmt
fmt: ## Run go fmt against root and producer modules.
	go fmt ./...
	cd producer && go fmt ./...

.PHONY: vet
vet: ## Run go vet against root and producer modules.
	go vet ./...
	cd producer && go vet ./...

.PHONY: test
test: fmt vet setup-envtest ## Run tests (root module and producer submodule).
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out
	cd producer && go test ./... -coverprofile=cover-producer.out

# Creates a multi-node Kind cluster
# Adds emulated GPU labels and capacities per node
.PHONY: create-kind-cluster
create-kind-cluster:
	export KIND=$(KIND) KUBECTL=$(KUBECTL) && \
		deploy/kind-emulator/setup.sh -t $(CLUSTER_GPU_TYPE) -n $(CLUSTER_NODES) -g $(CLUSTER_GPUS)

# Destroys the Kind cluster created by `create-kind-cluster`
.PHONY: destroy-kind-cluster
destroy-kind-cluster:
	export KIND=$(KIND) KUBECTL=$(KUBECTL) && \
        deploy/kind-emulator/teardown.sh

# Deploys the Async Processor on a pre-existing Kind cluster or creates one if specified
.PHONY: deploy-ap-emulated-on-kind
deploy-ap-emulated-on-kind:
	@echo ">>> Deploying async processor (cluster args: $(KIND_ARGS), image: $(IMG))"
	KIND=$(KIND) KUBECTL=$(KUBECTL) IMG=$(IMG) DEPLOY_REDIS=$(DEPLOY_REDIS) DEPLOY_LLM_D=$(DEPLOY_LLM_D) ENVIRONMENT=kind-emulator CREATE_CLUSTER=$(CREATE_CLUSTER) CLUSTER_GPU_TYPE=$(CLUSTER_GPU_TYPE) CLUSTER_NODES=$(CLUSTER_NODES) CLUSTER_GPUS=$(CLUSTER_GPUS) NAMESPACE_SCOPED=false \
		deploy/install.sh

## Undeploy Async Processor from the emulated environment on Kind.
.PHONY: undeploy-ap-emulated-on-kind
undeploy-ap-emulated-on-kind:
	@echo ">>> Undeploying async processor from Kind"
	KIND=$(KIND) KUBECTL=$(KUBECTL) ENVIRONMENT=kind-emulator DEPLOY_REDIS=$(DEPLOY_REDIS) DEPLOY_LLM_D=$(DEPLOY_LLM_D) DELETE_NAMESPACES=$(DELETE_NAMESPACES) DELETE_CLUSTER=$(DELETE_CLUSTER) \
		deploy/install.sh --undeploy

## Deploy AP on Kubernetes with the specified image.
.PHONY: deploy-ap-on-k8s
deploy-ap-on-k8s: kustomize ## Deploy AP on Kubernetes with the specified image.
	@echo "Deploying AP on Kubernetes with image: $(IMG)"
	@echo "Target namespace: $(or $(NAMESPACE),async-processor-system)"
	NAMESPACE=$(or $(NAMESPACE),async-processor-system) IMG=$(IMG) ENVIRONMENT=kubernetes DEPLOY_LLM_D=$(DEPLOY_LLM_D) ./deploy/install.sh

## Undeploy AP from Kubernetes.
.PHONY: undeploy-ap-on-k8s
undeploy-ap-on-k8s:
	@echo ">>> Undeploying async-processor from Kubernetes"
	export KIND=$(KIND) KUBECTL=$(KUBECTL) ENVIRONMENT=kubernetes && \
		ENVIRONMENT=kubernetes DEPLOY_LLM_D=$(DEPLOY_LLM_D)  deploy/install.sh --undeploy

# E2E integration tests
#
# The suite deploys the async-processor, EPP, llm-d-inference-sim, Envoy,
# Prometheus, and Redis into a Kind cluster.
#
# By default the EPP image is pulled from the registry and InferencePool CRDs
# are fetched from the GAIE GitHub repo at the matching tag. Set GAIE_ROOT to a
# local gateway-api-inference-extension checkout to build EPP from source and
# use that checkout's CRDs instead.
#
# The llm-d-inference-sim image is pulled from the registry by default.
# Set SIM_ROOT to build from a local checkout instead.
#
# Optional env vars:
#   GAIE_ROOT        — GAIE checkout; enables local EPP build and CRDs
#   SIM_ROOT         — llm-d-inference-sim checkout; enables local sim build
#   AP_IMAGE         — async-processor image tag        (default: $(IMAGE_TAG_BASE)/async-processor:e2e-test)
#   EPP_IMAGE        — EPP image tag                    (default: registry.k8s.io/.../epp:v1.5.0)
#   SIM_IMAGE        — inference-sim image tag          (default: ghcr.io/llm-d/llm-d-inference-sim:v0.0.0-test)
#   CONTAINER_TOOL   — container runtime                (default: docker)
#   E2E_SKIP_CLEANUP — set "true" to keep the Kind cluster after tests
#
# NodePort overrides (change if defaults conflict):
#   E2E_INTEGRATION_REDIS_PORT, E2E_INTEGRATION_PROM_PORT,
#   E2E_INTEGRATION_SIM_PORT, E2E_INTEGRATION_ENVOY_PORT,
#   E2E_INTEGRATION_ENVOY_ADMIN_PORT
E2E_IMG ?= $(IMAGE_TAG_BASE)/async-processor:e2e-test

.PHONY: test-e2e
test-e2e: ## Run e2e integration tests against a Kind cluster
	@command -v kind >/dev/null 2>&1 || { echo "kind is not installed"; exit 1; }
	CONTAINER_TOOL=$(CONTAINER_TOOL) AP_IMAGE=$(E2E_IMG) go test ./test/e2e/ -timeout 30m -v -ginkgo.v \
		$(if $(FOCUS),-ginkgo.focus="$(FOCUS)",) \
		$(if $(SKIP),-ginkgo.skip="$(SKIP)",)

.PHONY: check-dco
check-dco: ## Check that all commits since main have a DCO Signed-off-by trailer
	@scripts/check-dco.sh

.PHONY: ci
ci: fmt vet lint test ## Run all CI checks (fmt, vet, lint, test)

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	$(GOLANGCI_LINT) run  --timeout 5m

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix  --timeout 5m

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	$(GOLANGCI_LINT) config verify

##@ Build

.PHONY: build
build:   fmt vet ## Build manager binary.
	go build -ldflags "$(LDFLAGS)" -o bin/manager cmd/main.go

.PHONY: run
run:   fmt vet ## Run a controller from your host.
	go run -ldflags "$(LDFLAGS)" ./cmd/main.go

.PHONY: clean
clean: ## Clean binaries.
	rm -rf bin/

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build --build-arg LDFLAGS="$(LDFLAGS)" -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64
BUILDER_NAME ?= async-processor-builder

.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name $(BUILDER_NAME)
	$(CONTAINER_TOOL) buildx use $(BUILDER_NAME)
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --build-arg LDFLAGS="$(LDFLAGS)" --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm $(BUILDER_NAME)
	rm Dockerfile.cross

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default | $(KUBECTL) apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/default | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
LOCALBIN := $(LOCALBIN)
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.6.0
CONTROLLER_TOOLS_VERSION ?= v0.17.2
#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell go list -m -f "{{ .Version }}" sigs.k8s.io/controller-runtime | awk -F'[v.]' '{printf "release-%d.%d", $$2, $$3}')
ENVTEST_VERSION := $(ENVTEST_VERSION)
#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell go list -m -f "{{ .Version }}" k8s.io/api | awk -F'[v.]' '{printf "1.%d", $$3}')
ENVTEST_K8S_VERSION := $(ENVTEST_K8S_VERSION)
GOLANGCI_LINT_VERSION ?= v2.11.4

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))



.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

GINKGO ?= $(LOCALBIN)/ginkgo
GINKGO_VERSION ?= v2.28.1

.PHONY: ginkgo
ginkgo: $(GINKGO) ## Download ginkgo locally if necessary.
$(GINKGO): $(LOCALBIN)
	$(call go-install-tool,$(GINKGO),github.com/onsi/ginkgo/v2/ginkgo,$(GINKGO_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef

## Copied from https://github.com/llm-d-incubation/batch-gateway
## publish-helm-chart: Patch chart for VERSION, package, append chart to SHA256SUMS, push to oci://ghcr.io/llm-d-incubation/charts (requires VERSION, yq, helm; GITHUB_TOKEN, GITHUB_ACTOR for push).
.PHONY: publish-helm-chart
publish-helm-chart:
	@if [ -z "$(VERSION)" ]; then \
	  echo "VERSION is required (e.g. VERSION=v1.0.0 make publish-helm-chart)"; exit 1; \
	fi
	@export VERSION="$(VERSION)"; \
	export GITHUB_TOKEN="$(GITHUB_TOKEN)"; \
	export GITHUB_ACTOR="$(GITHUB_ACTOR)"; \
	./scripts/publish-helm-chart.sh
