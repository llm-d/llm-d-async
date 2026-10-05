package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	k8slog "sigs.k8s.io/controller-runtime/pkg/log"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/env"
	testutils "sigs.k8s.io/gateway-api-inference-extension/test/utils"
)

const (
	kindClusterName = "e2e-integration-tests"
	nsName          = "e2e-integration"

	// Manifests
	redisManifest      = "./yaml/redis.yaml"
	pubsubManifest     = "./yaml/pubsub.yaml"
	simManifest        = "./yaml/sim.yaml"
	eppManifest        = "./yaml/epp.yaml"
	eppConfigNoFC      = "./yaml/epp-config-no-fc.yaml"
	eppConfigFC        = "./yaml/epp-config-fc.yaml"
	simDeployManifest  = "./yaml/sim-deploy.yaml"
	envoyManifest      = "./yaml/envoy.yaml"
	prometheusManifest = "./yaml/prometheus.yaml"
	jaegerManifest     = "./yaml/jaeger.yaml"

	// Helm chart and per-instance values for llm-d-async deployments.
	chartPath     = "../../charts/llm-d-async"
	helmValuesDir = "./helm"
)

var (
	redisPort      string = env.GetEnvString("E2E_INTEGRATION_REDIS_PORT", "30480", ginkgo.GinkgoLogr)
	pubsubPort     string = env.GetEnvString("E2E_INTEGRATION_PUBSUB_PORT", "30485", ginkgo.GinkgoLogr)
	promPort       string = env.GetEnvString("E2E_INTEGRATION_PROM_PORT", "30491", ginkgo.GinkgoLogr)
	simPort        string = env.GetEnvString("E2E_INTEGRATION_SIM_PORT", "30490", ginkgo.GinkgoLogr)
	envoyPort      string = env.GetEnvString("E2E_INTEGRATION_ENVOY_PORT", "30492", ginkgo.GinkgoLogr)
	envoyAdminPort string = env.GetEnvString("E2E_INTEGRATION_ENVOY_ADMIN_PORT", "30493", ginkgo.GinkgoLogr)
	jaegerPort     string = env.GetEnvString("E2E_INTEGRATION_JAEGER_PORT", "30494", ginkgo.GinkgoLogr)

	containerRuntime = detectContainerRuntime()
	apImage          = env.GetEnvString("AP_IMAGE", "ghcr.io/llm-d/llm-d-async:e2e-test", ginkgo.GinkgoLogr)
	// TODO: switch EPP_IMAGE and ROUTER_VERSION to a release tag than "main"
	eppImage = env.GetEnvString("EPP_IMAGE", "ghcr.io/llm-d/llm-d-router-endpoint-picker:main", ginkgo.GinkgoLogr)
	// gaieVersion selects the gateway-api-inference-extension release whose
	// CRDs are fetched; decoupled from the EPP image tag because the image now
	// comes from llm-d-router, which pins its own gaie version in go.mod.
	gaieVersion = env.GetEnvString("GAIE_VERSION", "v1.6.2", ginkgo.GinkgoLogr)
	simImage    = env.GetEnvString("SIM_IMAGE", "ghcr.io/llm-d/llm-d-inference-sim:v0.10.0", ginkgo.GinkgoLogr)
	redisImage  = env.GetEnvString("REDIS_IMAGE", "valkey/valkey:8-alpine", ginkgo.GinkgoLogr)
	pubsubImage = env.GetEnvString("PUBSUB_IMAGE", "gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators", ginkgo.GinkgoLogr)
	gaieRoot    = os.Getenv("GAIE_ROOT")
	routerRoot  = os.Getenv("ROUTER_ROOT")
	// Router main is required for EndpointPickerConfig at llm-d.ai/v1.
	routerVersion = env.GetEnvString("ROUTER_VERSION", "main", ginkgo.GinkgoLogr)
	simRoot       = os.Getenv("SIM_ROOT")

	testConfig *testutils.TestConfig

	// kindKubeconfig is an isolated kubeconfig file for the Kind cluster,
	// so we never modify the user's default ~/.kube/config or current context.
	kindKubeconfig string

	rdb           *redis.Client
	pubsubClient  *pubsub.Client
	promURL       string
	simAdminURL   string
	envoyURL      string
	envoyAdminURL string
	jaegerURL     string
)

func TestEndToEnd(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t,
		"End To End Test Suite",
	)
}

var _ = ginkgo.BeforeSuite(func() {
	setupK8sCluster()
	gomega.Expect(os.Setenv("KUBECONFIG", kindKubeconfig)).To(gomega.Succeed())
	gomega.Expect(os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:"+pubsubPort)).To(gomega.Succeed())
	testConfig = testutils.NewTestConfig(nsName, "")
	setupK8sClient()
	setupNameSpace()
	applyManifests()
	setupClients()
})

var _ = ginkgo.AfterSuite(func() {
	if pubsubClient != nil {
		pubsubClient.Close() //nolint:errcheck
	}
	if rdb != nil {
		rdb.Close() //nolint:errcheck
	}

	skipCleanup := env.GetEnvString("E2E_SKIP_CLEANUP", "false", ginkgo.GinkgoLogr)
	if skipCleanup == "true" {
		fmt.Println("Skipping cluster cleanup (E2E_SKIP_CLEANUP=true)")
		return
	}

	ginkgo.By("Deleting kind cluster " + kindClusterName)
	command := exec.Command("kind", "delete", "cluster", "--name", kindClusterName,
		"--kubeconfig", kindKubeconfig)
	session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	if err != nil {
		ginkgo.GinkgoLogr.Error(err, "Failed to delete kind cluster")
	} else {
		gomega.Eventually(session).WithTimeout(60 * time.Second).Should(gexec.Exit())
	}
})

// detectContainerRuntime returns the container runtime to use.
// Priority: CONTAINER_TOOL env > CONTAINER_RUNTIME env > docker (if daemon running) > podman.
func detectContainerRuntime() string {
	if tool := os.Getenv("CONTAINER_TOOL"); tool != "" {
		return tool
	}
	if tool := os.Getenv("CONTAINER_RUNTIME"); tool != "" {
		return tool
	}
	if _, err := exec.LookPath("docker"); err == nil {
		if exec.Command("docker", "info").Run() == nil {
			return "docker"
		}
	}
	if _, err := exec.LookPath("podman"); err == nil {
		return "podman"
	}
	return "docker"
}

func pullIfMissing(image string) {
	inspectCmd := exec.Command(containerRuntime, "image", "inspect", image)
	// always pull "main" image instead of using a stale local copy.
	if err := inspectCmd.Run(); err == nil && !strings.HasSuffix(image, ":main") {
		ginkgo.By("Image " + image + " already exists locally, skipping pull")
		return
	}

	ginkgo.By("Pulling image " + image)
	command := exec.Command(containerRuntime, "pull", image)
	session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))
}

func loadEPPImage() {
	if routerRoot != "" {
		ginkgo.By("Building EPP image from " + routerRoot)
		command := exec.Command(containerRuntime, "build", "-f", filepath.Join(routerRoot, "Dockerfile.epp"), "-t", eppImage, routerRoot)
		session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))
	} else {
		pullIfMissing(eppImage)
	}

	kindLoadImage(eppImage)
}

func setupK8sCluster() {
	kindKubeconfig = filepath.Join(os.TempDir(), "kind-kubeconfig-"+kindClusterName)

	// Check if cluster already exists
	checkCmd := exec.Command("kind", "get", "clusters")
	output, err := checkCmd.Output()
	if err == nil && strings.Contains(string(output), kindClusterName) {
		ginkgo.By("Kind cluster " + kindClusterName + " already exists, rebuilding and loading llm-d-async image")
		command := exec.Command(containerRuntime, "build", "-t", apImage, projectRoot())
		session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))
		kindLoadImage(apImage)
		loadEPPImage()
		return
	}

	if containerRuntime == "podman" {
		ginkgo.By("Setting KIND_EXPERIMENTAL_PROVIDER=podman")
		os.Setenv("KIND_EXPERIMENTAL_PROVIDER", "podman") //nolint:errcheck
	}

	ginkgo.By("Creating Kind cluster " + kindClusterName)
	command := exec.Command("kind", "create", "cluster", "--name", kindClusterName,
		"--kubeconfig", kindKubeconfig, "--wait", "120s", "--config", "-")
	stdin, err := command.StdinPipe()
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	go func() {
		defer func() {
			gomega.Expect(stdin.Close()).To(gomega.Succeed())
		}()
		cfg := strings.ReplaceAll(kindClusterConfig, "${REDIS_PORT}", redisPort)
		cfg = strings.ReplaceAll(cfg, "${PUBSUB_PORT}", pubsubPort)
		cfg = strings.ReplaceAll(cfg, "${PROM_PORT}", promPort)
		cfg = strings.ReplaceAll(cfg, "${SIM_PORT}", simPort)
		cfg = strings.ReplaceAll(cfg, "${ENVOY_PORT}", envoyPort)
		cfg = strings.ReplaceAll(cfg, "${ENVOY_ADMIN_PORT}", envoyAdminPort)
		cfg = strings.ReplaceAll(cfg, "${JAEGER_PORT}", jaegerPort)
		_, err := io.WriteString(stdin, cfg)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	}()
	session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))

	ginkgo.By("Building llm-d-async image")
	command = exec.Command(containerRuntime, "build", "-t", apImage, projectRoot())
	session, err = gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))

	loadEPPImage()

	if simRoot != "" {
		ginkgo.By("Building sim image from " + simRoot)
		command = exec.Command(containerRuntime, "build", "-t", simImage, simRoot)
		session, err = gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))
	} else {
		pullIfMissing(simImage)
	}

	kindLoadImage(apImage)
	kindLoadImage(simImage)

	pullIfMissing(redisImage)
	kindLoadImage(redisImage)

	pullIfMissing(pubsubImage)
	kindLoadImage(pubsubImage)

	pullIfMissing("docker.io/envoyproxy/envoy:distroless-v1.33.2")
	kindLoadImage("docker.io/envoyproxy/envoy:distroless-v1.33.2")

	pullIfMissing("prom/prometheus:v2.53.0")
	kindLoadImage("prom/prometheus:v2.53.0")

	pullIfMissing("jaegertracing/all-in-one:1.76.0")
	kindLoadImage("jaegertracing/all-in-one:1.76.0")

	// The sim image is pulled with imagePullPolicy: Always directly by the cluster.
}

func kindLoadImage(image string) {
	ginkgo.By(fmt.Sprintf("Loading %s into the cluster %s (runtime=%s)", image, kindClusterName, containerRuntime))

	if containerRuntime == "podman" {
		// Podman: tag unqualified images so kind can find them, then pipe via
		// podman save | kind load image-archive.
		qualifiedImage := image
		if !strings.Contains(image, "/") {
			qualifiedImage = "docker.io/library/" + image
			cmd := exec.Command("podman", "tag", image, qualifiedImage)
			session, err := gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Eventually(session).WithTimeout(60 * time.Second).Should(gexec.Exit(0))
		}

		shell := fmt.Sprintf("podman save --format docker-archive %s | kind load image-archive /dev/stdin --name %s",
			qualifiedImage, kindClusterName)
		command := exec.Command("bash", "-c", shell)
		session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))
	} else {
		// Pipe through ctr import WITHOUT --all-platforms. Kind's built-in
		// "kind load docker-image" passes --all-platforms to ctr, which
		// fails for multi-arch images when only one platform's layers are
		// present locally (e.g. arm64-only pull on Apple Silicon).
		//
		// This applies to ALL images loaded into the cluster (not just the
		// MQ image). It assumes a single-node Kind cluster with the default
		// control-plane node name and overlayfs snapshotter.
		node := kindClusterName + "-control-plane"
		shell := fmt.Sprintf(
			"docker save %s | docker exec -i %s ctr --namespace=k8s.io images import --digests --snapshotter=overlayfs -",
			image, node)
		command := exec.Command("bash", "-c", shell)
		session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))
	}
}

func setupK8sClient() {
	k8sCfg, err := config.GetConfigWithContext("kind-" + kindClusterName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.ExpectWithOffset(1, k8sCfg).NotTo(gomega.BeNil())

	err = clientgoscheme.AddToScheme(testConfig.Scheme)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	testConfig.CreateCli()

	k8slog.SetLogger(ginkgo.GinkgoLogr)
}

// setupNameSpace sets up the specified namespace if it doesn't exist.
// Uses kubectl (not the Go API client) to ensure the same kubeconfig/context
// as all other manifest operations.
func setupNameSpace() {
	ginkgo.By("Creating namespace " + nsName + " (kubeconfig=" + kindKubeconfig + ")")
	command := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"create", "namespace", nsName)
	session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(30 * time.Second).Should(gexec.Exit())

	// Verify namespace actually exists.
	command = exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"get", "namespace", nsName)
	session, err = gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(30 * time.Second).Should(gexec.Exit(0))
}

func applyManifests() {
	// All manifests are applied via kubectl to avoid scheme registration issues
	// with GAIE custom resources (InferencePool, CRDs).
	ginkgo.By("Applying InferencePool and router CRDs")
	for _, crd := range inferenceCRDs() {
		kubectlApplyFile(crd, nil)
	}

	ginkgo.By("Applying Redis manifest")
	kubectlApplyFile(redisManifest, map[string]string{"${REDIS_IMAGE}": redisImage})

	ginkgo.By("Applying PubSub emulator manifest")
	kubectlApplyFile(pubsubManifest, map[string]string{"${PUBSUB_IMAGE}": pubsubImage})

	ginkgo.By("Waiting for PubSub emulator deployment rollout")
	cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"-n", nsName, "rollout", "status", "deployment/pubsub-emulator", "--timeout=60s")
	sess, err := gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(sess).WithTimeout(60 * time.Second).Should(gexec.Exit(0))

	setupPubSubEmulatorTopicsAndSubscriptions()

	ginkgo.By("Applying sim manifest")
	kubectlApplyFile(simManifest, nil)
	kubectlApplyFile(simDeployManifest, map[string]string{"${SIM_IMAGE}": simImage})

	// Deploy EPP without flow control initially so the cascade test
	// (which runs first) sees no primary metric.
	ginkgo.By("Applying EPP manifest (without flow control)")
	kubectlApplyFile(eppManifest, map[string]string{"${EPP_IMAGE}": eppImage})
	kubectlApplyFile(eppConfigNoFC, nil)

	ginkgo.By("Applying Prometheus manifest")
	kubectlApplyFile(prometheusManifest, nil)

	ginkgo.By("Applying Jaeger manifest")
	kubectlApplyFile(jaegerManifest, nil)

	ginkgo.By("Applying Envoy manifest")
	kubectlApplyFile(envoyManifest, map[string]string{
		"${EPP_SVC}":               "epp-svc",
		"${ENVOY_ADMIN_NODE_PORT}": envoyAdminPort,
	})
	// Patch Envoy service to NodePort so test code can reach it for probe requests.
	kubectlPatchEnvoyNodePort()

	ginkgo.By("Installing llm-d-async helm releases")
	imageRepo, imageTag := splitImage(apImage)
	for _, r := range []struct{ name, values string }{
		{"integration", helmValuesDir + "/integration.yaml"},
		{"pubsub", helmValuesDir + "/pubsub.yaml"},
		{"saturation", helmValuesDir + "/saturation.yaml"},
		{"budget", helmValuesDir + "/budget.yaml"},
		{"redis-gate", helmValuesDir + "/redis-gate.yaml"},
		{"quota", helmValuesDir + "/quota.yaml"},
		{"composite", helmValuesDir + "/composite.yaml"},
		{"prometheus-query", helmValuesDir + "/prometheus-query.yaml"},
		{"endpoint-scrape", helmValuesDir + "/endpoint-scrape.yaml"},
		{"short-drain", helmValuesDir + "/short-drain.yaml"},
		{"multitenant", helmValuesDir + "/multitenant.yaml"},
		{"tier-priority", helmValuesDir + "/tier-priority.yaml"},
		{"mt-merge", helmValuesDir + "/mt-merge.yaml"},
		{"benchmark", helmValuesDir + "/benchmark.yaml"},
		{"benchmark-pool-gate", helmValuesDir + "/benchmark-pool-gate.yaml"},
		{"benchmark-pubsub", helmValuesDir + "/benchmark-pubsub.yaml"},
	} {
		helmInstall(r.name, r.values, map[string]string{
			"ap.image.repository": imageRepo,
			"ap.image.tag":        imageTag,
		})
	}
}

// kubectlPatchEnvoyNodePort patches the Envoy service to NodePort so test code
// outside the cluster can reach it for probe requests.
func kubectlPatchEnvoyNodePort() {
	patch := fmt.Sprintf(`{"spec":{"type":"NodePort","ports":[{"name":"http-8081","port":8081,"targetPort":8081,"nodePort":%s}]}}`, envoyPort)
	command := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"-n", nsName, "patch", "service", "envoy",
		"--type=merge", "--patch", patch)
	session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(30 * time.Second).Should(gexec.Exit(0))
}

// kubectlApplyFile applies a YAML manifest via kubectl, optionally substituting
// template variables. Uses stdin to avoid temp files.
func kubectlApplyFile(path string, substitutions map[string]string) {
	kubectlApplyFileInNamespace(path, "", substitutions)
}

func kubectlApplyFileInNamespace(path, namespace string, substitutions map[string]string) {
	content, err := os.ReadFile(path)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(), "reading manifest %s", path)

	yaml := string(content)
	yaml = strings.ReplaceAll(yaml, "${NAMESPACE}", nsName)
	for k, v := range substitutions {
		yaml = strings.ReplaceAll(yaml, k, v)
	}

	args := []string{"--kubeconfig", kindKubeconfig, "apply"}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	args = append(args, "-f", "-")

	command := exec.Command("kubectl", args...)
	command.Stdin = strings.NewReader(yaml)
	session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(60 * time.Second).Should(gexec.Exit(0))
}

// splitImage splits a container image reference into repo and tag.
// Does not handle digest references (repo@sha256:...).
func splitImage(image string) (string, string) {
	if i := strings.LastIndex(image, ":"); i >= 0 {
		return image[:i], image[i+1:]
	}
	return image, "latest"
}

func helmInstall(releaseName, valuesFile string, sets map[string]string) {
	args := []string{"upgrade", "--install", releaseName, chartPath,
		"-f", valuesFile, "-n", nsName, "--wait", "--timeout=120s"}
	for k, v := range sets {
		args = append(args, "--set", k+"="+v)
	}
	command := exec.Command("helm", args...)
	command.Env = append(os.Environ(), "KUBECONFIG="+kindKubeconfig)
	session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(), "helm install %s", releaseName)
	gomega.Eventually(session).WithTimeout(180 * time.Second).Should(gexec.Exit(0))
}

func setupClients() {
	promURL = "http://localhost:" + promPort
	simAdminURL = "http://localhost:" + simPort
	envoyURL = "http://localhost:" + envoyPort
	envoyAdminURL = "http://localhost:" + envoyAdminPort
	jaegerURL = "http://localhost:" + jaegerPort

	ginkgo.By("Creating Redis client on localhost:" + redisPort)
	rdb = redis.NewClient(&redis.Options{Addr: "localhost:" + redisPort})
	gomega.Eventually(func() error {
		return rdb.Ping(context.Background()).Err()
	}, 30*time.Second, 1*time.Second).Should(gomega.Succeed())

	ginkgo.By("Creating PubSub client on localhost:" + pubsubPort)
	var err error
	pubsubClient, err = pubsub.NewClient(context.Background(), pubsubProjectID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Waiting for Prometheus to be ready")
	gomega.Eventually(func(g gomega.Gomega) {
		resp, err := http.Get(promURL + "/-/ready")
		g.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close() //nolint:errcheck
		g.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
	}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())

	ginkgo.By("Waiting for sim to be ready")
	gomega.Eventually(func(g gomega.Gomega) {
		resp, err := http.Get(simAdminURL + "/metrics")
		g.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close() //nolint:errcheck
		g.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
	}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())

	ginkgo.By("Waiting for Jaeger to be ready")
	gomega.Eventually(func(g gomega.Gomega) {
		resp, err := http.Get(jaegerURL + "/")
		g.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close() //nolint:errcheck
		g.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
	}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())

	ginkgo.By("Waiting for Envoy to be ready")
	gomega.Eventually(func(g gomega.Gomega) {
		resp, err := http.Get(envoyURL + "/v1/completions")
		g.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close() //nolint:errcheck
		g.Expect(resp.StatusCode).To(gomega.BeNumerically("<", 500))
	}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())
}

// inferenceCRDs loads InferencePool from GAIE and the objective/model-rewrite
// CRDs from llm-d-router. Local checkouts override the corresponding downloads.
func inferenceCRDs() []string {
	paths := crdPaths("kubernetes-sigs/gateway-api-inference-extension", gaieVersion, gaieRoot,
		[]string{"inference.networking.k8s.io_inferencepools.yaml"})
	return append(paths, crdPaths("llm-d/llm-d-router", routerVersion, routerRoot,
		[]string{"llm-d.ai_inferenceobjectives.yaml", "llm-d.ai_inferencemodelrewrites.yaml"})...)
}

func crdPaths(repository, version, localRoot string, names []string) []string {
	base := filepath.Join(localRoot, "config", "crd", "bases")
	if localRoot == "" {
		base = filepath.Join(projectRoot(), ".cache", "crds", repository, version)
		gomega.Expect(os.MkdirAll(base, 0755)).To(gomega.Succeed())
	}
	paths := make([]string, len(names))
	for i, name := range names {
		paths[i] = filepath.Join(base, name)
		if localRoot != "" {
			continue
		}
		if _, err := os.Stat(paths[i]); err == nil && version != "main" {
			continue
		}

		ginkgo.By("Fetching CRD " + name + " from " + repository + " at " + version)
		url := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/config/crd/bases/%s", repository, version, name)
		resp, err := http.Get(url) //nolint:gosec
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK),
			"failed to fetch CRD %s from %s at %s", name, repository, version)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close() //nolint:errcheck
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(os.WriteFile(paths[i], data, 0644)).To(gomega.Succeed())
	}
	return paths
}

func projectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..")
}

// redeployEPPWithFlowControl re-applies the EPP manifest with flow control
// enabled, restarts the deployment, and waits for the new pod to be ready.
var redeployEPPOnce sync.Once

func redeployEPPWithFlowControl() {
	redeployEPPOnce.Do(doRedeployEPPWithFlowControl)
}

func doRedeployEPPWithFlowControl() {
	ginkgo.By("Redeploying EPP with flow control enabled")
	// Delete the existing ConfigMap first so kubectl apply detects the change.
	// Without this, kubectl apply sometimes reports "unchanged" when only the
	// embedded data string differs between multi-document YAML files.
	command := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"-n", nsName, "delete", "configmap", "epp-plugins-config", "--ignore-not-found")
	session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(30 * time.Second).Should(gexec.Exit(0))

	kubectlApplyFile(eppConfigFC, nil)

	// Rollout restart to pick up the new ConfigMap.
	command = exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"-n", nsName, "rollout", "restart", "deployment/epp")
	session, err = gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(30 * time.Second).Should(gexec.Exit(0))

	// Wait for the rollout to complete.
	command = exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"-n", nsName, "rollout", "status", "deployment/epp", "--timeout=120s")
	session, err = gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(180 * time.Second).Should(gexec.Exit(0))

	// Restart all processor deployments so they get fresh connections to
	// Envoy/EPP. Without this, processors that poll during the EPP restart
	// window can get 404s from Envoy that are classified as non-retryable.
	for _, deploy := range []string{
		"integration-llm-d-async",
		"pubsub-llm-d-async",
		"saturation-llm-d-async",
		"budget-llm-d-async",
		"redis-gate-llm-d-async",
		"quota-llm-d-async",
		"composite-llm-d-async",
		"prometheus-query-llm-d-async",
		"endpoint-scrape-llm-d-async",
		"short-drain-llm-d-async",
		"multitenant-llm-d-async",
		"tier-priority-llm-d-async",
		"mt-merge-llm-d-async",
		"benchmark-llm-d-async",
		"benchmark-pool-gate-llm-d-async",
		"benchmark-pubsub-llm-d-async",
	} {
		cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
			"-n", nsName, "rollout", "restart", "deployment/"+deploy)
		s, e := gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
		gomega.Expect(e).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(s).WithTimeout(30 * time.Second).Should(gexec.Exit(0))
	}
	for _, deploy := range []string{
		"integration-llm-d-async",
		"pubsub-llm-d-async",
		"saturation-llm-d-async",
		"budget-llm-d-async",
		"redis-gate-llm-d-async",
		"quota-llm-d-async",
		"composite-llm-d-async",
		"prometheus-query-llm-d-async",
		"endpoint-scrape-llm-d-async",
		"short-drain-llm-d-async",
		"multitenant-llm-d-async",
		"tier-priority-llm-d-async",
		"mt-merge-llm-d-async",
		"benchmark-llm-d-async",
		"benchmark-pool-gate-llm-d-async",
		"benchmark-pubsub-llm-d-async",
	} {
		cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
			"-n", nsName, "rollout", "status", "deployment/"+deploy, "--timeout=120s")
		s, e := gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
		gomega.Expect(e).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(s).WithTimeout(180 * time.Second).Should(gexec.Exit(0))
	}

	// Wait for the full Envoy → EPP → sim pipeline to be healthy by
	// sending a real inference request and checking for a 200 response.
	gomega.Eventually(func() error {
		body := []byte(`{"model":"test-model","prompt":"probe"}`)
		req, err := http.NewRequest(http.MethodPost, envoyURL+"/v1/completions", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close() //nolint:errcheck
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("envoy returned %d, want 200", resp.StatusCode)
		}
		return nil
	}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())
}

const kindClusterConfig = `
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- extraPortMappings:
  - containerPort: 30480
    hostPort: ${REDIS_PORT}
    protocol: TCP
  - containerPort: 30485
    hostPort: ${PUBSUB_PORT}
    protocol: TCP
  - containerPort: 30491
    hostPort: ${PROM_PORT}
    protocol: TCP
  - containerPort: 30490
    hostPort: ${SIM_PORT}
    protocol: TCP
  - containerPort: 30492
    hostPort: ${ENVOY_PORT}
    protocol: TCP
  - containerPort: 30493
    hostPort: ${ENVOY_ADMIN_PORT}
    protocol: TCP
  - containerPort: 30494
    hostPort: ${JAEGER_PORT}
    protocol: TCP
`

func setupPubSubEmulatorTopicsAndSubscriptions() {
	ginkgo.By("Setting up PubSub emulator topics and subscriptions")
	ctx := context.Background()

	pairs := []struct {
		reqTopic string
		reqSub   string
		resTopic string
		resSub   string
	}{
		{
			reqTopic: fmt.Sprintf("projects/%s/topics/%s", pubsubProjectID, pubsubRequestTopic),
			reqSub:   fmt.Sprintf("projects/%s/subscriptions/%s", pubsubProjectID, pubsubRequestSub),
			resTopic: fmt.Sprintf("projects/%s/topics/%s", pubsubProjectID, pubsubResultTopic),
			resSub:   fmt.Sprintf("projects/%s/subscriptions/%s", pubsubProjectID, pubsubResultSub),
		},
		{
			reqTopic: fmt.Sprintf("projects/%s/topics/%s", pubsubProjectID, pubsubBenchRequestTopic),
			reqSub:   fmt.Sprintf("projects/%s/subscriptions/%s", pubsubProjectID, pubsubBenchRequestSub),
			resTopic: fmt.Sprintf("projects/%s/topics/%s", pubsubProjectID, pubsubBenchResultTopic),
			resSub:   fmt.Sprintf("projects/%s/subscriptions/%s", pubsubProjectID, pubsubBenchResultSub),
		},
	}

	var client *pubsub.Client
	gomega.Eventually(func() error {
		var err error
		client, err = pubsub.NewClient(ctx, pubsubProjectID)
		if err != nil {
			return err
		}
		for _, p := range pairs {
			_, err = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: p.reqTopic})
			if err != nil && status.Code(err) != codes.AlreadyExists {
				_ = client.Close()
				return err
			}
		}
		return nil
	}, 120*time.Second, 2*time.Second).Should(gomega.Succeed())
	defer client.Close() //nolint:errcheck

	for _, p := range pairs {
		_, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: p.resTopic})
		if err != nil && status.Code(err) != codes.AlreadyExists {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		_, err = client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name:  p.reqSub,
			Topic: p.reqTopic,
		})
		if err != nil && status.Code(err) != codes.AlreadyExists {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		_, err = client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name:  p.resSub,
			Topic: p.resTopic,
		})
		if err != nil && status.Code(err) != codes.AlreadyExists {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}
}
