package e2e

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var _ = ginkgo.Describe("Health Probes", func() {
	ginkgo.It("exposes healthy /healthz and /readyz endpoints", func() {
		// Pick an ephemeral local port for port-forward.
		ln, err := net.Listen("tcp", ":0")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		localPort := ln.Addr().(*net.TCPAddr).Port
		gomega.Expect(ln.Close()).To(gomega.Succeed())

		// Port-forward the health port (8081) of the integration async-processor pod.
		cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
			"-n", nsName, "port-forward",
			"deployment/integration-async-processor",
			fmt.Sprintf("%d:8081", localPort))
		session, err := gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer session.Kill()

		baseURL := fmt.Sprintf("http://localhost:%d", localPort)

		// Wait for port-forward to be ready.
		gomega.Eventually(func() error {
			resp, err := http.Get(baseURL + "/healthz")
			if err != nil {
				return err
			}
			resp.Body.Close() //nolint:errcheck
			return nil
		}, 30*time.Second, 500*time.Millisecond).Should(gomega.Succeed())

		ginkgo.By("Verifying /healthz returns 200")
		resp, err := http.Get(baseURL + "/healthz")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close() //nolint:errcheck
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))

		var healthResp map[string]string
		gomega.Expect(json.NewDecoder(resp.Body).Decode(&healthResp)).To(gomega.Succeed())
		gomega.Expect(healthResp["status"]).To(gomega.Equal("ok"))

		ginkgo.By("Verifying /readyz returns 200")
		resp2, err := http.Get(baseURL + "/readyz")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp2.Body.Close() //nolint:errcheck
		gomega.Expect(resp2.StatusCode).To(gomega.Equal(http.StatusOK))

		var readyResp map[string]string
		gomega.Expect(json.NewDecoder(resp2.Body).Decode(&readyResp)).To(gomega.Succeed())
		gomega.Expect(readyResp["status"]).To(gomega.Equal("ready"))
	})
})
