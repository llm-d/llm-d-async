package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/llm-d/llm-d-async/api"
)

const (
	multiReplicaDeployment = "multi-replica-llm-d-async"
	multiReplicaInstance   = "multi-replica"
	multiReplicaWorkers    = 2
	multiReplicaLeaseTTL   = 6 * time.Second
	multiReplicaReclaim    = 500 * time.Millisecond
	reclaimScheduleBound   = 5 * time.Second
)

var _ = ginkgo.Describe("Multi-replica Redis Sorted Set E2E", ginkgo.Ordered, func() {
	var ctx context.Context

	ginkgo.BeforeAll(func() {
		ctx = context.Background()
		redeployEPPWithFlowControl()
		scaleAsyncDeployment(multiReplicaDeployment, 2)
	})

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		setEnvoyFaultDelay(envoyAdminURL, 0)
		clearMultiReplicaState(ctx)
		gomega.Expect(rdb.Set(ctx, multiReplicaBudgetKey, "1", 0).Err()).To(gomega.Succeed())
		scaleAsyncDeployment(multiReplicaDeployment, 2)
	})

	ginkgo.AfterEach(func() {
		setEnvoyFaultDelay(envoyAdminURL, 0)
		clearMultiReplicaState(ctx)
	})

	ginkgo.AfterAll(func() {
		scaleAsyncDeployment(multiReplicaDeployment, 1)
	})

	ginkgo.It("scales worker capacity linearly with ready replicas", func() {
		assertWorkerLimits(multiReplicaInstance, 2, multiReplicaWorkers)

		scaleAsyncDeployment(multiReplicaDeployment, 3)
		assertWorkerLimits(multiReplicaInstance, 3, multiReplicaWorkers)
	})

	ginkgo.It("dispatches exactly once while scaling up and down under load", func() {
		const requests = 24
		initialPods := asyncPodNames(multiReplicaInstance)
		gomega.Expect(initialPods).To(gomega.HaveLen(2))
		// The dispatch counter is created lazily, so a pod with no dispatches has
		// no series yet; a missing baseline is therefore equivalent to zero.
		dispatchBaseline := asyncMetricByPod(multiReplicaInstance,
			"llm_d_async_async_dispatched_requests_total")
		setEnvoyFaultDelay(envoyAdminURL, 100)

		for i := range requests {
			enqueueMessage(ctx, rdb, multiReplicaRequestQueue,
				makeTrackedRequestMessage(fmt.Sprintf("multi-scale-%02d", i), 5*time.Minute))
		}
		assertPodsDispatchingAtCapacity(multiReplicaInstance, initialPods, dispatchBaseline, multiReplicaWorkers)

		// Add the third replica while both existing replicas are actively dispatching.
		scaleAsyncDeployment(multiReplicaDeployment, 3)
		threePods := asyncPodNames(multiReplicaInstance)
		gomega.Expect(threePods).To(gomega.HaveLen(3))
		assertWorkerLimits(multiReplicaInstance, 3, multiReplicaWorkers)
		assertPodsDispatchingAtCapacity(multiReplicaInstance, threePods, dispatchBaseline, multiReplicaWorkers)

		// Remove one replica while all three still have active downstream calls.
		scaleAsyncDeployment(multiReplicaDeployment, 2)
		twoPods := asyncPodNames(multiReplicaInstance)
		gomega.Expect(twoPods).To(gomega.HaveLen(2))
		assertWorkerLimits(multiReplicaInstance, 2, multiReplicaWorkers)
		assertPodsInFlightAtCapacity(multiReplicaInstance, twoPods, multiReplicaWorkers)
		setEnvoyFaultDelay(envoyAdminURL, 0)

		assertExactTerminalResults(ctx, "multi-scale-", requests, 90*time.Second)
		assertSimulatorDispatchesExactlyOnce("multi-scale-", requests)
	})

	ginkgo.It("redelivers after lease expiry within the reclaim and scheduling bound", func() {
		scaleAsyncDeployment(multiReplicaDeployment, 1)
		claimant := asyncPodNames(multiReplicaInstance)
		gomega.Expect(claimant).To(gomega.HaveLen(1))

		setEnvoyFaultDelay(envoyAdminURL, 100)
		enqueueMessage(ctx, rdb, multiReplicaRequestQueue,
			makeTrackedRequestMessage("multi-kill-00", 5*time.Minute))
		gomega.Eventually(func() int64 {
			return rdb.HLen(ctx, multiReplicaRequestQueue+":claimed").Val()
		}, 30*time.Second, 250*time.Millisecond).Should(gomega.Equal(int64(1)))

		// Add a survivor before killing the only pod that could own the claim.
		scaleAsyncDeployment(multiReplicaDeployment, 2)
		gomega.Expect(rdb.HLen(ctx, multiReplicaRequestQueue+":claimed").Val()).To(gomega.Equal(int64(1)))
		gomega.Expect(getResultCount(ctx, rdb, multiReplicaResultQueue)).To(gomega.Equal(int64(0)))
		deleteAsyncPod(claimant[0])
		expiry := onlyClaimExpiry(ctx, multiReplicaRequestQueue)
		redisNow, err := rdb.Time(ctx).Result()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		remainingLease := expiry.Sub(redisNow)
		gomega.Expect(remainingLease).To(gomega.BeNumerically(">", 0))
		gomega.Expect(remainingLease).To(gomega.BeNumerically("<=", multiReplicaLeaseTTL+time.Second))

		started := time.Now()
		setEnvoyFaultDelay(envoyAdminURL, 0)
		if beforeExpiry := remainingLease - 500*time.Millisecond; beforeExpiry > 0 {
			gomega.Consistently(func() int64 {
				return getResultCount(ctx, rdb, multiReplicaResultQueue)
			}, beforeExpiry, 100*time.Millisecond).Should(gomega.Equal(int64(0)),
				"the survivor must not reclaim before the recorded lease expiry")
		}

		completionBound := remainingLease + multiReplicaReclaim + reclaimScheduleBound
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, multiReplicaResultQueue)
		}, completionBound, 100*time.Millisecond).Should(gomega.Equal(int64(1)))
		elapsed := time.Since(started)
		gomega.Expect(elapsed).To(gomega.BeNumerically(">=", remainingLease-time.Second),
			"redelivery must follow lease expiry")
		gomega.Expect(elapsed).To(gomega.BeNumerically("<", completionBound),
			"redelivery must finish within remaining TTL + reclaim interval + scheduling bound")
		assertExactTerminalResults(ctx, "multi-kill-", 1, time.Second)
		assertSimulatorDispatchesExactlyOnce("multi-kill-", 1)
	})

	ginkgo.It("holds and releases a batch across three replicas when saturation changes", func() {
		const requests = 18
		scaleAsyncDeployment("saturation-llm-d-async", 3)
		ginkgo.DeferCleanup(func() {
			scaleAsyncDeployment("saturation-llm-d-async", 1)
			setSimWaitingRequests(simAdminURL, 0)
			clearQueueState(ctx, saturationRequestQueue, saturationResultQueue)
		})

		clearQueueState(ctx, saturationRequestQueue, saturationResultQueue)
		setSimWaitingRequests(simAdminURL, 10)
		waitForSaturation(promURL, envoyURL, func(v float64) bool { return v >= 0.7 })
		gateBaseline := waitForGateCounterCount(3)

		for i := range requests {
			enqueueMessage(ctx, rdb, saturationRequestQueue,
				makeTrackedRequestMessage(fmt.Sprintf("multi-saturation-%02d", i), 5*time.Minute))
		}
		type queueState struct{ pending, claimed, results int64 }
		gomega.Consistently(func() queueState {
			return queueState{
				pending: rdb.ZCard(ctx, saturationRequestQueue).Val(),
				claimed: rdb.HLen(ctx, saturationRequestQueue+":claimed").Val(),
				results: getResultCount(ctx, rdb, saturationResultQueue),
			}
		}, 8*time.Second, time.Second).Should(gomega.Equal(queueState{pending: requests}),
			"saturation must leave the full batch pending and unclaimed")
		assertPodCountersAdvanced("saturation", gateBaseline, 3)

		setSimWaitingRequests(simAdminURL, 0)
		assertExactResultsFromQueue(ctx, saturationResultQueue, "multi-saturation-", requests, 90*time.Second)
		assertSimulatorDispatchesExactlyOnce("multi-saturation-", requests)
	})
})

func scaleAsyncDeployment(deployment string, replicas int) {
	cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"-n", nsName, "scale", "deployment/"+deployment,
		fmt.Sprintf("--replicas=%d", replicas), "--timeout=60s")
	session, err := gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gomega.EventuallyWithOffset(1, session).WithTimeout(60 * time.Second).Should(gexec.Exit(0))

	cmd = exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"-n", nsName, "rollout", "status", "deployment/"+deployment, "--timeout=120s")
	session, err = gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gomega.EventuallyWithOffset(1, session).WithTimeout(150 * time.Second).Should(gexec.Exit(0))

	gomega.EventuallyWithOffset(1, func() int {
		return readyReplicaCount(deployment)
	}, 90*time.Second, time.Second).Should(gomega.Equal(replicas))
}

func readyReplicaCount(deployment string) int {
	cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"-n", nsName, "get", "deployment/"+deployment,
		"-o", "jsonpath={.status.readyReplicas}")
	out, err := cmd.Output()
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	if len(out) == 0 {
		return 0
	}
	count, err := strconv.Atoi(string(out))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return count
}

func asyncPodNames(instance string) []string {
	cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"-n", nsName, "get", "pods",
		"-l", "app.kubernetes.io/instance="+instance,
		"-o", "json")
	out, err := cmd.Output()
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	var pods struct {
		Items []struct {
			Metadata struct {
				Name              string  `json:"name"`
				DeletionTimestamp *string `json:"deletionTimestamp"`
			} `json:"metadata"`
			Status struct {
				Phase string `json:"phase"`
			} `json:"status"`
		} `json:"items"`
	}
	gomega.ExpectWithOffset(1, json.Unmarshal(out, &pods)).To(gomega.Succeed())
	names := make([]string, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.Metadata.DeletionTimestamp == nil && pod.Status.Phase == "Running" {
			names = append(names, pod.Metadata.Name)
		}
	}
	return names
}

func deleteAsyncPod(name string) {
	cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"-n", nsName, "delete", "pod", name, "--grace-period=0", "--force", "--wait=true", "--timeout=30s")
	session, err := gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gomega.EventuallyWithOffset(1, session).WithTimeout(30 * time.Second).Should(gexec.Exit(0))
	gomega.EventuallyWithOffset(1, func() bool {
		get := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
			"-n", nsName, "get", "pod", name)
		return get.Run() != nil
	}, 30*time.Second, 250*time.Millisecond).Should(gomega.BeTrue(), "deleted claim owner must be gone")
}

func assertWorkerLimits(instance string, replicas, workers int) {
	podQuery := fmt.Sprintf(`max by (pod) (llm_d_async_async_pool_worker_limit{job="async-processors",pod=~"%s-.*"})`, instance)
	gomega.EventuallyWithOffset(1, func() bool {
		values := queryPromVectorByLabel(promURL, podQuery, "pod")
		if len(values) != replicas {
			return false
		}
		for _, value := range values {
			if value != float64(workers) {
				return false
			}
		}
		return true
	}, 30*time.Second, time.Second).Should(gomega.BeTrue(), "each ready pod must report W workers")

	aggregateQuery := fmt.Sprintf(`sum(llm_d_async_async_pool_worker_limit{job="async-processors",pod=~"%s-.*"})`, instance)
	gomega.EventuallyWithOffset(1, func() float64 {
		return queryProm(promURL, aggregateQuery)
	}, 30*time.Second, time.Second).Should(gomega.Equal(float64(replicas*workers)),
		"aggregate worker capacity must equal N x W")
}

func asyncMetricByPod(instance, metric string) map[string]float64 {
	query := fmt.Sprintf(`sum by (pod) (%s{job="async-processors",pod=~"%s-.*"})`, metric, instance)
	return queryPromVectorByLabel(promURL, query, "pod")
}

func assertPodsDispatchingAtCapacity(instance string, pods []string, baseline map[string]float64, workers int) {
	dispatchMetric := "llm_d_async_async_dispatched_requests_total"
	inflightMetric := "llm_d_async_async_inflight_requests"
	gomega.EventuallyWithOffset(1, func() bool {
		dispatched := asyncMetricByPod(instance, dispatchMetric)
		inflight := asyncMetricByPod(instance, inflightMetric)
		for _, pod := range pods {
			if dispatched[pod] <= baseline[pod] || inflight[pod] != float64(workers) {
				return false
			}
		}
		return true
	}, 30*time.Second, time.Second).Should(gomega.BeTrue(),
		"every ready pod must dispatch and hold W in-flight requests")
	assertPodsInFlightAtCapacity(instance, pods, workers)
}

func assertPodsInFlightAtCapacity(instance string, pods []string, workers int) {
	inflightMetric := "llm_d_async_async_inflight_requests"
	gomega.EventuallyWithOffset(1, func() bool {
		inflight := asyncMetricByPod(instance, inflightMetric)
		if len(inflight) != len(pods) {
			return false
		}
		for _, pod := range pods {
			if inflight[pod] != float64(workers) {
				return false
			}
		}
		return true
	}, 30*time.Second, time.Second).Should(gomega.BeTrue(),
		"per-pod in-flight metrics must show W active dispatches")

	aggregateQuery := fmt.Sprintf(`sum(llm_d_async_async_inflight_requests{job="async-processors",pod=~"%s-.*"})`, instance)
	gomega.EventuallyWithOffset(1, func() float64 {
		return queryProm(promURL, aggregateQuery)
	}, 30*time.Second, time.Second).Should(gomega.Equal(float64(len(pods)*workers)),
		"aggregate in-flight metric must equal N x W")
}

func assertPodCountersAdvanced(instance string, baseline map[string]float64, replicas int) {
	gomega.EventuallyWithOffset(1, func() bool {
		current := saturationGateCountersByPod(instance)
		if len(current) != replicas {
			return false
		}
		for pod, value := range current {
			if value <= baseline[pod] {
				return false
			}
		}
		return true
	}, 30*time.Second, time.Second).Should(gomega.BeTrue(),
		"every replica must report a new gate-closed decision")
}

func waitForGateCounterCount(replicas int) map[string]float64 {
	var values map[string]float64
	gomega.EventuallyWithOffset(1, func() int {
		values = saturationGateCountersByPod("saturation")
		return len(values)
	}, 30*time.Second, time.Second).Should(gomega.Equal(replicas))
	return values
}

func saturationGateCountersByPod(instance string) map[string]float64 {
	query := fmt.Sprintf(`sum by (pod) (llm_d_async_async_gate_decisions_total{job="async-processors",pod=~"%s-.*",queue_name="saturation-request-sortedset",reason="gate_closed"})`, instance)
	return queryPromVectorByLabel(promURL, query, "pod")
}

func onlyClaimExpiry(ctx context.Context, queue string) time.Time {
	claims, err := rdb.ZRangeWithScores(ctx, queue+":claims-idx", 0, -1).Result()
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gomega.ExpectWithOffset(1, claims).To(gomega.HaveLen(1))
	return time.Unix(int64(math.Ceil(claims[0].Score)), 0)
}

func clearQueueState(ctx context.Context, requestQueue, resultQueue string) {
	keys := []string{
		requestQueue,
		resultQueue,
		requestQueue + ":claimed",
		requestQueue + ":claim-owners",
		requestQueue + ":claims-idx",
	}
	gomega.ExpectWithOffset(1, rdb.Del(ctx, keys...).Err()).To(gomega.Succeed())
}

func makeTrackedRequestMessage(id string, deadlineOffset time.Duration) api.RequestMessage {
	msg := makeRequestMessage(id, deadlineOffset)
	msg.Payload = testPayload(map[string]any{"model": "test-model", "prompt": "test"})
	msg.Headers = map[string]string{"x-e2e-request-id": id}
	return msg
}

func assertSimulatorDispatchesExactlyOnce(prefix string, count int) {
	pattern := regexp.MustCompile(regexp.QuoteMeta(prefix) + `[0-9]{2}`)
	gomega.EventuallyWithOffset(1, func() bool {
		cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
			"-n", nsName, "logs", "deployment/llm-sim")
		out, err := cmd.Output()
		if err != nil {
			return false
		}
		dispatches := make(map[string]int, count)
		for _, line := range strings.Split(string(out), "\n") {
			for _, id := range pattern.FindAllString(line, -1) {
				dispatches[id]++
			}
		}
		if len(dispatches) != count {
			return false
		}
		for i := range count {
			id := fmt.Sprintf("%s%02d", prefix, i)
			if dispatches[id] != 1 {
				return false
			}
		}
		return true
	}, 30*time.Second, time.Second).Should(gomega.BeTrue(),
		"the simulator must observe exactly one dispatch for every request ID")
}

func clearMultiReplicaState(ctx context.Context) {
	keys := []string{
		multiReplicaRequestQueue,
		multiReplicaResultQueue,
		multiReplicaBudgetKey,
		multiReplicaRequestQueue + ":claimed",
		multiReplicaRequestQueue + ":claim-owners",
		multiReplicaRequestQueue + ":claims-idx",
	}
	gomega.ExpectWithOffset(1, rdb.Del(ctx, keys...).Err()).To(gomega.Succeed())
}

func assertExactTerminalResults(ctx context.Context, prefix string, count int, timeout time.Duration) {
	assertExactResultsFromQueue(ctx, multiReplicaResultQueue, prefix, count, timeout)
}

func assertExactResultsFromQueue(ctx context.Context, queue, prefix string, count int, timeout time.Duration) {
	gomega.EventuallyWithOffset(1, func() int64 {
		return getResultCount(ctx, rdb, queue)
	}, timeout, 500*time.Millisecond).Should(gomega.Equal(int64(count)))

	seen := make(map[string]struct{}, count)
	for range count {
		result := popResult(ctx, rdb, queue)
		gomega.ExpectWithOffset(1, result).NotTo(gomega.BeNil())
		gomega.ExpectWithOffset(1, result.ID).To(gomega.HavePrefix(prefix))
		gomega.ExpectWithOffset(1, result.StatusCode).To(gomega.Equal(http.StatusOK), "result %+v", result)
		gomega.ExpectWithOffset(1, result.ErrorCode).To(gomega.BeEmpty(), "result %+v", result)
		gomega.ExpectWithOffset(1, seen).NotTo(gomega.HaveKey(result.ID),
			"each request must have exactly one terminal record")
		seen[result.ID] = struct{}{}
	}
	gomega.ExpectWithOffset(1, seen).To(gomega.HaveLen(count))
	gomega.ConsistentlyWithOffset(1, func() int64 {
		return getResultCount(ctx, rdb, queue)
	}, 3*time.Second, 500*time.Millisecond).Should(gomega.Equal(int64(0)))
}
