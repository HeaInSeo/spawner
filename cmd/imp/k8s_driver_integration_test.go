//go:build integration

package imp

import (
	"context"
	"os"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/HeaInSeo/spawner/pkg/api"
)

// Run with:
//   KUBECONFIG=~/.kube/config go test -v -tags=integration -run TestDriverK8s_Smoke ./cmd/imp/
//
// Prereqs: kind-poc cluster running, Kueue installed, LocalQueue poc-standard-lq exists in default ns.

func TestDriverK8s_Smoke(t *testing.T) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("HOME") + "/.kube/config"
	}

	drv, err := NewK8sFromKubeconfig("default", kubeconfig)
	if err != nil {
		t.Fatalf("NewK8sFromKubeconfig: %v", err)
	}

	spec := api.RunSpec{
		RunID:    "smoke-test-001",
		ImageRef: "busybox:1.36",
		Command:  []string{"sh", "-c", "echo hello-poc && sleep 2"},
		Labels: map[string]string{
			"kueue.x-k8s.io/queue-name": "poc-standard-lq",
		},
		Resources: api.Resources{CPU: "100m", Memory: "64Mi"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Prepare
	p, err := drv.Prepare(ctx, spec)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	t.Log("Prepare OK")

	// Start
	h, err := drv.Start(ctx, p)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Log("Start OK — Job submitted (suspend=true, Kueue will admit)")

	// Wait
	ev, err := drv.Wait(ctx, h)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	t.Logf("Wait OK — state=%s", ev.State)

	if ev.State != api.StateSucceeded {
		t.Errorf("expected StateSucceeded, got %s: %s", ev.State, ev.Message)
	}

	// Cleanup: Cancel is idempotent after Job completes (delete if still exists)
	_ = drv.Cancel(context.Background(), h)
}

func TestDriverK8s_Cancel(t *testing.T) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("HOME") + "/.kube/config"
	}

	drv, err := NewK8sFromKubeconfig("default", kubeconfig)
	if err != nil {
		t.Fatalf("NewK8sFromKubeconfig: %v", err)
	}

	spec := api.RunSpec{
		RunID:    "cancel-test-001",
		ImageRef: "busybox:1.36",
		Command:  []string{"sh", "-c", "sleep 300"},
		Labels: map[string]string{
			"kueue.x-k8s.io/queue-name": "poc-standard-lq",
		},
		Resources: api.Resources{CPU: "100m", Memory: "64Mi"},
	}

	ctx := context.Background()

	p, err := drv.Prepare(ctx, spec)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	h, err := drv.Start(ctx, p)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Log("Job submitted")

	// Cancel immediately
	if err := drv.Cancel(ctx, h); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	t.Log("Cancel accepted — waiting for background propagation to actually remove the Job and its pods")

	hd, ok := h.(handleJob)
	if !ok {
		t.Fatalf("handle type assertion failed: expected handleJob, got %T", h)
	}

	// Cancel only issues a background-propagation delete: the API server
	// can accept it while a finalizer blocks removal, or an orphaning-policy
	// regression could leave pods behind even though the Job itself is
	// gone. Assert both actually disappear rather than trusting Cancel's
	// nil error alone.
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, err := drv.clientset.BatchV1().Jobs(hd.ns).Get(ctx, hd.name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatalf("get job %s during cancel-propagation poll: %v", hd.name, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s was not removed within 60s of Cancel (background propagation stuck or blocked by a finalizer)", hd.name)
		}
		time.Sleep(1 * time.Second)
	}
	t.Log("Job removed")

	deadline = time.Now().Add(60 * time.Second)
	for {
		pods, err := drv.clientset.CoreV1().Pods(hd.ns).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + hd.name})
		if err != nil {
			t.Fatalf("list pods for job %s during cancel-propagation poll: %v", hd.name, err)
		}
		if len(pods.Items) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s still has %d pod(s) 60s after Cancel — background propagation did not remove them", hd.name, len(pods.Items))
		}
		time.Sleep(1 * time.Second)
	}
	t.Log("Cancel OK — Job and its pods removed")
}
