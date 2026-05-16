//go:build integration

package imp

import (
	"context"
	"os"
	"testing"
	"time"

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
	t.Log("Cancel OK — Job deleted")
}
