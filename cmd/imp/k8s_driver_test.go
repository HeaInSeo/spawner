package imp

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/driver"
)

type testPrepared struct{ driver.BasePrepared }
type testHandle struct{ driver.BaseHandle }

func TestDriverK8sPrepare_ValidatesRequiredFields(t *testing.T) {
	d := NewK8s("default", k8sfake.NewSimpleClientset())

	if _, err := d.Prepare(context.Background(), api.RunSpec{}); err == nil {
		t.Fatal("expected missing RunID to fail")
	}

	if _, err := d.Prepare(context.Background(), api.RunSpec{RunID: "run-1"}); err == nil {
		t.Fatal("expected missing ImageRef to fail")
	}
}

func TestBuildJob_MapsSpecFields(t *testing.T) {
	spec := api.RunSpec{
		SpecVersion: 1,
		RunID:       "My.Run_01",
		ImageRef:    "busybox:1.36",
		Command:     []string{"sh", "-c", "echo hi"},
		WorkingDir:  "/workspace",
		Env: map[string]string{
			"FOO": "bar",
		},
		EnvFieldRefs: map[string]string{
			"NODE_NAME": "spec.nodeName",
		},
		Labels: map[string]string{
			"kueue.x-k8s.io/queue-name": "poc-standard-lq",
		},
		Annotations: map[string]string{
			"example.com/team": "genomics",
		},
		Mounts: []api.Mount{
			{Source: "pvc-a", Target: "/data", ReadOnly: false},
		},
		Resources:          api.Resources{CPU: "100m", Memory: "64Mi"},
		ServiceAccountName: "jumi-runner",
		CorrelationID:      "sample-001",
		Cleanup:            api.CleanupPolicy{TTLSecondsAfterFinished: 600},
		Placement:          &api.Placement{NodeSelector: map[string]string{"kubernetes.io/hostname": "lab-worker-1"}},
	}

	job := buildJob(spec, "default")

	if job.Name != "my-run-01" {
		t.Fatalf("unexpected sanitized job name: %q", job.Name)
	}
	if job.Namespace != "default" {
		t.Fatalf("unexpected namespace: %q", job.Namespace)
	}
	if job.Labels["pipeline-lite/run-id"] != spec.RunID {
		t.Fatalf("expected run-id label %q, got %q", spec.RunID, job.Labels["pipeline-lite/run-id"])
	}
	if q := job.Labels["kueue.x-k8s.io/queue-name"]; q != "poc-standard-lq" {
		t.Fatalf("unexpected queue label: %q", q)
	}
	if got := job.Annotations["example.com/team"]; got != "genomics" {
		t.Fatalf("unexpected annotation: %q", got)
	}
	if got := job.Annotations["spawner.correlation-id"]; got != "sample-001" {
		t.Fatalf("unexpected correlation annotation: %q", got)
	}
	if job.Spec.Suspend == nil || !*job.Spec.Suspend {
		t.Fatal("expected Job to be created suspended for Kueue")
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Fatal("expected backoffLimit=0")
	}
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != 600 {
		t.Fatalf("expected ttlSecondsAfterFinished=600, got %+v", job.Spec.TTLSecondsAfterFinished)
	}
	if len(job.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(job.Spec.Template.Spec.Containers))
	}
	c := job.Spec.Template.Spec.Containers[0]
	if c.Image != spec.ImageRef {
		t.Fatalf("unexpected image: %q", c.Image)
	}
	if c.WorkingDir != "/workspace" {
		t.Fatalf("unexpected working dir: %q", c.WorkingDir)
	}
	if got := envValueByName(c.Env, "FOO"); got != "bar" {
		t.Fatalf("unexpected env value: %q", got)
	}
	if got := envFieldRefByName(c.Env, "NODE_NAME"); got != "spec.nodeName" {
		t.Fatalf("unexpected env fieldRef: %q", got)
	}
	cpuQty := c.Resources.Requests[corev1.ResourceCPU]
	if got := cpuQty.String(); got != "100m" {
		t.Fatalf("unexpected cpu request: %q", got)
	}
	memQty := c.Resources.Requests[corev1.ResourceMemory]
	if got := memQty.String(); got != "64Mi" {
		t.Fatalf("unexpected memory request: %q", got)
	}
	if len(job.Spec.Template.Spec.Volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(job.Spec.Template.Spec.Volumes))
	}
	if got := job.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"]; got != "lab-worker-1" {
		t.Fatalf("unexpected nodeSelector: %q", got)
	}
	if job.Spec.Template.Spec.Affinity != nil {
		t.Fatal("expected no affinity for plain nodeSelector placement")
	}
	if got := job.Spec.Template.Spec.ServiceAccountName; got != "jumi-runner" {
		t.Fatalf("unexpected serviceAccountName: %q", got)
	}
	if got := job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != "pvc-a" {
		t.Fatalf("unexpected pvc claim: %q", got)
	}
	if len(c.VolumeMounts) != 1 || c.VolumeMounts[0].MountPath != "/data" {
		t.Fatal("expected volume mount at /data")
	}
	if got := job.Spec.Template.Annotations["spawner.correlation-id"]; got != "sample-001" {
		t.Fatalf("expected template correlation annotation, got %q", got)
	}
}

func TestBuildJob_MapsRequiredPlacementToHostnameSelector(t *testing.T) {
	spec := api.RunSpec{
		SpecVersion: 1,
		RunID:       "run-required-placement",
		ImageRef:    "busybox:1.36",
		Command:     []string{"sh", "-c", "echo hi"},
		Placement:   &api.Placement{RequiredNodeName: "lab-worker-2"},
	}

	job := buildJob(spec, "default")

	if got := job.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"]; got != "lab-worker-2" {
		t.Fatalf("required placement selector = %q, want lab-worker-2", got)
	}
	if job.Spec.Template.Spec.Affinity != nil {
		t.Fatal("expected no affinity for required-node-only placement")
	}
}

func TestBuildJob_MapsPreferredPlacementToNodeAffinity(t *testing.T) {
	spec := api.RunSpec{
		SpecVersion: 1,
		RunID:       "run-preferred-placement",
		ImageRef:    "busybox:1.36",
		Command:     []string{"sh", "-c", "echo hi"},
		Placement: &api.Placement{
			PreferredNodes: []api.WeightedNodePreference{
				{NodeName: "lab-worker-1", Weight: 100},
				{NodeName: "lab-worker-2", Weight: 50},
			},
		},
	}

	job := buildJob(spec, "default")

	if len(job.Spec.Template.Spec.NodeSelector) != 0 {
		t.Fatalf("expected no nodeSelector for preferred-only placement, got %+v", job.Spec.Template.Spec.NodeSelector)
	}
	affinity := job.Spec.Template.Spec.Affinity
	if affinity == nil || affinity.NodeAffinity == nil {
		t.Fatal("expected node affinity for preferred placement")
	}
	terms := affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 2 {
		t.Fatalf("preferred terms = %d, want 2", len(terms))
	}
	if terms[0].Weight != 100 || terms[0].Preference.MatchExpressions[0].Values[0] != "lab-worker-1" {
		t.Fatalf("unexpected first preferred term: %+v", terms[0])
	}
	if terms[1].Weight != 50 || terms[1].Preference.MatchExpressions[0].Values[0] != "lab-worker-2" {
		t.Fatalf("unexpected second preferred term: %+v", terms[1])
	}
}

func envValueByName(vars []corev1.EnvVar, name string) string {
	for _, v := range vars {
		if v.Name == name {
			return v.Value
		}
	}
	return ""
}

func envFieldRefByName(vars []corev1.EnvVar, name string) string {
	for _, v := range vars {
		if v.Name == name && v.ValueFrom != nil && v.ValueFrom.FieldRef != nil {
			return v.ValueFrom.FieldRef.FieldPath
		}
	}
	return ""
}

func TestDriverK8sStartAndCancel_WithFakeClient(t *testing.T) {
	cs := k8sfake.NewSimpleClientset()
	d := NewK8s("default", cs)

	p, err := d.Prepare(context.Background(), api.RunSpec{
		RunID:    "run-1",
		ImageRef: "busybox:1.36",
		Command:  []string{"sh", "-c", "echo hi"},
	})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	h, err := d.Start(context.Background(), p)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if _, err := cs.BatchV1().Jobs("default").Get(context.Background(), "run-1", metav1.GetOptions{}); err != nil {
		t.Fatalf("expected created job: %v", err)
	}

	if err := d.Cancel(context.Background(), h); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	if _, err := cs.BatchV1().Jobs("default").Get(context.Background(), "run-1", metav1.GetOptions{}); err == nil {
		t.Fatal("expected job to be deleted after Cancel")
	}
}

func TestDriverK8sWait_ContextCancelled(t *testing.T) {
	d := NewK8s("default", k8sfake.NewSimpleClientset())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := d.Wait(ctx, handleJob{name: "run-1", ns: "default"})
	if err == nil {
		t.Fatal("expected Wait to return context cancellation")
	}
}

func TestDriverK8sWait_ReturnsSucceededEvent(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default"},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			},
		},
	}
	d := NewK8s("default", k8sfake.NewSimpleClientset(job))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ev, err := d.Wait(ctx, handleJob{name: "run-1", ns: "default"})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if ev.State != api.StateSucceeded {
		t.Fatalf("expected succeeded event, got %s", ev.State)
	}
	if ev.RunID != "run-1" {
		t.Fatalf("unexpected RunID: %q", ev.RunID)
	}
}

func TestDriverK8sWait_ReturnsFailedEvent(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default"},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "boom"},
			},
		},
	}
	d := NewK8s("default", k8sfake.NewSimpleClientset(job))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ev, err := d.Wait(ctx, handleJob{name: "run-1", ns: "default"})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if ev.State != api.StateFailed {
		t.Fatalf("expected failed event, got %s", ev.State)
	}
	if ev.Message != "boom" {
		t.Fatalf("unexpected failure message: %q", ev.Message)
	}
}

func TestDriverK8sHandleMismatchErrors(t *testing.T) {
	d := NewK8s("default", k8sfake.NewSimpleClientset())

	if _, err := d.Start(context.Background(), testPrepared{}); err == nil {
		t.Fatal("expected Start to reject foreign prepared type")
	}
	if _, err := d.Wait(context.Background(), testHandle{}); err == nil {
		t.Fatal("expected Wait to reject foreign handle type")
	}
	if err := d.Signal(context.Background(), testHandle{}, api.Signal{}); err == nil {
		t.Fatal("expected Signal to reject foreign handle type")
	}
	if err := d.Cancel(context.Background(), testHandle{}); err == nil {
		t.Fatal("expected Cancel to reject foreign handle type")
	}
}

var _ driver.Prepared = preparedJob{}
var _ driver.Handle = handleJob{}
