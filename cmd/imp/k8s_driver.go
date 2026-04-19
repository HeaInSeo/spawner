package imp

import (
	"context"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/seoyhaein/spawner/pkg/api"
	"github.com/seoyhaein/spawner/pkg/driver"
)

// preparedJob holds the K8s Job object built during Prepare.
type preparedJob struct {
	driver.BasePrepared
	job *batchv1.Job
}

// handleJob holds the identity of a submitted Job.
type handleJob struct {
	driver.BaseHandle
	name string
	ns   string
}

// DriverK8s implements driver.Driver using the Kubernetes batch/v1 Job API.
// Kueue integration is optional: if the queue-name label is present, the Job
// starts suspended so Kueue can admit it; otherwise it starts immediately as a
// plain Kubernetes Job.
type DriverK8s struct {
	driver.BaseDriver
	ns        string
	clientset kubernetes.Interface
}

var _ driver.Driver = (*DriverK8s)(nil)

// NewK8s creates a DriverK8s with a pre-built clientset (for testing).
func NewK8s(ns string, clientset kubernetes.Interface) *DriverK8s {
	return &DriverK8s{ns: ns, clientset: clientset}
}

// NewK8sFromKubeconfig creates a DriverK8s using the given kubeconfig path.
// Pass "" to try in-cluster config first, then fall back to the default
// kubeconfig (~/.kube/config or KUBECONFIG env var).
func NewK8sFromKubeconfig(ns, kubeconfigPath string) (*DriverK8s, error) {
	cfg, err := buildConfig(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("k8s config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("k8s clientset: %w", err)
	}
	return NewK8s(ns, cs), nil
}

// Prepare validates the RunSpec and builds the batchv1.Job object.
// No K8s API call is made here.
func (d *DriverK8s) Prepare(ctx context.Context, spec api.RunSpec) (driver.Prepared, error) {
	if spec.RunID == "" {
		return nil, fmt.Errorf("RunID is required")
	}
	if spec.ImageRef == "" {
		return nil, fmt.Errorf("ImageRef is required")
	}
	job := buildJob(spec, d.ns)
	return preparedJob{job: job}, nil
}

// Start submits the prepared Job to Kubernetes.
func (d *DriverK8s) Start(ctx context.Context, p driver.Prepared) (driver.Handle, error) {
	pp, ok := p.(preparedJob)
	if !ok {
		return nil, fmt.Errorf("prepared origin mismatch: expected preparedJob")
	}
	created, err := d.clientset.BatchV1().Jobs(d.ns).Create(ctx, pp.job, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create job %s: %w", pp.job.Name, err)
	}
	return handleJob{name: created.Name, ns: created.Namespace}, nil
}

// Wait polls the Job until it succeeds, fails, or ctx is cancelled.
// Poll interval: 2s. Suitable for PoC; replace with Watch for production.
func (d *DriverK8s) Wait(ctx context.Context, h driver.Handle) (api.Event, error) {
	hd, ok := h.(handleJob)
	if !ok {
		return api.Event{}, fmt.Errorf("handle origin mismatch: expected handleJob")
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return api.Event{}, ctx.Err()
		case <-ticker.C:
			job, err := d.clientset.BatchV1().Jobs(hd.ns).Get(ctx, hd.name, metav1.GetOptions{})
			if err != nil {
				return api.Event{}, fmt.Errorf("get job %s: %w", hd.name, err)
			}
			if jobSucceeded(job) {
				return api.Event{
					RunID: hd.name,
					When:  time.Now(),
					State: api.StateSucceeded,
				}, nil
			}
			if jobFailed(job) {
				return api.Event{
					RunID:   hd.name,
					When:    time.Now(),
					State:   api.StateFailed,
					Message: jobFailureMessage(job),
				}, nil
			}
		}
	}
}

// Signal is not implemented for K8s Jobs in this PoC.
func (d *DriverK8s) Signal(_ context.Context, h driver.Handle, _ api.Signal) error {
	if _, ok := h.(handleJob); !ok {
		return fmt.Errorf("handle origin mismatch")
	}
	return nil
}

// Cancel deletes the Job and its pods (background propagation).
func (d *DriverK8s) Cancel(ctx context.Context, h driver.Handle) error {
	hd, ok := h.(handleJob)
	if !ok {
		return fmt.Errorf("handle origin mismatch: expected handleJob")
	}
	propagation := metav1.DeletePropagationBackground
	err := d.clientset.BatchV1().Jobs(hd.ns).Delete(ctx, hd.name, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
	if err != nil {
		return fmt.Errorf("delete job %s: %w", hd.name, err)
	}
	return nil
}

// buildJob constructs a batchv1.Job from a RunSpec.
// Key behaviours:
//   - suspend: true only when kueue.x-k8s.io/queue-name is present
//   - plain Kubernetes Jobs start unsuspended by default
//   - PVC volumes — each Mount.Source is treated as a PVC claim name
func buildJob(spec api.RunSpec, ns string) *batchv1.Job {
	jobName := sanitizeName(spec.RunID)

	labels := make(map[string]string, len(spec.Labels)+1)
	labels["pipeline-lite/run-id"] = spec.RunID
	for k, v := range spec.Labels {
		labels[k] = v
	}

	annotations := make(map[string]string, len(spec.Annotations)+1)
	for k, v := range spec.Annotations {
		annotations[k] = v
	}
	if spec.CorrelationID != "" {
		annotations["spawner.correlation-id"] = spec.CorrelationID
	}

	container := corev1.Container{
		Name:            "main",
		Image:           spec.ImageRef,
		Command:         spec.Command,
		Env:             buildEnvVars(spec.Env, spec.EnvFieldRefs),
		Resources:       buildResources(spec.Resources),
		ImagePullPolicy: corev1.PullIfNotPresent,
	}

	volumes, volumeMounts := buildVolumes(spec.Mounts)
	container.VolumeMounts = volumeMounts

	_, useKueue := labels["kueue.x-k8s.io/queue-name"]
	suspend := useKueue
	backoffLimit := int32(0) // PoC: no retry; fail immediately on pod failure
	var ttlSecondsAfterFinished *int32
	if spec.Cleanup.TTLSecondsAfterFinished > 0 {
		ttl := spec.Cleanup.TTLSecondsAfterFinished
		ttlSecondsAfterFinished = &ttl
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   ns,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			Suspend:                 &suspend,
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: ttlSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: annotations,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{container},
					Volumes:       volumes,
					NodeSelector:  buildNodeSelector(spec.Placement),
				},
			},
		},
	}
}

func buildEnvVars(env map[string]string, fieldRefs map[string]string) []corev1.EnvVar {
	vars := make([]corev1.EnvVar, 0, len(env)+len(fieldRefs))
	for k, v := range env {
		vars = append(vars, corev1.EnvVar{Name: k, Value: v})
	}
	for k, path := range fieldRefs {
		vars = append(vars, corev1.EnvVar{
			Name: k,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: path},
			},
		})
	}
	return vars
}

func buildNodeSelector(p *api.Placement) map[string]string {
	if p == nil || len(p.NodeSelector) == 0 {
		return nil
	}
	out := make(map[string]string, len(p.NodeSelector))
	for k, v := range p.NodeSelector {
		out[k] = v
	}
	return out
}

func buildResources(r api.Resources) corev1.ResourceRequirements {
	req := corev1.ResourceList{}
	lim := corev1.ResourceList{}
	if r.CPU != "" {
		q := resource.MustParse(r.CPU)
		req[corev1.ResourceCPU] = q
		lim[corev1.ResourceCPU] = q
	}
	if r.Memory != "" {
		q := resource.MustParse(r.Memory)
		req[corev1.ResourceMemory] = q
		lim[corev1.ResourceMemory] = q
	}
	return corev1.ResourceRequirements{Requests: req, Limits: lim}
}

// buildVolumes converts RunSpec.Mounts to K8s Volumes + VolumeMounts.
// Each Mount.Source is treated as a PVC claim name.
func buildVolumes(mounts []api.Mount) ([]corev1.Volume, []corev1.VolumeMount) {
	volumes := make([]corev1.Volume, 0, len(mounts))
	volumeMounts := make([]corev1.VolumeMount, 0, len(mounts))
	for i, m := range mounts {
		volName := fmt.Sprintf("vol-%d", i)
		volumes = append(volumes, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: m.Source,
					ReadOnly:  m.ReadOnly,
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      volName,
			MountPath: m.Target,
			ReadOnly:  m.ReadOnly,
		})
	}
	return volumes, volumeMounts
}

// sanitizeName converts a RunID into a valid K8s resource name.
// K8s names: lowercase alphanumeric and '-', max 63 chars.
func sanitizeName(id string) string {
	s := strings.ToLower(id)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	name := strings.Trim(b.String(), "-")
	if len(name) > 63 {
		name = name[:63]
	}
	return name
}

func jobSucceeded(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFailureMessage(job *batchv1.Job) string {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed {
			return c.Message
		}
	}
	return "job failed"
}
