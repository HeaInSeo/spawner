package runtime

import (
	"fmt"
	"time"
)

// AttemptState is the normalized lifecycle state of a single execution attempt.
//
// External state progression (visible to JUMI via WatchAttempt):
//
//	Submitted → Pending → Running → Succeeded
//	                              ↘ Failed
//	                              ↘ Cancelled
//
// Any non-terminal state may transition directly to Failed or Cancelled.
// Terminal states: Succeeded, Failed, Cancelled.
//
// Accepted is a Runtime-internal state and is NOT emitted via WatchAttempt.
// It represents the brief window between SubmitAttempt() call and
// JobClient.Create() success. JUMI never observes this state.
type AttemptState string

const (
	// AttemptStateAccepted is Runtime-internal only. NOT emitted via WatchAttempt.
	AttemptStateAccepted  AttemptState = "accepted"
	AttemptStateSubmitted AttemptState = "submitted"
	AttemptStatePending   AttemptState = "pending"
	AttemptStateRunning   AttemptState = "running"
	AttemptStateSucceeded AttemptState = "succeeded"
	AttemptStateFailed    AttemptState = "failed"
	AttemptStateCancelled AttemptState = "cancelled"
)

func (s AttemptState) IsTerminal() bool {
	return s == AttemptStateSucceeded ||
		s == AttemptStateFailed ||
		s == AttemptStateCancelled
}

// Reason constants for AttemptEvent.Reason and JobWatchError.Reason.
const (
	ReasonDeadlineExceeded = "deadline-exceeded"
	ReasonOOMKilled        = "oom-killed"
	ReasonNodeLost         = "node-lost"
	ReasonImagePullFailed  = "image-pull-failed"
	ReasonUnschedulable    = "unschedulable"
	ReasonBackoffExceeded  = "backoff-exceeded"

	ReasonUserCancel   = "user-cancel"
	ReasonSystemCancel = "system-cancel"

	ReasonKueueWaiting      = "kueue-waiting"
	ReasonScheduling        = "scheduling"
	ReasonImagePulling      = "image-pulling"
	ReasonContainerCreating = "container-creating"

	ReasonWatchDisconnected     = "watch-disconnected"
	ReasonResourceVersionTooOld = "resource-version-too-old"
	ReasonPermissionDenied      = "permission-denied"
)

// AttemptRequest is the execution intent JUMI sends to spawner.Runtime.
//
// Neutral type: no K8s imports, no AH/nan types.
// spawner does not interpret the semantic meaning of these fields.
// The JobClient implementation is responsible for translating them into a
// backend-specific job spec (e.g. K8s batchv1.JobSpec).
type AttemptRequest struct {
	AttemptID     string
	RunID         string
	CorrelationID string

	ImageRef   string
	Command    []string
	WorkingDir string
	// Env contains all environment variables including JUMI-injected values.
	Env map[string]string

	Resources Resources

	// ServiceAccountName is passed through to the JobClient implementation.
	// The JobClient is responsible for tenant allowlist checks and default SA enforcement.
	ServiceAccountName string

	Mounts []Mount

	// Placement carries hints, not decisions.
	// The JobClient translates these into K8s NodeSelector / Affinity / tolerations.
	Placement *Placement

	// UserLabels and UserAnnotations are JUMI-controlled values.
	// The JobClient adds system labels/annotations separately and must reject
	// reserved-prefix conflicts (e.g. "spawner.io/").
	UserLabels      map[string]string
	UserAnnotations map[string]string

	Cleanup CleanupPolicy

	// AttemptTimeout is the wall-clock limit for this attempt.
	// 0 = no timeout. Timer starts after JobClient.Create succeeds (Submitted).
	// Covers: Kueue admission + scheduling + image pull + running time.
	// Distinct from K8s API call timeouts, which are a JobClient concern.
	AttemptTimeout time.Duration
}

// Validate performs structural checks on AttemptRequest.
// Does NOT validate backend-specific constraints (CPU/Memory syntax, SA existence, PVC existence).
func (r AttemptRequest) Validate() error {
	if r.AttemptID == "" {
		return fmt.Errorf("AttemptID is required")
	}
	if r.ImageRef == "" {
		return fmt.Errorf("ImageRef is required")
	}
	if r.Placement != nil {
		if r.Placement.RequiredNodeName != "" && len(r.Placement.PreferredNodes) > 0 {
			return fmt.Errorf("Placement.RequiredNodeName and PreferredNodes are mutually exclusive")
		}
		for i, n := range r.Placement.PreferredNodes {
			if n.Weight < 1 || n.Weight > 100 {
				return fmt.Errorf("Placement.PreferredNodes[%d].Weight must be between 1 and 100, got %d", i, n.Weight)
			}
		}
	}
	return nil
}

// Resources describes CPU and memory in Kubernetes quantity syntax.
// Parsing and validation (resource.ParseQuantity) are the responsibility of the JobClient.
type Resources struct {
	CPU    string // e.g. "500m", "2"
	Memory string // e.g. "512Mi", "4Gi"
}

// MountKind identifies the volume source type.
type MountKind string

const (
	MountKindPVC MountKind = "pvc"
	// MountKindHostPath is for lab environments only.
	// The JobClient must apply a policy gate and validate Source against an allowlist.
	MountKindHostPath MountKind = "hostpath"
)

// Mount describes a single volume attachment.
type Mount struct {
	Kind     MountKind
	Source   string // PVC claim name, or absolute host path
	Target   string // container mount path
	ReadOnly bool
}

// Placement expresses scheduling hints in neutral terms.
// The JobClient translates these into K8s NodeSelector / Affinity / tolerations.
type Placement struct {
	NodeSelector map[string]string
	// RequiredNodeName pins the attempt to a specific node.
	// Mutually exclusive with PreferredNodes (enforced by Validate).
	RequiredNodeName string
	// PreferredNodes expresses soft affinity preferences with weights (1–100).
	// Mutually exclusive with RequiredNodeName.
	PreferredNodes []PreferredNode
}

type PreferredNode struct {
	NodeName string
	Weight   int32 // 1–100; validated by AttemptRequest.Validate()
}

type CleanupPolicy struct {
	// TTLSecondsAfterFinished controls backend job retention after terminal state.
	// 0 = backend default.
	TTLSecondsAfterFinished int32
}

// BackendRef is a durable, serializable reference to the underlying backend resource.
//
// Invariant: when returned by SubmitAttempt, BackendRef is always fully populated
// (including UID for K8s) because SubmitAttempt blocks until JobClient.Create succeeds.
//
// All fields are plain value types. Safe to marshal/unmarshal across restarts.
type BackendRef struct {
	Kind string `json:"kind"`
	// ID is the canonical identifier for the resource. Required.
	// For K8s: "namespace/name". Use NewK8sJobBackendRef to construct.
	ID        string            `json:"id"`
	Namespace string            `json:"namespace,omitempty"`
	Name      string            `json:"name,omitempty"`
	UID       string            `json:"uid,omitempty"`
	Extra     map[string]string `json:"extra,omitempty"`
}

// NewK8sJobBackendRef constructs a BackendRef for a Kubernetes Job.
// JobClient implementations must use this constructor to ensure ID consistency.
func NewK8sJobBackendRef(namespace, name, uid string) BackendRef {
	return BackendRef{
		Kind:      "k8s-job",
		ID:        namespace + "/" + name,
		Namespace: namespace,
		Name:      name,
		UID:       uid,
	}
}

// Validate checks that required fields are present.
func (r BackendRef) Validate() error {
	if r.Kind == "" {
		return fmt.Errorf("BackendRef.Kind is required")
	}
	if r.ID == "" {
		return fmt.Errorf("BackendRef.ID is required")
	}
	return nil
}

// AttemptHandle is returned by Runtime.SubmitAttempt.
// Fully serializable value type. JUMI persists this to its execution store
// and passes it back to WatchAttempt or CancelAttempt, including across restarts.
type AttemptHandle struct {
	AttemptID string `json:"attempt_id"`
	// RuntimeID identifies the Runtime instance. Reserved for future multi-Runtime routing.
	RuntimeID  string     `json:"runtime_id,omitempty"`
	BackendRef BackendRef `json:"backend_ref"`
}

// AttemptEvent is emitted on the channel returned by Runtime.WatchAttempt.
type AttemptEvent struct {
	AttemptID string
	State     AttemptState
	Message   string
	// Reason is machine-readable (see Reason* constants).
	// Non-empty on terminal states and on Pending with a known sub-cause.
	Reason    string
	Timestamp time.Time
}
