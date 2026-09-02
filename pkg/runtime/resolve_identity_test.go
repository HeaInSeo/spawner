package runtime

import (
	"strings"
	"testing"
)

// TestResolveIdentity_DeterministicAndSingleSource verifies ResolveIdentity is a
// pure function of NamingSalt/Namespace/AttemptID and matches the single-source
// naming used by SubmitAttempt (jobNameFor/attemptMarkerFor), with no I/O.
func TestResolveIdentity_DeterministicAndSingleSource(t *testing.T) {
	cfg := RuntimeConfig{NamingSalt: "salt-xyz", Namespace: "jumi-jobs"}
	if err := cfg.validate(); err != nil {
		t.Fatalf("cfg.validate: %v", err)
	}
	// A nil JobClient is acceptable: ResolveIdentity must never touch the backend.
	rt, err := NewRuntime(nil, cfg)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	const attemptID = "run1:nodeA:1"
	id, err := rt.ResolveIdentity(attemptID)
	if err != nil {
		t.Fatalf("ResolveIdentity: %v", err)
	}

	if id.Namespace != cfg.Namespace {
		t.Errorf("Namespace = %q, want %q", id.Namespace, cfg.Namespace)
	}
	if want := jobNameFor(cfg.NamingSalt, attemptID); id.JobName != want {
		t.Errorf("JobName = %q, want single-source %q", id.JobName, want)
	}
	if want := attemptMarkerFor(cfg.NamingSalt, attemptID); id.AttemptMarker != want {
		t.Errorf("AttemptMarker = %q, want single-source %q", id.AttemptMarker, want)
	}
	// JobName (minus "spw-") must be a prefix of the marker (same hash).
	if !strings.HasPrefix(id.AttemptMarker, strings.TrimPrefix(id.JobName, "spw-")) {
		t.Errorf("JobName %q is not a prefix of marker %q", id.JobName, id.AttemptMarker)
	}

	// Deterministic: repeat call is identical.
	id2, err := rt.ResolveIdentity(attemptID)
	if err != nil {
		t.Fatalf("ResolveIdentity (2): %v", err)
	}
	if id2 != id {
		t.Errorf("not deterministic: %+v vs %+v", id2, id)
	}

	// Different attempt → different identity.
	other, err := rt.ResolveIdentity("run1:nodeA:2")
	if err != nil {
		t.Fatalf("ResolveIdentity (other): %v", err)
	}
	if other.JobName == id.JobName || other.AttemptMarker == id.AttemptMarker {
		t.Errorf("different attemptID produced same identity: %+v", other)
	}
}

// TestBackendIdentity_MarkerIsAnnotationScale is a regression guard for the
// F3-B1 marker-storage ruling: the full AttemptMarker is 64 hex chars, which
// EXCEEDS Kubernetes' 63-char label-value limit. Ownership must therefore be
// verified against the authoritative "jumi.io/attempt-marker" annotation, never
// a label. This test fails if a future change shortens the marker to "fit in a
// label" (which the closed ruling forbids) — preventing the impossible
// "store the full marker as a label" contract from returning.
func TestBackendIdentity_MarkerIsAnnotationScale(t *testing.T) {
	const k8sLabelValueMax = 63
	rt, err := NewRuntime(nil, RuntimeConfig{NamingSalt: "salt", Namespace: "ns"})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	id, err := rt.ResolveIdentity("run:node:1")
	if err != nil {
		t.Fatalf("ResolveIdentity: %v", err)
	}
	if len(id.AttemptMarker) != 64 {
		t.Fatalf("AttemptMarker len = %d, want 64 (full sha256 hex)", len(id.AttemptMarker))
	}
	if len(id.AttemptMarker) <= k8sLabelValueMax {
		t.Fatalf("AttemptMarker (%d chars) unexpectedly fits a K8s label (<=%d); the annotation-authority ruling would regress",
			len(id.AttemptMarker), k8sLabelValueMax)
	}
	// JobName IS used as a resource name and must stay within the limit.
	if len(id.JobName) > k8sLabelValueMax {
		t.Errorf("JobName len = %d exceeds K8s name limit %d", len(id.JobName), k8sLabelValueMax)
	}
}

func TestResolveIdentity_EmptyAttemptID(t *testing.T) {
	rt, err := NewRuntime(nil, RuntimeConfig{NamingSalt: "s", Namespace: "n"})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if _, err := rt.ResolveIdentity(""); err == nil {
		t.Fatal("ResolveIdentity(\"\") = nil error, want validation error")
	}
}
