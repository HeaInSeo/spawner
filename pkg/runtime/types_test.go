package runtime

import (
	"strings"
	"testing"
)

// ── AttemptState ──────────────────────────────────────────────────────────────

func TestAttemptState_IsTerminal(t *testing.T) {
	terminal := []AttemptState{
		AttemptStateSucceeded,
		AttemptStateFailed,
		AttemptStateCancelled,
	}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("%q should be terminal", s)
		}
	}

	nonTerminal := []AttemptState{
		AttemptStateAccepted,
		AttemptStateSubmitted,
		AttemptStatePending,
		AttemptStateRunning,
	}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("%q should not be terminal", s)
		}
	}
}

// ── AttemptRequest.Validate ───────────────────────────────────────────────────

func TestAttemptRequest_Validate_RequiredFields(t *testing.T) {
	if err := (AttemptRequest{}).Validate(); err == nil || !strings.Contains(err.Error(), "AttemptID") {
		t.Fatalf("expected AttemptID error, got %v", err)
	}

	if err := (AttemptRequest{AttemptID: "a"}).Validate(); err == nil || !strings.Contains(err.Error(), "ImageRef") {
		t.Fatalf("expected ImageRef error, got %v", err)
	}
}

func TestAttemptRequest_Validate_MinimalValid(t *testing.T) {
	req := AttemptRequest{AttemptID: "a1", ImageRef: "busybox:1"}
	if err := req.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAttemptRequest_Validate_PlacementMutuallyExclusive(t *testing.T) {
	req := AttemptRequest{
		AttemptID: "a1",
		ImageRef:  "busybox:1",
		Placement: &Placement{
			RequiredNodeName: "node-1",
			PreferredNodes:   []PreferredNode{{NodeName: "node-2", Weight: 50}},
		},
	}
	if err := req.Validate(); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected mutually exclusive error, got %v", err)
	}
}

func TestAttemptRequest_Validate_PreferredNodeWeight(t *testing.T) {
	cases := []struct {
		weight int32
		valid  bool
	}{
		{0, false},
		{1, true},
		{100, true},
		{101, false},
	}
	for _, c := range cases {
		req := AttemptRequest{
			AttemptID: "a1",
			ImageRef:  "busybox:1",
			Placement: &Placement{
				PreferredNodes: []PreferredNode{{NodeName: "n", Weight: c.weight}},
			},
		}
		err := req.Validate()
		if c.valid && err != nil {
			t.Errorf("weight=%d should be valid, got %v", c.weight, err)
		}
		if !c.valid && err == nil {
			t.Errorf("weight=%d should be invalid", c.weight)
		}
	}
}

// ── BackendRef ────────────────────────────────────────────────────────────────

func TestBackendRef_Validate(t *testing.T) {
	if err := (BackendRef{}).Validate(); err == nil || !strings.Contains(err.Error(), "Kind") {
		t.Fatalf("expected Kind error, got %v", err)
	}
	if err := (BackendRef{Kind: "k8s-job"}).Validate(); err == nil || !strings.Contains(err.Error(), "ID") {
		t.Fatalf("expected ID error, got %v", err)
	}
	if err := (BackendRef{Kind: "k8s-job", ID: "ns/name"}).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewK8sJobBackendRef_IDConsistency(t *testing.T) {
	ref := NewK8sJobBackendRef("ns-a", "job-b", "uid-c")

	if ref.Kind != "k8s-job" {
		t.Errorf("Kind = %q, want k8s-job", ref.Kind)
	}
	if ref.ID != "ns-a/job-b" {
		t.Errorf("ID = %q, want ns-a/job-b", ref.ID)
	}
	if ref.Namespace != "ns-a" {
		t.Errorf("Namespace = %q, want ns-a", ref.Namespace)
	}
	if ref.Name != "job-b" {
		t.Errorf("Name = %q, want job-b", ref.Name)
	}
	if ref.UID != "uid-c" {
		t.Errorf("UID = %q, want uid-c", ref.UID)
	}
	if err := ref.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestNewK8sJobBackendRef_RoundTrip(t *testing.T) {
	a := NewK8sJobBackendRef("default", "spw-abc123", "uid-1")
	b := NewK8sJobBackendRef("default", "spw-abc123", "uid-1")

	if a.ID != b.ID {
		t.Errorf("same inputs produced different IDs: %q vs %q", a.ID, b.ID)
	}
}
