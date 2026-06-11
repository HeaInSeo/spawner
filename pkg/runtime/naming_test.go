package runtime

import (
	"regexp"
	"strings"
	"testing"
)

var k8sNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9\-]{0,61}[a-z0-9]$`)

func TestJobNameFor_Deterministic(t *testing.T) {
	a := jobNameFor("salt-stable", "attempt-001")
	b := jobNameFor("salt-stable", "attempt-001")
	if a != b {
		t.Fatalf("non-deterministic: %q != %q", a, b)
	}
}

func TestJobNameFor_DifferentSalt(t *testing.T) {
	a := jobNameFor("salt-A", "attempt-001")
	b := jobNameFor("salt-B", "attempt-001")
	if a == b {
		t.Fatal("different salts should produce different names")
	}
}

func TestJobNameFor_DifferentAttemptID(t *testing.T) {
	a := jobNameFor("salt-stable", "attempt-001")
	b := jobNameFor("salt-stable", "attempt-002")
	if a == b {
		t.Fatal("different attempt IDs should produce different names")
	}
}

func TestJobNameFor_K8sNameConstraints(t *testing.T) {
	name := jobNameFor("salt-stable", "attempt-001")

	if !strings.HasPrefix(name, "spw-") {
		t.Errorf("expected spw- prefix, got %q", name)
	}
	if len(name) > 63 {
		t.Errorf("name too long: %d chars", len(name))
	}
	// "spw-" + 32 hex = 36 chars, well within 63
	if len(name) != 36 {
		t.Errorf("expected 36 chars (spw- + 32 hex), got %d: %q", len(name), name)
	}
	if !k8sNameRe.MatchString(name) {
		t.Errorf("name %q does not match K8s name constraints", name)
	}
}

func TestAttemptMarkerFor_Deterministic(t *testing.T) {
	a := attemptMarkerFor("salt-stable", "attempt-001")
	b := attemptMarkerFor("salt-stable", "attempt-001")
	if a != b {
		t.Fatalf("non-deterministic: %q != %q", a, b)
	}
}

func TestAttemptMarkerFor_IsJobNamePrefix(t *testing.T) {
	salt := "salt-stable"
	attemptID := "attempt-001"

	name := jobNameFor(salt, attemptID)
	marker := attemptMarkerFor(salt, attemptID)

	// jobName = "spw-" + marker[:32], so marker[:32] == name[4:]
	if len(marker) != 64 {
		t.Errorf("marker should be 64 hex chars, got %d", len(marker))
	}
	nameHex := strings.TrimPrefix(name, "spw-")
	if !strings.HasPrefix(marker, nameHex) {
		t.Errorf("marker %q should have job name hex %q as prefix", marker, nameHex)
	}
}

func TestAttemptMarkerFor_DifferentSalt(t *testing.T) {
	a := attemptMarkerFor("salt-A", "attempt-001")
	b := attemptMarkerFor("salt-B", "attempt-001")
	if a == b {
		t.Fatal("different salts should produce different markers")
	}
}

func TestRuntimeConfig_Validate(t *testing.T) {
	valid := RuntimeConfig{NamingSalt: "s", Namespace: "ns"}
	if err := valid.validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := (RuntimeConfig{Namespace: "ns"}).validate(); err == nil {
		t.Fatal("expected error for missing NamingSalt")
	}
	if err := (RuntimeConfig{NamingSalt: "s"}).validate(); err == nil {
		t.Fatal("expected error for missing Namespace")
	}
}

func TestRuntimeConfig_ApplyDefaults(t *testing.T) {
	cfg := RuntimeConfig{NamingSalt: "s", Namespace: "ns"}
	cfg.applyDefaults()

	if cfg.MaxConcurrency != 10 {
		t.Errorf("MaxConcurrency = %d, want 10", cfg.MaxConcurrency)
	}
	if cfg.SubmitTimeout == 0 {
		t.Error("SubmitTimeout should have a default")
	}
	if cfg.CreateTimeout == 0 {
		t.Error("CreateTimeout should have a default")
	}
}

func TestRuntimeConfig_ExplicitValuesNotOverridden(t *testing.T) {
	cfg := RuntimeConfig{
		NamingSalt:     "s",
		Namespace:      "ns",
		MaxConcurrency: 5,
	}
	cfg.applyDefaults()

	if cfg.MaxConcurrency != 5 {
		t.Errorf("explicit MaxConcurrency should not be overridden, got %d", cfg.MaxConcurrency)
	}
}
