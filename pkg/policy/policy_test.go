package policy_test

import (
	"testing"
	"time"

	"github.com/HeaInSeo/spawner/pkg/policy"
)

func TestDefaultAttemptPolicy_MatchesFastFailRecoveryDefaults(t *testing.T) {
	p := policy.DefaultAttemptPolicy()

	if p.UseNewAttempt(policy.AttemptPhaseInitialSubmit) {
		t.Fatal("initial submit must not allocate a new numbered attempt beyond attempt-1")
	}
	if p.UseNewAttempt(policy.AttemptPhaseRecoveryReplay) {
		t.Fatal("default recovery replay should keep the existing attempt")
	}
	if !p.UseNewAttempt(policy.AttemptPhaseManualRequeue) {
		t.Fatal("default manual requeue should allocate a new attempt")
	}
	if !p.UseNewAttempt(policy.AttemptPhaseAutoRetry) {
		t.Fatal("default auto retry should allocate a new attempt")
	}
}

func TestAdmitPolicy_WithDefaults(t *testing.T) {
	p := (policy.AdmitPolicy{}).WithDefaults()

	if p.Timeout != 30*time.Minute {
		t.Fatalf("expected default timeout 30m, got %s", p.Timeout)
	}
	if p.KillAfter != 30*time.Second {
		t.Fatalf("expected default kill-after 30s, got %s", p.KillAfter)
	}
	if p.SlotClass != "actor" {
		t.Fatalf("expected default slot class actor, got %q", p.SlotClass)
	}
	if p.Retry.JitterPct != 20 {
		t.Fatalf("expected default retry jitter 20, got %d", p.Retry.JitterPct)
	}
}

func TestAdmitPolicy_ValidateRejectsInvalidValues(t *testing.T) {
	p := policy.DefaultPolicyB(time.Second)
	p.Retry.JitterPct = 200
	if err := p.Validate(); err != policy.ErrInvalidPolicy {
		t.Fatalf("expected ErrInvalidPolicy for jitter, got %v", err)
	}

	p = policy.DefaultPolicyB(time.Second)
	p.CancelOnDisconnect = policy.DiscGrace
	p.AutoCancelAfter = 0
	if err := p.Validate(); err != policy.ErrInvalidPolicy {
		t.Fatalf("expected ErrInvalidPolicy for DiscGrace without auto cancel, got %v", err)
	}
}

func TestAdmitPolicy_MergePatch(t *testing.T) {
	base := policy.DefaultPolicyB(5 * time.Minute)
	priority := 7
	slotClass := "run"
	backoff := 9 * time.Second
	jitter := 33
	cancelMode := policy.CancelHard

	merged := base.MergePatch(policy.AdmitPolicyPatch{
		Priority:          &priority,
		SlotClass:         &slotClass,
		Labels:            map[string]string{"queue": "gold"},
		RetryBackoff:      &backoff,
		RetryJitterPct:    &jitter,
		CancelModeDefault: &cancelMode,
	})

	if merged.Priority != 7 || merged.SlotClass != "run" {
		t.Fatalf("unexpected merged policy: %+v", merged)
	}
	if merged.Labels["queue"] != "gold" {
		t.Fatalf("expected merged label, got %+v", merged.Labels)
	}
	if merged.Retry.Backoff != 9*time.Second || merged.Retry.JitterPct != 33 {
		t.Fatalf("expected merged retry config, got %+v", merged.Retry)
	}
	if merged.CancelModeDefault != policy.CancelHard {
		t.Fatalf("expected cancel mode hard, got %v", merged.CancelModeDefault)
	}
}
