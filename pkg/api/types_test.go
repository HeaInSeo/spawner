package api_test

import (
	"errors"
	"testing"
	"time"

	"github.com/HeaInSeo/spawner/pkg/api"
	sErr "github.com/HeaInSeo/spawner/pkg/error"
	"github.com/HeaInSeo/spawner/pkg/policy"
)

func TestRunSpecValidate(t *testing.T) {
	valid := api.RunSpec{
		SpecVersion:   1,
		RunID:         "run-1",
		ImageRef:      "busybox:1.36",
		Annotations:   map[string]string{"team": "genomics"},
		CorrelationID: "sample-001",
		Cleanup:       api.CleanupPolicy{TTLSecondsAfterFinished: 300},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected valid spec, got %v", err)
	}

	if err := (api.RunSpec{}).Validate(); !errors.Is(err, sErr.ErrInvalidCommand) {
		t.Fatalf("expected ErrInvalidCommand for empty spec, got %v", err)
	}

	if err := (api.RunSpec{
		RunID:    "run-1",
		ImageRef: "busybox:1.36",
		Mounts:   []api.Mount{{Source: "", Target: "/data"}},
	}).Validate(); !errors.Is(err, sErr.ErrInvalidCommand) {
		t.Fatalf("expected ErrInvalidCommand for invalid mount, got %v", err)
	}

	if err := (api.RunSpec{
		RunID:    "run-1",
		ImageRef: "busybox:1.36",
		Cleanup:  api.CleanupPolicy{TTLSecondsAfterFinished: -1},
	}).Validate(); !errors.Is(err, sErr.ErrInvalidCommand) {
		t.Fatalf("expected ErrInvalidCommand for invalid cleanup ttl, got %v", err)
	}

	if err := (api.RunSpec{
		RunID:    "run-1",
		ImageRef: "busybox:1.36",
		Placement: &api.Placement{
			PreferredNodes: []api.WeightedNodePreference{{NodeName: "node-a", Weight: 0}},
		},
	}).Validate(); !errors.Is(err, sErr.ErrInvalidCommand) {
		t.Fatalf("expected ErrInvalidCommand for invalid preferred weight, got %v", err)
	}

	if err := (api.RunSpec{
		RunID:    "run-1",
		ImageRef: "busybox:1.36",
		Placement: &api.Placement{
			RequiredNodeName: "node-a",
			PreferredNodes:   []api.WeightedNodePreference{{NodeName: "node-b", Weight: 50}},
		},
	}).Validate(); !errors.Is(err, sErr.ErrInvalidCommand) {
		t.Fatalf("expected ErrInvalidCommand for mixed required/preferred placement, got %v", err)
	}

	if err := (api.RunSpec{
		RunID:    "run-1",
		ImageRef: "busybox:1.36",
		Placement: &api.Placement{
			RequiredNodeName: "node-a",
			NodeSelector:     map[string]string{"kubernetes.io/hostname": "node-b"},
		},
	}).Validate(); !errors.Is(err, sErr.ErrInvalidCommand) {
		t.Fatalf("expected ErrInvalidCommand for conflicting hostname selector, got %v", err)
	}
}

func TestCommandConstructorsAndValidate(t *testing.T) {
	runCmd, err := api.NewRunCommand(&api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"}, policy.DefaultPolicyB(time.Second))
	if err != nil {
		t.Fatalf("NewRunCommand: %v", err)
	}
	if err := runCmd.Validate(); err != nil {
		t.Fatalf("Validate run command: %v", err)
	}

	cancelCmd, err := api.NewCancelCommand(&api.CancelReq{RunID: "run-1"}, policy.DefaultPolicyB(0))
	if err != nil {
		t.Fatalf("NewCancelCommand: %v", err)
	}
	if err := cancelCmd.Validate(); err != nil {
		t.Fatalf("Validate cancel command: %v", err)
	}

	signalCmd, err := api.NewSignalCommand(&api.Signal{RunID: "run-1", Name: "term"}, policy.DefaultPolicyB(0))
	if err != nil {
		t.Fatalf("NewSignalCommand: %v", err)
	}
	if err := signalCmd.Validate(); err != nil {
		t.Fatalf("Validate signal command: %v", err)
	}

	bindCmd, err := api.NewBindCommand(&api.Bind{SpawnKey: "tenant:run-1"})
	if err != nil {
		t.Fatalf("NewBindCommand: %v", err)
	}
	if err := bindCmd.Validate(); err != nil {
		t.Fatalf("Validate bind command: %v", err)
	}

	unbindCmd := api.NewUnbindCommand()
	if err := unbindCmd.Validate(); err != nil {
		t.Fatalf("Validate unbind command: %v", err)
	}
}

func TestCommandValidateRejectsInvalidPayloads(t *testing.T) {
	cases := []api.Command{
		{Kind: api.CmdRun},
		{Kind: api.CmdCancel},
		{Kind: api.CmdSignal, Signal: &api.Signal{}},
		{Kind: api.CmdBind, Bind: &api.Bind{}},
		{Kind: api.CmdUnbind},
	}

	for _, tc := range cases {
		if err := tc.Validate(); !errors.Is(err, sErr.ErrInvalidCommand) {
			t.Fatalf("expected ErrInvalidCommand for %+v, got %v", tc, err)
		}
	}
}

func TestRunEnvelope_PreservesIdentityFields(t *testing.T) {
	env := api.RunEnvelope{
		Version: 1,
		Kind:    api.CmdRun,
		Identity: api.RunIdentity{
			LogicalRunID: "teamA:run-1",
			AttemptID:    "teamA:run-1/attempt-1",
			SpawnKey:     "teamA:run-1",
			TenantID:     "teamA",
			TraceID:      "trace-1",
		},
		Run: &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
	}

	if env.Identity.LogicalRunID == env.Identity.AttemptID {
		t.Fatal("logical run id and attempt id must remain distinct")
	}
	if env.Run == nil || env.Run.RunID != "run-1" {
		t.Fatalf("expected run payload to be preserved, got %+v", env.Run)
	}
}
