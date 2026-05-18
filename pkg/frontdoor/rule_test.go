package frontdoor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/HeaInSeo/spawner/pkg/api"
	sErr "github.com/HeaInSeo/spawner/pkg/error"
	"github.com/HeaInSeo/spawner/pkg/frontdoor"
	"github.com/HeaInSeo/spawner/pkg/policy"
)

func TestPredicates_ComposeAsExpected(t *testing.T) {
	in := frontdoor.ResolveInput{
		Req: &api.RunSpec{RunID: "run-1"},
		Meta: frontdoor.MetaContext{
			RPC:       "RunE",
			TenantID:  "team-a",
			Principal: "user/alice",
		},
	}
	in.Meta.Set("hdr.x-cmd", "Run")

	if !frontdoor.And(
		frontdoor.HasRPC("RunE"),
		frontdoor.HasTenant(),
		frontdoor.TenantIn("team-a", "team-b"),
		frontdoor.HasReqType[api.RunSpec](),
		frontdoor.NotSystemPrincipal(),
		frontdoor.MetaEq("hdr.x-cmd", "Run"),
	)(in) {
		t.Fatal("expected composed predicate to match")
	}

	if frontdoor.Or(
		frontdoor.HasRPC("Cancel"),
		frontdoor.TenantIn("team-x"),
	)(in) {
		t.Fatal("expected Or predicate to fail")
	}

	if !frontdoor.Not(frontdoor.HasRPC("Cancel"))(in) {
		t.Fatal("expected Not predicate to invert the result")
	}
}

func TestCancelRule_PrefersSpawnIDThenIdemKey(t *testing.T) {
	withSpawnID := frontdoor.ResolveInput{
		Req:  &api.CancelReq{SpawnID: "spawn-123", RunID: "run-1"},
		Meta: frontdoor.MetaContext{RPC: "Cancel", TenantID: "team-a"},
	}
	if !frontdoor.CancelRule.Match(withSpawnID) {
		t.Fatal("expected cancel rule to match Cancel RPC")
	}
	if got := frontdoor.CancelRule.SpawnKeyFn(withSpawnID); got != "spawn-123" {
		t.Fatalf("expected SpawnID to win, got %q", got)
	}

	cmd, err := frontdoor.CancelRule.BuildCmd(withSpawnID)
	if err != nil {
		t.Fatalf("BuildCmd: %v", err)
	}
	if cmd.Kind != api.CmdCancel {
		t.Fatalf("expected CmdCancel, got %v", cmd.Kind)
	}
	if cmd.Cancel == nil || cmd.Cancel.RunID != "run-1" {
		t.Fatal("expected CancelReq to be preserved in the command")
	}

	withIdemKey := frontdoor.ResolveInput{
		Req:  &api.CancelReq{IdemKey: "idem-1"},
		Meta: frontdoor.MetaContext{RPC: "Cancel", TenantID: "team-a"},
	}
	if got := frontdoor.CancelRule.SpawnKeyFn(withIdemKey); got != "" {
		t.Fatalf("expected unresolved idemKey lookup to return empty string, got %q", got)
	}
}

func TestTableFrontDoor_RejectsEmptySpawnKey(t *testing.T) {
	fd := frontdoor.NewTableFrontDoor(frontdoor.Rule{
		Match:      func(frontdoor.ResolveInput) bool { return true },
		SpawnKeyFn: func(frontdoor.ResolveInput) string { return "   " },
		BuildCmd: func(in frontdoor.ResolveInput) (api.Command, error) {
			return api.Command{Kind: api.CmdRun, Run: &api.RunSpec{RunID: "run-1"}}, nil
		},
	})

	_, err := fd.Resolve(context.Background(), frontdoor.ResolveInput{
		Req:  &api.RunSpec{RunID: "run-1"},
		Meta: frontdoor.MetaContext{RPC: "RunE"},
	})
	if !errors.Is(err, sErr.ErrInvalidSpawnKey) {
		t.Fatalf("expected ErrInvalidSpawnKey, got %v", err)
	}
}

func TestTableFrontDoor_RejectsInvalidPolicy(t *testing.T) {
	fd := frontdoor.NewTableFrontDoor(frontdoor.Rule{
		Match:      func(frontdoor.ResolveInput) bool { return true },
		SpawnKeyFn: func(frontdoor.ResolveInput) string { return "team-a:run-1" },
		BuildCmd: func(in frontdoor.ResolveInput) (api.Command, error) {
			cmd, err := api.NewRunCommand(&api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"}, policy.DefaultPolicyB(30*time.Second))
			if err != nil {
				return api.Command{}, err
			}
			cmd.Policy.Retry.JitterPct = 101
			return cmd, nil
		},
	})

	_, err := fd.Resolve(context.Background(), frontdoor.ResolveInput{
		Req:  &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Meta: frontdoor.MetaContext{RPC: "RunE"},
	})
	if !errors.Is(err, policy.ErrInvalidPolicy) {
		t.Fatalf("expected invalid policy error, got %v", err)
	}
}
