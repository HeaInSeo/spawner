package frontdoor_test

import (
	"context"
	"testing"
	"time"

	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/frontdoor"
	ply "github.com/HeaInSeo/spawner/pkg/policy"
)

func TestMetaContext_CloneCopiesBag(t *testing.T) {
	orig := frontdoor.MetaContext{
		RPC:      "RunE",
		TenantID: "team-a",
		Bag: map[string]string{
			"x-trace": "one",
		},
	}

	cp := orig.Clone()
	cp.Set("x-trace", "two")
	cp.Set("x-extra", "v")

	if got, _ := orig.Get("x-trace"); got != "one" {
		t.Fatalf("expected original bag to stay unchanged, got %q", got)
	}
	if _, ok := orig.Get("x-extra"); ok {
		t.Fatal("expected clone mutation to stay isolated from original")
	}
}

func TestResolveInput_CloneCopiesMetaBag(t *testing.T) {
	in := frontdoor.ResolveInput{
		Req: &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Meta: frontdoor.MetaContext{
			RPC:      "RunE",
			TenantID: "team-a",
			Bag: map[string]string{
				"x-trace": "one",
			},
		},
	}

	cp := in.Clone()
	cp.Meta.Set("x-trace", "two")

	if got, _ := in.Meta.Get("x-trace"); got != "one" {
		t.Fatalf("expected original resolve input meta to stay unchanged, got %q", got)
	}
}

func TestTableFrontDoor_ResolveUsesClonedInputMeta(t *testing.T) {
	fd := frontdoor.NewTableFrontDoor(frontdoor.Rule{
		Match: func(in frontdoor.ResolveInput) bool {
			in.Meta.Bag["rule-mutated"] = "yes"
			return true
		},
		SpawnKeyFn: func(in frontdoor.ResolveInput) string {
			if got, _ := in.Meta.Get("rule-mutated"); got != "yes" {
				t.Fatalf("expected cloned input to remain mutable inside resolve, got %q", got)
			}
			return "team-a:run-1"
		},
		BuildCmd: func(in frontdoor.ResolveInput) (api.Command, error) {
			return api.NewRunCommand(
				&api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
				ply.DefaultPolicyB(30*time.Second),
			)
		},
	})

	orig := frontdoor.ResolveInput{
		Req: &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Meta: frontdoor.MetaContext{
			RPC:      "RunE",
			TenantID: "team-a",
			Bag: map[string]string{
				"x-trace": "one",
			},
		},
	}

	if _, err := fd.Resolve(context.Background(), orig); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if _, ok := orig.Meta.Get("rule-mutated"); ok {
		t.Fatal("expected Resolve to isolate meta mutations from caller input")
	}
}
