package main

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/HeaInSeo/spawner/cmd/imp"
	"github.com/HeaInSeo/spawner/pkg/actor"
	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/dispatcher"
	"github.com/HeaInSeo/spawner/pkg/driver"
	sErr "github.com/HeaInSeo/spawner/pkg/error"
	"github.com/HeaInSeo/spawner/pkg/factory"
	fdr "github.com/HeaInSeo/spawner/pkg/frontdoor"
	ply "github.com/HeaInSeo/spawner/pkg/policy"
	"github.com/HeaInSeo/spawner/pkg/store"
)

const defaultRunStorePath = "/tmp/spawner-runstore.json"

func runRule() fdr.Rule {
	return fdr.Rule{
		Match: func(in fdr.ResolveInput) bool { return in.Meta.RPC == "RunE" },
		SpawnKeyFn: func(in fdr.ResolveInput) string {
			rs := in.Req.(*api.RunSpec)
			return in.Meta.TenantID + ":" + rs.RunID
		},
		BuildCmd: func(in fdr.ResolveInput) (api.Command, error) {
			rs := in.Req.(*api.RunSpec)
			return api.NewRunCommand(rs, ply.DefaultPolicyB(5*time.Minute))
		},
	}
}

func runStorePath(getenv func(string) string) string {
	if path := getenv("RUN_STORE_PATH"); path != "" {
		return path
	}
	return defaultRunStorePath
}

func sampleInput() fdr.ResolveInput {
	return fdr.ResolveInput{
		Req: &api.RunSpec{
			SpecVersion: 1,
			RunID:       "run-001",
			ImageRef:    "ghcr.io/acme/tool@sha256:deadbeef...",
			Env:         map[string]string{"SAMPLE_ID": "HG001"},
			Annotations: map[string]string{"example.com/pipeline": "genomics"},
			Mounts: []api.Mount{
				{Source: "/data/HG001", Target: "/in", ReadOnly: true},
				{Source: "workvol", Target: "/work", ReadOnly: false},
			},
			Resources:     api.Resources{CPU: "2", Memory: "4Gi"},
			CorrelationID: "sample-HG001",
			Cleanup:       api.CleanupPolicy{TTLSecondsAfterFinished: 900},
		},
		Meta: fdr.MetaContext{
			RPC:       "RunE",
			TenantID:  "teamA",
			Principal: "alice",
			TraceID:   "trace-xyz",
		},
	}
}

func logBootstrap(recovered []dispatcher.RecoverableRun) {
	if len(recovered) == 0 {
		return
	}
	log.Printf("[server] bootstrap: %d run(s) pending re-dispatch", len(recovered))
	// Recovery remains intentionally narrow: only queued or admitted runs with a
	// valid run envelope are replay candidates. Fast-failed terminal runs are
	// not auto-retried by bootstrap.
}

func main() {
	rootCtx := context.Background()

	// ── RunStore: durable-lite JsonRunStore for restart recovery ──────────────
	// Queued/admitted runs survive process restart.
	// ASSUMPTION: production replaces with PostgreSQL/Redis.
	storePath := runStorePath(os.Getenv)
	rs, err := store.NewJsonRunStore(storePath)
	if err != nil {
		log.Fatalf("runstore init: %v", err)
	}
	log.Printf("[server] RunStore: %s", storePath)

	// ── Execution backend init: graceful init — no panic on failure ───────────
	// If the backend is unreachable, NopDriver is used and Dispatcher marks
	// backendAvailable=false.
	// Incoming runs are held in the RunStore rather than dispatched to Actor.
	// No execution API call is attempted against an unavailable backend.
	//
	// ASSUMPTION: a health-check loop (not shown here) calls d.SetBackendAvailable(true)
	// and d.Bootstrap() once connectivity is restored.
	var drvFn factory.DriverMaker
	dispOpts := []dispatcher.Option{
		dispatcher.WithRunStore(rs),
	}

	drv, k8sErr := imp.NewK8sFromKubeconfig("default", "")
	if k8sErr != nil {
		log.Printf("[server] WARN: backend init failed (%v) — NopDriver active; runs will be held", k8sErr)
		drvFn = func(_ string) driver.Driver { return &imp.NopDriver{} }
		dispOpts = append(dispOpts, dispatcher.WithBackendUnavailable())
	} else {
		log.Printf("[server] backend connected")
		drvFn = func(_ string) driver.Driver { return drv }
	}

	r := fdr.NewTableFrontDoor(runRule())

	af := factory.NewFactory(
		drvFn,
		func(key string, d driver.Driver, mb int) actor.Actor {
			return imp.NewK8sActor(key, d, mb)
		},
		128,
	)

	d := dispatcher.NewDispatcher(r, af, 2, dispOpts...)

	// ── Bootstrap: recover runs from previous process instance ────────────────
	recovered, bootstrapErr := d.RecoverableRuns(rootCtx)
	if bootstrapErr != nil {
		log.Printf("[server] bootstrap error: %v", bootstrapErr)
	} else {
		logBootstrap(recovered)
	}

	// ── Example submission ────────────────────────────────────────────────────
	in := sampleInput()

	if err := d.Handle(rootCtx, in, nil); err != nil {
		if errors.Is(err, sErr.ErrBackendUnavailable) {
			log.Printf("[server] run-001 → held (backend unavailable; preserved in RunStore)")
		} else {
			log.Printf("[server] Handle error: %v", err)
		}
	} else {
		log.Printf("[server] run-001 → admitted-to-dag")
	}
}
