package factory_test

import (
	"context"
	"sync"
	"testing"

	"github.com/HeaInSeo/spawner/pkg/actor"
	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/driver"
	"github.com/HeaInSeo/spawner/pkg/factory"
)

type testDriver struct{ driver.UnimplementedDriver }

type testActor struct{ id int }

func (testActor) EnqueueTry(api.Command) bool                  { return true }
func (testActor) EnqueueCtx(context.Context, api.Command) bool { return true }
func (testActor) OnIdle(func())                                {}
func (testActor) OnTerminate(func())                           {}
func (testActor) Loop(context.Context)                         {}

func TestFactory_BindRegisterGetUnbind(t *testing.T) {
	var created int

	f := factory.NewFactory(
		func(string) driver.Driver { return &testDriver{} },
		func(string, driver.Driver, int) actor.Actor {
			created++
			return &testActor{id: created}
		},
		8,
	)

	act1, created1, err := f.Bind("tenant:run-1")
	if err != nil {
		t.Fatalf("Bind #1: %v", err)
	}
	if !created1 {
		t.Fatal("expected first Bind to create a new actor")
	}
	if created != 1 {
		t.Fatalf("expected 1 actor creation, got %d", created)
	}

	// Bind now registers atomically, so the actor is immediately visible via Get.
	got, ok := f.Get("tenant:run-1")
	if !ok || got != act1 {
		t.Fatal("registered actor was not returned by Get")
	}

	act2, created2, err := f.Bind("tenant:run-1")
	if err != nil {
		t.Fatalf("Bind #2: %v", err)
	}
	if created2 {
		t.Fatal("expected Bind on existing spawnKey to reuse the bound actor")
	}
	if act2 != act1 {
		t.Fatal("expected the same bound actor to be returned")
	}

	f.Unbind("tenant:run-1", act1)

	if _, ok := f.Get("tenant:run-1"); ok {
		t.Fatal("actor should not remain bound after Unbind")
	}

	act3, created3, err := f.Bind("tenant:run-2")
	if err != nil {
		t.Fatalf("Bind #3: %v", err)
	}
	if !created3 {
		t.Fatal("expected Bind on new spawnKey to create a fresh actor")
	}
	// No idle pool reuse: a new actor is always created for a new spawnKey.
	if act3 == act1 {
		t.Fatal("expected a fresh actor (no idle pool reuse)")
	}
	if created != 2 {
		t.Fatalf("expected 2 actor creations (no idle pool reuse), got %d", created)
	}
}

func TestFactory_ConcurrentBind_SingleActor(t *testing.T) {
	var mu sync.Mutex
	var created int

	f := factory.NewFactory(
		func(key string) driver.Driver { return &testDriver{} },
		func(key string, drv driver.Driver, mbSize int) actor.Actor {
			mu.Lock()
			created++
			mu.Unlock()
			return &testActor{id: created}
		},
		8,
	)

	const N = 10
	results := make([]struct {
		act     actor.Actor
		created bool
	}, N)
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			act, c, err := f.Bind("same-key")
			if err != nil {
				t.Errorf("Bind: %v", err)
				return
			}
			results[i].act = act
			results[i].created = c
		}()
	}
	wg.Wait()

	// Exactly one actor should be created
	mu.Lock()
	total := created
	mu.Unlock()
	if total != 1 {
		t.Fatalf("expected 1 actor creation, got %d", total)
	}

	// All goroutines should get the same actor
	var first actor.Actor
	for _, r := range results {
		if r.act == nil {
			continue
		}
		if first == nil {
			first = r.act
		}
		if r.act != first {
			t.Fatal("concurrent Bind returned different actors for same spawnKey")
		}
	}

	// Exactly one should have created=true
	createdCount := 0
	for _, r := range results {
		if r.created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("expected exactly 1 created=true, got %d", createdCount)
	}
}

func TestFactory_UnbindWrongActorDoesNothing(t *testing.T) {
	f := factory.NewFactory(
		func(string) driver.Driver { return &testDriver{} },
		func(string, driver.Driver, int) actor.Actor { return &testActor{id: 1} },
		8,
	)

	act1, _, err := f.Bind("tenant:run-1")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	f.Register("tenant:run-1", act1)

	wrong := &testActor{id: 999}
	f.Unbind("tenant:run-1", wrong)

	got, ok := f.Get("tenant:run-1")
	if !ok || got != act1 {
		t.Fatal("wrong actor unbind should not disturb the bound actor")
	}
}
