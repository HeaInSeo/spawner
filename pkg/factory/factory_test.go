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

	act1, created1, needsBind1, err := f.Bind("tenant:run-1")
	if err != nil {
		t.Fatalf("Bind #1: %v", err)
	}
	if !created1 {
		t.Fatal("expected first Bind to create a new actor")
	}
	if !needsBind1 {
		t.Fatal("expected first Bind to return needsBind=true")
	}
	if created != 1 {
		t.Fatalf("expected 1 actor creation, got %d", created)
	}

	// Before Activate: NOT visible via Get
	if _, ok := f.Get("tenant:run-1"); ok {
		t.Fatal("actor should not be visible via Get before Activate")
	}

	// After Activate: visible via Get
	f.Activate("tenant:run-1")
	got, ok := f.Get("tenant:run-1")
	if !ok || got != act1 {
		t.Fatal("actor should be visible via Get after Activate")
	}

	// Second Bind: actor in regBound → created=false, needsBind=false
	act2, created2, needsBind2, err := f.Bind("tenant:run-1")
	if err != nil {
		t.Fatalf("Bind #2: %v", err)
	}
	if created2 || needsBind2 {
		t.Fatal("expected created=false, needsBind=false for existing active actor")
	}
	if act2 != act1 {
		t.Fatal("expected same actor")
	}

	f.Unbind("tenant:run-1", act1)

	// After Unbind: not visible
	if _, ok := f.Get("tenant:run-1"); ok {
		t.Fatal("should not be visible after Unbind")
	}

	// New spawnKey
	act3, created3, needsBind3, err := f.Bind("tenant:run-2")
	if err != nil {
		t.Fatalf("Bind #3: %v", err)
	}
	if !created3 || !needsBind3 {
		t.Fatal("expected created=true, needsBind=true for new key")
	}
	if act3 == act1 {
		t.Fatal("expected fresh actor")
	}
	if created != 2 {
		t.Fatalf("expected 2 total creations, got %d", created)
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
			act, c, _, err := f.Bind("same-key")
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

	act1, _, _, err := f.Bind("tenant:run-1")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	f.Activate("tenant:run-1")

	wrong := &testActor{id: 999}
	f.Unbind("tenant:run-1", wrong)

	got, ok := f.Get("tenant:run-1")
	if !ok || got != act1 {
		t.Fatal("wrong actor unbind should not disturb the bound actor")
	}
}

func TestFactory_BindingActorNotExposedViaGet(t *testing.T) {
	f := factory.NewFactory(
		func(string) driver.Driver { return &testDriver{} },
		func(string, driver.Driver, int) actor.Actor { return &testActor{id: 1} },
		8,
	)

	act, created, needsBind, err := f.Bind("key-1")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if !created || !needsBind {
		t.Fatalf("expected created=true needsBind=true, got created=%v needsBind=%v", created, needsBind)
	}

	// Before Activate: Get must NOT return the actor
	if _, ok := f.Get("key-1"); ok {
		t.Fatal("actor in regBinding must not be visible via Get")
	}

	// Concurrent Bind for same key: created=false, needsBind=true (initializing)
	act2, created2, needsBind2, err := f.Bind("key-1")
	if err != nil {
		t.Fatalf("second Bind: %v", err)
	}
	if created2 {
		t.Fatal("second Bind must not create a new actor")
	}
	if !needsBind2 {
		t.Fatal("second Bind for initializing key must return needsBind=true")
	}
	if act2 != act {
		t.Fatal("second Bind must return same actor")
	}

	// Activate: now visible to Get
	f.Activate("key-1")
	got, ok := f.Get("key-1")
	if !ok || got != act {
		t.Fatal("after Activate, actor must be visible via Get")
	}

	// Third Bind for active key: created=false, needsBind=false
	act3, created3, needsBind3, err := f.Bind("key-1")
	if err != nil {
		t.Fatalf("third Bind: %v", err)
	}
	if created3 || needsBind3 {
		t.Fatalf("third Bind for active key: expected created=false needsBind=false, got created=%v needsBind=%v", created3, needsBind3)
	}
	if act3 != act {
		t.Fatal("third Bind must return same actor")
	}
}

func TestFactory_Unbind_CleansRegBinding(t *testing.T) {
	f := factory.NewFactory(
		func(string) driver.Driver { return &testDriver{} },
		func(string, driver.Driver, int) actor.Actor { return &testActor{id: 1} },
		8,
	)

	act, _, _, err := f.Bind("key-1")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	// Unbind before Activate (cleanup on CmdBind enqueue failure)
	f.Unbind("key-1", act)

	// Should be completely gone: a new Bind must create a fresh actor
	_, created, needsBind, err := f.Bind("key-1")
	if err != nil {
		t.Fatalf("second Bind after cleanup: %v", err)
	}
	if !created || !needsBind {
		t.Fatal("after Unbind of regBinding actor, next Bind must create fresh actor")
	}
}
