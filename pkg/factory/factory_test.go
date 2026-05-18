package factory_test

import (
	"context"
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

func TestFactory_BindRegisterGetUnbindReuse(t *testing.T) {
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

	if _, ok := f.Get("tenant:run-1"); ok {
		t.Fatal("actor should not be visible via Get before Register")
	}

	f.Register("tenant:run-1", act1)

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
		t.Fatal("expected Bind after Unbind to reuse from idle pool as created=true")
	}
	if act3 != act1 {
		t.Fatal("expected actor to be reused from idle pool")
	}
	if created != 1 {
		t.Fatalf("expected no additional actor creation during reuse, got %d", created)
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
