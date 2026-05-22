package factory

import (
	"sync"

	"github.com/HeaInSeo/spawner/pkg/actor"
	"github.com/HeaInSeo/spawner/pkg/driver"
)

// Factory supports both:
// 1) legacy fixed spawnKey ↔ actor registration
// 2) reusable workers via Bind/Activate/Unbind

type Factory interface {
	Get(spawnKey string) (actor.Actor, bool)

	// Bind returns:
	//   created=true,  needsBind=true  — new actor created (in regBinding)
	//   created=false, needsBind=true  — actor found in regBinding (initializing)
	//   created=false, needsBind=false — actor found in regBound (already active)
	Bind(spawnKey string) (act actor.Actor, created bool, needsBind bool, err error)

	// Activate moves act from regBinding to regBound (visible to Get).
	// Returns false if act is no longer in regBinding (stale or cleaned up).
	// Must be called only after both CmdBind and the main command have been
	// successfully enqueued.
	Activate(spawnKey string, act actor.Actor) bool

	// Register is a no-op kept for interface compatibility.
	Register(spawnKey string, act actor.Actor)

	// Unbind removes the actor from both regBinding and regBound.
	Unbind(spawnKey string, act actor.Actor)
}

type DriverMaker func(spawnKey string) driver.Driver
type ActorMaker func(spawnKey string, drv driver.Driver, mbSize int) actor.Actor

type FactoryImp struct {
	mu         sync.RWMutex
	regBound   map[string]actor.Actor // activated actors, visible to Get
	regBinding map[string]actor.Actor // initializing actors, NOT visible to Get
	makeDrv    DriverMaker
	makeActor  ActorMaker
	mbSize     int
}

func NewFactory(mkDrv DriverMaker, mkActor ActorMaker, mbSize int) *FactoryImp {
	return &FactoryImp{
		regBound:   make(map[string]actor.Actor),
		regBinding: make(map[string]actor.Actor),
		makeDrv:    mkDrv,
		makeActor:  mkActor,
		mbSize:     mbSize,
	}
}

func (f *FactoryImp) Get(spawnKey string) (actor.Actor, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	a, ok := f.regBound[spawnKey]
	return a, ok
}

// Bind checks for an existing actor or atomically creates a new one.
func (f *FactoryImp) Bind(spawnKey string) (actor.Actor, bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if a, ok := f.regBound[spawnKey]; ok {
		return a, false, false, nil // already active
	}
	if a, ok := f.regBinding[spawnKey]; ok {
		return a, false, true, nil // initializing
	}

	drv := f.makeDrv(spawnKey)
	act := f.makeActor(spawnKey, drv, f.mbSize)
	f.regBinding[spawnKey] = act
	return act, true, true, nil // newly created
}

// Activate moves act from regBinding to regBound.
// Returns false if act is not the current regBinding entry (stale or cleaned up).
func (f *FactoryImp) Activate(spawnKey string, act actor.Actor) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, ok := f.regBinding[spawnKey]
	if !ok || cur != act {
		return false
	}
	delete(f.regBinding, spawnKey)
	f.regBound[spawnKey] = act
	return true
}

// Register is a no-op; kept for interface compatibility.
func (f *FactoryImp) Register(_ string, _ actor.Actor) {}

// Unbind removes the actor from both regBinding and regBound.
func (f *FactoryImp) Unbind(spawnKey string, act actor.Actor) {
	f.mu.Lock()
	if cur, ok := f.regBound[spawnKey]; ok && cur == act {
		delete(f.regBound, spawnKey)
	}
	if cur, ok := f.regBinding[spawnKey]; ok && cur == act {
		delete(f.regBinding, spawnKey)
	}
	f.mu.Unlock()
}
