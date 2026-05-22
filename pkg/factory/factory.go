package factory

import (
	"sync"

	"github.com/HeaInSeo/spawner/pkg/actor"
	"github.com/HeaInSeo/spawner/pkg/driver"
)

// Factory supports both:
// 1) legacy fixed spawnKey ↔ actor registration
// 2) reusable workers via Bind/Register/Unbind

type Factory interface {
	// Get 이미 등록된 액터를 조회 (루프 상태와 무관)
	// 현재 "바운드된" 액터(있다면)를 조회. (재활용 모드에선 regBound를 우선 확인)
	Get(spawnKey string) (actor.Actor, bool)

	// Bind returns the already-bound actor for spawnKey (created=false),
	// or atomically creates and registers a new one (created=true).
	Bind(spawnKey string) (act actor.Actor, created bool, err error)
	// Register is a no-op; Bind now handles registration atomically.
	Register(spawnKey string, act actor.Actor)
	// Unbind : 바인딩 해제
	Unbind(spawnKey string, act actor.Actor)
}

type DriverMaker func(spawnKey string) driver.Driver
type ActorMaker func(spawnKey string, drv driver.Driver, mbSize int) actor.Actor

type FactoryImp struct {
	mu sync.RWMutex

	// 재활용 모드: 현재 바운드된 액터
	regBound map[string]actor.Actor // spawnKey -> actor (바인딩 중)

	makeDrv   DriverMaker
	makeActor ActorMaker
	mbSize    int
}

func NewFactory(mkDrv DriverMaker, mkActor ActorMaker, mbSize int) *FactoryImp {
	return &FactoryImp{
		regBound:  make(map[string]actor.Actor),
		makeDrv:   mkDrv,
		makeActor: mkActor,
		mbSize:    mbSize,
	}
}

func (f *FactoryImp) Get(spawnKey string) (actor.Actor, bool) {
	// 재활용 경로 우선(바운딩된 액터가 있으면 그걸 반환)
	f.mu.RLock()
	if a, ok := f.regBound[spawnKey]; ok {
		f.mu.RUnlock()
		return a, true
	}
	f.mu.RUnlock()
	return nil, false
}

// ===== 재활용 경로 =====

// Bind checks for an existing bound actor, or atomically creates and registers a new one.
func (f *FactoryImp) Bind(spawnKey string) (actor.Actor, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if a, ok := f.regBound[spawnKey]; ok {
		return a, false, nil
	}

	drv := f.makeDrv(spawnKey)
	act := f.makeActor(spawnKey, drv, f.mbSize)
	f.regBound[spawnKey] = act // atomic: creation + registration under same lock
	return act, true, nil
}

// Register is a no-op; Bind handles registration atomically.
func (f *FactoryImp) Register(_ string, _ actor.Actor) {}

// Unbind 는 바인딩을 해제한다.
func (f *FactoryImp) Unbind(spawnKey string, act actor.Actor) {
	f.mu.Lock()
	if cur, ok := f.regBound[spawnKey]; ok && cur == act {
		delete(f.regBound, spawnKey)
	}
	f.mu.Unlock()
}
