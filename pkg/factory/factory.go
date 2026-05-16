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

	// (a) 이미 해당 spawnKey로 바운드된 액터가 있으면 그걸 반환(created=false),
	// (b) 아니면 idle 풀에서 하나 꺼내거나 새로 생성해 반환(created=true).
	// 이 단계에서는 등록하지 않는다(Dispatcher가 Register 호출).

	Bind(spawnKey string) (act actor.Actor, created bool, err error)
	// Register : spawnKey ↔ actor 바인딩 등록
	Register(spawnKey string, act actor.Actor)
	// Unbind : 바인딩 해제 후 actor를 idle 풀로 되돌림
	Unbind(spawnKey string, act actor.Actor)
}

type DriverMaker func(spawnKey string) driver.Driver
type ActorMaker func(spawnKey string, drv driver.Driver, mbSize int) actor.Actor

type FactoryImp struct {
	mu sync.RWMutex

	// 재활용 모드: 현재 바운드된 액터와 idle 풀
	regBound map[string]actor.Actor // spawnKey -> actor (바인딩 중)
	idlePool []actor.Actor          // 바인딩 해제된 워커(루프는 살아있다고 가정)

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

// Bind 은 현재 바운드된 액터가 있으면 그걸 반환(created=false).
// 없으면 idle 풀에서 하나 꺼내거나 새로 생성(created=true)하여 반환한다.
// 주의: 여기서는 regBound에 등록하지 않는다. (Dispatcher가 Register를 호출)
func (f *FactoryImp) Bind(spawnKey string) (actor.Actor, bool, error) {
	// fast path: 이미 바운드되어 있다면 그걸 반환
	f.mu.RLock()
	if a, ok := f.regBound[spawnKey]; ok {
		f.mu.RUnlock()
		return a, false, nil
	}
	f.mu.RUnlock()

	f.mu.Lock()
	defer f.mu.Unlock()

	// 경합 재확인
	if a, ok := f.regBound[spawnKey]; ok {
		return a, false, nil
	}

	// idle 풀에서 하나 재사용
	var act actor.Actor
	if n := len(f.idlePool); n > 0 {
		act = f.idlePool[n-1]
		f.idlePool = f.idlePool[:n-1]
	} else {
		// 없으면 새로 생성
		drv := f.makeDrv(spawnKey)
		act = f.makeActor(spawnKey, drv, f.mbSize)
	}
	// 등록은 Dispatcher.Register가 수행
	return act, true, nil
}

// Register 는 바인딩 등록을 수행한다.
func (f *FactoryImp) Register(spawnKey string, act actor.Actor) {
	f.mu.Lock()
	f.regBound[spawnKey] = act
	f.mu.Unlock()
}

// Unbind 는 바인딩을 해제하고 워커를 idle 풀로 환원한다.
func (f *FactoryImp) Unbind(spawnKey string, act actor.Actor) {
	f.mu.Lock()
	if cur, ok := f.regBound[spawnKey]; ok && cur == act {
		delete(f.regBound, spawnKey)
		f.idlePool = append(f.idlePool, act)
	}
	f.mu.Unlock()
}
