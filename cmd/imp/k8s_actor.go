package imp

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/HeaInSeo/spawner/pkg/actor"
	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/driver"
)

// K8sActor can run multiple runIDs concurrently within the same actor.
// - active tracks per-run handle/cancel state
// - execSem bounds internal parallelism
// - execWG waits for run goroutines during shutdown

type K8sActor struct {
	key string
	mb  *actor.Mailbox[api.Command]
	drv driver.Driver // 구체타입(DriverK8s) 말고 인터페이스

	onIdle func()
	onTerm func()

	mu sync.Mutex
	// 다중 실행 관리: runID -> (handle, cancel)
	active map[string]struct {
		h      driver.Handle
		cancel context.CancelFunc
	}
	// 내부 동시 실행 제한(버퍼 크기가 병렬도)
	execSem chan struct{}
	// 실행 고루틴 추적(종료 시 누수 방지)
	execWG sync.WaitGroup

	// 추가
	boundKey string // ""면 Idle
}

func NewK8sActor(key string, drv driver.Driver, mbSize int) *K8sActor {
	return &K8sActor{
		key:     key,
		drv:     drv,
		mb:      actor.NewMailbox[api.Command](mbSize),
		execSem: make(chan struct{}, 2), // 기본 병렬도(필요 시 옵션화)
		active: make(map[string]struct {
			h      driver.Handle
			cancel context.CancelFunc
		}),
	}
}

func (a *K8sActor) OnIdle(fn func())      { a.onIdle = fn }
func (a *K8sActor) OnTerminate(fn func()) { a.onTerm = fn }

func (a *K8sActor) EnqueueTry(c api.Command) bool                      { return a.mb.TryEnqueue(c) }
func (a *K8sActor) EnqueueCtx(ctx context.Context, c api.Command) bool { return a.mb.Enqueue(ctx, c) }

func (a *K8sActor) Loop(ctx context.Context) {
	defer func() {
		// 더 이상 새 메시지 금지 + 생산자 종료 후 데이터채널 close
		a.mb.Close()

		// 모든 active 실행 취소 및 종료 대기
		a.mu.Lock()
		for _, st := range a.active {
			if st.h != nil {
				_ = a.drv.Cancel(context.WithoutCancel(ctx), st.h)
			}
			if st.cancel != nil {
				st.cancel()
			}
		}
		a.mu.Unlock()
		a.execWG.Wait()

		if a.onTerm != nil {
			a.onTerm()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case cmd, ok := <-a.mb.C():
			if !ok {
				return // 메일박스가 닫혀서 데이터 채널이 닫힌 경우
			}

			switch cmd.Kind {

			// ===== 재활용 수명 관리 =====
			case api.CmdBind:
				if cmd.Bind == nil || strings.TrimSpace(cmd.Bind.SpawnKey) == "" {
					emitErr(cmd.Sink, a.key, "", errors.New("empty bind key"))
					break
				}
				bk := strings.TrimSpace(cmd.Bind.SpawnKey)

				a.mu.Lock()
				// 이미 다른 키에 바인딩되어 있으면 거부
				if a.boundKey != "" && a.boundKey != bk {
					a.mu.Unlock()
					emitErr(cmd.Sink, a.key, "", errors.New("actor already bound"))
					break
				}
				a.boundKey = bk
				a.mu.Unlock()

				emitState(cmd.Sink, a.key, "", api.StateStarting, "bound")

			case api.CmdUnbind:
				a.mu.Lock()
				// 활성 실행이 남아 있으면 언바인드 거부 (정책에 따라 강제 취소도 가능)
				if len(a.active) > 0 {
					a.mu.Unlock()
					emitErr(cmd.Sink, a.key, "", errors.New("cannot unbind: runs still active"))
					break
				}
				a.boundKey = ""
				a.mu.Unlock()

				emitState(cmd.Sink, a.key, "", api.StateIdle, "unbound")

			// ===== 실행/제어 =====
			case api.CmdRun:
				if cmd.Run == nil {
					emitErr(cmd.Sink, a.key, "", errors.New("missing run payload"))
					break
				}
				// 바인딩 여부 가드
				a.mu.Lock()
				if a.boundKey == "" {
					a.mu.Unlock()
					emitErr(cmd.Sink, a.key, cmd.Run.RunID, errors.New("not bound"))
					break
				}
				a.mu.Unlock()

				runID := cmd.Run.RunID

				// 동일 runID 중복 방지
				a.mu.Lock()
				if _, exists := a.active[runID]; exists {
					a.mu.Unlock()
					emitErr(cmd.Sink, a.key, runID, errors.New("duplicate runID in progress"))
					break
				}
				a.mu.Unlock()

				emitState(cmd.Sink, a.key, runID, api.StateStarting, "")

				// 개별 실행 컨텍스트(Timeout/Cancel 정책)
				var runCtx context.Context
				var cancel context.CancelFunc
				if d := cmd.Policy.Timeout; d > 0 {
					runCtx, cancel = context.WithTimeout(ctx, d)
				} else {
					runCtx, cancel = context.WithCancel(ctx)
				}

				// 내부 병렬도 슬롯 점유 + active 등록
				a.execWG.Add(1)
				a.execSem <- struct{}{}
				a.mu.Lock()
				a.active[runID] = struct {
					h      driver.Handle
					cancel context.CancelFunc
				}{h: nil, cancel: cancel}
				a.mu.Unlock()

				go func(runID string, c api.Command, runCtx context.Context, cancel context.CancelFunc) {
					defer func() {
						var becameIdle bool
						var idleFn func()

						// 종료 처리: cancel 호출, active에서 제거, 슬롯/카운터 반납
						cancel()
						a.mu.Lock()
						delete(a.active, runID)
						if len(a.active) == 0 && a.boundKey != "" {
							a.boundKey = ""
							becameIdle = true
							idleFn = a.onIdle
						}
						a.mu.Unlock()
						<-a.execSem
						a.execWG.Done()

						if becameIdle {
							emitState(c.Sink, a.key, "", api.StateIdle, "unbound")
							if idleFn != nil {
								idleFn()
							}
						}
					}()

					p, err := a.drv.Prepare(runCtx, *c.Run)
					if err == nil {
						h, err2 := a.drv.Start(runCtx, p)
						if err2 == nil {
							// 핸들 저장
							a.mu.Lock()
							cur := a.active[runID]
							cur.h = h
							a.active[runID] = cur
							a.mu.Unlock()

							emitState(c.Sink, a.key, runID, api.StateRunning, "")
							_, err = a.drv.Wait(runCtx, h)
						} else {
							err = err2
						}
					}
					if err != nil {
						emitErr(c.Sink, a.key, runID, err)
						return
					}
					emitState(c.Sink, a.key, runID, api.StateSucceeded, "")
				}(runID, cmd, runCtx, cancel)

			case api.CmdCancel:
				if cmd.Cancel == nil {
					emitErr(cmd.Sink, a.key, "", errors.New("missing cancel payload"))
					break
				}
				// 바인딩 여부 가드
				a.mu.Lock()
				if a.boundKey == "" {
					a.mu.Unlock()
					emitErr(cmd.Sink, a.key, strings.TrimSpace(cmd.Cancel.RunID), errors.New("not bound"))
					break
				}
				a.mu.Unlock()

				target := strings.TrimSpace(cmd.Cancel.RunID)

				a.mu.Lock()
				if target == "" {
					// (옵션) 빈 타겟이면 모두 취소
					for id, st := range a.active {
						if st.h != nil {
							_ = a.drv.Cancel(context.WithoutCancel(ctx), st.h)
						}
						if st.cancel != nil {
							st.cancel()
						}
						emitState(cmd.Sink, a.key, id, api.StateCancelling, "")
					}
					a.mu.Unlock()
					break
				}
				st, ok := a.active[target]
				a.mu.Unlock()

				if !ok {
					emitErr(cmd.Sink, a.key, target, errors.New("no such run in progress"))
					break
				}
				if st.h != nil {
					_ = a.drv.Cancel(context.WithoutCancel(ctx), st.h)
				}
				if st.cancel != nil {
					st.cancel()
				}
				emitState(cmd.Sink, a.key, target, api.StateCancelling, "")

			case api.CmdSignal:
				// 바인딩 여부/타겟 가드
				if cmd.Signal == nil || strings.TrimSpace(cmd.Signal.RunID) == "" {
					emitErr(cmd.Sink, a.key, "", errors.New("empty signal or missing runID"))
					break
				}
				a.mu.Lock()
				if a.boundKey == "" {
					a.mu.Unlock()
					emitErr(cmd.Sink, a.key, cmd.Signal.RunID, errors.New("not bound"))
					break
				}
				st, ok := a.active[strings.TrimSpace(cmd.Signal.RunID)]
				a.mu.Unlock()

				if !ok || st.h == nil {
					emitErr(cmd.Sink, a.key, cmd.Signal.RunID, errors.New("no active handle for signal"))
					break
				}
				_ = a.drv.Signal(context.WithoutCancel(ctx), st.h, *cmd.Signal)
			}
		}
	}
}
