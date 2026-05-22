package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HeaInSeo/spawner/pkg/actor"
	"github.com/HeaInSeo/spawner/pkg/api"
	sErr "github.com/HeaInSeo/spawner/pkg/error"
	fac "github.com/HeaInSeo/spawner/pkg/factory"
	"github.com/HeaInSeo/spawner/pkg/frontdoor"
	ply "github.com/HeaInSeo/spawner/pkg/policy"
	"github.com/HeaInSeo/spawner/pkg/store"
)

type Dispatcher struct {
	FD          frontdoor.FrontDoor
	AF          fac.Factory
	Sem         chan struct{} // slot=actor, 바인딩 기간 동안만 점유
	defaultSink api.EventSink // 지정 안 하면 NoopSink 사용
	// 추가
	loopBaseCtx    context.Context // (옵션) 액터 루프 베이스 컨텍스트
	enqueueTimeout time.Duration   // (옵션) EnqueueCtx 타임아웃
	// ingress boundary
	runStore         store.RunStore // nil = skip RunStore (backward-compat)
	backendAvailable atomic.Bool    // false = backend unreachable; runs held, not dispatched
	attemptPolicy    ply.AttemptPolicy
}

type RecoverableRun struct {
	Record   store.RunRecord
	Envelope api.RunEnvelope
}

const (
	metaLogicalRunID = "spawner.logical_run_id"
	metaAttemptID    = "spawner.attempt_id"
	metaAttemptPhase = "spawner.attempt_phase"
)

func (r RecoverableRun) ResolveInput() frontdoor.ResolveInput {
	return r.ResolveInputWithAttempt(r.Envelope.Identity.AttemptID)
}

func (r RecoverableRun) ResolveInputWithAttempt(attemptID string) frontdoor.ResolveInput {
	meta := frontdoor.MetaContext{
		RPC:       "RunE",
		TenantID:  r.Envelope.Identity.TenantID,
		Principal: r.Envelope.Identity.Principal,
		TraceID:   r.Envelope.Identity.TraceID,
		RequestID: r.Envelope.Identity.RequestID,
	}
	meta.Set(metaLogicalRunID, r.Envelope.Identity.LogicalRunID)
	if strings.TrimSpace(attemptID) != "" {
		meta.Set(metaAttemptID, attemptID)
	}
	return frontdoor.ResolveInput{
		Req:  r.Envelope.Run,
		Meta: meta,
	}
}

// New is deprecated. Use NewDispatcher with options instead.
func New(fd frontdoor.FrontDoor, af fac.Factory, maxActors int) *Dispatcher {
	return &Dispatcher{
		FD:  fd,
		AF:  af,
		Sem: make(chan struct{}, maxActors),
	}
}

func NewDispatcher(fd frontdoor.FrontDoor, af fac.Factory, semSize int, opts ...Option) *Dispatcher {
	d := &Dispatcher{
		FD:            fd,
		AF:            af,
		Sem:           make(chan struct{}, semSize),
		attemptPolicy: ply.DefaultAttemptPolicy(),
	}
	d.backendAvailable.Store(true) // assume available unless WithBackendUnavailable is set
	for _, o := range opts {
		o(d)
	}
	return d
}

type Option func(*Dispatcher)

func WithDefaultSink(s api.EventSink) Option {
	return func(d *Dispatcher) { d.defaultSink = s }
}

// 추가

func WithLoopBaseCtx(base context.Context) Option {
	return func(d *Dispatcher) { d.loopBaseCtx = base }
}
func WithEnqueueTimeout(dur time.Duration) Option {
	return func(d *Dispatcher) { d.enqueueTimeout = dur }
}
func WithAttemptPolicy(p ply.AttemptPolicy) Option {
	return func(d *Dispatcher) { d.attemptPolicy = p }
}

// WithRunStore attaches a RunStore to the Dispatcher.
// When set, Handle() enqueues the run as StateQueued before dispatching
// and transitions to StateAdmittedToDag on success.
// Bootstrap() uses the store to recover runs across restarts.
func WithRunStore(s store.RunStore) Option {
	return func(d *Dispatcher) { d.runStore = s }
}

// WithBackendUnavailable marks the execution backend as unreachable at startup.
// Handle() will transition queued runs to StateHeld instead of dispatching
// to the Actor, preventing API calls to an unavailable backend.
// ASSUMPTION: a health-check loop (not implemented here) calls SetBackendAvailable
// once connectivity is restored.
func WithBackendUnavailable() Option {
	return func(d *Dispatcher) { d.backendAvailable.Store(false) }
}

// SetBackendAvailable toggles backend availability at runtime.
// When availability transitions false→true, call Bootstrap() to re-queue
// held runs.
func (d *Dispatcher) SetBackendAvailable(available bool) {
	d.backendAvailable.Store(available)
}

// Bootstrap scans the RunStore for runs in StateQueued and StateAdmittedToDag
// and returns them for the caller to re-dispatch.
//
// "queued" runs were never admitted (e.g., restart before dispatch).
// "admitted-to-dag" runs were admitted but the process died mid-execution.
//
// ASSUMPTION: actual re-dispatch requires the original RunSpec serialized in
// RunRecord.Payload. Callers should unmarshal Payload and call Handle() again.
// This implementation logs and returns the records; re-dispatch is the caller's
// responsibility.
func (d *Dispatcher) Bootstrap(ctx context.Context) ([]store.RunRecord, error) {
	recoverable, err := d.RecoverableRuns(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]store.RunRecord, 0, len(recoverable))
	for _, r := range recoverable {
		out = append(out, r.Record)
	}
	return out, nil
}

// RecoverableRuns returns restart-time replay candidates with decoded
// envelopes. This is intentionally narrower than "all stored runs":
// fast-fail policy means terminal failures are not retried automatically, and
// held runs remain an explicit operator or availability decision.
func (d *Dispatcher) RecoverableRuns(ctx context.Context) ([]RecoverableRun, error) {
	if d.runStore == nil {
		return nil, nil
	}
	queued, err := d.runStore.ListByState(ctx, store.StateQueued)
	if err != nil {
		return nil, err
	}
	admitted, err := d.runStore.ListByState(ctx, store.StateAdmittedToDag)
	if err != nil {
		return nil, err
	}
	all := append(queued, admitted...)
	out := make([]RecoverableRun, 0, len(all))
	for _, r := range all {
		if !store.IsRecoverable(r.State) {
			continue
		}
		env, err := decodeRunEnvelope(r)
		if err != nil {
			log.Printf("[bootstrap] skipping corrupt run record %s (state=%s): %v", r.RunID, r.State, err)
			continue
		}
		log.Printf("[bootstrap] recovered run: id=%s state=%s created=%s",
			r.RunID, r.State, r.CreatedAt.Format(time.RFC3339))
		out = append(out, RecoverableRun{Record: r, Envelope: env})
	}
	if len(out) == 0 {
		log.Printf("[bootstrap] no pending runs to recover")
	}
	return out, nil
}

// ReplayRecoverableRun replays a single recoverable run through the normal
// dispatcher ingress path. This preserves fast-fail behavior by only accepting
// runs that already passed RecoverableRuns state filtering and envelope decode.
func (d *Dispatcher) ReplayRecoverableRun(ctx context.Context, rr RecoverableRun, sink api.EventSink) error {
	return d.ReplayRecoverableRunWithPhase(ctx, rr, ply.AttemptPhaseRecoveryReplay, sink)
}

func (d *Dispatcher) PrepareReplayInput(rr RecoverableRun, phase ply.AttemptPhase) (frontdoor.ResolveInput, error) {
	if !store.IsRecoverable(rr.Record.State) {
		return frontdoor.ResolveInput{}, fmt.Errorf("%w: non-recoverable state %s", sErr.ErrInvalidCommand, rr.Record.State)
	}
	if rr.Envelope.Kind != api.CmdRun || rr.Envelope.Run == nil {
		return frontdoor.ResolveInput{}, fmt.Errorf("%w: replay requires run envelope", sErr.ErrInvalidCommand)
	}

	attemptID := rr.Envelope.Identity.AttemptID
	if d.attemptPolicy.UseNewAttempt(phase) {
		attemptID = nextAttemptID(rr.Envelope.Identity.LogicalRunID, rr.Envelope.Identity.AttemptID)
	}
	in := rr.ResolveInputWithAttempt(attemptID)
	in.Meta.Set(metaAttemptPhase, attemptPhaseName(phase))
	return in, nil
}

func (d *Dispatcher) ReplayRecoverableRunWithPhase(
	ctx context.Context,
	rr RecoverableRun,
	phase ply.AttemptPhase,
	sink api.EventSink,
) error {
	in, err := d.PrepareReplayInput(rr, phase)
	if err != nil {
		return err
	}
	return d.Handle(ctx, in, sink)
}

// ReplayRecoverableRuns replays all current recoverable runs in store order.
// It is intentionally fail-fast: the first replay error stops the loop and is
// returned to the caller.
func (d *Dispatcher) ReplayRecoverableRuns(ctx context.Context, sink api.EventSink) error {
	recoverable, err := d.RecoverableRuns(ctx)
	if err != nil {
		return err
	}
	for _, rr := range recoverable {
		if err := d.ReplayRecoverableRun(ctx, rr, sink); err != nil {
			return err
		}
	}
	return nil
}

// logicalRunIDFromInput extracts the stable logical run identifier.
// For runnable inputs, this should be derived from tenant + run spec identity,
// not from transport-scoped ids such as trace ids.
func logicalRunIDFromInput(in frontdoor.ResolveInput) string {
	if v, ok := in.Meta.Get(metaLogicalRunID); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if rs, ok := in.Req.(*api.RunSpec); ok && rs.RunID != "" {
		if in.Meta.TenantID != "" {
			return in.Meta.TenantID + ":" + rs.RunID
		}
		return rs.RunID
	}
	return ""
}

func initialAttemptID(logicalRunID string) string {
	if logicalRunID == "" {
		return ""
	}
	return logicalRunID + "/attempt-1"
}

func nextAttemptID(logicalRunID, current string) string {
	if logicalRunID == "" {
		return ""
	}
	const marker = "/attempt-"
	if strings.HasPrefix(current, logicalRunID+marker) {
		suffix := strings.TrimPrefix(current, logicalRunID+marker)
		var n int
		if _, err := fmt.Sscanf(suffix, "%d", &n); err == nil && n >= 1 {
			return fmt.Sprintf("%s/attempt-%d", logicalRunID, n+1)
		}
	}
	return logicalRunID + "/attempt-2"
}

func attemptIDFromInput(in frontdoor.ResolveInput, logicalRunID string) string {
	if v, ok := in.Meta.Get(metaAttemptID); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return initialAttemptID(logicalRunID)
}

func attemptPhaseName(phase ply.AttemptPhase) string {
	switch phase {
	case ply.AttemptPhaseRecoveryReplay:
		return "recovery-replay"
	case ply.AttemptPhaseManualRequeue:
		return "manual-requeue"
	case ply.AttemptPhaseAutoRetry:
		return "auto-retry"
	default:
		return "initial-submit"
	}
}

func attemptCauseFromInput(in frontdoor.ResolveInput) store.AttemptCause {
	switch v, _ := in.Meta.Get(metaAttemptPhase); v {
	case "recovery-replay":
		return store.AttemptCauseRecoveryReplay
	case "manual-requeue":
		return store.AttemptCauseManualRequeue
	case "auto-retry":
		return store.AttemptCauseAutoRetry
	default:
		return store.AttemptCauseInitialSubmit
	}
}

func buildRunEnvelope(in frontdoor.ResolveInput, rr frontdoor.ResolveResult) (api.RunEnvelope, error) {
	rs, ok := in.Req.(*api.RunSpec)
	if !ok || rs == nil {
		return api.RunEnvelope{}, sErr.ErrInvalidCommand
	}

	logicalRunID := logicalRunIDFromInput(in)
	if logicalRunID == "" {
		return api.RunEnvelope{}, fmt.Errorf("%w: missing logical run id", sErr.ErrInvalidCommand)
	}

	env := api.RunEnvelope{
		Version: 1,
		Kind:    api.CmdRun,
		Identity: api.RunIdentity{
			LogicalRunID: logicalRunID,
			AttemptID:    attemptIDFromInput(in, logicalRunID),
			SpawnKey:     rr.SpawnKey,
			TenantID:     in.Meta.TenantID,
			TraceID:      in.Meta.TraceID,
			RequestID:    in.Meta.RequestID,
			Principal:    in.Meta.Principal,
		},
		Run: rs,
	}
	return env, nil
}

func decodeRunEnvelope(rec store.RunRecord) (api.RunEnvelope, error) {
	var env api.RunEnvelope
	if len(rec.Payload) == 0 {
		return api.RunEnvelope{}, fmt.Errorf("%w: empty run payload for %s", sErr.ErrInvalidCommand, rec.RunID)
	}
	if err := json.Unmarshal(rec.Payload, &env); err != nil {
		return api.RunEnvelope{}, fmt.Errorf("%w: decode run envelope for %s: %v", sErr.ErrInvalidCommand, rec.RunID, err)
	}
	if env.Version != 1 || env.Kind != api.CmdRun || env.Run == nil {
		return api.RunEnvelope{}, fmt.Errorf("%w: malformed run envelope for %s", sErr.ErrInvalidCommand, rec.RunID)
	}
	if env.Identity.LogicalRunID != rec.RunID {
		return api.RunEnvelope{}, fmt.Errorf("%w: run id mismatch for %s", sErr.ErrInvalidCommand, rec.RunID)
	}
	if err := env.Run.Validate(); err != nil {
		return api.RunEnvelope{}, err
	}
	return env, nil
}

func validateResolvedCommand(rr frontdoor.ResolveResult) error {
	if strings.TrimSpace(rr.SpawnKey) == "" {
		return sErr.ErrInvalidSpawnKey
	}
	return rr.Cmd.Validate()
}

// Handle Resolve → (없으면) 세마확보 → Bind → Register → CmdBind → Enqueue
// 이미 바운드된 경우엔 세마 재획득/바인드 불필요, 바로 Enqueue.
//
// RunStore 경계 (runStore != nil 일 때):
//  1. Handle() 진입 시 run을 StateQueued로 Enqueue (idempotent).
//  2. backend 불가 상태(backendAvailable=false)이면 queued→held 전이 후 ErrBackendUnavailable 반환.
//     run은 RunStore에 held 상태로 보존된다 — panic 없음.
//  3. 디스패치 성공 시 queued→admitted-to-dag 전이.
//  4. ErrSaturated 시 run은 queued 상태 그대로 유지 (자연 재시도 가능).
func (d *Dispatcher) Handle(ctx context.Context, in frontdoor.ResolveInput, sink api.EventSink) error {
	// 0) 라우팅
	rr, err := d.FD.Resolve(ctx, in)
	if err != nil {
		return err
	}
	if err := validateResolvedCommand(rr); err != nil {
		return err
	}

	// 1) sink 기본값 확정
	s := sink
	if s == nil {
		if d.defaultSink != nil {
			s = d.defaultSink
		} else {
			s = NoopSink{}
		}
	}

	// ── ingress gate: RunStore 경계 ────────────────────────────────────────────
	logicalRunID := logicalRunIDFromInput(in)
	if rr.Cmd.Kind == api.CmdRun && d.runStore != nil && logicalRunID != "" {
		env, err := buildRunEnvelope(in, rr)
		if err != nil {
			return err
		}
		payload, err := json.Marshal(env)
		if err != nil {
			return err
		}
		enqErr := d.runStore.Enqueue(ctx, store.RunRecord{
			RunID:   logicalRunID,
			State:   store.StateQueued,
			Payload: payload,
		})
		if enqErr != nil {
			if !errors.Is(enqErr, store.ErrAlreadyExists) {
				return enqErr
			}
			latest, ok, err := d.runStore.GetLatestAttempt(ctx, logicalRunID)
			if err != nil {
				return err
			}
			if !ok || latest.AttemptID != env.Identity.AttemptID {
				if err := d.runStore.AppendAttempt(ctx, store.AttemptRecord{
					AttemptID: env.Identity.AttemptID,
					RunID:     logicalRunID,
					State:     store.StateQueued,
					Payload:   payload,
					Cause:     attemptCauseFromInput(in),
				}); err != nil {
					return err
				}
			}
		}

		if !d.backendAvailable.Load() {
			_ = d.runStore.UpdateState(ctx, logicalRunID, store.StateQueued, store.StateHeld)
			log.Printf("[ingress] backend unavailable: run %s → held", logicalRunID)
			return sErr.ErrBackendUnavailable
		}
	}
	// ──────────────────────────────────────────────────────────────────────────

	// 2) 바운드 조회
	act, ok := d.AF.Get(rr.SpawnKey)
	if !ok {
		// 3) 바인딩 기간 동안만 세마 점유 (non-blocking)
		select {
		case d.Sem <- struct{}{}:
			var created, needsBind bool
			act, created, needsBind, err = d.AF.Bind(rr.SpawnKey)
			if err != nil {
				<-d.Sem
				return err
			}

			if created {
				// New actor: start Loop, send CmdBind, then Activate (makes visible to Get)
				base := d.loopBaseCtx
				if base == nil {
					base = ctx
				}
				releaseOnce := syncRelease(d.Sem, d.AF, rr.SpawnKey, act)
				act.OnIdle(releaseOnce)
				act.OnTerminate(releaseOnce)
				go act.Loop(base)

				bindCmd, err := api.NewBindCommand(&api.Bind{SpawnKey: rr.SpawnKey})
				if err != nil {
					d.AF.Unbind(rr.SpawnKey, act)
					<-d.Sem
					return err
				}
				bindCmd.Sink = s

				sendCtxBind := ctx
				cancelBind := func() {}
				if d.enqueueTimeout > 0 {
					sendCtxBind, cancelBind = context.WithTimeout(ctx, d.enqueueTimeout)
				}
				defer cancelBind()

				if ok := act.EnqueueCtx(sendCtxBind, bindCmd); !ok {
					d.AF.Unbind(rr.SpawnKey, act)
					<-d.Sem
					return sErr.ErrMailboxFull
				}

				// Activate only after CmdBind is in the mailbox
				d.AF.Activate(rr.SpawnKey)

			} else if needsBind {
				// Actor is initializing (regBinding): release Sem, send CmdBind before rr.Cmd
				<-d.Sem

				bindCmd, err := api.NewBindCommand(&api.Bind{SpawnKey: rr.SpawnKey})
				if err != nil {
					return err
				}
				bindCmd.Sink = s

				sendCtxBind := ctx
				cancelBind := func() {}
				if d.enqueueTimeout > 0 {
					sendCtxBind, cancelBind = context.WithTimeout(ctx, d.enqueueTimeout)
				}
				defer cancelBind()

				if ok := act.EnqueueCtx(sendCtxBind, bindCmd); !ok {
					return sErr.ErrMailboxFull
				}
			} else {
				// Actor already active (regBound): release Sem, no CmdBind needed
				<-d.Sem
			}

		default:
			return sErr.ErrSaturated
		}
	}

	// 7) 본 작업 커맨드 전송
	rr.Cmd.Sink = s

	sendCtx := ctx
	cancel := func() {}
	if d.enqueueTimeout > 0 {
		sendCtx, cancel = context.WithTimeout(ctx, d.enqueueTimeout)
	}
	defer cancel()

	if ok := act.EnqueueCtx(sendCtx, rr.Cmd); !ok {
		return sErr.ErrMailboxFull
	}

	// ── admitted: queued → admitted-to-dag ────────────────────────────────────
	if rr.Cmd.Kind == api.CmdRun && d.runStore != nil && logicalRunID != "" {
		if err := d.runStore.UpdateState(ctx, logicalRunID, store.StateQueued, store.StateAdmittedToDag); err != nil {
			// Log but don't fail: run is already dispatched.
			// ErrAlreadyExists-equivalent: run was re-submitted and already advanced.
			log.Printf("[ingress] warn: UpdateState admitted: %v", err)
		}
	}
	// ──────────────────────────────────────────────────────────────────────────

	return nil
}

func syncRelease(sem chan struct{}, af fac.Factory, spawnKey string, act actor.Actor) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			af.Unbind(spawnKey, act)
			select {
			case <-sem:
			default:
			}
		})
	}
}
