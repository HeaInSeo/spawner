package api

import (
	"strings"
	"time"

	sErr "github.com/HeaInSeo/spawner/pkg/error"
	ply "github.com/HeaInSeo/spawner/pkg/policy"
)

type CmdKind int

const (
	CmdRun CmdKind = iota
	CmdCancel
	CmdSignal
	CmdBind
	CmdUnbind
)

type (
	Bind   struct{ SpawnKey string }
	Unbind struct{}
)

type Command struct {
	Kind CmdKind

	// 추가
	Bind   *Bind
	Unbind *Unbind

	Run    *RunSpec
	Cancel *CancelReq // 추가: 취소 명령 페이로드
	Signal *Signal
	Policy ply.AdmitPolicy // 라우터/디스패처가 확정한 정책 스냅샷
	Sink   EventSink
}

func NewRunCommand(run *RunSpec, policy ply.AdmitPolicy) (Command, error) {
	cmd := Command{Kind: CmdRun, Run: run, Policy: policy}
	return cmd, cmd.Validate()
}

func NewCancelCommand(cancel *CancelReq, policy ply.AdmitPolicy) (Command, error) {
	cmd := Command{Kind: CmdCancel, Cancel: cancel, Policy: policy}
	return cmd, cmd.Validate()
}

func NewSignalCommand(sig *Signal, policy ply.AdmitPolicy) (Command, error) {
	cmd := Command{Kind: CmdSignal, Signal: sig, Policy: policy}
	return cmd, cmd.Validate()
}

func NewBindCommand(bind *Bind) (Command, error) {
	cmd := Command{Kind: CmdBind, Bind: bind}
	return cmd, cmd.Validate()
}

func NewUnbindCommand() Command {
	return Command{Kind: CmdUnbind, Unbind: &Unbind{}}
}

func (c Command) Validate() error {
	switch c.Kind {
	case CmdRun:
		if c.Run == nil {
			return sErr.ErrInvalidCommand
		}
		return c.Run.Validate()
	case CmdCancel:
		if c.Cancel == nil {
			return sErr.ErrInvalidCommand
		}
		return nil
	case CmdSignal:
		if c.Signal == nil || strings.TrimSpace(c.Signal.RunID) == "" {
			return sErr.ErrInvalidCommand
		}
		return nil
	case CmdBind:
		if c.Bind == nil || strings.TrimSpace(c.Bind.SpawnKey) == "" {
			return sErr.ErrInvalidCommand
		}
		return nil
	case CmdUnbind:
		if c.Unbind == nil {
			return sErr.ErrInvalidCommand
		}
		return nil
	default:
		return sErr.ErrInvalidCommand
	}
}

type RunSpec struct {
	SpecVersion        int
	RunID              string
	ImageRef           string // digest-locked preferred
	Command            []string
	WorkingDir         string
	Env                map[string]string
	EnvFieldRefs       map[string]string
	Labels             map[string]string // K8s labels; kueue.x-k8s.io/queue-name goes here
	Annotations        map[string]string
	Mounts             []Mount
	Resources          Resources
	ServiceAccountName string
	CorrelationID      string
	Cleanup            CleanupPolicy
	Placement          *Placement
}

func (r RunSpec) Validate() error {
	const hostnameLabel = "kubernetes.io/hostname"
	if r.SpecVersion < 0 {
		return sErr.ErrInvalidCommand
	}
	if strings.TrimSpace(r.RunID) == "" {
		return sErr.ErrInvalidCommand
	}
	if strings.TrimSpace(r.ImageRef) == "" {
		return sErr.ErrInvalidCommand
	}
	for _, m := range r.Mounts {
		if strings.TrimSpace(m.Source) == "" || strings.TrimSpace(m.Target) == "" {
			return sErr.ErrInvalidCommand
		}
	}
	if r.Cleanup.TTLSecondsAfterFinished < 0 {
		return sErr.ErrInvalidCommand
	}
	if r.Placement != nil {
		required := strings.TrimSpace(r.Placement.RequiredNodeName)
		if required != "" && r.Placement.NodeSelector != nil {
			if hostname := strings.TrimSpace(r.Placement.NodeSelector[hostnameLabel]); hostname != "" && hostname != required {
				return sErr.ErrInvalidCommand
			}
		}
		if required != "" && len(r.Placement.PreferredNodes) > 0 {
			return sErr.ErrInvalidCommand
		}
		for _, pref := range r.Placement.PreferredNodes {
			if strings.TrimSpace(pref.NodeName) == "" {
				return sErr.ErrInvalidCommand
			}
			if pref.Weight < 1 || pref.Weight > 100 {
				return sErr.ErrInvalidCommand
			}
		}
	}
	return nil
}

type CleanupPolicy struct {
	TTLSecondsAfterFinished int32
}

type Mount struct {
	Source   string
	Target   string
	ReadOnly bool
}

type Resources struct {
	CPU    string
	Memory string
}

type WeightedNodePreference struct {
	NodeName string
	Weight   int32
}

type Placement struct {
	NodeSelector     map[string]string
	RequiredNodeName string
	PreferredNodes   []WeightedNodePreference
}

type RunIdentity struct {
	LogicalRunID string
	AttemptID    string
	SpawnKey     string
	TenantID     string
	TraceID      string
	RequestID    string
	Principal    string
}

type RunEnvelope struct {
	Version  int
	Kind     CmdKind
	Identity RunIdentity
	Run      *RunSpec
}

type Signal struct {
	RunID string
	Name  string
}

// === Events (server → client) ===

type State string

const (
	StateIdle       State = "idle" // 아무 것도 안 도는 상태
	StateQueued     State = "queued"
	StateStarting   State = "starting" // prepare/start 호출 사이 -> driver 참고.
	StateRunning    State = "running"
	StateCancelling State = "cancelling"
	StateCancelled  State = "cancelled"
	StateSucceeded  State = "succeeded"
	StateFailed     State = "failed"
)

// === Cancel ===

type CancelMode uint8

const (
	CancelSoft CancelMode = iota // 협조적: drv.Cancel + runCtx cancel
	CancelHard                   // 강제: TERM→(grace)→KILL (grace는 Policy로)
)

type CancelReq struct {
	SpawnID     string        // 라우터/디스패처에서 채워줄 수도 있음
	RunID       string        // (옵션) 현재 실행 중인 run과 매칭 확인용
	IdemKey     string        // (옵션) idemKey→spawnID 매핑용
	Mode        CancelMode    // Soft|Hard
	Grace       time.Duration // (옵션) Hard일 때 TERM→KILL까지 대기; 0이면 Policy.KillAfter 사용
	Reason      string        // 예: "user-cancel" | "transport"
	RequestedBy string        // principal(감사로그용)
}

type Event struct {
	RunID    string
	SpawnKey string
	When     time.Time
	State    State
	Message  string
	Details  map[string]string
}

// EventSink is used by actor to push events (streaming RPC, bus, etc.)
// Implemented in dispatcher layer for your gRPC server.
// Keep it simple here to decouple from transport.
type EventSink interface {
	Send(Event)
}

type TryEventSink interface {
	TrySend(Event, time.Duration) bool
}
