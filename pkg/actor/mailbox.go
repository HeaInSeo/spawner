package actor

import (
	"context"
	"sync"
)

type Mailbox[T any] struct {
	ch     chan T
	closed chan struct{}  // 쓰기/읽기 중단 신호
	once   sync.Once      // Close idempotent
	wg     sync.WaitGroup // 생산자(보내는 쪽) 수 관리
}

func NewMailbox[T any](size int) *Mailbox[T] {
	return &Mailbox[T]{
		ch:     make(chan T, size),
		closed: make(chan struct{}),
	}
}

// 다중 생산자일 경우, 각 생산 루틴 시작 전에 AddSender(), 종료 시 SenderDone() 호출

func (m *Mailbox[T]) AddSender()  { m.wg.Add(1) }
func (m *Mailbox[T]) SenderDone() { m.wg.Done() }

// 논블로킹 시도

func (m *Mailbox[T]) TryEnqueue(v T) bool {
	select {
	case <-m.closed:
		return false
	default:
	}

	select {
	case <-m.closed:
		return false
	case m.ch <- v:
		return true
	default:
		return false
	}
}

// 컨텍스트로 대기 가능(블로킹 가능)

func (m *Mailbox[T]) Enqueue(ctx context.Context, v T) bool {
	// TODO 방어적 코드가 필요할까?
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-m.closed:
		return false
	default:
	}

	select {
	case <-m.closed:
		return false
	case m.ch <- v:
		return true
	case <-ctx.Done():
		return false
	}
}

// 소비자용: 읽기 전용 채널만 노출

func (m *Mailbox[T]) C() <-chan T { return m.ch }

// Close는 “더 이상 새 메시지 금지”를 알리고, 모든 생산자가 끝나면 ch를 닫음.

func (m *Mailbox[T]) Close() {
	m.once.Do(func() {
		close(m.closed)
		// 모든 생산자 종료 후 데이터 채널 닫기 → range m.C() 종료
		go func() {
			m.wg.Wait()
			close(m.ch)
		}()
	})
}

// utils 살펴보자.

// WithProducer AddSender()/SenderDone()를 자동 적용하는 래퍼. fn 안에서 m.TryEnqueue, m.Enqueue를 바로 사용하게 내린다.
func WithProducer[T any](m *Mailbox[T], fn func(
	try func(T) bool,
	sendCtx func(context.Context, T) bool,
)) {
	m.AddSender()
	defer m.SenderDone()
	fn(m.TryEnqueue, m.Enqueue)
}

// ForwardChan 외부 입력 채널(in)을 Mailbox로 안전히 포워딩.
//   - ctx 취소되면 종료
//   - m.Close()로 closed되면 드롭/종료
//   - 버퍼 풀땐 TryEnqueue가 false → 드롭(정책은 필요시 바꿔도 됨)
func ForwardChan[T any](ctx context.Context, m *Mailbox[T], in <-chan T) {
	WithProducer(m, func(try func(T) bool, sendCtx func(context.Context, T) bool) {
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return // 입력 채널 종료 → 생산자 종료
				}
				// 드롭 정책(논블로킹). 백프레셔 원하면 sendCtx(ctx, v)로 교체.
				_ = try(v)
			}
		}
	})
}

// StartProducerPool N개의 생산자 고루틴을 띄우고, cancel로 종료. 각 생산자는 gen 콜백을 실행(내부에서 try/sendCtx 제공) 반환된 cancel을 호출하면 모든 생산자 종료를 유도
// AddSender is called synchronously before each goroutine starts so that Close() called
// immediately after StartProducerPool returns cannot race with wg.Add inside the goroutine.
func StartProducerPool[T any](
	parent context.Context,
	m *Mailbox[T],
	n int,
	gen func(ctx context.Context, id int, try func(T) bool, sendCtx func(context.Context, T) bool),
) (cancel context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	for i := 0; i < n; i++ {
		m.AddSender()
		go func(id int) {
			defer m.SenderDone()
			gen(ctx, id, m.TryEnqueue, m.Enqueue)
		}(i)
	}
	return cancel
}
