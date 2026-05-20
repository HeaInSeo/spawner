package actor_test

import (
	"context"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/HeaInSeo/spawner/pkg/actor"
)

func TestMailbox_TryEnqueueAndReceive(t *testing.T) {
	mb := actor.NewMailbox[int](1)
	mb.AddSender()
	defer mb.SenderDone()

	if !mb.TryEnqueue(7) {
		t.Fatal("expected first enqueue to succeed")
	}
	if mb.TryEnqueue(8) {
		t.Fatal("expected second enqueue to fail when mailbox is full")
	}

	got := <-mb.C()
	if got != 7 {
		t.Fatalf("expected 7, got %d", got)
	}
}

func TestMailbox_EnqueueHonorsContextAndClose(t *testing.T) {
	mb := actor.NewMailbox[int](1)
	mb.AddSender()
	defer mb.SenderDone()

	if !mb.Enqueue(context.Background(), 1) {
		t.Fatal("expected initial enqueue to succeed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if mb.Enqueue(ctx, 2) {
		t.Fatal("expected enqueue to fail after context timeout on full mailbox")
	}

	<-mb.C()
	mb.Close()
	if mb.Enqueue(context.Background(), 3) {
		t.Fatal("expected enqueue to fail after mailbox close")
	}
}

func TestMailbox_CloseClosesDataChannelAfterSendersFinish(t *testing.T) {
	mb := actor.NewMailbox[int](1)
	done := make(chan struct{})

	go func() {
		mb.AddSender()
		defer mb.SenderDone()
		if !mb.Enqueue(context.Background(), 11) {
			t.Error("expected enqueue before close to succeed")
		}
		close(done)
	}()

	<-done
	mb.Close()

	if got := <-mb.C(); got != 11 {
		t.Fatalf("expected queued value 11, got %d", got)
	}

	select {
	case _, ok := <-mb.C():
		if ok {
			t.Fatal("expected channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for closed mailbox channel")
	}
}

func TestWithProducer_RegistersSenderLifecycle(t *testing.T) {
	mb := actor.NewMailbox[int](1)

	actor.WithProducer(mb, func(try func(int) bool, _ func(context.Context, int) bool) {
		if !try(21) {
			t.Fatal("expected try enqueue to succeed")
		}
	})

	mb.Close()
	if got := <-mb.C(); got != 21 {
		t.Fatalf("expected produced value 21, got %d", got)
	}
	if _, ok := <-mb.C(); ok {
		t.Fatal("expected mailbox channel to close after producer completion")
	}
}

func TestForwardChan_ForwardsUntilInputClosed(t *testing.T) {
	mb := actor.NewMailbox[int](4)
	in := make(chan int, 3)
	in <- 1
	in <- 2
	in <- 3
	close(in)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go actor.ForwardChan(ctx, mb, in)

	values := collectInts(t, mb, 3)
	if values[0] != 1 || values[1] != 2 || values[2] != 3 {
		t.Fatalf("unexpected forwarded values: %v", values)
	}
}

func TestStartProducerPool_CancelStopsProducers(t *testing.T) {
	mb := actor.NewMailbox[int](32)
	ctx := context.Background()

	cancel := actor.StartProducerPool(ctx, mb, 2, func(ctx context.Context, id int, try func(int) bool, _ func(context.Context, int) bool) {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = try(id)
			}
		}
	})

	time.Sleep(20 * time.Millisecond)
	cancel()
	mb.Close()

	values := drainInts(mb)
	if len(values) == 0 {
		t.Fatal("expected producer pool to emit at least one value before cancellation")
	}
	for _, v := range values {
		if v != 0 && v != 1 {
			t.Fatalf("unexpected producer id value: %d", v)
		}
	}
}

// TestStartProducerPool_SendersRegisteredBeforeReturn: StartProducerPool 반환 시점에
// 모든 생산자의 wg.Add(1)이 완료되어 있어야 한다.
// 수정 전에는 AddSender가 고루틴 내부에서 지연 실행되어, 반환 직후 Close를 호출하면
// wg.Wait()이 count=0인 채로 통과해 채널을 조기 종료하는 lifecycle race가 있었다.
func TestStartProducerPool_SendersRegisteredBeforeReturn(t *testing.T) {
	mb := actor.NewMailbox[int](4)
	// 반환 즉시 cancel+Close: 수정 전이면 -race에서 탐지됨
	cancel := actor.StartProducerPool(context.Background(), mb, 3, func(ctx context.Context, _ int, _ func(int) bool, _ func(context.Context, int) bool) {
		<-ctx.Done()
	})
	cancel()
	mb.Close()
	for range mb.C() {
	}
	goleak.VerifyNone(t)
}

// TestMailbox_CloseAfterCancel_ChannelClosed: cancel 후 Close를 호출하면
// 모든 생산자 고루틴이 종료된 뒤 데이터 채널이 닫혀야 한다.
func TestMailbox_CloseAfterCancel_ChannelClosed(t *testing.T) {
	mb := actor.NewMailbox[int](4)
	cancel := actor.StartProducerPool(context.Background(), mb, 2, func(ctx context.Context, id int, try func(int) bool, _ func(context.Context, int) bool) {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = try(id)
			}
		}
	})

	cancel()
	mb.Close()

	// 채널이 닫힐 때까지 drain; 닫히지 않으면 타임아웃
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range mb.C() {
		}
	}()

	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("mailbox channel not closed after cancel+Close")
	}
	goleak.VerifyNone(t)
}

func TestMailbox_ProducerPool_NoLeak(t *testing.T) {
	mb := actor.NewMailbox[int](8)
	ctx := context.Background()

	cancel := actor.StartProducerPool(ctx, mb, 2, func(ctx context.Context, id int, try func(int) bool, _ func(context.Context, int) bool) {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = try(id)
			}
		}
	})

	cancel()
	mb.Close()

	for range mb.C() {
	}

	goleak.VerifyNone(t)
}

// TestMailbox_CloseIdempotent: Close를 여러 번 호출해도 panic 없이 채널이 정확히 한 번 닫혀야 한다.
func TestMailbox_CloseIdempotent(t *testing.T) {
	mb := actor.NewMailbox[int](4)
	mb.AddSender()

	mb.Close()
	mb.Close()
	mb.Close()

	mb.SenderDone()

	select {
	case _, ok := <-mb.C():
		if ok {
			t.Fatal("expected closed channel, got open")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("channel not closed after Close")
	}
}

// TestMailbox_TryEnqueueAfterClose_ReturnsFalse: Close 후 TryEnqueue는 블로킹 없이 false를 반환해야 한다.
func TestMailbox_TryEnqueueAfterClose_ReturnsFalse(t *testing.T) {
	mb := actor.NewMailbox[int](4)
	mb.AddSender()
	mb.Close()

	if mb.TryEnqueue(42) {
		t.Fatal("expected TryEnqueue to return false after Close")
	}

	mb.SenderDone()
	for range mb.C() {
	}
}

// TestMailbox_ProducerTryEnqueueDuringClose_NoPanic: Close 신호 이후 생산자가 TryEnqueue를 시도해도
// 패닉 없이 false를 반환해야 한다.
// Invariant: AddSender는 Close 이전에 호출하므로 wg.Wait()는 SenderDone 전에 완료되지 않는다.
// 따라서 m.ch는 생산자가 살아 있는 동안 close되지 않으며 panic이 발생할 수 없다.
func TestMailbox_ProducerTryEnqueueDuringClose_NoPanic(t *testing.T) {
	mb := actor.NewMailbox[int](4)
	mb.AddSender()

	proceed := make(chan struct{})
	producerDone := make(chan struct{})

	go func() {
		defer close(producerDone)
		defer mb.SenderDone()
		<-proceed
		// m.closed는 닫혀 있어야 하므로 TryEnqueue는 false를 반환해야 함
		if mb.TryEnqueue(99) {
			t.Error("expected TryEnqueue to return false after Close")
		}
	}()

	mb.Close()     // m.closed 닫힘; ch close는 SenderDone 대기 중
	close(proceed) // 생산자 진행

	<-producerDone

	select {
	case _, ok := <-mb.C():
		if ok {
			t.Fatal("expected closed channel after producer done")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("channel not closed after producer completed SenderDone")
	}
}

func collectInts(t *testing.T, mb *actor.Mailbox[int], n int) []int {
	t.Helper()
	values := make([]int, 0, n)
	timeout := time.After(200 * time.Millisecond)
	for len(values) < n {
		select {
		case v := <-mb.C():
			values = append(values, v)
		case <-timeout:
			t.Fatalf("timed out collecting %d mailbox values", n)
		}
	}
	return values
}

func drainInts(mb *actor.Mailbox[int]) []int {
	var values []int
	timeout := time.After(200 * time.Millisecond)
	for {
		select {
		case v, ok := <-mb.C():
			if !ok {
				return values
			}
			values = append(values, v)
		case <-timeout:
			return values
		}
	}
}
