package actor_test

import (
	"context"
	"testing"
	"time"

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
