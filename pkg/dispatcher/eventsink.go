package dispatcher

import (
	"time"

	"github.com/HeaInSeo/spawner/pkg/api"
)

type NoopSink struct{}

func (NoopSink) Send(api.Event) {}

type BufferedSink struct {
	ch chan api.Event
}

func NewBufferedSink(size int) *BufferedSink {
	if size <= 0 {
		size = 1
	}
	return &BufferedSink{ch: make(chan api.Event, size)}
}

func (s *BufferedSink) Send(ev api.Event) {
	s.ch <- ev
}

func (s *BufferedSink) TrySend(ev api.Event, timeout time.Duration) bool {
	if timeout <= 0 {
		select {
		case s.ch <- ev:
			return true
		default:
			return false
		}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case s.ch <- ev:
		return true
	case <-timer.C:
		return false
	}
}

func (s *BufferedSink) C() <-chan api.Event {
	return s.ch
}
