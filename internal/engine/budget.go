package engine

import (
	"context"
	"sync"
)

// admissionBudget is a cancellation-aware weighted semaphore for both active
// executions and argument memory. Oversized single calls consume the entire
// byte budget, matching upstream's "one huge call may still run" rule.
type admissionBudget struct {
	mu       sync.Mutex
	changed  chan struct{}
	closed   bool
	active   int
	bytes    int64
	maxCalls int
	maxBytes int64
}

func newAdmissionBudget(maxCalls int, maxBytes int64) *admissionBudget {
	return &admissionBudget{
		changed:  make(chan struct{}),
		maxCalls: maxCalls,
		maxBytes: maxBytes,
	}
}

func (b *admissionBudget) acquire(ctx context.Context, bytes int64) error {
	if bytes > b.maxBytes {
		bytes = b.maxBytes
	}
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return ErrClosed
		}
		if b.active < b.maxCalls && b.bytes+bytes <= b.maxBytes {
			b.active++
			b.bytes += bytes
			b.mu.Unlock()
			return nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (b *admissionBudget) release(bytes int64) {
	if bytes > b.maxBytes {
		bytes = b.maxBytes
	}
	b.mu.Lock()
	if b.active > 0 {
		b.active--
	}
	b.bytes -= bytes
	if b.bytes < 0 {
		b.bytes = 0
	}
	b.signalLocked()
	b.mu.Unlock()
}

func (b *admissionBudget) close() {
	b.mu.Lock()
	b.closed = true
	b.signalLocked()
	b.mu.Unlock()
}

func (b *admissionBudget) signalLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}
