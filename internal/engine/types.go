package engine

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"time"
)

// Scope fences all speculative state to one exact runtime generation and model
// attempt. A stale frame can never claim work from another scope.
type Scope struct {
	Generation uint64
	SessionID  string
	TurnID     string
	AttemptID  string
}

// Valid reports whether the scope carries enough identity to isolate work.
func (s Scope) Valid() bool {
	return s.Generation > 0 && s.SessionID != "" && s.TurnID != "" && s.AttemptID != ""
}

// Call is one concrete candidate recovered from a provider stream. CallID is
// provider identity used to retract a prediction when later streamed content
// changes it. Arguments must be a complete JSON object before Observe.
type Call struct {
	CallID        string
	Tool          string
	Arguments     json.RawMessage
	Deterministic bool
}

// StartRequest asks the host to start one already canonicalized candidate.
type StartRequest struct {
	Scope Scope
	Call  Call
	Key   Key
}

// Handle is an opaque host-owned speculative execution token.
type Handle string

// Completion is the terminal execution state reported by the host.
type Completion string

const (
	CompletionReady  Completion = "ready"
	CompletionFailed Completion = "failed"
)

// Backend starts and cancels host-owned speculative executions. Start must
// return after admission with an opaque handle; execution completion arrives
// separately through Engine.Complete.
type Backend interface {
	Start(context.Context, StartRequest) (Handle, error)
	Cancel(context.Context, Handle) error
}

// Config bounds concurrency and memory. Zero values select conservative
// defaults. MaxInflightBytes measures canonical argument bytes admitted but not
// yet reported complete.
type Config struct {
	Workers              int
	QueueDepth           int
	TurnQueueDepth       int
	CancelWorkers        int
	MaxInflight          int
	MaxInflightBytes     int64
	MaxDispatchesPerTurn int
	CancelTimeout        time.Duration
}

func (c Config) normalized() Config {
	if c.Workers <= 0 {
		c.Workers = max(4, runtime.GOMAXPROCS(0)*2)
	}
	if c.QueueDepth <= 0 {
		c.QueueDepth = c.Workers * 8
	}
	if c.TurnQueueDepth <= 0 {
		c.TurnQueueDepth = 256
	}
	if c.CancelWorkers <= 0 {
		c.CancelWorkers = min(4, c.Workers)
	}
	if c.MaxInflight <= 0 {
		c.MaxInflight = 64
	}
	if c.MaxInflightBytes <= 0 {
		c.MaxInflightBytes = 8 << 20
	}
	if c.MaxDispatchesPerTurn <= 0 {
		c.MaxDispatchesPerTurn = 2048
	}
	if c.CancelTimeout <= 0 {
		c.CancelTimeout = 2 * time.Second
	}
	return c
}

// Metrics is one turn's immutable speculation accounting.
type Metrics struct {
	Dispatched    uint64
	Hits          uint64
	Misses        uint64
	Evictions     uint64
	Rejected      uint64
	StartFailures uint64
	Cancelled     uint64
}

// ErrClosed reports an engine that is shutting down.
var ErrClosed = errors.New("specptc: engine closed")

// ErrStaleScope reports a frame for a scope that is absent or already ended.
var ErrStaleScope = errors.New("specptc: stale scope")
