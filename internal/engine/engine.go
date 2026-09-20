package engine

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

const scopeShardCount = 32

type scopeShard struct {
	mu    sync.RWMutex
	turns map[Scope]*turn
}

type startJob struct {
	turn  *turn
	entry *entry
}

type cancelJob struct {
	handle Handle
}

// Engine owns all active turn actors and the bounded global worker pools.
// Provider callbacks enqueue call-local work; one actor serializes each turn's
// store while independent turns and executions run concurrently.
type Engine struct {
	backend Backend
	cfg     Config

	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool
	budget *admissionBudget
	starts chan startJob
	stops  chan cancelJob
	shards [scopeShardCount]scopeShard
	wg     sync.WaitGroup
}

// New starts a speculation engine. Close must be called to cancel active work
// and join its workers.
func New(backend Backend, cfg Config) (*Engine, error) {
	if backend == nil {
		return nil, errors.New("specptc: nil backend")
	}
	cfg = cfg.normalized()
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		backend: backend,
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
		budget:  newAdmissionBudget(cfg.MaxInflight, cfg.MaxInflightBytes),
		starts:  make(chan startJob, cfg.QueueDepth),
		stops:   make(chan cancelJob, cfg.QueueDepth),
	}
	for i := range e.shards {
		e.shards[i].turns = make(map[Scope]*turn)
	}
	for range cfg.Workers {
		e.wg.Add(1)
		go e.startWorker()
	}
	for range cfg.CancelWorkers {
		e.wg.Add(1)
		go e.cancelWorker()
	}
	return e, nil
}

// Begin creates an isolated turn actor. Repeating an active scope is rejected;
// callers must End the old scope instead of merging lifetimes.
func (e *Engine) Begin(parent context.Context, scope Scope) error {
	if e.closed.Load() {
		return ErrClosed
	}
	if !scope.Valid() {
		return errors.New("specptc: invalid scope")
	}
	shard := e.shard(scope)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if _, exists := shard.turns[scope]; exists {
		return errors.New("specptc: scope already active")
	}
	t := newTurn(e, parent, scope)
	shard.turns[scope] = t
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		t.run()
	}()
	return nil
}

// Observe queues a concrete streamed call without blocking the provider read
// loop. false means the turn is stale or saturated; baseline execution remains
// authoritative in either case.
func (e *Engine) Observe(scope Scope, call Call) bool {
	return e.ObserveSeq(scope, 0, call)
}

// ObserveSeq queues an observation with a monotonic per-scope sequence. A zero
// sequence keeps compatibility with direct callers that do not need barriers.
func (e *Engine) ObserveSeq(scope Scope, seq uint64, call Call) bool {
	t := e.lookup(scope)
	if t == nil {
		return false
	}
	arguments := append(json.RawMessage(nil), call.Arguments...)
	call.Arguments = arguments
	return t.trySend(observeMessage{seq: seq, call: call})
}

// AdvanceSeq records an intentionally ignored observation so later barriers do
// not mistake an ineligible tool for a dropped frame.
func (e *Engine) AdvanceSeq(scope Scope, seq uint64) bool {
	t := e.lookup(scope)
	if t == nil || seq == 0 {
		return false
	}
	return t.trySend(observeMessage{seq: seq, skip: true})
}

// Barrier waits until every sequenced observation through seq has been applied.
// A false result is a stale scope or caller cancellation and must degrade to a
// normal non-speculative execution.
func (e *Engine) Barrier(ctx context.Context, scope Scope, seq uint64) bool {
	if seq == 0 {
		return e.lookup(scope) != nil
	}
	t := e.lookup(scope)
	if t == nil {
		return false
	}
	reply := make(chan bool, 1)
	if !t.send(ctx, barrierMessage{seq: seq, reply: reply}) {
		return false
	}
	select {
	case ok := <-reply:
		return ok
	case <-ctx.Done():
		return false
	}
}

// Claim returns the oldest matching speculative handle. Deterministic calls
// may reuse one handle for each observed occurrence; nondeterministic calls
// consume independent FIFO entries.
func (e *Engine) Claim(ctx context.Context, scope Scope, tool string, arguments []byte) (Handle, bool) {
	return e.claim(ctx, scope, "", tool, arguments)
}

// ClaimCall claims the execution attached to an exact streamed call while still
// validating its canonical tool/argument identity. This prevents parallel host
// execution from swapping nondeterministic FIFO results when goroutines reach
// the claim boundary out of provider order.
func (e *Engine) ClaimCall(ctx context.Context, scope Scope, call Call) (Handle, bool) {
	if call.CallID == "" {
		return "", false
	}
	return e.claim(ctx, scope, call.CallID, call.Tool, call.Arguments)
}

func (e *Engine) claim(ctx context.Context, scope Scope, callID, tool string, arguments []byte) (Handle, bool) {
	t := e.lookup(scope)
	if t == nil {
		return "", false
	}
	key, err := CanonicalKey(tool, arguments)
	if err != nil {
		return "", false
	}
	reply := make(chan *entry, 1)
	if !t.send(ctx, claimMessage{key: key, callID: callID, reply: reply}) {
		return "", false
	}
	var selected *entry
	select {
	case <-ctx.Done():
		return "", false
	case selected = <-reply:
	}
	if selected == nil {
		return "", false
	}
	select {
	case <-ctx.Done():
		t.send(context.Background(), claimOutcomeMessage{entry: selected, hit: false})
		return "", false
	case <-selected.started:
		if selected.startErr != nil || selected.handle == "" {
			if !t.send(ctx, claimOutcomeMessage{entry: selected, hit: false}) {
				return "", false
			}
			return "", false
		}
		if !t.send(ctx, claimOutcomeMessage{entry: selected, hit: true}) {
			return "", false
		}
		return selected.handle, true
	}
}

// Complete releases global admission for one host execution and updates whether
// it remains claimable. Unknown or stale handles are ignored.
func (e *Engine) Complete(scope Scope, handle Handle, completion Completion) bool {
	t := e.lookup(scope)
	if t == nil {
		return false
	}
	return t.trySend(completeMessage{handle: handle, completion: completion})
}

// End fences the scope, cancels unclaimed work, and returns final metrics.
func (e *Engine) End(ctx context.Context, scope Scope) (Metrics, error) {
	t := e.remove(scope)
	if t == nil {
		return Metrics{}, ErrStaleScope
	}
	reply := make(chan Metrics, 1)
	if !t.send(ctx, endMessage{reply: reply}) {
		t.cancel()
		return Metrics{}, ctx.Err()
	}
	select {
	case metrics := <-reply:
		return metrics, nil
	case <-ctx.Done():
		return Metrics{}, ctx.Err()
	}
}

// Close cancels every active turn and joins all actor and worker goroutines.
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	for i := range e.shards {
		shard := &e.shards[i]
		shard.mu.Lock()
		for scope, t := range shard.turns {
			delete(shard.turns, scope)
			t.trySend(endMessage{})
		}
		shard.mu.Unlock()
	}
	e.cancel()
	e.budget.close()
	e.wg.Wait()
	return nil
}

func (e *Engine) startWorker() {
	defer e.wg.Done()
	for {
		select {
		case <-e.ctx.Done():
			return
		case job := <-e.starts:
			e.runStart(job)
		}
	}
}

func (e *Engine) runStart(job startJob) {
	entry := job.entry
	bytes := int64(len(entry.call.Arguments))
	if err := e.budget.acquire(entry.ctx, bytes); err != nil {
		entry.finishStart("", err)
		job.turn.sendInternal(startResultMessage{entry: entry, err: err})
		return
	}
	handle, err := e.backend.Start(entry.ctx, StartRequest{Scope: job.turn.scope, Call: entry.call, Key: entry.key})
	entry.finishStart(handle, err)
	result := startResultMessage{entry: entry, handle: handle, err: err, budgetBytes: bytes, admitted: true}
	if !job.turn.sendInternal(result) {
		if handle != "" {
			e.queueCancel(handle)
		}
		e.budget.release(bytes)
	}
}

func (e *Engine) cancelWorker() {
	defer e.wg.Done()
	for {
		select {
		case <-e.ctx.Done():
			return
		case job := <-e.stops:
			ctx, cancel := context.WithTimeout(context.Background(), e.cfg.CancelTimeout)
			_ = e.backend.Cancel(ctx, job.handle)
			cancel()
		}
	}
}

func (e *Engine) queueStart(t *turn, entry *entry) bool {
	select {
	case <-e.ctx.Done():
		return false
	case e.starts <- startJob{turn: t, entry: entry}:
		return true
	default:
		return false
	}
}

func (e *Engine) queueCancel(handle Handle) {
	if handle == "" {
		return
	}
	select {
	case <-e.ctx.Done():
	case e.stops <- cancelJob{handle: handle}:
	}
}

func (e *Engine) lookup(scope Scope) *turn {
	shard := e.shard(scope)
	shard.mu.RLock()
	t := shard.turns[scope]
	shard.mu.RUnlock()
	return t
}

func (e *Engine) remove(scope Scope) *turn {
	shard := e.shard(scope)
	shard.mu.Lock()
	t := shard.turns[scope]
	delete(shard.turns, scope)
	shard.mu.Unlock()
	return t
}

func (e *Engine) shard(scope Scope) *scopeShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(scope.SessionID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(scope.TurnID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(scope.AttemptID))
	return &e.shards[h.Sum32()%scopeShardCount]
}
