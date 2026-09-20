package engine

import (
	"context"
	"sync"
)

type entryState uint8

const (
	entryPending entryState = iota
	entryRunning
	entryReady
	entryFailed
	entryEvicted
)

type entry struct {
	ctx    context.Context
	cancel context.CancelFunc
	call   Call
	key    Key

	started   chan struct{}
	startOnce sync.Once
	handle    Handle
	startErr  error

	// The fields below are actor-owned.
	state            entryState
	refs             int
	claims           int
	successfulClaims int
	callIDs          map[string]struct{}
	admitted         bool
	budgetReleased   bool
	budgetBytes      int64
}

func (e *entry) finishStart(handle Handle, err error) {
	e.startOnce.Do(func() {
		e.handle = handle
		e.startErr = err
		close(e.started)
	})
}

type observeMessage struct {
	seq  uint64
	call Call
	skip bool
}
type barrierMessage struct {
	seq   uint64
	reply chan<- bool
}
type barrierWaiter struct {
	seq   uint64
	reply chan<- bool
}
type claimMessage struct {
	key    Key
	callID string
	reply  chan<- *entry
}
type claimOutcomeMessage struct {
	entry *entry
	hit   bool
}
type startResultMessage struct {
	entry       *entry
	handle      Handle
	err         error
	budgetBytes int64
	admitted    bool
}
type completeMessage struct {
	handle     Handle
	completion Completion
}
type endMessage struct{ reply chan<- Metrics }

type turn struct {
	engine *Engine
	scope  Scope
	ctx    context.Context
	cancel context.CancelFunc
	events chan any
	done   chan struct{}

	byCall      map[string]*entry
	queues      map[Key][]*entry
	handles     map[Handle]*entry
	completions map[Handle]Completion
	entries     map[*entry]struct{}
	pending     map[uint64]observeMessage
	barriers    []barrierWaiter
	lastSeq     uint64
	metrics     Metrics
}

func newTurn(engine *Engine, parent context.Context, scope Scope) *turn {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &turn{
		engine:      engine,
		scope:       scope,
		ctx:         ctx,
		cancel:      cancel,
		events:      make(chan any, engine.cfg.TurnQueueDepth),
		done:        make(chan struct{}),
		byCall:      make(map[string]*entry),
		queues:      make(map[Key][]*entry),
		handles:     make(map[Handle]*entry),
		completions: make(map[Handle]Completion),
		entries:     make(map[*entry]struct{}),
		pending:     make(map[uint64]observeMessage),
	}
}

func (t *turn) run() {
	defer close(t.done)
	for {
		select {
		case <-t.engine.ctx.Done():
			t.finish(nil)
			return
		case <-t.ctx.Done():
			t.finish(nil)
			return
		case message := <-t.events:
			switch msg := message.(type) {
			case observeMessage:
				t.observeSequenced(msg)
			case barrierMessage:
				t.barrier(msg)
			case claimMessage:
				msg.reply <- t.selectClaim(msg.key, msg.callID)
			case claimOutcomeMessage:
				t.claimOutcome(msg)
			case startResultMessage:
				t.startResult(msg)
			case completeMessage:
				t.complete(msg)
			case endMessage:
				t.finish(msg.reply)
				return
			}
		}
	}
}

func (t *turn) observeSequenced(msg observeMessage) {
	if msg.seq == 0 {
		t.observe(msg.call)
		return
	}
	if msg.seq <= t.lastSeq {
		return
	}
	t.pending[msg.seq] = msg
	for {
		next := t.lastSeq + 1
		observation, ok := t.pending[next]
		if !ok {
			break
		}
		delete(t.pending, next)
		if !observation.skip {
			t.observe(observation.call)
		}
		t.lastSeq = next
	}
	t.releaseBarriers()
}

func (t *turn) barrier(msg barrierMessage) {
	if t.lastSeq >= msg.seq {
		msg.reply <- true
		return
	}
	t.barriers = append(t.barriers, barrierWaiter{seq: msg.seq, reply: msg.reply})
}

func (t *turn) releaseBarriers() {
	kept := t.barriers[:0]
	for _, waiter := range t.barriers {
		if t.lastSeq >= waiter.seq {
			waiter.reply <- true
		} else {
			kept = append(kept, waiter)
		}
	}
	t.barriers = kept
}

func (t *turn) observe(call Call) {
	if call.CallID == "" || call.Tool == "" {
		t.metrics.Rejected++
		return
	}
	key, err := CanonicalKey(call.Tool, call.Arguments)
	if err != nil {
		t.metrics.Rejected++
		return
	}
	if prior := t.byCall[call.CallID]; prior != nil {
		if prior.key == key && prior.call.Deterministic == call.Deterministic {
			return
		}
		t.detachCall(prior, call.CallID, true)
	}
	if int(t.metrics.Dispatched) >= t.engine.cfg.MaxDispatchesPerTurn {
		t.metrics.Rejected++
		return
	}
	if call.Deterministic {
		if existing := t.reusable(key); existing != nil {
			existing.refs++
			existing.callIDs[call.CallID] = struct{}{}
			t.byCall[call.CallID] = existing
			return
		}
	}
	entryCtx, cancel := context.WithCancel(t.ctx)
	candidate := &entry{
		ctx:     entryCtx,
		cancel:  cancel,
		call:    call,
		key:     key,
		started: make(chan struct{}),
		state:   entryPending,
		refs:    1,
		callIDs: map[string]struct{}{call.CallID: {}},
	}
	t.entries[candidate] = struct{}{}
	t.byCall[call.CallID] = candidate
	t.queues[key] = append(t.queues[key], candidate)
	if !t.engine.queueStart(t, candidate) {
		t.metrics.Rejected++
		t.evict(candidate, false)
		return
	}
	t.metrics.Dispatched++
}

func (t *turn) reusable(key Key) *entry {
	for _, candidate := range t.queues[key] {
		if candidate.call.Deterministic && candidate.state != entryFailed && candidate.state != entryEvicted {
			return candidate
		}
	}
	return nil
}

func (t *turn) selectClaim(key Key, callID string) *entry {
	if callID != "" {
		candidate := t.byCall[callID]
		if candidate == nil || candidate.key != key || candidate.state == entryFailed ||
			candidate.state == entryEvicted || candidate.claims >= candidate.refs {
			t.metrics.Misses++
			return nil
		}
		candidate.claims++
		if !candidate.call.Deterministic {
			for id := range candidate.callIDs {
				if t.byCall[id] == candidate {
					delete(t.byCall, id)
				}
			}
		}
		if !candidate.call.Deterministic || candidate.claims >= candidate.refs {
			t.removeFromQueue(candidate)
		}
		return candidate
	}
	queue := t.queues[key]
	for len(queue) > 0 {
		candidate := queue[0]
		if candidate.state == entryFailed || candidate.state == entryEvicted || candidate.claims >= candidate.refs {
			queue = queue[1:]
			continue
		}
		candidate.claims++
		if !candidate.call.Deterministic || candidate.claims >= candidate.refs {
			queue = queue[1:]
		}
		t.queues[key] = queue
		if len(queue) == 0 {
			delete(t.queues, key)
		}
		if !candidate.call.Deterministic {
			for callID := range candidate.callIDs {
				if t.byCall[callID] == candidate {
					delete(t.byCall, callID)
				}
			}
		}
		return candidate
	}
	delete(t.queues, key)
	t.metrics.Misses++
	return nil
}

func (t *turn) claimOutcome(msg claimOutcomeMessage) {
	candidate := msg.entry
	if _, exists := t.entries[candidate]; !exists {
		if msg.hit {
			t.metrics.Hits++
		} else {
			t.metrics.Misses++
		}
		return
	}
	if msg.hit {
		candidate.successfulClaims++
		t.metrics.Hits++
		return
	}
	t.metrics.Misses++
	if !candidate.call.Deterministic || candidate.successfulClaims == 0 && candidate.claims >= candidate.refs {
		t.evict(candidate, false)
	}
}

func (t *turn) startResult(msg startResultMessage) {
	candidate := msg.entry
	candidate.admitted = msg.admitted
	candidate.budgetBytes = msg.budgetBytes
	if _, exists := t.entries[candidate]; !exists || candidate.state == entryEvicted {
		if msg.handle != "" {
			t.engine.queueCancel(msg.handle)
		}
		t.releaseBudget(candidate)
		return
	}
	if msg.err != nil || msg.handle == "" {
		candidate.state = entryFailed
		t.metrics.StartFailures++
		t.releaseBudget(candidate)
		t.remove(candidate)
		return
	}
	candidate.state = entryRunning
	t.handles[msg.handle] = candidate
	if completion, ok := t.completions[msg.handle]; ok {
		delete(t.completions, msg.handle)
		t.complete(completeMessage{handle: msg.handle, completion: completion})
	}
}

func (t *turn) complete(msg completeMessage) {
	candidate := t.handles[msg.handle]
	if candidate == nil {
		if len(t.completions) < t.engine.cfg.TurnQueueDepth {
			t.completions[msg.handle] = msg.completion
		}
		return
	}
	t.releaseBudget(candidate)
	if msg.completion == CompletionReady {
		candidate.state = entryReady
		return
	}
	candidate.state = entryFailed
	delete(t.handles, msg.handle)
	t.remove(candidate)
}

func (t *turn) detachCall(candidate *entry, callID string, countEviction bool) {
	delete(t.byCall, callID)
	delete(candidate.callIDs, callID)
	if candidate.refs > 0 {
		candidate.refs--
	}
	if candidate.refs <= candidate.claims && candidate.successfulClaims == 0 {
		t.evict(candidate, countEviction)
	}
}

func (t *turn) evict(candidate *entry, count bool) {
	if candidate == nil || candidate.state == entryEvicted {
		return
	}
	candidate.state = entryEvicted
	candidate.cancel()
	candidate.finishStart("", context.Canceled)
	if count {
		t.metrics.Evictions++
	}
	if candidate.handle != "" && candidate.successfulClaims == 0 {
		t.engine.queueCancel(candidate.handle)
		t.metrics.Cancelled++
	}
	t.releaseBudget(candidate)
	t.remove(candidate)
}

func (t *turn) removeFromQueue(candidate *entry) {
	queue := t.queues[candidate.key]
	kept := queue[:0]
	for _, item := range queue {
		if item != candidate {
			kept = append(kept, item)
		}
	}
	if len(kept) == 0 {
		delete(t.queues, candidate.key)
	} else {
		t.queues[candidate.key] = kept
	}
}

func (t *turn) remove(candidate *entry) {
	delete(t.entries, candidate)
	if candidate.handle != "" {
		delete(t.handles, candidate.handle)
	}
	for callID := range candidate.callIDs {
		if t.byCall[callID] == candidate {
			delete(t.byCall, callID)
		}
	}
	t.removeFromQueue(candidate)
}

func (t *turn) releaseBudget(candidate *entry) {
	if candidate.admitted && !candidate.budgetReleased {
		candidate.budgetReleased = true
		t.engine.budget.release(candidate.budgetBytes)
	}
}

func (t *turn) finish(reply chan<- Metrics) {
	for candidate := range t.entries {
		if candidate.successfulClaims == 0 {
			t.evict(candidate, true)
		} else {
			candidate.cancel()
			candidate.finishStart("", context.Canceled)
			t.releaseBudget(candidate)
			t.remove(candidate)
		}
	}
	for _, waiter := range t.barriers {
		waiter.reply <- false
	}
	t.barriers = nil
	t.cancel()
	if reply != nil {
		reply <- t.metrics
	}
}

func (t *turn) trySend(message any) bool {
	select {
	case <-t.done:
		return false
	case t.events <- message:
		return true
	default:
		return false
	}
}

func (t *turn) send(ctx context.Context, message any) bool {
	select {
	case <-ctx.Done():
		return false
	case <-t.done:
		return false
	case t.events <- message:
		return true
	}
}

func (t *turn) sendInternal(message any) bool {
	select {
	case <-t.done:
		return false
	case <-t.engine.ctx.Done():
		return false
	case t.events <- message:
		return true
	}
}
