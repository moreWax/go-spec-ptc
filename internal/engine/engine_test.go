package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeBackend struct {
	mu          sync.Mutex
	starts      []StartRequest
	cancels     []Handle
	startGate   <-chan struct{}
	failCallIDs map[string]bool
	started     chan StartRequest
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{started: make(chan StartRequest, 128), failCallIDs: make(map[string]bool)}
}

func (b *fakeBackend) Start(ctx context.Context, req StartRequest) (Handle, error) {
	if b.startGate != nil {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-b.startGate:
		}
	}
	b.mu.Lock()
	b.starts = append(b.starts, req)
	fail := b.failCallIDs[req.Call.CallID]
	b.mu.Unlock()
	select {
	case b.started <- req:
	default:
	}
	if fail {
		return "", fmt.Errorf("start %s failed", req.Call.CallID)
	}
	return Handle("h-" + req.Call.CallID), nil
}

func (b *fakeBackend) Cancel(_ context.Context, handle Handle) error {
	b.mu.Lock()
	b.cancels = append(b.cancels, handle)
	b.mu.Unlock()
	return nil
}

func (b *fakeBackend) counts() (starts int, cancels []Handle) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.starts), append([]Handle(nil), b.cancels...)
}

func testScope(turn string) Scope {
	return Scope{Generation: 1, SessionID: "session", TurnID: turn, AttemptID: "attempt"}
}

func beginTestEngine(t *testing.T, backend Backend, cfg Config, scope Scope) *Engine {
	t.Helper()
	eng, err := New(backend, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	if err := eng.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	return eng
}

func observe(t *testing.T, eng *Engine, scope Scope, id, args string, deterministic bool) {
	t.Helper()
	if ok := eng.Observe(scope, Call{CallID: id, Tool: "query", Arguments: []byte(args), Deterministic: deterministic}); !ok {
		t.Fatalf("Observe(%s) rejected", id)
	}
}

func waitStarts(t *testing.T, backend *fakeBackend, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-backend.started:
		case <-deadline:
			t.Fatalf("timed out waiting for %d starts", n)
		}
	}
}

func claim(t *testing.T, eng *Engine, scope Scope, args string) Handle {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	handle, ok := eng.Claim(ctx, scope, "query", []byte(args))
	if !ok {
		t.Fatalf("Claim(%s) missed", args)
	}
	return handle
}

func endTurn(t *testing.T, eng *Engine, scope Scope) Metrics {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	metrics, err := eng.End(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	return metrics
}

func TestNondeterministicCallsClaimIndependentFIFOExecutions(t *testing.T) {
	backend := newFakeBackend()
	scope := testScope("fifo")
	eng := beginTestEngine(t, backend, Config{Workers: 4}, scope)
	for i := 1; i <= 3; i++ {
		observe(t, eng, scope, fmt.Sprintf("c%d", i), `{"prompt":"same"}`, false)
	}
	waitStarts(t, backend, 3)
	for i := 1; i <= 3; i++ {
		want := Handle(fmt.Sprintf("h-c%d", i))
		if got := claim(t, eng, scope, `{"prompt":"same"}`); got != want {
			t.Fatalf("claim %d = %q, want %q", i, got, want)
		}
	}
	metrics := endTurn(t, eng, scope)
	if metrics.Dispatched != 3 || metrics.Hits != 3 || metrics.Misses != 0 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

func TestDeterministicCallsShareOneExecution(t *testing.T) {
	backend := newFakeBackend()
	scope := testScope("deterministic")
	eng := beginTestEngine(t, backend, Config{}, scope)
	for i := 1; i <= 3; i++ {
		observe(t, eng, scope, fmt.Sprintf("c%d", i), `{"prompt":"same"}`, true)
	}
	waitStarts(t, backend, 1)
	for i := 0; i < 3; i++ {
		if got := claim(t, eng, scope, `{"prompt":"same"}`); got != "h-c1" {
			t.Fatalf("claim %d = %q, want h-c1", i, got)
		}
	}
	if starts, _ := backend.counts(); starts != 1 {
		t.Fatalf("starts = %d, want 1", starts)
	}
	metrics := endTurn(t, eng, scope)
	if metrics.Dispatched != 1 || metrics.Hits != 3 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

func TestChangedStreamedCallRetractsOldPrediction(t *testing.T) {
	backend := newFakeBackend()
	scope := testScope("retract")
	eng := beginTestEngine(t, backend, Config{}, scope)
	observe(t, eng, scope, "c1", `{"prompt":"old"}`, false)
	waitStarts(t, backend, 1)
	observe(t, eng, scope, "c1", `{"prompt":"new"}`, false)
	waitStarts(t, backend, 1)
	if got := claim(t, eng, scope, `{"prompt":"new"}`); got != "h-c1" {
		t.Fatalf("new claim = %q", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, ok := eng.Claim(ctx, scope, "query", []byte(`{"prompt":"old"}`)); ok {
		t.Fatal("old prediction remained claimable")
	}
	metrics := endTurn(t, eng, scope)
	if metrics.Evictions == 0 {
		t.Fatalf("metrics = %+v, want an eviction", metrics)
	}
}

func TestStartFailureDegradesToMiss(t *testing.T) {
	backend := newFakeBackend()
	backend.failCallIDs["bad"] = true
	scope := testScope("failure")
	eng := beginTestEngine(t, backend, Config{}, scope)
	observe(t, eng, scope, "bad", `{"prompt":"x"}`, false)
	waitStarts(t, backend, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, ok := eng.Claim(ctx, scope, "query", []byte(`{"prompt":"x"}`)); ok {
		t.Fatal("failed speculation was claimable")
	}
	metrics := endTurn(t, eng, scope)
	if metrics.StartFailures != 1 || metrics.Misses != 1 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

func TestCompletionReleasesConcurrencyBudget(t *testing.T) {
	backend := newFakeBackend()
	scope := testScope("budget")
	eng := beginTestEngine(t, backend, Config{Workers: 4, MaxInflight: 2}, scope)
	for i := 1; i <= 4; i++ {
		observe(t, eng, scope, fmt.Sprintf("c%d", i), fmt.Sprintf(`{"n":%d}`, i), false)
	}
	started := make([]StartRequest, 0, 2)
	for len(started) < 2 {
		select {
		case req := <-backend.started:
			started = append(started, req)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for initial starts")
		}
	}
	select {
	case req := <-backend.started:
		t.Fatalf("unexpected third start before completion: %s", req.Call.CallID)
	case <-time.After(50 * time.Millisecond):
	}
	if !eng.Complete(scope, Handle("h-"+started[0].Call.CallID), CompletionReady) {
		t.Fatal("completion rejected")
	}
	waitStarts(t, backend, 1)
	if !eng.Complete(scope, Handle("h-"+started[1].Call.CallID), CompletionReady) {
		t.Fatal("completion rejected")
	}
	waitStarts(t, backend, 1)
	_ = endTurn(t, eng, scope)
}

func TestEndCancelsEveryUnclaimedExecution(t *testing.T) {
	backend := newFakeBackend()
	scope := testScope("cancel")
	eng := beginTestEngine(t, backend, Config{}, scope)
	observe(t, eng, scope, "a", `{"n":1}`, false)
	observe(t, eng, scope, "b", `{"n":2}`, false)
	waitStarts(t, backend, 2)
	metrics := endTurn(t, eng, scope)
	if metrics.Cancelled != 2 || metrics.Evictions != 2 {
		t.Fatalf("metrics = %+v", metrics)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, cancels := backend.counts()
		if len(cancels) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cancels = %v, want 2", cancels)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestClaimCallPreservesIdentityUnderParallelClaimOrder(t *testing.T) {
	backend := newFakeBackend()
	scope := testScope("claim-call")
	eng := beginTestEngine(t, backend, Config{}, scope)
	arguments := []byte(`{"same":true}`)
	observe(t, eng, scope, "first", string(arguments), false)
	observe(t, eng, scope, "second", string(arguments), false)
	waitStarts(t, backend, 2)
	second, ok := eng.ClaimCall(context.Background(), scope, Call{CallID: "second", Tool: "query", Arguments: arguments})
	if !ok || second != "h-second" {
		t.Fatalf("second claim = %q, %v", second, ok)
	}
	first, ok := eng.ClaimCall(context.Background(), scope, Call{CallID: "first", Tool: "query", Arguments: arguments})
	if !ok || first != "h-first" {
		t.Fatalf("first claim = %q, %v", first, ok)
	}
	eng.Complete(scope, second, CompletionReady)
	eng.Complete(scope, first, CompletionReady)
	_ = endTurn(t, eng, scope)
}

func TestAdvanceSeqReleasesBarrierWithoutDispatch(t *testing.T) {
	backend := newFakeBackend()
	scope := testScope("advance")
	eng := beginTestEngine(t, backend, Config{}, scope)
	if !eng.AdvanceSeq(scope, 1) {
		t.Fatal("AdvanceSeq rejected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !eng.Barrier(ctx, scope, 1) {
		t.Fatal("barrier did not observe ignored sequence")
	}
	if starts, _ := backend.counts(); starts != 0 {
		t.Fatalf("ignored observation dispatched %d starts", starts)
	}
}

func TestObservationBarrierOrdersConcurrentFrames(t *testing.T) {
	backend := newFakeBackend()
	scope := testScope("barrier")
	eng := beginTestEngine(t, backend, Config{}, scope)
	if !eng.ObserveSeq(scope, 2, Call{CallID: "b", Tool: "query", Arguments: []byte(`{"n":2}`)}) {
		t.Fatal("ObserveSeq(2) rejected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	barrier := make(chan bool, 1)
	go func() { barrier <- eng.Barrier(ctx, scope, 2) }()
	select {
	case <-barrier:
		t.Fatal("barrier crossed before missing sequence arrived")
	case <-time.After(30 * time.Millisecond):
	}
	if !eng.ObserveSeq(scope, 1, Call{CallID: "a", Tool: "query", Arguments: []byte(`{"n":1}`)}) {
		t.Fatal("ObserveSeq(1) rejected")
	}
	select {
	case ok := <-barrier:
		if !ok {
			t.Fatal("barrier failed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("barrier did not cross")
	}
	waitStarts(t, backend, 2)
	_ = endTurn(t, eng, scope)
}

func TestStaleScopeCannotObserveClaimOrComplete(t *testing.T) {
	backend := newFakeBackend()
	scope := testScope("stale")
	eng := beginTestEngine(t, backend, Config{}, scope)
	_ = endTurn(t, eng, scope)
	if eng.Observe(scope, Call{CallID: "late", Tool: "query", Arguments: []byte(`{}`)}) {
		t.Fatal("stale Observe succeeded")
	}
	if _, ok := eng.Claim(context.Background(), scope, "query", []byte(`{}`)); ok {
		t.Fatal("stale Claim succeeded")
	}
	if eng.Complete(scope, "late", CompletionReady) {
		t.Fatal("stale Complete succeeded")
	}
}
