package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	extension "github.com/esengine/DeepSeek-Reasonix/sdk/go"

	"github.com/moreWax/go-spec-ptc/internal/engine"
)

func TestEndCleansScopeWhenBarrierFrameWasDropped(t *testing.T) {
	backend := &hostBackend{}
	eng, err := engine.New(backend, engine.Config{CancelTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	scope := engine.Scope{Generation: 1, SessionID: "s", TurnID: "t", AttemptID: "a"}
	if err := eng.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	if !eng.AdvanceSeq(scope, 1) {
		t.Fatal("advance sequence 1")
	}
	p := &plugin{backend: backend, engine: eng, cfg: engine.Config{CancelTimeout: 20 * time.Millisecond}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := p.end(ctx, extension.SpeculationEndParams{
		Scope:      extension.SpeculationScope{Generation: 1, SessionID: "s", TurnID: "t", AttemptID: "a"},
		BarrierSeq: 2,
	}); err != nil {
		t.Fatalf("end: %v", err)
	}
	if eng.AdvanceSeq(scope, 2) {
		t.Fatal("ended scope still accepted observations")
	}
	if err := eng.Begin(context.Background(), scope); err != nil {
		t.Fatalf("scope was not removed after failed barrier: %v", err)
	}
}

func TestHostBackendCompletionOrderingAndScopeCleanup(t *testing.T) {
	backend := &hostBackend{}
	scope := extension.SpeculationScope{Generation: 1, SessionID: "s", TurnID: "t", AttemptID: "a"}
	other := extension.SpeculationScope{Generation: 1, SessionID: "s", TurnID: "t", AttemptID: "other"}
	backend.beginScope(scope)
	backend.beginScope(other)
	const count = 512
	var wg sync.WaitGroup
	for i := range count {
		handle := engine.Handle(fmt.Sprintf("handle-%d", i))
		wg.Add(2)
		go func() {
			defer wg.Done()
			backend.complete(scope, handle)
		}()
		go func() {
			defer wg.Done()
			backend.trackStart(scope, handle)
		}()
	}
	wg.Wait()
	backend.trackStart(other, "other-handle")
	backend.complete(other, "other-pending")
	backend.endScope(scope)

	backend.handlesMu.Lock()
	defer backend.handlesMu.Unlock()
	if _, active := backend.active[scope]; active {
		t.Fatal("ended scope remained active")
	}
	if _, active := backend.active[other]; !active {
		t.Fatal("ending one scope deactivated another scope")
	}
	for handle, mapped := range backend.scopes {
		if mapped == scope {
			t.Fatalf("scope mapping leaked after end: %s", handle)
		}
	}
	for handle, mapped := range backend.completed {
		if mapped == scope {
			t.Fatalf("completion tombstone leaked after end: %s", handle)
		}
	}
	if backend.scopes["other-handle"] != other || backend.completed["other-pending"] != other {
		t.Fatal("ending one scope removed another scope's bookkeeping")
	}
}

func TestParsePoliciesDefaultsAndOverrides(t *testing.T) {
	defaults := parsePolicies("")
	if _, ok := defaults["read_file"]; !ok || len(defaults) != 1 {
		t.Fatalf("default policies = %#v, want only read_file", defaults)
	}
	if len(parsePolicies("none")) != 0 {
		t.Fatal("none must disable every policy")
	}
	custom := parsePolicies("query:deterministic, fetch")
	if !custom["query"].deterministic || custom["fetch"].deterministic || len(custom) != 2 {
		t.Fatalf("custom policies = %#v", custom)
	}
}
