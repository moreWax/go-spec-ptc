// Command reasonix-spec-ptc is a pure-Go speculative tool scheduler for
// Reasonix. It ports the store, budget, FIFO claim, deterministic reuse,
// cancellation, and stale-turn semantics of alexzhang13/spec-ptc while using
// Reasonix's structured tool stream instead of a Python shadow REPL.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	extension "github.com/esengine/DeepSeek-Reasonix/sdk/go"

	"github.com/moreWax/go-spec-ptc/internal/engine"
)

type toolPolicy struct {
	deterministic bool
}

type hostBackend struct {
	mu    sync.RWMutex
	host  extension.SpeculationHost
	bound bool

	handlesMu sync.Mutex
	active    map[extension.SpeculationScope]struct{}
	scopes    map[engine.Handle]extension.SpeculationScope
	completed map[engine.Handle]extension.SpeculationScope
}

func (b *hostBackend) bind(host extension.SpeculationHost) {
	b.mu.Lock()
	b.host = host
	b.bound = true
	b.mu.Unlock()
}

func (b *hostBackend) Start(ctx context.Context, req engine.StartRequest) (engine.Handle, error) {
	b.mu.RLock()
	host, bound := b.host, b.bound
	b.mu.RUnlock()
	if !bound {
		return "", errors.New("specptc: host is not bound")
	}
	scope := toWireScope(req.Scope)
	result, err := host.Start(ctx, extension.HostSpeculationStartParams{
		Scope: scope,
		Call: extension.SpeculationCall{
			ID:        req.Call.CallID,
			Name:      req.Call.Tool,
			Arguments: req.Call.Arguments,
		},
		Reusable: req.Call.Deterministic,
	})
	if err != nil {
		return "", err
	}
	if !result.Accepted {
		if result.Reason == "" {
			result.Reason = "host rejected speculative execution"
		}
		return "", errors.New(result.Reason)
	}
	handle := engine.Handle(result.Handle)
	b.trackStart(scope, handle)
	return handle, nil
}

func (b *hostBackend) Cancel(ctx context.Context, handle engine.Handle) error {
	b.handlesMu.Lock()
	scope, ok := b.scopes[handle]
	delete(b.scopes, handle)
	b.handlesMu.Unlock()
	if !ok {
		return nil
	}
	b.mu.RLock()
	host, bound := b.host, b.bound
	b.mu.RUnlock()
	if !bound {
		return nil
	}
	_, err := host.Cancel(ctx, extension.HostSpeculationCancelParams{
		Scope:  scope,
		Handle: string(handle),
	})
	return err
}

func (b *hostBackend) beginScope(scope extension.SpeculationScope) {
	b.handlesMu.Lock()
	if b.active == nil {
		b.active = make(map[extension.SpeculationScope]struct{})
	}
	b.active[scope] = struct{}{}
	b.handlesMu.Unlock()
}

func (b *hostBackend) trackStart(scope extension.SpeculationScope, handle engine.Handle) {
	b.handlesMu.Lock()
	if _, active := b.active[scope]; !active {
		b.handlesMu.Unlock()
		return
	}
	if _, completed := b.completed[handle]; completed {
		delete(b.completed, handle)
	} else {
		if b.scopes == nil {
			b.scopes = make(map[engine.Handle]extension.SpeculationScope)
		}
		b.scopes[handle] = scope
	}
	b.handlesMu.Unlock()
}

func (b *hostBackend) complete(scope extension.SpeculationScope, handle engine.Handle) {
	b.handlesMu.Lock()
	if _, active := b.active[scope]; !active {
		b.handlesMu.Unlock()
		return
	}
	if _, started := b.scopes[handle]; started {
		delete(b.scopes, handle)
	} else {
		if b.completed == nil {
			b.completed = make(map[engine.Handle]extension.SpeculationScope)
		}
		b.completed[handle] = scope
	}
	b.handlesMu.Unlock()
}

func (b *hostBackend) endScope(scope extension.SpeculationScope) {
	b.handlesMu.Lock()
	delete(b.active, scope)
	for handle, mapped := range b.scopes {
		if mapped == scope {
			delete(b.scopes, handle)
		}
	}
	for handle, mapped := range b.completed {
		if mapped == scope {
			delete(b.completed, handle)
		}
	}
	b.handlesMu.Unlock()
}

func (b *hostBackend) reset() {
	b.handlesMu.Lock()
	clear(b.active)
	clear(b.scopes)
	clear(b.completed)
	b.handlesMu.Unlock()
}

type plugin struct {
	log      *log.Logger
	policies map[string]toolPolicy
	backend  *hostBackend
	cfg      engine.Config

	mu     sync.Mutex
	engine *engine.Engine
}

func (p *plugin) Initialize(context.Context, extension.InitializeParams) (*extension.InitializeResult, error) {
	return &extension.InitializeResult{
		Name:     "spec-ptc",
		Version:  "0.1.0",
		Replaces: []string{"speculation"},
	}, nil
}

func (p *plugin) begin(ctx context.Context, params extension.SpeculationBeginParams) (extension.SpeculationBeginResult, error) {
	host, err := extension.CaptureSpeculationHost(ctx)
	if err != nil {
		return extension.SpeculationBeginResult{Reason: err.Error()}, nil
	}
	p.backend.bind(host)
	eng, err := p.runtime()
	if err != nil {
		return extension.SpeculationBeginResult{Reason: err.Error()}, nil
	}
	if err := eng.Begin(context.Background(), fromWireScope(params.Scope)); err != nil {
		return extension.SpeculationBeginResult{Reason: err.Error()}, nil
	}
	p.backend.beginScope(params.Scope)
	return extension.SpeculationBeginResult{Accepted: true}, nil
}

func (p *plugin) observe(_ context.Context, params extension.SpeculationObserveParams) (extension.SpeculationObserveResult, error) {
	policy, eligible := p.policies[params.Call.Name]
	eng := p.current()
	if eng == nil {
		return extension.SpeculationObserveResult{Reason: "speculation scope is not active"}, nil
	}
	if !eligible {
		accepted := eng.AdvanceSeq(fromWireScope(params.Scope), params.Seq)
		return extension.SpeculationObserveResult{Accepted: accepted}, nil
	}
	accepted := eng.ObserveSeq(fromWireScope(params.Scope), params.Seq, engine.Call{
		CallID:        params.Call.ID,
		Tool:          params.Call.Name,
		Arguments:     params.Call.Arguments,
		Deterministic: policy.deterministic,
	})
	result := extension.SpeculationObserveResult{Accepted: accepted}
	if !accepted {
		result.Reason = "scope is stale or its queue is saturated"
	}
	return result, nil
}

func (p *plugin) claim(ctx context.Context, params extension.SpeculationClaimParams) (extension.SpeculationClaimResult, error) {
	eng := p.current()
	if eng == nil || !eng.Barrier(ctx, fromWireScope(params.Scope), params.BarrierSeq) {
		return extension.SpeculationClaimResult{}, nil
	}
	handle, hit := eng.ClaimCall(ctx, fromWireScope(params.Scope), engine.Call{
		CallID: params.Call.ID, Tool: params.Call.Name, Arguments: params.Call.Arguments,
	})
	return extension.SpeculationClaimResult{Hit: hit, Handle: string(handle)}, nil
}

func (p *plugin) complete(ctx context.Context, params extension.SpeculationCompleteParams) (extension.SpeculationCompleteResult, error) {
	eng := p.current()
	if eng == nil {
		return extension.SpeculationCompleteResult{}, nil
	}
	completion := engine.CompletionFailed
	if params.Completion == extension.SpeculationReady {
		completion = engine.CompletionReady
	}
	handle := engine.Handle(params.Handle)
	accepted := eng.Complete(ctx, fromWireScope(params.Scope), handle, completion)
	if accepted {
		p.backend.complete(params.Scope, handle)
	}
	return extension.SpeculationCompleteResult{Accepted: accepted}, nil
}

func (p *plugin) end(ctx context.Context, params extension.SpeculationEndParams) (extension.SpeculationEndResult, error) {
	defer p.backend.endScope(params.Scope)
	eng := p.current()
	if eng == nil {
		return extension.SpeculationEndResult{}, nil
	}
	scope := fromWireScope(params.Scope)
	_ = eng.Barrier(ctx, scope, params.BarrierSeq)
	cleanupTimeout := p.cfg.CancelTimeout
	if cleanupTimeout <= 0 {
		cleanupTimeout = 2 * time.Second
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	metrics, err := eng.End(cleanupCtx, scope)
	cancel()
	if err != nil && !errors.Is(err, engine.ErrStaleScope) {
		return extension.SpeculationEndResult{}, err
	}
	return extension.SpeculationEndResult{Metrics: extension.SpeculationMetrics{
		Dispatched: metrics.Dispatched, Hits: metrics.Hits, Misses: metrics.Misses,
		Evictions: metrics.Evictions, Rejected: metrics.Rejected,
		StartFailures: metrics.StartFailures, Cancelled: metrics.Cancelled,
	}}, nil
}

func (p *plugin) runtime() (*engine.Engine, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.engine != nil {
		return p.engine, nil
	}
	eng, err := engine.New(p.backend, p.cfg)
	if err != nil {
		return nil, err
	}
	p.engine = eng
	return eng, nil
}

func (p *plugin) current() *engine.Engine {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.engine
}

func (p *plugin) shutdown(context.Context) {
	p.mu.Lock()
	eng := p.engine
	p.engine = nil
	p.mu.Unlock()
	if eng != nil {
		_ = eng.Close()
	}
	p.backend.reset()
}

func toWireScope(scope engine.Scope) extension.SpeculationScope {
	return extension.SpeculationScope{
		Generation: scope.Generation, SessionID: scope.SessionID,
		TurnID: scope.TurnID, AttemptID: scope.AttemptID,
	}
}

func fromWireScope(scope extension.SpeculationScope) engine.Scope {
	return engine.Scope{
		Generation: scope.Generation, SessionID: scope.SessionID,
		TurnID: scope.TurnID, AttemptID: scope.AttemptID,
	}
}

func parsePolicies(value string) map[string]toolPolicy {
	if strings.TrimSpace(value) == "" {
		value = "read_file"
	}
	out := make(map[string]toolPolicy)
	for item := range strings.SplitSeq(value, ",") {
		item = strings.TrimSpace(item)
		if strings.EqualFold(item, "none") {
			return map[string]toolPolicy{}
		}
		if item == "" {
			continue
		}
		name, option, _ := strings.Cut(item, ":")
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		out[name] = toolPolicy{deterministic: strings.EqualFold(strings.TrimSpace(option), "deterministic")}
	}
	return out
}

func envInt(name string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func main() {
	logger := log.New(os.Stderr, "spec-ptc: ", log.LstdFlags)
	policies := parsePolicies(os.Getenv("REASONIX_SPEC_PTC_TOOLS"))
	if len(policies) == 0 {
		logger.Print("no tools enabled; REASONIX_SPEC_PTC_TOOLS=none disables speculation")
	}
	backend := &hostBackend{}
	p := &plugin{
		log: logger, policies: policies, backend: backend,
		cfg: engine.Config{
			Workers:              envInt("REASONIX_SPEC_PTC_WORKERS", max(8, runtime.GOMAXPROCS(0)*4)),
			MaxInflight:          envInt("REASONIX_SPEC_PTC_MAX_INFLIGHT", 64),
			MaxDispatchesPerTurn: envInt("REASONIX_SPEC_PTC_MAX_DISPATCHES", 2048),
			MaxInflightBytes:     int64(envInt("REASONIX_SPEC_PTC_MAX_ARGUMENT_BYTES", 8<<20)),
			CancelTimeout:        2 * time.Second,
		},
	}
	err := extension.Serve(context.Background(), p, extension.Options{
		Name: "spec-ptc", Version: "0.1.0",
		Speculation: extension.SpeculationHandler{
			Begin: p.begin, Observe: p.observe, Claim: p.claim,
			Complete: p.complete, End: p.end,
		},
		Shutdown: p.shutdown,
		Logger:   logger,
	})
	if err != nil {
		logger.Printf("extension stopped: %v", err)
		os.Exit(1)
	}
}
