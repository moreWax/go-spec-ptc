package engine

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
)

type benchmarkBackend struct {
	next atomic.Uint64
}

func (b *benchmarkBackend) Start(context.Context, StartRequest) (Handle, error) {
	return Handle(fmt.Sprintf("h-%d", b.next.Add(1))), nil
}
func (*benchmarkBackend) Cancel(context.Context, Handle) error { return nil }

func BenchmarkConcurrentObserveClaim(b *testing.B) {
	backend := &benchmarkBackend{}
	eng, err := New(backend, Config{
		Workers:              max(8, runtime.GOMAXPROCS(0)*4),
		MaxInflight:          max(64, runtime.GOMAXPROCS(0)*8),
		MaxDispatchesPerTurn: b.N + 1,
		MaxInflightBytes:     int64(max(b.N, 1)) * 64,
		TurnQueueDepth:       max(1024, runtime.GOMAXPROCS(0)*128),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()
	scope := Scope{Generation: 1, SessionID: "bench", TurnID: "1", AttemptID: "1"}
	if err := eng.Begin(context.Background(), scope); err != nil {
		b.Fatal(err)
	}
	var next atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := next.Add(1)
			callID := fmt.Sprintf("c-%d", id)
			args := []byte(fmt.Sprintf(`{"n":%d}`, id))
			if !eng.Observe(scope, Call{CallID: callID, Tool: "query", Arguments: args}) {
				b.Errorf("observe %s rejected", callID)
				continue
			}
			handle, ok := eng.Claim(context.Background(), scope, "query", args)
			if !ok {
				b.Errorf("claim %s missed", callID)
				continue
			}
			eng.Complete(context.Background(), scope, handle, CompletionReady)
		}
	})
	b.StopTimer()
	if _, err := eng.End(context.Background(), scope); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkCanonicalKey(b *testing.B) {
	arguments := []byte(`{"query":"needle","path":"internal/agent","limit":200,"flags":{"hidden":false,"case":"smart"}}`)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := CanonicalKey("grep", arguments); err != nil {
			b.Fatal(err)
		}
	}
}
