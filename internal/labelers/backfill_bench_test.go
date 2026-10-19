package labelers

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkPendingURIs times draining a queue of record URIs through a
// labeler that only answers for exact URIs, with several workers at once.
func BenchmarkPendingURIs(b *testing.B) {
	for range b.N {
		b.StopTimer()

		t := &testing.T{}
		f := newFakeLabeler(t)
		m, s := setup(t, f)
		m.QueryRate = 1e6
		_, _ = s.AddLabeler(labelerDID)
		wildcards := false
		_ = s.UpdateLabeler(labelerDID, map[string]any{"wildcards": &wildcards, "status": "connected"})
		_, _ = s.AddSubjects([]string{"did:plc:alice"})
		_ = s.MarkBackfilled("did:plc:alice")

		uris := make([]string, 20000)
		for i := range uris {
			uris[i] = fmt.Sprintf("at://did:plc:alice/app.example.post/%06d", i)
		}
		_, _ = s.AddPendingURIs(uris)

		f.mu.Lock()
		f.queries = nil
		f.mu.Unlock()

		ctx, cancel := context.WithCancel(context.Background())
		b.StartTimer()

		go m.Backfill(ctx, 4)

		for {
			left, _ := s.PendingURIs(1)
			if len(left) == 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}

		b.StopTimer()
		cancel()

		f.mu.Lock()
		b.ReportMetric(float64(len(f.queries)), "queries")
		f.mu.Unlock()
	}
}
