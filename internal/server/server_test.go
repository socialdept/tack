package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/socialdept/tack/internal/labelers"
	"github.com/socialdept/tack/internal/store"
)

func setup(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()

	s, err := store.Open("sqlite://" + filepath.Join(t.TempDir(), "tack.db"))
	if err != nil {
		t.Fatal(err)
	}

	m := &labelers.Manager{Store: s, Directory: identity.NewMockDirectory(), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	srv := &Server{Context: context.Background(), Store: s, Labelers: m, Password: "secret"}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	return ts, s
}

func call(t *testing.T, ts *httptest.Server, method, path, body string, authed bool) (int, map[string]any) {
	t.Helper()

	req, _ := http.NewRequest(method, ts.URL+path, bytes.NewBufferString(body))
	if authed {
		req.SetBasicAuth("admin", "secret")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)

	return resp.StatusCode, out
}

func TestHealthIsOpenAndEverythingElseNeedsThePassword(t *testing.T) {
	ts, _ := setup(t)

	if status, _ := call(t, ts, "GET", "/health", "", false); status != http.StatusOK {
		t.Errorf("health = %d", status)
	}

	if status, _ := call(t, ts, "GET", "/stats", "", false); status != http.StatusUnauthorized {
		t.Errorf("stats without auth = %d, want 401", status)
	}

	if status, _ := call(t, ts, "GET", "/stats", "", true); status != http.StatusOK {
		t.Errorf("stats with auth = %d", status)
	}
}

func TestAddingASubjectQueuesTheLabelsAlreadyStoredForIt(t *testing.T) {
	ts, s := setup(t)
	_, _ = s.PutLabel(store.Label{Src: "did:plc:mod", URI: "at://did:plc:alice/app.bsky.feed.post/3k", Val: "spam", Subject: "did:plc:alice", Cts: "2026-01-01T00:00:00.000Z", Verified: true}, false)
	_, _ = s.PutLabel(store.Label{Src: "did:plc:mod", URI: "did:plc:bob", Val: "spam", Subject: "did:plc:bob", Cts: "2026-01-01T00:00:00.000Z"}, false)

	status, out := call(t, ts, "POST", "/subjects/add", `{"dids":["did:plc:alice"]}`, true)
	if status != http.StatusOK || out["added"] != float64(1) || out["labelsQueued"] != float64(1) {
		t.Fatalf("add = %d %v", status, out)
	}

	// Adding it again queues nothing new.
	if _, out := call(t, ts, "POST", "/subjects/add", `{"dids":["did:plc:alice"]}`, true); out["labelsQueued"] != float64(0) {
		t.Errorf("second add queued %v", out["labelsQueued"])
	}

	pending, _ := s.Pending(10)
	if len(pending) != 1 {
		t.Fatalf("pending = %d, want alice's label only", len(pending))
	}

	_, out = call(t, ts, "GET", "/subjects/did:plc:alice", "", true)
	if out["subject"] != true || len(out["labels"].([]any)) != 1 {
		t.Errorf("subject = %v", out)
	}
}

func TestSubjectsMustBeDIDs(t *testing.T) {
	ts, _ := setup(t)

	if status, _ := call(t, ts, "POST", "/subjects/add", `{"dids":["alice.test"]}`, true); status != http.StatusBadRequest {
		t.Errorf("handle accepted as a subject: %d", status)
	}
}
