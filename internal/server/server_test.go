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

func TestAddingSubjectsQueuesTheirRecordURIs(t *testing.T) {
	ts, s := setup(t)

	status, out := call(t, ts, "POST", "/subjects/add", `{"dids":["did:plc:alice"],"uris":["at://did:plc:alice/app.example.post/1","at://did:plc:bob/app.example.post/2"]}`, true)
	if status != http.StatusOK || out["added"] != float64(1) || out["urisQueued"] != float64(1) {
		t.Fatalf("add = %d %v", status, out)
	}

	if _, out := call(t, ts, "POST", "/subjects/uris", `{"uris":["at://did:plc:alice/app.example.post/3"]}`, true); out["urisQueued"] != float64(1) {
		t.Errorf("uris = %v", out)
	}

	if pending, _ := s.PendingURIs(10); len(pending) != 2 {
		t.Errorf("pending = %d, want alice's two records", len(pending))
	}

	_, out = call(t, ts, "GET", "/subjects/did:plc:alice", "", true)
	if out["subject"] != true || out["backfilledAt"] != nil {
		t.Errorf("subject = %v", out)
	}
}

func TestSubjectsMustBeDIDs(t *testing.T) {
	ts, _ := setup(t)

	if status, _ := call(t, ts, "POST", "/subjects/add", `{"dids":["alice.test"]}`, true); status != http.StatusBadRequest {
		t.Errorf("handle accepted as a subject: %d", status)
	}
}
