package labelers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/gorilla/websocket"
	"github.com/socialdept/tack/internal/store"
)

const labelerDID = "did:plc:labeler"

// fakeLabeler serves a labeler's PDS (its declaration) and its label stream.
type fakeLabeler struct {
	t           *testing.T
	declaration string
	key         *atcrypto.PrivateKeyK256
	server      *httptest.Server
	mu          sync.Mutex
	frames      [][]byte
	cursors     []string
}

func newFakeLabeler(t *testing.T) *fakeLabeler {
	t.Helper()

	key, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		t.Fatal(err)
	}

	f := &fakeLabeler{t: t, key: key}
	upgrader := websocket.Upgrader{}

	mux := http.NewServeMux()
	mux.HandleFunc("/xrpc/com.atproto.repo.getRecord", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		values := f.declaration
		f.mu.Unlock()
		if values == "" {
			values = `["spam"]`
		}
		_, _ = io.WriteString(w, `{"uri":"at://did:plc:labeler/app.bsky.labeler.service/self","value":{"$type":"app.bsky.labeler.service","policies":{"labelValues":`+values+`}}}`)
	})
	mux.HandleFunc("/xrpc/com.atproto.label.subscribeLabels", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.cursors = append(f.cursors, r.URL.Query().Get("cursor"))
		frames := append([][]byte(nil), f.frames...)
		f.mu.Unlock()

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		for _, frame := range frames {
			if err := conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
				return
			}
		}

		// Hold the stream open, as a labeler with nothing new does.
		_, _, _ = conn.ReadMessage()
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)

	return f
}

func (f *fakeLabeler) directory() identity.Directory {
	dir := identity.NewMockDirectory()
	pub, _ := f.key.PublicKey()
	dir.Insert(identity.Identity{
		DID:    syntax.DID(labelerDID),
		Handle: syntax.Handle("labeler.test"),
		Services: map[string]identity.ServiceEndpoint{
			"atproto_labeler": {Type: "AtprotoLabeler", URL: f.server.URL},
			"atproto_pds":     {Type: "AtprotoPersonalDataServer", URL: f.server.URL},
		},
		Keys: map[string]identity.VerificationMethod{
			"atproto_label": {Type: "Multikey", PublicKeyMultibase: pub.Multibase()},
		},
	})

	return dir
}

// label builds a signed label, or one signed by another key when forged.
func (f *fakeLabeler) label(uri, val string, neg, forged bool) *comatproto.LabelDefs_Label {
	f.t.Helper()

	l := labeling.Label{SourceDID: labelerDID, URI: uri, Val: val, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), Version: labeling.ATPROTO_LABEL_VERSION}
	if neg {
		l.Negated = &neg
	}

	key := f.key
	if forged {
		key, _ = atcrypto.GeneratePrivateKeyK256()
	}

	if err := l.Sign(key); err != nil {
		f.t.Fatal(err)
	}

	lex := l.ToLexicon()

	return &lex
}

func (f *fakeLabeler) send(seq int64, labels ...*comatproto.LabelDefs_Label) {
	f.t.Helper()

	var buf bytes.Buffer
	header := events.EventHeader{Op: events.EvtKindMessage, MsgType: "#labels"}
	if err := header.MarshalCBOR(&buf); err != nil {
		f.t.Fatal(err)
	}

	body := comatproto.LabelSubscribeLabels_Labels{Seq: seq, Labels: labels}
	if err := body.MarshalCBOR(&buf); err != nil {
		f.t.Fatal(err)
	}

	f.mu.Lock()
	f.frames = append(f.frames, buf.Bytes())
	f.mu.Unlock()
}

func setup(t *testing.T, f *fakeLabeler) (*Manager, *store.Store) {
	t.Helper()

	s, err := store.Open("sqlite://" + filepath.Join(t.TempDir(), "tack.db"))
	if err != nil {
		t.Fatal(err)
	}

	return &Manager{Store: s, Directory: f.directory(), Verify: true, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}, s
}

// waitFor polls until cond holds, or fails the test.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", what)
}

func queued(t *testing.T, s *store.Store) []map[string]any {
	t.Helper()

	events, err := s.Pending(100)
	if err != nil {
		t.Fatal(err)
	}

	var out []map[string]any
	for _, e := range events {
		var body map[string]any
		_ = json.Unmarshal([]byte(e.Payload), &body)
		out = append(out, body)
	}

	return out
}

func TestLabelsAreVerifiedStoredAndQueuedOnlyForSubjects(t *testing.T) {
	f := newFakeLabeler(t)
	m, s := setup(t, f)
	_, _ = s.AddSubjects([]string{"did:plc:alice"})

	f.send(1,
		f.label("at://did:plc:alice/app.bsky.feed.post/3k", "spam", false, false),
		f.label("did:plc:stranger", "spam", false, false),
		f.label("did:plc:alice", "forged", false, true),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := m.Add(ctx, labelerDID); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the cursor to move", func() bool {
		l, _ := s.Labeler(labelerDID)
		return l != nil && l.Cursor == 1
	})

	counts, _ := s.Counts()
	if counts["labels"] != 1 {
		t.Errorf("stored %d labels, want 1: the forged one dropped, the stranger's not kept", counts["labels"])
	}

	l, _ := s.Labeler(labelerDID)
	if l.Received != 3 || l.Rejected != 1 {
		t.Errorf("received=%d rejected=%d, want 3 and 1", l.Received, l.Rejected)
	}

	var types []string
	var labelURIs []string
	for _, e := range queued(t, s) {
		types = append(types, e["type"].(string))
		if e["type"] == "label" {
			labelURIs = append(labelURIs, e["label"].(map[string]any)["uri"].(string))
			if e["verified"] != true {
				t.Errorf("label not marked verified: %v", e)
			}
		}
	}

	if strings.Join(types, ",") != "labeler,label" {
		t.Errorf("queued %v, want the declaration then alice's label", types)
	}

	if len(labelURIs) != 1 || labelURIs[0] != "at://did:plc:alice/app.bsky.feed.post/3k" {
		t.Errorf("queued labels for %v, want only alice's post", labelURIs)
	}
}

func TestANegationIsQueuedAndTheCursorResumes(t *testing.T) {
	f := newFakeLabeler(t)
	m, s := setup(t, f)
	_, _ = s.AddSubjects([]string{"did:plc:alice"})

	f.send(7, f.label("did:plc:alice", "spam", false, false))
	f.send(8, f.label("did:plc:alice", "spam", true, false))

	ctx, cancel := context.WithCancel(context.Background())
	if err := m.Add(ctx, labelerDID); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "both frames", func() bool {
		l, _ := s.Labeler(labelerDID)
		return l != nil && l.Cursor == 8
	})
	cancel()

	if labels, _ := s.LabelsFor("did:plc:alice"); len(labels) != 0 {
		t.Errorf("negated label still stored: %+v", labels)
	}

	var negs []bool
	for _, e := range queued(t, s) {
		if e["type"] == "label" {
			negs = append(negs, e["label"].(map[string]any)["neg"].(bool))
		}
	}

	if len(negs) != 2 || negs[0] || !negs[1] {
		t.Errorf("queued negs %v, want the label then its negation", negs)
	}

	// A new run picks up after the last frame.
	m2 := &Manager{Store: s, Directory: f.directory(), Verify: true, Log: m.Log}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	if err := m2.Start(ctx2); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "a second connection", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.cursors) == 2
	})

	// The first connection starts at the labeler's current label.
	if f.cursors[0] != "" || f.cursors[1] != "8" {
		t.Errorf("cursors = %q, want none then 8", f.cursors)
	}
}

func TestRemovingALabelerTellsTheApp(t *testing.T) {
	f := newFakeLabeler(t)
	m, s := setup(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = m.Add(ctx, labelerDID)

	waitFor(t, "the connection", func() bool {
		l, _ := s.Labeler(labelerDID)
		return l != nil && l.Status == "connected"
	})

	if err := m.Remove(labelerDID); err != nil {
		t.Fatal(err)
	}

	events := queued(t, s)
	last := events[len(events)-1]
	if last["type"] != "labeler" || last["labeler"].(map[string]any)["removed"] != true {
		t.Errorf("last event %v, want the labeler removed", last)
	}
}

func TestAChangedDeclarationIsSentWhileConnected(t *testing.T) {
	f := newFakeLabeler(t)
	m, s := setup(t, f)
	m.DeclarationEvery = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = m.Add(ctx, labelerDID)

	waitFor(t, "the first declaration", func() bool { return len(queued(t, s)) == 1 })

	// Unchanged: nothing more is sent.
	time.Sleep(150 * time.Millisecond)
	if n := len(queued(t, s)); n != 1 {
		t.Fatalf("%d events for an unchanged declaration", n)
	}

	f.mu.Lock()
	f.declaration = `["spam","scam"]`
	f.mu.Unlock()

	waitFor(t, "the changed declaration", func() bool { return len(queued(t, s)) == 2 })
}

func TestEveryAccountMeansEveryLabelDeliveredAndNoneKept(t *testing.T) {
	f := newFakeLabeler(t)
	m, s := setup(t, f)
	m.AllSubjects = true

	f.send(1, f.label("did:plc:stranger", "spam", false, false), f.label("did:plc:other", "spam", false, false))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = m.Add(ctx, labelerDID)

	waitFor(t, "the frame", func() bool {
		l, _ := s.Labeler(labelerDID)
		return l != nil && l.Cursor == 1
	})

	labels := 0
	for _, e := range queued(t, s) {
		if e["type"] == "label" {
			labels++
		}
	}

	counts, _ := s.Counts()
	if labels != 2 || counts["labels"] != 0 {
		t.Errorf("delivered %d and kept %d, want 2 and 0", labels, counts["labels"])
	}
}
