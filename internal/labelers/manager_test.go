package labelers

import (
	"bytes"
	"context"
	"encoding/base64"
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
	// history is what queryLabels serves, in the order given.
	history   []*comatproto.LabelDefs_Label
	wildcards bool
	queries   [][]string
	key       *atcrypto.PrivateKeyK256
	server    *httptest.Server
	mu        sync.Mutex
	frames    [][]byte
	cursors   []string
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
	mux.HandleFunc("/xrpc/com.atproto.label.queryLabels", func(w http.ResponseWriter, r *http.Request) {
		patterns := r.URL.Query()["uriPatterns"]

		f.mu.Lock()
		f.queries = append(f.queries, patterns)
		history := append([]*comatproto.LabelDefs_Label(nil), f.history...)
		wildcards := f.wildcards
		f.mu.Unlock()

		out := []map[string]any{}
		for _, p := range patterns {
			if strings.Contains(p, "*") && !wildcards {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":"InvalidRequest","message":"Support for wildcard label queries is temporarily disabled"}`)

				return
			}
		}

		for _, l := range history {
			for _, p := range patterns {
				prefix, wild := strings.CutSuffix(p, "*")
				if l.Uri == p || (wild && strings.HasPrefix(l.Uri, prefix)) {
					entry := map[string]any{"ver": 1, "src": l.Src, "uri": l.Uri, "val": l.Val, "cts": l.Cts, "sig": map[string]string{"$bytes": base64.RawStdEncoding.EncodeToString(l.Sig)}}
					if l.Neg != nil {
						entry["neg"] = *l.Neg
					}
					out = append(out, entry)

					break
				}
			}
		}

		_ = json.NewEncoder(w).Encode(map[string]any{"labels": out})
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
	return f.labelAt(uri, val, neg, forged, time.Now().UTC())
}

func (f *fakeLabeler) labelAt(uri, val string, neg, forged bool, at time.Time) *comatproto.LabelDefs_Label {
	f.t.Helper()

	l := labeling.Label{SourceDID: labelerDID, URI: uri, Val: val, CreatedAt: at.Format("2006-01-02T15:04:05.000Z"), Version: labeling.ATPROTO_LABEL_VERSION}
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

// liveLabels returns the label values the outbox holds for delivery.
func liveVals(t *testing.T, s *store.Store) map[string]bool {
	t.Helper()

	vals := map[string]bool{}
	for _, e := range queued(t, s) {
		if e["type"] == "label" {
			l := e["label"].(map[string]any)
			vals[l["uri"].(string)+" "+l["val"].(string)] = l["neg"] != true
		}
	}

	return vals
}

func TestANewSubjectsLabelsAreFetchedWithAWildcardWhereTheLabelerTakesOne(t *testing.T) {
	f := newFakeLabeler(t)
	f.wildcards = true
	day := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.history = []*comatproto.LabelDefs_Label{
		// A withdrawal served before the label it withdraws, as Ozone may.
		f.labelAt("at://did:plc:alice/app.example.post/1", "graphic", true, false, day.Add(2*time.Hour)),
		f.labelAt("at://did:plc:alice/app.example.post/1", "graphic", false, false, day),
		f.labelAt("did:plc:alice", "spam", false, false, day),
		f.labelAt("at://did:plc:alice/app.example.post/2", "porn", false, false, day),
		f.labelAt("did:plc:alice", "forged", false, true, day),
		f.labelAt("did:plc:bob", "spam", false, false, day),
	}
	m, s := setup(t, f)
	_, _ = s.AddLabeler(labelerDID)
	_, _ = s.AddSubjects([]string{"did:plc:alice"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Backfill(ctx, 1)

	waitFor(t, "alice to be backfilled", func() bool {
		sub, _ := s.Subject("did:plc:alice")
		return sub != nil && sub.BackfilledAt != nil
	})

	labels, _ := s.LabelsFor("did:plc:alice")
	got := map[string]bool{}
	for _, l := range labels {
		got[l.URI+" "+l.Val] = true
	}

	want := map[string]bool{"did:plc:alice spam": true, "at://did:plc:alice/app.example.post/2 porn": true}
	if len(got) != len(want) || !got["did:plc:alice spam"] || !got["at://did:plc:alice/app.example.post/2 porn"] {
		t.Errorf("stored %v, want %v: the withdrawn and forged labels dropped", got, want)
	}

	if vals := liveVals(t, s); !vals["did:plc:alice spam"] || !vals["at://did:plc:alice/app.example.post/2 porn"] {
		t.Errorf("queued %v", vals)
	}

	l, _ := s.Labeler(labelerDID)
	if l.Wildcards == nil || !*l.Wildcards {
		t.Error("the labeler was not recorded as taking wildcards")
	}
}

func TestALabelerWithoutWildcardsIsAskedAboutTheRecordURIsTheAppSends(t *testing.T) {
	f := newFakeLabeler(t)
	day := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.history = []*comatproto.LabelDefs_Label{
		f.labelAt("did:plc:alice", "spam", false, false, day),
		f.labelAt("at://did:plc:alice/app.example.post/1", "graphic", false, false, day),
	}
	m, s := setup(t, f)
	_, _ = s.AddLabeler(labelerDID)
	_, _ = s.AddSubjects([]string{"did:plc:alice"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Backfill(ctx, 1)

	waitFor(t, "alice to be backfilled", func() bool {
		sub, _ := s.Subject("did:plc:alice")
		return sub != nil && sub.BackfilledAt != nil
	})

	if labels, _ := s.LabelsFor("did:plc:alice"); len(labels) != 1 || labels[0].Val != "spam" {
		t.Fatalf("labels = %+v, want the account label only so far", labels)
	}

	l, _ := s.Labeler(labelerDID)
	if l.Wildcards == nil || *l.Wildcards {
		t.Fatal("the labeler was not recorded as refusing wildcards")
	}

	_, _ = s.AddPendingURIs([]string{"at://did:plc:alice/app.example.post/1"})
	m.WakeBackfill()

	waitFor(t, "the record label", func() bool {
		labels, _ := s.LabelsFor("did:plc:alice")
		return len(labels) == 2
	})

	if pending, _ := s.PendingURIs(10); len(pending) != 0 {
		t.Errorf("pending = %+v", pending)
	}

	// The refusal is remembered: later subjects skip the wildcard query.
	f.mu.Lock()
	f.queries = nil
	f.mu.Unlock()
	_, _ = s.AddSubjects([]string{"did:plc:carol"})
	m.WakeBackfill()

	waitFor(t, "carol to be backfilled", func() bool {
		sub, _ := s.Subject("did:plc:carol")
		return sub != nil && sub.BackfilledAt != nil
	})

	f.mu.Lock()
	defer f.mu.Unlock()
	for _, q := range f.queries {
		for _, p := range q {
			if strings.Contains(p, "*") {
				t.Errorf("asked with a wildcard again: %v", q)
			}
		}
	}
}
