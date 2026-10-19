package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/socialdept/tack/internal/store"
)

type webhook struct {
	mu       sync.Mutex
	status   int
	requests []json.RawMessage
	auth     []string
}

func (w *webhook) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	w.mu.Lock()
	defer w.mu.Unlock()

	user, pass, _ := r.BasicAuth()
	w.auth = append(w.auth, user+":"+pass)

	body, _ := io.ReadAll(r.Body)
	w.requests = append(w.requests, body)

	rw.WriteHeader(w.status)
}

func (w *webhook) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return len(w.requests)
}

func newStore(t *testing.T) *store.Store {
	t.Helper()

	s, err := store.Open("sqlite://" + filepath.Join(t.TempDir(), "tack.db"))
	if err != nil {
		t.Fatal(err)
	}

	return s
}

func enqueue(t *testing.T, s *store.Store, vals ...string) {
	t.Helper()

	for _, v := range vals {
		if err := s.Enqueue("label", `{"type":"label","label":{"val":"`+v+`"}}`); err != nil {
			t.Fatal(err)
		}
	}
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func webhookOutbox(t *testing.T, batch int) (*Outbox, *store.Store, *webhook) {
	t.Helper()

	s := newStore(t)
	hook := &webhook{status: http.StatusOK}
	server := httptest.NewServer(hook)
	t.Cleanup(server.Close)

	return &Outbox{Store: s, Log: quiet(), Mode: ModeWebhook, URL: server.URL, Password: "secret", BatchSize: batch}, s, hook
}

func drain(t *testing.T, o *Outbox) {
	t.Helper()

	for {
		n, err := o.deliverOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
	}
}

func TestModeIsPickedAsTapPicksIt(t *testing.T) {
	cases := []struct {
		url    string
		noAcks bool
		want   Mode
	}{
		{"https://app.example/hook", false, ModeWebhook},
		{"https://app.example/hook", true, ModeWebhook},
		{"", false, ModeWebsocketAck},
		{"", true, ModeFireAndForget},
	}

	for _, c := range cases {
		if got := ModeFor(c.url, c.noAcks); got != c.want {
			t.Errorf("ModeFor(%q, %v) = %s, want %s", c.url, c.noAcks, got, c.want)
		}
	}
}

func TestAWebhookWithoutBatchingGetsOneEventPerRequest(t *testing.T) {
	o, s, hook := webhookOutbox(t, 0)
	enqueue(t, s, "a", "b")

	drain(t, o)

	if hook.count() != 2 {
		t.Fatalf("%d requests, want 2", hook.count())
	}

	var event map[string]any
	_ = json.Unmarshal(hook.requests[0], &event)
	if event["id"] != float64(1) || event["type"] != "label" || event["label"].(map[string]any)["val"] != "a" {
		t.Errorf("first request = %s, want the event itself", hook.requests[0])
	}

	if hook.auth[0] != "admin:secret" {
		t.Errorf("auth = %q", hook.auth[0])
	}
}

func TestABatchedWebhookGetsEventsInOrderAndDropsThemOnceAcknowledged(t *testing.T) {
	o, s, hook := webhookOutbox(t, 2)
	enqueue(t, s, "a", "b", "c")

	drain(t, o)

	var vals []string
	for _, req := range hook.requests {
		var body struct {
			Events []map[string]any `json:"events"`
		}
		_ = json.Unmarshal(req, &body)
		for _, e := range body.Events {
			vals = append(vals, e["label"].(map[string]any)["val"].(string))
		}
	}

	if strings.Join(vals, "") != "abc" || hook.count() != 2 {
		t.Errorf("%d requests carrying %v, want 2 carrying a b c", hook.count(), vals)
	}

	if left, _ := s.Pending(10); len(left) != 0 {
		t.Errorf("%d left", len(left))
	}
}

func TestAPartialBatchWaitsForMoreBeforeItIsSent(t *testing.T) {
	o, s, hook := webhookOutbox(t, 10)
	o.Flush = 200 * time.Millisecond
	enqueue(t, s, "a")

	go func() {
		time.Sleep(50 * time.Millisecond)
		enqueue(t, s, "b")
	}()

	if n, err := o.deliverOnce(context.Background()); err != nil || n != 2 {
		t.Fatalf("sent %d (err %v), want both in one batch", n, err)
	}

	if hook.count() != 1 {
		t.Errorf("%d requests, want 1", hook.count())
	}
}

func TestAnUnacknowledgedRequestIsSentAgainWithTheSameIds(t *testing.T) {
	o, s, hook := webhookOutbox(t, 0)
	hook.status = http.StatusInternalServerError
	enqueue(t, s, "a")

	if _, err := o.deliverOnce(context.Background()); err == nil {
		t.Fatal("a 500 was taken as delivered")
	}

	hook.status = http.StatusOK
	drain(t, o)

	var first, second map[string]any
	_ = json.Unmarshal(hook.requests[0], &first)
	_ = json.Unmarshal(hook.requests[1], &second)
	if first["id"] != second["id"] {
		t.Error("the retried event changed id")
	}
}

func TestParallelBatchesDeliverEveryEventExactlyOnce(t *testing.T) {
	o, s, hook := webhookOutbox(t, 3)
	o.Parallelism = 4
	o.Idle = 20 * time.Millisecond

	var want []string
	for i := range 50 {
		want = append(want, fmt.Sprint(i))
	}
	enqueue(t, s, want...)

	ctx, cancel := context.WithCancel(context.Background())
	go o.Run(ctx)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if left, _ := s.Pending(100); len(left) == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()

	seen := map[float64]int{}
	hook.mu.Lock()
	for _, req := range hook.requests {
		var body struct {
			Events []map[string]any `json:"events"`
		}
		_ = json.Unmarshal(req, &body)
		for _, e := range body.Events {
			seen[e["id"].(float64)]++
		}
	}
	hook.mu.Unlock()

	if len(seen) != 50 {
		t.Fatalf("%d distinct events delivered, want 50", len(seen))
	}

	for id, n := range seen {
		if n != 1 {
			t.Errorf("event %v delivered %d times", id, n)
		}
	}
}

// channel connects a websocket client to an outbox in a websocket mode.
func channel(t *testing.T, o *Outbox) (*websocket.Conn, *httptest.Server) {
	t.Helper()

	server := httptest.NewServer(&Channel{Outbox: o})
	t.Cleanup(server.Close)

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn, server
}

func read(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		t.Fatal(err)
	}

	return event
}

func waitPending(t *testing.T, s *store.Store, want int) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if left, _ := s.Pending(100); len(left) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	left, _ := s.Pending(100)
	t.Fatalf("%d pending, want %d", len(left), want)
}

func TestTheChannelWaitsForAcksAndResendsWhatIsNotAcked(t *testing.T) {
	s := newStore(t)
	o := &Outbox{Store: s, Log: quiet(), Mode: ModeWebsocketAck, RetryTimeout: 200 * time.Millisecond, Idle: 20 * time.Millisecond}
	enqueue(t, s, "a", "b")

	conn, _ := channel(t, o)

	a, b := read(t, conn), read(t, conn)
	if a["label"].(map[string]any)["val"] != "a" || b["label"].(map[string]any)["val"] != "b" {
		t.Fatalf("got %v then %v", a, b)
	}

	_ = conn.WriteJSON(map[string]any{"type": "ack", "id": a["id"]})
	waitPending(t, s, 1)

	// b was never acknowledged: it comes again after the retry timeout.
	if again := read(t, conn); again["id"] != b["id"] {
		t.Errorf("resent %v, want b", again)
	}
}

func TestAnUnackedEventGoesToTheNextConnection(t *testing.T) {
	s := newStore(t)
	o := &Outbox{Store: s, Log: quiet(), Mode: ModeWebsocketAck, Idle: 20 * time.Millisecond}
	enqueue(t, s, "a")

	conn, server := channel(t, o)
	first := read(t, conn)
	conn.Close()

	// The handler notices the close and puts the event back.
	deadline := time.Now().Add(3 * time.Second)
	for o.InFlight() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	conn2, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	if again := read(t, conn2); again["id"] != first["id"] {
		t.Errorf("second connection got %v, want the unacked event", again)
	}
}

func TestFireAndForgetDropsEventsOnceWritten(t *testing.T) {
	s := newStore(t)
	o := &Outbox{Store: s, Log: quiet(), Mode: ModeFireAndForget, Idle: 20 * time.Millisecond}
	enqueue(t, s, "a")

	conn, _ := channel(t, o)
	read(t, conn)

	waitPending(t, s, 0)
}

func TestOneChannelClientAtATime(t *testing.T) {
	s := newStore(t)
	o := &Outbox{Store: s, Log: quiet(), Mode: ModeWebsocketAck, Idle: 20 * time.Millisecond}

	_, server := channel(t, o)
	time.Sleep(50 * time.Millisecond)

	_, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusConflict {
		t.Errorf("second client: err=%v resp=%v, want a 409", err, resp)
	}
}

func TestTheChannelIsClosedWhileAWebhookIsSet(t *testing.T) {
	s := newStore(t)
	o := &Outbox{Store: s, Log: quiet(), Mode: ModeWebhook}

	rec := httptest.NewRecorder()
	(&Channel{Outbox: o}).ServeHTTP(rec, httptest.NewRequest("GET", "/channel", nil))

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d", rec.Code)
	}
}
