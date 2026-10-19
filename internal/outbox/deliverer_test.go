package outbox

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/socialdept/tack/internal/store"
)

type webhook struct {
	mu      sync.Mutex
	status  int
	batches [][]map[string]any
	auth    []string
}

func (w *webhook) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	w.mu.Lock()
	defer w.mu.Unlock()

	user, pass, _ := r.BasicAuth()
	w.auth = append(w.auth, user+":"+pass)

	var body struct {
		Events []map[string]any `json:"events"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	w.batches = append(w.batches, body.Events)

	rw.WriteHeader(w.status)
}

func setup(t *testing.T, status int) (*Deliverer, *store.Store, *webhook) {
	t.Helper()

	s, err := store.Open("sqlite://" + filepath.Join(t.TempDir(), "tack.db"))
	if err != nil {
		t.Fatal(err)
	}

	hook := &webhook{status: status}
	server := httptest.NewServer(hook)
	t.Cleanup(server.Close)

	return &Deliverer{Store: s, URL: server.URL, Password: "secret", BatchSize: 2, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}, s, hook
}

func TestBatchesAreDeliveredInOrderWithTheirIdsAndDroppedOnceAcknowledged(t *testing.T) {
	d, s, hook := setup(t, http.StatusOK)

	for _, val := range []string{"a", "b", "c"} {
		_ = s.Enqueue("label", `{"type":"label","label":{"val":"`+val+`"}}`)
	}

	for {
		n, err := d.DeliverOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}

	if len(hook.batches) != 2 || len(hook.batches[0]) != 2 || len(hook.batches[1]) != 1 {
		t.Fatalf("batches = %v, want 2 then 1", hook.batches)
	}

	first := hook.batches[0][0]
	if first["id"] != float64(1) || first["type"] != "label" || first["label"].(map[string]any)["val"] != "a" {
		t.Errorf("first event = %v", first)
	}

	if hook.auth[0] != "admin:secret" {
		t.Errorf("auth = %q", hook.auth[0])
	}

	if left, _ := s.Pending(10); len(left) != 0 {
		t.Errorf("%d events left after acknowledgement", len(left))
	}
}

func TestAnUnacknowledgedBatchStaysQueued(t *testing.T) {
	d, s, hook := setup(t, http.StatusInternalServerError)
	_ = s.Enqueue("label", `{"type":"label"}`)

	if _, err := d.DeliverOnce(context.Background()); err == nil {
		t.Fatal("expected an error for a 500")
	}

	if left, _ := s.Pending(10); len(left) != 1 {
		t.Errorf("%d events left, want the one that failed", len(left))
	}

	hook.status = http.StatusOK
	if n, err := d.DeliverOnce(context.Background()); err != nil || n != 1 {
		t.Errorf("redelivery: n=%d err=%v", n, err)
	}

	if hook.batches[0][0]["id"] != hook.batches[1][0]["id"] {
		t.Error("the redelivered event changed id")
	}
}
