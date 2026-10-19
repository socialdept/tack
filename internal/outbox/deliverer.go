// Package outbox delivers queued events to the app's webhook in batches,
// oldest first, and drops them only once a batch is acknowledged.
package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/socialdept/tack/internal/store"
)

// Deliverer posts batches to the webhook. One batch is in flight at a time,
// so the app receives events in the order they were queued.
type Deliverer struct {
	Store     *store.Store
	URL       string
	Password  string // sent as basic auth, user admin, as Tap does
	BatchSize int
	Client    *http.Client
	Log       *slog.Logger

	// Idle is how long to wait for new events when the outbox is empty.
	Idle time.Duration

	wake chan struct{}
}

// Wake tells the deliverer there is something new, so it does not wait out
// the idle interval.
func (d *Deliverer) Wake() {
	d.init()

	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *Deliverer) init() {
	if d.wake == nil {
		d.wake = make(chan struct{}, 1)
	}
}

// Run delivers until the context ends.
func (d *Deliverer) Run(ctx context.Context) {
	d.init()

	idle := d.Idle
	if idle == 0 {
		idle = time.Second
	}

	backoff := time.Second

	for ctx.Err() == nil {
		delivered, err := d.DeliverOnce(ctx)

		switch {
		case err != nil:
			d.Log.Warn("webhook delivery failed, retrying", "err", err, "in", backoff)
			sleep(ctx, backoff)
			backoff = min(backoff*2, time.Minute)
		case delivered == 0:
			backoff = time.Second
			select {
			case <-ctx.Done():
			case <-d.wake:
			case <-time.After(idle):
			}
		default:
			backoff = time.Second
		}
	}
}

// DeliverOnce sends the next batch, if any, and reports how many events the
// app acknowledged.
func (d *Deliverer) DeliverOnce(ctx context.Context) (int, error) {
	size := d.BatchSize
	if size <= 0 {
		size = 100
	}

	events, err := d.Store.Pending(size)
	if err != nil || len(events) == 0 {
		return 0, err
	}

	body, ids, err := batch(events)
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "tack")

	if d.Password != "" {
		req.SetBasicAuth("admin", d.Password)
	}

	client := d.Client
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))

		return 0, fmt.Errorf("the webhook answered %d: %s", resp.StatusCode, bytes.TrimSpace(detail))
	}

	if err := d.Store.Acknowledge(ids); err != nil {
		return 0, err
	}

	return len(ids), nil
}

// batch builds the request body: each event's payload with its id added.
func batch(events []store.Event) ([]byte, []uint64, error) {
	out := make([]map[string]json.RawMessage, 0, len(events))
	ids := make([]uint64, 0, len(events))

	for _, e := range events {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(e.Payload), &fields); err != nil {
			return nil, nil, fmt.Errorf("event %d: %w", e.ID, err)
		}

		id, _ := json.Marshal(e.ID)
		fields["id"] = id
		out = append(out, fields)
		ids = append(ids, e.ID)
	}

	body, err := json.Marshal(map[string]any{"events": out})

	return body, ids, err
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
