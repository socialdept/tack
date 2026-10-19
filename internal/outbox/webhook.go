package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/socialdept/tack/internal/store"
)

// webhookWorker sends one request at a time. With Parallelism above one,
// several run, and events may reach the app out of order: a label carries
// its own time, so the app can keep the newest.
func (o *Outbox) webhookWorker(ctx context.Context) {
	backoff := time.Second

	for ctx.Err() == nil {
		sent, err := o.deliverOnce(ctx)

		switch {
		case err != nil:
			o.Log.Warn("webhook delivery failed, retrying", "err", err, "in", backoff)
			sleep(ctx, backoff)
			backoff = min(backoff*2, time.Minute)
		case sent == 0:
			backoff = time.Second
			select {
			case <-ctx.Done():
			case <-o.wake:
			case <-time.After(o.Idle):
			}
		default:
			backoff = time.Second
		}
	}
}

// deliverOnce sends the next request's worth of events and reports how many
// the app acknowledged. A batch smaller than BatchSize waits up to Flush for
// more, so a trickle of events still travels in batches.
func (o *Outbox) deliverOnce(ctx context.Context) (int, error) {
	size := max(o.BatchSize, 1)

	events, err := o.claim(size)
	if err != nil || len(events) == 0 {
		return 0, err
	}

	if o.BatchSize > 0 && len(events) < size && o.Flush > 0 {
		o.release(ids(events))
		sleep(ctx, o.Flush)

		if events, err = o.claim(size); err != nil || len(events) == 0 {
			return 0, err
		}
	}

	if err := o.post(ctx, events); err != nil {
		o.release(ids(events))

		return 0, err
	}

	if err := o.Ack(ids(events)); err != nil {
		return 0, err
	}

	return len(events), nil
}

// post sends one event as the body, or a batch as {"events": [...]}.
func (o *Outbox) post(ctx context.Context, events []store.Event) error {
	var payload any

	if o.BatchSize > 0 {
		batch := make([]map[string]json.RawMessage, 0, len(events))
		for _, e := range events {
			b, err := body(e)
			if err != nil {
				return err
			}
			batch = append(batch, b)
		}
		payload = map[string]any{"events": batch}
	} else {
		b, err := body(events[0])
		if err != nil {
			return err
		}
		payload = b
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.URL, bytes.NewReader(encoded))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "tack")

	if o.Password != "" {
		req.SetBasicAuth("admin", o.Password)
	}

	client := o.Client
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))

		return fmt.Errorf("the webhook answered %d: %s", resp.StatusCode, bytes.TrimSpace(detail))
	}

	return nil
}
