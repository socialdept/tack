// Package outbox delivers queued events to the app, the ways Tap does: to a
// webhook one event or one batch per request, or over a websocket at
// /channel, with or without acknowledgements. An event leaves the outbox only
// once it is acknowledged.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/socialdept/tack/internal/store"
)

// Mode is how events reach the app.
type Mode string

const (
	// ModeWebhook POSTs events to a URL, acknowledged by a 2xx.
	ModeWebhook Mode = "webhook"
	// ModeWebsocketAck sends events over /channel and waits for the app to
	// acknowledge each one, sending it again after RetryTimeout.
	ModeWebsocketAck Mode = "websocket-ack"
	// ModeFireAndForget sends events over /channel and counts each as
	// delivered once it is written.
	ModeFireAndForget Mode = "fire-and-forget"
)

// ModeFor picks the mode as Tap does: a webhook when a URL is set, otherwise
// the websocket, with acknowledgements unless they are disabled.
func ModeFor(webhookURL string, disableAcks bool) Mode {
	switch {
	case webhookURL != "":
		return ModeWebhook
	case disableAcks:
		return ModeFireAndForget
	default:
		return ModeWebsocketAck
	}
}

// Outbox hands queued events to whichever delivery the mode calls for, and
// keeps track of the ones sent and not yet acknowledged.
type Outbox struct {
	Store *store.Store
	Log   *slog.Logger
	Mode  Mode

	// Webhook delivery.
	URL         string
	Password    string // basic auth, user admin, as Tap sends it
	BatchSize   int    // events per request as {"events": [...]}, or 0 for one event per request
	Flush       time.Duration
	Parallelism int // requests in flight at once
	Client      *http.Client

	// RetryTimeout is how long an event sent over the websocket waits for an
	// acknowledgement before it is sent again.
	RetryTimeout time.Duration

	// Idle is how long to wait for new events when there are none.
	Idle time.Duration

	once     sync.Once
	wake     chan struct{}
	mu       sync.Mutex
	inFlight map[uint64]time.Time // event id: when it was sent
}

func (o *Outbox) init() {
	o.once.Do(func() {
		o.wake = make(chan struct{}, 1)
		o.inFlight = map[uint64]time.Time{}

		if o.Idle == 0 {
			o.Idle = time.Second
		}

		if o.RetryTimeout == 0 {
			o.RetryTimeout = time.Minute
		}

		if o.Parallelism < 1 {
			o.Parallelism = 1
		}
	})
}

// Wake tells the outbox there is something new.
func (o *Outbox) Wake() {
	o.init()

	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// Run delivers to the webhook until the context ends. In a websocket mode
// delivery happens in the /channel handler instead, and Run only waits.
func (o *Outbox) Run(ctx context.Context) {
	o.init()

	if o.Mode != ModeWebhook {
		<-ctx.Done()

		return
	}

	var wg sync.WaitGroup
	for range o.Parallelism {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o.webhookWorker(ctx)
		}()
	}

	wg.Wait()
}

// claim takes up to limit of the oldest events not already in flight. In
// websocket-ack mode an event whose acknowledgement is overdue counts as not
// in flight, so it is sent again.
func (o *Outbox) claim(limit int) ([]store.Event, error) {
	o.init()

	o.mu.Lock()
	defer o.mu.Unlock()

	events, err := o.Store.Pending(limit + len(o.inFlight))
	if err != nil {
		return nil, err
	}

	now := time.Now()
	claimed := make([]store.Event, 0, limit)

	for _, e := range events {
		if len(claimed) == limit {
			break
		}

		if sentAt, busy := o.inFlight[e.ID]; busy && !(o.Mode == ModeWebsocketAck && now.Sub(sentAt) > o.RetryTimeout) {
			continue
		}

		o.inFlight[e.ID] = now
		claimed = append(claimed, e)
	}

	return claimed, nil
}

// release puts events back, unacknowledged, to be sent again.
func (o *Outbox) release(ids []uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for _, id := range ids {
		delete(o.inFlight, id)
	}
}

// Ack drops acknowledged events from the outbox.
func (o *Outbox) Ack(ids []uint64) error {
	o.init()

	if err := o.Store.Acknowledge(ids); err != nil {
		return err
	}

	o.release(ids)

	return nil
}

// InFlight reports how many events are sent and waiting for acknowledgement.
func (o *Outbox) InFlight() int {
	o.init()

	o.mu.Lock()
	defer o.mu.Unlock()

	return len(o.inFlight)
}

// body is an event as the app receives it: its payload with its id added.
func body(e store.Event) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(e.Payload), &fields); err != nil {
		return nil, fmt.Errorf("event %d: %w", e.ID, err)
	}

	id, _ := json.Marshal(e.ID)
	fields["id"] = id

	return fields, nil
}

func ids(events []store.Event) []uint64 {
	out := make([]uint64, 0, len(events))
	for _, e := range events {
		out = append(out, e.ID)
	}

	return out
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
