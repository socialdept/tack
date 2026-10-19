package outbox

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Channel serves /channel: events as JSON text messages, one per message,
// shaped as a single webhook event. In websocket-ack mode the app answers
// each with {"type": "ack", "id": N}. One app connects at a time, so events
// keep their order.
type Channel struct {
	Outbox *Outbox

	mu        sync.Mutex
	connected bool
}

type ack struct {
	Type string `json:"type"`
	ID   uint64 `json:"id"`
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func (c *Channel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	o := c.Outbox
	o.init()

	if o.Mode == ModeWebhook {
		http.Error(w, "the websocket is not available while a webhook is set", http.StatusBadRequest)

		return
	}

	c.mu.Lock()
	if c.connected {
		c.mu.Unlock()
		http.Error(w, "another client is connected", http.StatusConflict)

		return
	}
	c.connected = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.connected = false
		c.mu.Unlock()
	}()

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Everything sent on this connection and not acknowledged goes back to
	// the outbox when it closes, for the next connection.
	var sent sync.Map

	defer func() {
		var unacked []uint64
		sent.Range(func(k, _ any) bool {
			unacked = append(unacked, k.(uint64))
			return true
		})
		o.release(unacked)
	}()

	go func() {
		defer cancel()

		for {
			var msg ack
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}

			if o.Mode == ModeWebsocketAck && msg.Type == "ack" {
				if err := o.Ack([]uint64{msg.ID}); err != nil {
					o.Log.Warn("could not acknowledge an event", "id", msg.ID, "err", err)
				}
				sent.Delete(msg.ID)
			}
		}
	}()

	o.Log.Info("channel connected")

	for ctx.Err() == nil {
		events, err := o.claim(100)
		if err != nil {
			o.Log.Warn("could not read the outbox", "err", err)
			sleep(ctx, time.Second)

			continue
		}

		if len(events) == 0 {
			select {
			case <-ctx.Done():
			case <-o.wake:
			case <-time.After(o.Idle):
			}

			continue
		}

		for i, e := range events {
			b, err := body(e)
			if err == nil {
				err = conn.WriteJSON(b)
			}

			if err != nil {
				o.release(ids(events[i:]))
				o.Log.Info("channel closed", "err", err)

				return
			}

			if o.Mode == ModeFireAndForget {
				if err := o.Ack([]uint64{e.ID}); err != nil {
					o.Log.Warn("could not drop a sent event", "id", e.ID, "err", err)
				}
			} else {
				sent.Store(e.ID, true)
			}
		}
	}

	o.Log.Info("channel disconnected")
}
