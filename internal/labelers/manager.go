// Package labelers follows labeling services: it resolves each one, reads
// its declaration, subscribes to its label stream from the saved cursor,
// verifies and stores every label, and queues what the app's subjects need.
package labelers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/gorilla/websocket"
	"github.com/socialdept/tack/internal/store"
)

// Manager runs one subscription per labeler.
type Manager struct {
	Store       *store.Store
	Directory   identity.Directory
	Verify      bool // drop labels whose signature does not check out
	AllSubjects bool // deliver labels about every account, not only subjects
	Log         *slog.Logger
	HTTP        *http.Client
	Dialer      *websocket.Dialer

	// Notify is called after events are queued, to wake the webhook.
	Notify func()

	// DeclarationEvery is how often a connected labeler's declaration is
	// read again. Six hours when zero.
	DeclarationEvery time.Duration

	mu      sync.Mutex
	running map[string]context.CancelFunc
}

// Start runs every stored labeler.
func (m *Manager) Start(ctx context.Context) error {
	labelers, err := m.Store.Labelers()
	if err != nil {
		return err
	}

	for _, l := range labelers {
		if l.Status != "paused" {
			m.run(ctx, l.DID)
		}
	}

	return nil
}

// Add follows a labeler and starts its subscription.
func (m *Manager) Add(ctx context.Context, did string) error {
	if _, err := syntax.ParseDID(did); err != nil {
		return fmt.Errorf("not a DID: %w", err)
	}

	if _, err := m.Store.AddLabeler(did); err != nil {
		return err
	}

	m.run(ctx, did)

	return nil
}

// Remove stops a labeler, forgets its labels, and tells the app.
func (m *Manager) Remove(did string) error {
	m.mu.Lock()
	if cancel, ok := m.running[did]; ok {
		cancel()
		delete(m.running, did)
	}
	m.mu.Unlock()

	err := m.Store.Tx(func(tx *store.Store) error {
		if err := tx.RemoveLabeler(did); err != nil {
			return err
		}

		return EnqueueLabeler(tx, LabelerPayload{DID: did, Removed: true})
	})
	m.notify()

	return err
}

func (m *Manager) run(ctx context.Context, did string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running == nil {
		m.running = map[string]context.CancelFunc{}
	}

	if _, ok := m.running[did]; ok {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	m.running[did] = cancel

	go m.loop(ctx, did)
}

// loop keeps one labeler subscribed, reconnecting with backoff.
func (m *Manager) loop(ctx context.Context, did string) {
	log := m.Log.With("labeler", did)
	backoff := time.Second

	for ctx.Err() == nil {
		started := time.Now()
		err := m.subscribe(ctx, did, log)

		if ctx.Err() != nil {
			return
		}

		msg := "disconnected"
		if err != nil {
			msg = err.Error()
		}

		log.Warn("labeler subscription ended", "err", msg)
		_ = m.Store.UpdateLabeler(did, map[string]any{"status": "error", "last_error": msg})

		// A connection that held for a while starts the backoff over.
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, 5*time.Minute)
	}
}

// labelerIdentity is what a labeler's DID document says about it.
type labelerIdentity struct {
	endpoint string
	pds      string
	key      atcrypto.PublicKey
}

func (m *Manager) resolve(ctx context.Context, did string, fresh bool) (*labelerIdentity, error) {
	atid, err := syntax.ParseAtIdentifier(did)
	if err != nil {
		return nil, err
	}

	if fresh {
		_ = m.Directory.Purge(ctx, atid)
	}

	ident, err := m.Directory.Lookup(ctx, atid)
	if err != nil {
		return nil, fmt.Errorf("resolving the DID: %w", err)
	}

	endpoint := ident.GetServiceEndpoint("atproto_labeler")
	if endpoint == "" {
		return nil, errors.New("the DID document has no #atproto_labeler service")
	}

	key, err := ident.GetPublicKey("atproto_label")
	if err != nil && m.Verify {
		return nil, fmt.Errorf("the DID document has no #atproto_label key: %w", err)
	}

	return &labelerIdentity{endpoint: strings.TrimRight(endpoint, "/"), pds: ident.PDSEndpoint(), key: key}, nil
}

func (m *Manager) subscribe(ctx context.Context, did string, log *slog.Logger) error {
	who, err := m.resolve(ctx, did, false)
	if err != nil {
		return err
	}

	if err := m.refreshDeclaration(ctx, did, who); err != nil {
		// Labels are still worth having without the declaration.
		log.Warn("could not read the labeler declaration", "err", err)
	}

	labeler, err := m.Store.Labeler(did)
	if err != nil {
		return err
	}

	stream, err := streamURL(who.endpoint, labeler.Cursor)
	if err != nil {
		return err
	}

	conn, _, err := m.dialer().DialContext(ctx, stream, http.Header{"User-Agent": []string{"tack"}})
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", stream, err)
	}
	defer conn.Close()

	now := time.Now().UTC()
	_ = m.Store.UpdateLabeler(did, map[string]any{"status": "connected", "last_error": "", "connected_at": &now, "endpoint": who.endpoint})
	log.Info("subscribed to labeler", "cursor", labeler.Cursor)

	// Closing the connection is what stops HandleRepoStream on cancel.
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	// Labelers add and redefine values while connected, so the declaration
	// is read again every few hours, not only on connect.
	go func() {
		ticker := time.NewTicker(m.declarationEvery())
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := m.refreshDeclaration(ctx, did, who); err != nil {
					log.Warn("could not read the labeler declaration", "err", err)
				}
			}
		}
	}()

	callbacks := &events.RepoStreamCallbacks{
		LabelLabels: func(evt *comatproto.LabelSubscribeLabels_Labels) error {
			return m.handleLabels(ctx, did, who, evt)
		},
		LabelInfo: func(evt *comatproto.LabelSubscribeLabels_Info) error {
			msg := ""
			if evt.Message != nil {
				msg = *evt.Message
			}
			log.Info("labeler info", "name", evt.Name, "message", msg)

			return nil
		},
	}

	return events.HandleRepoStream(ctx, conn, sequential.NewScheduler(did, callbacks.EventHandler), log)
}

func (m *Manager) declarationEvery() time.Duration {
	if m.DeclarationEvery > 0 {
		return m.DeclarationEvery
	}

	return 6 * time.Hour
}

func (m *Manager) dialer() *websocket.Dialer {
	if m.Dialer != nil {
		return m.Dialer
	}

	return websocket.DefaultDialer
}

func streamURL(endpoint string, cursor int64) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}

	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	}

	u.Path = strings.TrimRight(u.Path, "/") + "/xrpc/com.atproto.label.subscribeLabels"

	// No cursor yet: start at the labeler's current label, not its first.
	// Labels from before are fetched per subject, when one is added.
	if cursor > 0 {
		u.RawQuery = url.Values{"cursor": {fmt.Sprint(cursor)}}.Encode()
	}

	return u.String(), nil
}

// handleLabels stores one frame's labels and moves the cursor past it, in
// one transaction, so a restart never skips or repeats a frame.
func (m *Manager) handleLabels(ctx context.Context, did string, who *labelerIdentity, evt *comatproto.LabelSubscribeLabels_Labels) error {
	var received, rejected int64

	err := m.Store.Tx(func(tx *store.Store) error {
		for _, lex := range evt.Labels {
			if lex == nil {
				continue
			}

			label := labeling.FromLexicon(lex)
			received++

			verified, err := m.check(ctx, did, who, &label)
			if err != nil {
				rejected++
				m.Log.Debug("dropped a label", "labeler", did, "uri", label.URI, "val", label.Val, "err", err)

				continue
			}

			if err := m.store(tx, label, verified); err != nil {
				return err
			}
		}

		return tx.DB.Model(&store.Labeler{}).Where("did = ?", did).Updates(map[string]any{
			"cursor":   evt.Seq,
			"received": store.Expr("received + ?", received),
			"rejected": store.Expr("rejected + ?", rejected),
		}).Error
	})
	if err != nil {
		return err
	}

	m.notify()

	return nil
}

// check validates a label and, when verifying, its signature. A failed
// signature refetches the DID document once, in case the key rotated.
func (m *Manager) check(ctx context.Context, did string, who *labelerIdentity, label *labeling.Label) (bool, error) {
	if err := label.VerifySyntax(); err != nil {
		return false, err
	}

	if label.SourceDID != did {
		return false, fmt.Errorf("label from %s on %s's stream", label.SourceDID, did)
	}

	if !m.Verify {
		return false, nil
	}

	if who.key != nil && label.VerifySignature(who.key) == nil {
		return true, nil
	}

	fresh, err := m.resolve(ctx, did, true)
	if err != nil {
		return false, err
	}

	*who = *fresh

	if who.key == nil {
		return false, errors.New("no signing key")
	}

	if err := label.VerifySignature(who.key); err != nil {
		return false, fmt.Errorf("bad signature: %w", err)
	}

	return true, nil
}

// store keeps a label about a subject and queues it when it changed what
// Tack holds. Labels about anyone else are dropped, unless every account is
// wanted, in which case each is delivered as it comes and none is kept.
func (m *Manager) store(tx *store.Store, label labeling.Label, verified bool) error {
	negated := label.Negated != nil && *label.Negated
	payload := LabelPayload{
		Src: label.SourceDID, URI: label.URI, CID: label.CID, Val: label.Val,
		Neg: negated, Cts: label.CreatedAt, Exp: label.ExpiresAt,
	}

	if m.AllSubjects {
		return EnqueueLabel(tx, payload, verified)
	}

	subject := store.SubjectOf(label.URI)

	tracked, err := tx.IsSubject(subject)
	if err != nil || !tracked {
		return err
	}

	var exp *time.Time
	if label.ExpiresAt != nil {
		if t, err := syntax.ParseDatetimeLenient(*label.ExpiresAt); err == nil {
			parsed := t.Time().UTC()
			exp = &parsed
		}
	}

	changed, err := tx.PutLabel(store.Label{
		Src: label.SourceDID, URI: label.URI, Val: label.Val, Subject: subject,
		CID: label.CID, Cts: label.CreatedAt, Exp: exp, Sig: label.Sig, Verified: verified,
	}, negated)
	if err != nil || !changed {
		return err
	}

	return EnqueueLabel(tx, payload, verified)
}

// refreshDeclaration reads the labeler's app.bsky.labeler.service record
// from its PDS, and queues it for the app when it changed.
func (m *Manager) refreshDeclaration(ctx context.Context, did string, who *labelerIdentity) error {
	if who.pds == "" {
		return errors.New("no PDS in the DID document")
	}

	q := url.Values{"repo": {did}, "collection": {"app.bsky.labeler.service"}, "rkey": {"self"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(who.pds, "/")+"/xrpc/com.atproto.repo.getRecord?"+q.Encode(), nil)
	if err != nil {
		return err
	}

	client := m.HTTP
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("getRecord answered %d", resp.StatusCode)
	}

	var record struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(body, &record); err != nil {
		return err
	}

	labeler, err := m.Store.Labeler(did)
	if err != nil {
		return err
	}

	declaration := string(record.Value)
	if declaration == labeler.Declaration && labeler.Endpoint == who.endpoint {
		return nil
	}

	err = m.Store.Tx(func(tx *store.Store) error {
		if err := tx.UpdateLabeler(did, map[string]any{"declaration": declaration, "endpoint": who.endpoint}); err != nil {
			return err
		}

		return EnqueueLabeler(tx, LabelerPayload{DID: did, Endpoint: who.endpoint, Declaration: record.Value})
	})
	m.notify()

	return err
}

func (m *Manager) notify() {
	if m.Notify != nil {
		m.Notify()
	}
}
