package labelers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/socialdept/tack/internal/store"
	"golang.org/x/time/rate"
)

// Labelers only answer queryLabels for exact URIs unless their operator
// allows wildcards, which Ozone does not by default. So a new subject's
// labels are fetched in two ways:
//
//   - its account labels, by its DID and its profile record, from every
//     labeler
//   - its record labels, by at://did/* from labelers that take wildcards,
//     and by the exact record URIs the app sends from those that do not

// exactBatch is how many exact URIs go in one queryLabels request.
const exactBatch = 100

var errWildcardsOff = errors.New("the labeler does not take wildcard queries")

// Backfill fetches existing labels for new subjects and pending record URIs
// until the context ends.
func (m *Manager) Backfill(ctx context.Context, workers int) {
	if workers < 1 {
		workers = 1
	}

	m.backfillInit()

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.backfillWorker(ctx)
		}()
	}

	wg.Wait()
}

// WakeBackfill tells the workers there is something new to fetch.
func (m *Manager) WakeBackfill() {
	m.backfillInit()

	select {
	case m.backfillWake <- struct{}{}:
	default:
	}
}

func (m *Manager) backfillInit() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.backfillWake == nil {
		m.backfillWake = make(chan struct{}, 1)
		m.inFlight = map[string]bool{}
		m.limiters = map[string]*rate.Limiter{}
	}
}

func (m *Manager) backfillWorker(ctx context.Context) {
	for ctx.Err() == nil {
		did, err := m.claim()
		if err != nil {
			m.Log.Warn("could not read subjects to backfill", "err", err)
		}

		if did != "" {
			m.backfillSubject(ctx, did)
			m.release(did)

			continue
		}

		// Requests to each labeler are paced anyway, so workers checking the
		// same URIs at once would only repeat each other.
		checked := 0
		if m.pendingMu.TryLock() {
			checked, err = m.checkPendingURIs(ctx)
			m.pendingMu.Unlock()

			if err != nil {
				m.Log.Warn("could not check pending record URIs", "err", err)
			}
		}

		if checked > 0 {
			continue
		}

		select {
		case <-ctx.Done():
		case <-m.backfillWake:
		case <-time.After(5 * time.Second):
		}
	}
}

// claim takes the next subject no other worker is fetching.
func (m *Manager) claim() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var subjects []store.Subject
	err := m.Store.DB.Where("backfilled_at IS NULL").Order("created_at, did").Limit(len(m.inFlight) + 1).Find(&subjects).Error
	if err != nil {
		return "", err
	}

	for _, s := range subjects {
		if !m.inFlight[s.DID] {
			m.inFlight[s.DID] = true

			return s.DID, nil
		}
	}

	return "", nil
}

func (m *Manager) release(did string) {
	m.mu.Lock()
	delete(m.inFlight, did)
	m.mu.Unlock()
}

// backfillSubject asks every labeler for one subject's labels. A labeler
// whose stream is not connected is skipped, and one that cannot be reached
// is logged and skipped, so one that is down does not hold every new subject
// back. Its new labels arrive through its stream once it is back.
func (m *Manager) backfillSubject(ctx context.Context, did string) {
	labelers, err := m.Store.Labelers()
	if err != nil {
		m.Log.Warn("could not list labelers", "err", err)

		return
	}

	for _, l := range labelers {
		if !answering(l) {
			continue
		}

		if err := m.backfillFrom(ctx, l, did); err != nil {
			m.Log.Warn("could not fetch a subject's labels", "labeler", l.DID, "subject", did, "err", err)
		}
	}

	if err := m.Store.MarkBackfilled(did); err != nil {
		m.Log.Warn("could not mark a subject backfilled", "subject", did, "err", err)
	}
}

func (m *Manager) backfillFrom(ctx context.Context, l store.Labeler, did string) error {
	who, err := m.resolve(ctx, l.DID, false)
	if err != nil {
		return err
	}

	if err := m.ingestQuery(ctx, l.DID, who, []string{did, "at://" + did + "/app.bsky.actor.profile/self"}); err != nil {
		return err
	}

	if l.Wildcards != nil && !*l.Wildcards {
		return nil
	}

	err = m.ingestQuery(ctx, l.DID, who, []string{"at://" + did + "/*"})

	switch {
	case errors.Is(err, errWildcardsOff):
		return m.setWildcards(l.DID, false)
	case err != nil:
		return err
	case l.Wildcards == nil:
		return m.setWildcards(l.DID, true)
	}

	return nil
}

// answering is whether a labeler is worth asking: its stream is up, so its
// queryLabels most likely is too.
func answering(l store.Labeler) bool {
	return l.Status == "connected"
}

func (m *Manager) setWildcards(did string, on bool) error {
	return m.Store.UpdateLabeler(did, map[string]any{"wildcards": on})
}

// checkPendingURIs asks the labelers that only answer for exact URIs about
// the oldest pending record URIs, and drops them once asked.
func (m *Manager) checkPendingURIs(ctx context.Context) (int, error) {
	pending, err := m.Store.PendingURIs(exactBatch)
	if err != nil || len(pending) == 0 {
		return 0, err
	}

	uris := make([]string, 0, len(pending))
	for _, p := range pending {
		uris = append(uris, p.URI)
	}

	labelers, err := m.Store.Labelers()
	if err != nil {
		return 0, err
	}

	for _, l := range labelers {
		// Labelers that take wildcards answered for every record already.
		if !answering(l) || (l.Wildcards != nil && *l.Wildcards) {
			continue
		}

		who, err := m.resolve(ctx, l.DID, false)
		if err == nil {
			err = m.ingestQuery(ctx, l.DID, who, uris)
		}

		if err != nil {
			m.Log.Warn("could not check record URIs", "labeler", l.DID, "err", err)
		}
	}

	return len(uris), m.Store.DonePendingURIs(uris)
}

// ingestQuery fetches every page of labels matching the patterns, keeps the
// newest for each value, and stores them as stream labels are stored.
func (m *Manager) ingestQuery(ctx context.Context, labelerDID string, who *labelerIdentity, patterns []string) error {
	newest := map[string]labeling.Label{}
	cursor := ""

	for {
		page, next, err := m.queryLabels(ctx, labelerDID, who.endpoint, patterns, cursor)
		if err != nil {
			return err
		}

		// Ozone pages by row id with no ORDER BY, so a label and its later
		// withdrawal can come back in any order. The newest decides.
		for _, l := range page {
			key := l.URI + "\x00" + l.Val
			if prev, ok := newest[key]; !ok || l.CreatedAt > prev.CreatedAt {
				newest[key] = l
			}
		}

		if next == "" || next == cursor || len(page) == 0 {
			break
		}

		cursor = next
	}

	if len(newest) == 0 {
		return nil
	}

	var rejected int64

	err := m.Store.Tx(func(tx *store.Store) error {
		for _, label := range newest {
			verified, err := m.check(ctx, labelerDID, who, &label)
			if err != nil {
				rejected++

				continue
			}

			if err := m.store(tx, label, verified); err != nil {
				return err
			}
		}

		if rejected > 0 {
			return tx.DB.Model(&store.Labeler{}).Where("did = ?", labelerDID).Update("rejected", store.Expr("rejected + ?", rejected)).Error
		}

		return nil
	})
	if err != nil {
		return err
	}

	m.notify()

	return nil
}

// queryLabels fetches one page of com.atproto.label.queryLabels.
func (m *Manager) queryLabels(ctx context.Context, labelerDID, endpoint string, patterns []string, cursor string) ([]labeling.Label, string, error) {
	if err := m.limiter(labelerDID).Wait(ctx); err != nil {
		return nil, "", err
	}

	q := url.Values{"uriPatterns": patterns, "sources": {labelerDID}, "limit": {"250"}}
	if cursor != "" {
		q.Set("cursor", cursor)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/xrpc/com.atproto.label.queryLabels?"+q.Encode(), nil)
	if err != nil {
		return nil, "", err
	}

	req.Header.Set("User-Agent", "tack")

	resp, err := m.httpClient().Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, "", err
	}

	if resp.StatusCode == http.StatusBadRequest && strings.Contains(strings.ToLower(string(body)), "wildcard") {
		return nil, "", errWildcardsOff
	}

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("queryLabels answered %d: %s", resp.StatusCode, strings.TrimSpace(string(body[:min(len(body), 200)])))
	}

	var out struct {
		Cursor string       `json:"cursor"`
		Labels []queryLabel `json:"labels"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, "", fmt.Errorf("reading queryLabels: %w", err)
	}

	labels := make([]labeling.Label, 0, len(out.Labels))
	for _, l := range out.Labels {
		labels = append(labels, l.label())
	}

	return labels, out.Cursor, nil
}

// queryLabel is a label as queryLabels returns it, in the JSON data model,
// where bytes are {"$bytes": base64}.
type queryLabel struct {
	Ver *int64  `json:"ver"`
	Src string  `json:"src"`
	URI string  `json:"uri"`
	CID *string `json:"cid"`
	Val string  `json:"val"`
	Neg *bool   `json:"neg"`
	Cts string  `json:"cts"`
	Exp *string `json:"exp"`
	Sig *struct {
		Bytes string `json:"$bytes"`
	} `json:"sig"`
}

func (q queryLabel) label() labeling.Label {
	l := labeling.Label{SourceDID: q.Src, URI: q.URI, CID: q.CID, Val: q.Val, Negated: q.Neg, CreatedAt: q.Cts, ExpiresAt: q.Exp, Version: labeling.ATPROTO_LABEL_VERSION}
	if q.Ver != nil {
		l.Version = *q.Ver
	}

	if q.Sig != nil {
		if sig, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(q.Sig.Bytes, "=")); err == nil {
			l.Sig = sig
		}
	}

	return l
}

// limiter paces requests to one labeler, so a burst of new subjects is a
// steady trickle to it.
func (m *Manager) limiter(did string) *rate.Limiter {
	m.backfillInit()

	m.mu.Lock()
	defer m.mu.Unlock()

	l, ok := m.limiters[did]
	if !ok {
		perSecond := m.QueryRate
		if perSecond <= 0 {
			perSecond = 4
		}

		l = rate.NewLimiter(rate.Limit(perSecond), 1)
		m.limiters[did] = l
	}

	return l
}

func (m *Manager) httpClient() *http.Client {
	if m.HTTP != nil {
		return m.HTTP
	}

	return &http.Client{Timeout: 30 * time.Second}
}
