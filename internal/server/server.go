// Package server is Tack's admin API: health, stats, labelers and subjects.
package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/socialdept/tack/internal/labelers"
	"github.com/socialdept/tack/internal/store"
	"gorm.io/gorm"
)

// Server answers the admin API. Every route but /health needs basic auth
// with user admin and the admin password, as Tap's does.
type Server struct {
	// Context lives as long as Tack, so subscriptions added through the API
	// outlast the request that added them.
	Context  context.Context
	Store    *store.Store
	Labelers *labelers.Manager
	Password string
}

// Handler returns the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		write(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /stats", s.auth(s.stats))
	mux.HandleFunc("GET /labelers", s.auth(s.listLabelers))
	mux.HandleFunc("POST /labelers/add", s.auth(s.addLabeler))
	mux.HandleFunc("POST /labelers/remove", s.auth(s.removeLabeler))
	mux.HandleFunc("POST /subjects/add", s.auth(s.addSubjects))
	mux.HandleFunc("POST /subjects/remove", s.auth(s.removeSubjects))
	mux.HandleFunc("POST /subjects/uris", s.auth(s.addURIs))
	mux.HandleFunc("GET /subjects/{did}", s.auth(s.subject))

	return mux
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Password != "" {
			user, pass, ok := r.BasicAuth()
			if !ok || user != "admin" || subtle.ConstantTimeCompare([]byte(pass), []byte(s.Password)) != 1 {
				w.Header().Set("WWW-Authenticate", `Basic realm="tack"`)
				write(w, http.StatusUnauthorized, map[string]string{"error": "Unauthorized"})

				return
			}
		}

		next(w, r)
	}
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	counts, err := s.Store.Counts()
	if err != nil {
		fail(w, err)

		return
	}

	write(w, http.StatusOK, counts)
}

type labelerView struct {
	DID         string          `json:"did"`
	Endpoint    string          `json:"endpoint"`
	Status      string          `json:"status"`
	LastError   string          `json:"lastError,omitempty"`
	Cursor      int64           `json:"cursor"`
	Received    int64           `json:"received"`
	Rejected    int64           `json:"rejected"`
	Wildcards   *bool           `json:"wildcards"`
	ConnectedAt any             `json:"connectedAt"`
	Declaration json.RawMessage `json:"declaration,omitempty"`
}

func (s *Server) listLabelers(w http.ResponseWriter, r *http.Request) {
	rows, err := s.Store.Labelers()
	if err != nil {
		fail(w, err)

		return
	}

	views := make([]labelerView, 0, len(rows))
	for _, l := range rows {
		v := labelerView{DID: l.DID, Endpoint: l.Endpoint, Status: l.Status, LastError: l.LastError, Cursor: l.Cursor, Received: l.Received, Rejected: l.Rejected, Wildcards: l.Wildcards, ConnectedAt: l.ConnectedAt}
		if l.Declaration != "" {
			v.Declaration = json.RawMessage(l.Declaration)
		}
		views = append(views, v)
	}

	write(w, http.StatusOK, map[string]any{"labelers": views})
}

func (s *Server) addLabeler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DID string `json:"did"`
	}
	if !read(w, r, &body) {
		return
	}

	if err := s.Labelers.Add(s.Context, body.DID); err != nil {
		write(w, http.StatusBadRequest, map[string]string{"error": "InvalidRequest", "message": err.Error()})

		return
	}

	write(w, http.StatusOK, map[string]string{"did": body.DID})
}

func (s *Server) removeLabeler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DID string `json:"did"`
	}
	if !read(w, r, &body) {
		return
	}

	if err := s.Labelers.Remove(body.DID); err != nil {
		fail(w, err)

		return
	}

	write(w, http.StatusOK, map[string]string{"did": body.DID})
}

// addSubjects records accounts to deliver labels for. Their existing labels
// are fetched from each labeler in the background. uris are record URIs of
// theirs to ask about, for labelers that only answer for exact URIs.
func (s *Server) addSubjects(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DIDs []string `json:"dids"`
		URIs []string `json:"uris"`
	}
	if !read(w, r, &body) || !validDIDs(w, body.DIDs) {
		return
	}

	added, err := s.Store.AddSubjects(body.DIDs)
	if err != nil {
		fail(w, err)

		return
	}

	queued, err := s.Store.AddPendingURIs(body.URIs)
	if err != nil {
		fail(w, err)

		return
	}

	s.Labelers.WakeBackfill()
	write(w, http.StatusOK, map[string]any{"added": len(added), "urisQueued": queued})
}

// addURIs queues record URIs of existing subjects to ask labelers about,
// such as records the app indexed after adding the subject.
func (s *Server) addURIs(w http.ResponseWriter, r *http.Request) {
	var body struct {
		URIs []string `json:"uris"`
	}
	if !read(w, r, &body) {
		return
	}

	queued, err := s.Store.AddPendingURIs(body.URIs)
	if err != nil {
		fail(w, err)

		return
	}

	s.Labelers.WakeBackfill()
	write(w, http.StatusOK, map[string]any{"urisQueued": queued})
}

func (s *Server) removeSubjects(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DIDs []string `json:"dids"`
	}
	if !read(w, r, &body) || !validDIDs(w, body.DIDs) {
		return
	}

	if err := s.Store.RemoveSubjects(body.DIDs); err != nil {
		fail(w, err)

		return
	}

	write(w, http.StatusOK, map[string]any{"removed": len(body.DIDs)})
}

func (s *Server) subject(w http.ResponseWriter, r *http.Request) {
	did := r.PathValue("did")

	subject, err := s.Store.Subject(did)
	if err != nil {
		fail(w, err)

		return
	}

	labels, err := s.Store.LabelsFor(did)
	if err != nil {
		fail(w, err)

		return
	}

	payloads := make([]labelers.LabelPayload, 0, len(labels))
	for _, l := range labels {
		payloads = append(payloads, labelers.FromStored(l))
	}

	var backfilledAt any
	if subject != nil {
		backfilledAt = subject.BackfilledAt
	}

	write(w, http.StatusOK, map[string]any{"did": did, "subject": subject != nil, "backfilledAt": backfilledAt, "labels": payloads})
}

func validDIDs(w http.ResponseWriter, dids []string) bool {
	for _, did := range dids {
		if _, err := syntax.ParseDID(did); err != nil {
			write(w, http.StatusBadRequest, map[string]string{"error": "InvalidRequest", "message": "not a DID: " + did})

			return false
		}
	}

	return true
}

func read(w http.ResponseWriter, r *http.Request, into any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(into); err != nil {
		write(w, http.StatusBadRequest, map[string]string{"error": "InvalidRequest", "message": "the body is not valid JSON"})

		return false
	}

	return true
}

func fail(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, gorm.ErrRecordNotFound) {
		status = http.StatusNotFound
	}

	write(w, status, map[string]string{"error": strings.ReplaceAll(http.StatusText(status), " ", ""), "message": err.Error()})
}

func write(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
