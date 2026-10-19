package labelers

import (
	"encoding/json"

	"github.com/socialdept/tack/internal/store"
)

// LabelPayload is a label as the webhook delivers it.
type LabelPayload struct {
	Src string  `json:"src"`
	URI string  `json:"uri"`
	CID *string `json:"cid"`
	Val string  `json:"val"`
	Neg bool    `json:"neg"`
	Cts string  `json:"cts"`
	Exp *string `json:"exp"`
}

type labelEvent struct {
	Type     string       `json:"type"`
	Label    LabelPayload `json:"label"`
	Verified bool         `json:"verified"`
}

type labelerEvent struct {
	Type    string         `json:"type"`
	Labeler LabelerPayload `json:"labeler"`
}

// LabelerPayload is a labeler as the webhook delivers it: when it is added,
// when its declaration changes, and when it is removed.
type LabelerPayload struct {
	DID         string          `json:"did"`
	Endpoint    string          `json:"endpoint,omitempty"`
	Declaration json.RawMessage `json:"declaration,omitempty"`
	Removed     bool            `json:"removed,omitempty"`
}

// EnqueueLabel queues a label for delivery.
func EnqueueLabel(s *store.Store, label LabelPayload, verified bool) error {
	body, err := json.Marshal(labelEvent{Type: "label", Label: label, Verified: verified})
	if err != nil {
		return err
	}

	return s.Enqueue("label", string(body))
}

// EnqueueLabeler queues a labeler for delivery.
func EnqueueLabeler(s *store.Store, labeler LabelerPayload) error {
	body, err := json.Marshal(labelerEvent{Type: "labeler", Labeler: labeler})
	if err != nil {
		return err
	}

	return s.Enqueue("labeler", string(body))
}

// FromStored is a stored label as a payload, for delivering stored labels
// to a subject that was just added.
func FromStored(l store.Label) LabelPayload {
	var exp *string
	if l.Exp != nil {
		formatted := l.Exp.UTC().Format("2006-01-02T15:04:05.000Z")
		exp = &formatted
	}

	return LabelPayload{Src: l.Src, URI: l.URI, CID: l.CID, Val: l.Val, Cts: l.Cts, Exp: exp}
}

// AddSubjects records DIDs as subjects and queues every label already
// stored about the new ones, as Tap backfills a repo it starts tracking.
// It returns the DIDs that were new and how many labels were queued.
func AddSubjects(s *store.Store, dids []string) ([]string, int, error) {
	var added []string
	queued := 0

	err := s.Tx(func(tx *store.Store) error {
		var err error
		if added, err = tx.AddSubjects(dids); err != nil {
			return err
		}

		for _, did := range added {
			labels, err := tx.LabelsFor(did)
			if err != nil {
				return err
			}

			for _, l := range labels {
				if err := EnqueueLabel(tx, FromStored(l), l.Verified); err != nil {
					return err
				}
				queued++
			}
		}

		return nil
	})

	return added, queued, err
}
