package store

import (
	"path/filepath"
	"testing"
	"time"
)

func open(t *testing.T) *Store {
	t.Helper()

	s, err := Open("sqlite://" + filepath.Join(t.TempDir(), "tack.db"))
	if err != nil {
		t.Fatal(err)
	}

	return s
}

func TestSubjectOf(t *testing.T) {
	cases := map[string]string{
		"did:plc:alice": "did:plc:alice",
		"at://did:plc:alice/app.bsky.feed.post/3k": "did:plc:alice",
		"at://did:plc:alice":                       "did:plc:alice",
	}

	for uri, want := range cases {
		if got := SubjectOf(uri); got != want {
			t.Errorf("SubjectOf(%q) = %q, want %q", uri, got, want)
		}
	}
}

func TestANewerLabelReplacesAnOlderOneAndAReplayChangesNothing(t *testing.T) {
	s := open(t)
	label := Label{Src: "did:plc:mod", URI: "did:plc:alice", Val: "spam", Subject: "did:plc:alice", Cts: "2026-01-01T00:00:00.000Z"}

	if changed, err := s.PutLabel(label, false); err != nil || !changed {
		t.Fatalf("first put: changed=%v err=%v", changed, err)
	}

	if changed, _ := s.PutLabel(label, false); changed {
		t.Error("a replayed label changed something")
	}

	older := label
	older.Cts = "2025-01-01T00:00:00.000Z"
	if changed, _ := s.PutLabel(older, false); changed {
		t.Error("an older label replaced a newer one")
	}

	newer := label
	newer.Cts = "2026-02-01T00:00:00.000Z"
	if changed, _ := s.PutLabel(newer, false); !changed {
		t.Error("a newer label did not replace the older one")
	}

	labels, _ := s.LabelsFor("did:plc:alice")
	if len(labels) != 1 || labels[0].Cts != newer.Cts {
		t.Fatalf("labels = %+v", labels)
	}
}

func TestANegationRemovesTheLabel(t *testing.T) {
	s := open(t)
	label := Label{Src: "did:plc:mod", URI: "at://did:plc:alice/app.bsky.feed.post/3k", Val: "porn", Subject: "did:plc:alice", Cts: "2026-01-01T00:00:00.000Z"}
	_, _ = s.PutLabel(label, false)

	if changed, err := s.PutLabel(label, true); err != nil || !changed {
		t.Fatalf("negation: changed=%v err=%v", changed, err)
	}

	if changed, _ := s.PutLabel(label, true); changed {
		t.Error("negating a label that is gone changed something")
	}

	if labels, _ := s.LabelsFor("did:plc:alice"); len(labels) != 0 {
		t.Errorf("labels = %+v", labels)
	}
}

func TestExpiredLabelsAreLeftOutAndSwept(t *testing.T) {
	s := open(t)
	past := time.Now().UTC().Add(-time.Hour)
	future := time.Now().UTC().Add(time.Hour)
	_, _ = s.PutLabel(Label{Src: "did:plc:mod", URI: "did:plc:alice", Val: "old", Subject: "did:plc:alice", Cts: "2026-01-01T00:00:00.000Z", Exp: &past}, false)
	_, _ = s.PutLabel(Label{Src: "did:plc:mod", URI: "did:plc:alice", Val: "live", Subject: "did:plc:alice", Cts: "2026-01-01T00:00:00.000Z", Exp: &future}, false)

	labels, _ := s.LabelsFor("did:plc:alice")
	if len(labels) != 1 || labels[0].Val != "live" {
		t.Fatalf("labels = %+v", labels)
	}

	if swept, _ := s.SweepExpired(); swept != 1 {
		t.Errorf("swept %d, want 1", swept)
	}
}

func TestSubjectsAreAddedOnceAndRemoved(t *testing.T) {
	s := open(t)

	added, _ := s.AddSubjects([]string{"did:plc:alice", "did:plc:bob"})
	if len(added) != 2 {
		t.Fatalf("added = %v", added)
	}

	if added, _ := s.AddSubjects([]string{"did:plc:alice", "did:plc:carol"}); len(added) != 1 || added[0] != "did:plc:carol" {
		t.Errorf("second add = %v, want only carol", added)
	}

	_ = s.RemoveSubjects([]string{"did:plc:alice"})
	if ok, _ := s.IsSubject("did:plc:alice"); ok {
		t.Error("alice is still a subject")
	}
}

func TestTheOutboxKeepsEventsUntilAcknowledged(t *testing.T) {
	s := open(t)

	for _, body := range []string{"one", "two", "three"} {
		if err := s.Enqueue("label", body); err != nil {
			t.Fatal(err)
		}
	}

	pending, _ := s.Pending(2)
	if len(pending) != 2 || pending[0].Payload != "one" || pending[1].Payload != "two" {
		t.Fatalf("pending = %+v", pending)
	}

	_ = s.Acknowledge([]uint64{pending[0].ID, pending[1].ID})

	if left, _ := s.Pending(10); len(left) != 1 || left[0].Payload != "three" {
		t.Errorf("left = %+v", left)
	}
}

func TestRemovingALabelerForgetsItsLabels(t *testing.T) {
	s := open(t)
	_, _ = s.AddLabeler("did:plc:mod")
	_, _ = s.PutLabel(Label{Src: "did:plc:mod", URI: "did:plc:alice", Val: "spam", Subject: "did:plc:alice", Cts: "2026-01-01T00:00:00.000Z"}, false)

	if err := s.RemoveLabeler("did:plc:mod"); err != nil {
		t.Fatal(err)
	}

	counts, _ := s.Counts()
	if counts["labelers"] != 0 || counts["labels"] != 0 {
		t.Errorf("counts = %v", counts)
	}
}

func TestPendingURIsAreOnlyKeptForSubjects(t *testing.T) {
	s := open(t)
	_, _ = s.AddSubjects([]string{"did:plc:alice"})

	queued, err := s.AddPendingURIs([]string{
		"at://did:plc:alice/app.example.post/1",
		"at://did:plc:alice/app.example.post/1",
		"at://did:plc:stranger/app.example.post/2",
		"did:plc:alice",
	})
	if err != nil || queued != 1 {
		t.Fatalf("queued=%d err=%v, want only alice's record, once", queued, err)
	}

	pending, _ := s.PendingURIs(10)
	_ = s.DonePendingURIs([]string{pending[0].URI})

	if left, _ := s.PendingURIs(10); len(left) != 0 {
		t.Errorf("left = %+v", left)
	}
}

func TestSubjectsAreBackfilledOnceAndRemovingOneForgetsItsLabels(t *testing.T) {
	s := open(t)
	_, _ = s.AddSubjects([]string{"did:plc:alice"})

	next, _ := s.NextUnbackfilled()
	if next == nil || next.DID != "did:plc:alice" {
		t.Fatalf("next = %+v", next)
	}

	_ = s.MarkBackfilled("did:plc:alice")
	if next, _ := s.NextUnbackfilled(); next != nil {
		t.Errorf("alice is still waiting: %+v", next)
	}

	_, _ = s.PutLabel(Label{Src: "did:plc:mod", URI: "did:plc:alice", Val: "spam", Subject: "did:plc:alice", Cts: "2026-01-01T00:00:00.000Z"}, false)
	_ = s.RemoveSubjects([]string{"did:plc:alice"})

	if labels, _ := s.LabelsFor("did:plc:alice"); len(labels) != 0 {
		t.Errorf("labels kept after removal: %+v", labels)
	}
}

func TestALateWithdrawalDoesNotRemoveALabelAppliedAgain(t *testing.T) {
	s := open(t)
	label := Label{Src: "did:plc:mod", URI: "did:plc:alice", Val: "spam", Subject: "did:plc:alice", Cts: "2026-03-01T00:00:00.000Z"}
	_, _ = s.PutLabel(label, false)

	old := label
	old.Cts = "2026-02-01T00:00:00.000Z"
	if changed, _ := s.PutLabel(old, true); changed {
		t.Error("an older withdrawal removed a newer label")
	}

	newer := label
	newer.Cts = "2026-04-01T00:00:00.000Z"
	if changed, _ := s.PutLabel(newer, true); !changed {
		t.Error("a newer withdrawal did not remove the label")
	}
}
