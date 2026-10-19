// Package store keeps what Tack knows: the labelers it follows, every label
// they have emitted, the subjects the app cares about, and the events waiting
// to be delivered.
package store

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// Labeler is a labeling service Tack subscribes to.
type Labeler struct {
	DID         string `gorm:"column:did;primaryKey"`
	Endpoint    string
	Declaration string // the app.bsky.labeler.service record, as JSON
	Cursor      int64
	Status      string // connecting, connected, error, paused
	LastError   string
	ConnectedAt *time.Time
	Received    int64 // labels received since it was added
	Rejected    int64 // labels dropped for a bad signature or syntax
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Label is the newest label a labeler emitted for one value on one subject.
type Label struct {
	Src      string  `gorm:"primaryKey"`
	URI      string  `gorm:"column:uri;primaryKey"`
	Val      string  `gorm:"primaryKey"`
	Subject  string  `gorm:"index"` // the DID the label is about
	CID      *string `gorm:"column:cid"`
	Cts      string
	Exp      *time.Time `gorm:"index"`
	Sig      []byte
	Verified bool
}

// Subject is an account the app wants labels for.
type Subject struct {
	DID       string `gorm:"column:did;primaryKey"`
	CreatedAt time.Time
}

// Event waits in the outbox until a webhook delivery is acknowledged.
type Event struct {
	ID        uint64 `gorm:"primaryKey;autoIncrement"`
	Type      string // label or labeler
	Payload   string // the event body, as JSON
	CreatedAt time.Time
}

// Store wraps the database.
type Store struct {
	DB *gorm.DB
}

// Open connects to a database URL: sqlite://path or postgres://...
func Open(url string) (*Store, error) {
	var dialector gorm.Dialector

	switch {
	case strings.HasPrefix(url, "postgres://"), strings.HasPrefix(url, "postgresql://"):
		dialector = postgres.Open(url)
	case strings.HasPrefix(url, "sqlite://"):
		// WAL and a busy timeout, so the webhook loop and the labelers share
		// the file without locking each other out.
		dialector = sqlite.Open(strings.TrimPrefix(url, "sqlite://") + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)")
	default:
		return nil, fmt.Errorf("unsupported database url %q: use sqlite:// or postgres://", url)
	}

	db, err := gorm.Open(dialector, &gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
	if err != nil {
		return nil, err
	}

	// SQLite takes one writer at a time, and a transaction that reads before
	// it writes cannot wait for another to finish: it fails with SQLITE_BUSY.
	// One connection queues every labeler's writes in Go instead.
	if strings.HasPrefix(url, "sqlite://") {
		sqlDB, err := db.DB()
		if err != nil {
			return nil, err
		}

		sqlDB.SetMaxOpenConns(1)
	}

	if err := db.AutoMigrate(&Labeler{}, &Label{}, &Subject{}, &Event{}); err != nil {
		return nil, fmt.Errorf("migrating: %w", err)
	}

	return &Store{DB: db}, nil
}

// Tx runs fn in a transaction, with a Store bound to it.
func (s *Store) Tx(fn func(*Store) error) error {
	return s.DB.Transaction(func(tx *gorm.DB) error {
		return fn(&Store{DB: tx})
	})
}

// SubjectOf is the DID a label is about: the URI itself for an account, or
// the authority of an at:// URI for a record.
func SubjectOf(uri string) string {
	if !strings.HasPrefix(uri, "at://") {
		return uri
	}

	rest := strings.TrimPrefix(uri, "at://")
	if i := strings.Index(rest, "/"); i >= 0 {
		return rest[:i]
	}

	return rest
}

// PutLabel stores a label, replacing an older one for the same value, or
// deletes it when the label is a negation. It reports whether anything
// changed, so a replayed label is not delivered twice.
func (s *Store) PutLabel(l Label, negated bool) (bool, error) {
	if negated {
		res := s.DB.Where("src = ? AND uri = ? AND val = ?", l.Src, l.URI, l.Val).Delete(&Label{})

		return res.RowsAffected > 0, res.Error
	}

	var existing Label
	err := s.DB.Where("src = ? AND uri = ? AND val = ?", l.Src, l.URI, l.Val).Take(&existing).Error

	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		return true, s.DB.Create(&l).Error
	case err != nil:
		return false, err
	case existing.Cts >= l.Cts:
		// An older or identical label, as a replay sends.
		return false, nil
	}

	return true, s.DB.Save(&l).Error
}

// IsSubject reports whether the app wants labels about this DID.
func (s *Store) IsSubject(did string) (bool, error) {
	var count int64
	err := s.DB.Model(&Subject{}).Where("did = ?", did).Count(&count).Error

	return count > 0, err
}

// AddSubjects records DIDs as subjects and returns those that were new.
func (s *Store) AddSubjects(dids []string) ([]string, error) {
	var added []string

	for _, did := range dids {
		res := s.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&Subject{DID: did, CreatedAt: time.Now().UTC()})
		if res.Error != nil {
			return added, res.Error
		}

		if res.RowsAffected > 0 {
			added = append(added, did)
		}
	}

	return added, nil
}

// RemoveSubjects forgets DIDs as subjects. Their labels are kept, so adding
// them again delivers them again.
func (s *Store) RemoveSubjects(dids []string) error {
	return s.DB.Where("did IN ?", dids).Delete(&Subject{}).Error
}

// LabelsFor returns every live label about a subject.
func (s *Store) LabelsFor(did string) ([]Label, error) {
	var labels []Label
	err := s.DB.Where("subject = ? AND (exp IS NULL OR exp > ?)", did, time.Now().UTC()).Order("cts").Find(&labels).Error

	return labels, err
}

// SweepExpired deletes labels whose expiry has passed.
func (s *Store) SweepExpired() (int64, error) {
	res := s.DB.Where("exp IS NOT NULL AND exp <= ?", time.Now().UTC()).Delete(&Label{})

	return res.RowsAffected, res.Error
}

// Enqueue adds an event to the outbox.
func (s *Store) Enqueue(eventType, payload string) error {
	return s.DB.Create(&Event{Type: eventType, Payload: payload, CreatedAt: time.Now().UTC()}).Error
}

// AddLabeler records a labeler to follow. It reports whether it was new.
func (s *Store) AddLabeler(did string) (bool, error) {
	res := s.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&Labeler{DID: did, Status: "connecting"})

	return res.RowsAffected > 0, res.Error
}

// RemoveLabeler forgets a labeler and every label it emitted.
func (s *Store) RemoveLabeler(did string) error {
	return s.Tx(func(tx *Store) error {
		if err := tx.DB.Where("src = ?", did).Delete(&Label{}).Error; err != nil {
			return err
		}

		return tx.DB.Where("did = ?", did).Delete(&Labeler{}).Error
	})
}

// Labeler returns one labeler.
func (s *Store) Labeler(did string) (*Labeler, error) {
	var labeler Labeler
	if err := s.DB.Where("did = ?", did).Take(&labeler).Error; err != nil {
		return nil, err
	}

	return &labeler, nil
}

// Counts returns how many of each thing the store holds.
func (s *Store) Counts() (map[string]int64, error) {
	counts := map[string]int64{}

	for name, model := range map[string]any{"labelers": &Labeler{}, "labels": &Label{}, "subjects": &Subject{}, "outbox": &Event{}} {
		var n int64
		if err := s.DB.Model(model).Count(&n).Error; err != nil {
			return nil, err
		}

		counts[name] = n
	}

	return counts, nil
}

// Pending returns the oldest events waiting for delivery.
func (s *Store) Pending(limit int) ([]Event, error) {
	var events []Event
	err := s.DB.Order("id").Limit(limit).Find(&events).Error

	return events, err
}

// Acknowledge drops delivered events from the outbox.
func (s *Store) Acknowledge(ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}

	return s.DB.Where("id IN ?", ids).Delete(&Event{}).Error
}

// Labelers returns every labeler Tack follows.
func (s *Store) Labelers() ([]Labeler, error) {
	var labelers []Labeler
	err := s.DB.Order("created_at").Find(&labelers).Error

	return labelers, err
}

// UpdateLabeler saves some of a labeler's fields.
func (s *Store) UpdateLabeler(did string, fields map[string]any) error {
	return s.DB.Model(&Labeler{}).Where("did = ?", did).Updates(fields).Error
}

// Expr is a SQL expression for an update, such as a counter increment.
func Expr(sql string, args ...any) clause.Expr {
	return gorm.Expr(sql, args...)
}
