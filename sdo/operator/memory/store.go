// Package memory exposes logical operational outcomes to the operator.
// Repository layout and serialization are private to its adapters.
package memory

import (
	"context"
	"errors"
	"log/slog"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

var ErrNotFound = errors.New("outcome not found")
var ErrConflict = errors.New("outcome already exists with different content")

// Outcome exposes the typed logical record. The repository adapter privately
// retains the source document so unknown historical fields survive migration.
type Outcome struct {
	Record         *contracts.OutcomeRecord
	SchemaVersion  int
	IncidentID     string
	Classification string
	document       []byte
}

// Reader is the operational-memory view available to runtime and agents.
type Reader interface {
	Get(context.Context, string) (Outcome, error)
	List(context.Context) ([]Outcome, error)
}

// UnreadableOutcome records one stored outcome that could not be used. It is a
// note about that document only; the other outcomes stay available.
type UnreadableOutcome struct {
	Name   string
	Reason string
}

// NotedLister is implemented by readers that skip unusable outcomes and can
// say which ones and why, from the same scan as the list itself.
type NotedLister interface {
	ListWithNotes(context.Context) ([]Outcome, []UnreadableOutcome, error)
}

// Writer is the commit broker's accepted-state mutation capability.
type Writer interface {
	Put(context.Context, *contracts.OutcomeRecord) (Outcome, error)
}

// Store is the repository adapter's complete internal contract.
type Store interface {
	Reader
	Writer
}

// ListOutcomes lists the readable outcomes with the notes for those skipped
// in the same scan, and logs each skipped outcome under reading, so no reader
// drops stored evidence silently. A reader that cannot say reports no notes.
func ListOutcomes(ctx context.Context, reader Reader, reading string) ([]Outcome, []UnreadableOutcome, error) {
	var outcomes []Outcome
	var notes []UnreadableOutcome
	var err error
	if lister, ok := reader.(NotedLister); ok {
		outcomes, notes, err = lister.ListWithNotes(ctx)
	} else {
		outcomes, err = reader.List(ctx)
	}
	if err != nil {
		return nil, nil, err
	}
	for _, note := range notes {
		slog.WarnContext(ctx, "skipped unreadable outcome", "reading", reading, "file", note.Name, "reason", note.Reason)
	}
	return outcomes, notes, nil
}
