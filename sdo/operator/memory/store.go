// Package memory exposes logical operational outcomes to the operator.
// Repository layout and serialization are private to its adapters.
package memory

import (
	"context"
	"encoding/json"
	"errors"
)

var ErrNotFound = errors.New("outcome not found")
var ErrConflict = errors.New("outcome already exists with different content")

// Outcome retains the complete versioned document so unknown future fields are
// preserved across reads and migrations. Callers select by logical incident ID.
type Outcome struct {
	SchemaVersion  int
	IncidentID     string
	Classification string
	Document       json.RawMessage
}

// Reader is the operational-memory view available to runtime and agents.
type Reader interface {
	Get(context.Context, string) (Outcome, error)
	List(context.Context) ([]Outcome, error)
}

// Writer is the commit broker's accepted-state mutation capability.
type Writer interface {
	Put(context.Context, []byte) (Outcome, error)
}

// Store is the repository adapter's complete internal contract.
type Store interface {
	Reader
	Writer
}
