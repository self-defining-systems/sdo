// Package memory exposes logical operational outcomes to the operator.
// Repository layout and serialization are private to its adapters.
package memory

import (
	"context"
	"errors"

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

// Writer is the commit broker's accepted-state mutation capability.
type Writer interface {
	Put(context.Context, *contracts.OutcomeRecord) (Outcome, error)
}

// Store is the repository adapter's complete internal contract.
type Store interface {
	Reader
	Writer
}
