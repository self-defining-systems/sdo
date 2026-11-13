// Package appstate lets a health-class detector package expose state the
// application itself defines (an admin setting, a database setting, process
// state) as typed key-value pairs. A reader is Go code that implements Reader;
// the controller runs it on its own interval, off the detection path, and
// carries the last finished result in the observation. Detectors and
// playbooks read that result through Require and its predicates.
//
// A reader gets the narrow View, not program.ReadView: no command execution,
// no authenticated HTTP, no writes. Every read refuses credentials, and
// Values refuses credential-named keys, so Secret values are never read.
package appstate

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/appstate/carried"
	"sdo.dev/operator/sdk/program"
)

// MaxReads is the most reads (store, probe and Kubernetes together) one
// ReadState call may make.
const MaxReads = 8

// ErrReadBudget is returned by a read past MaxReads.
var ErrReadBudget = fmt.Errorf("app-state reader read budget of %d exhausted", MaxReads)

// Carried types, re-exported so readers and detectors import one package.
type (
	Values     = carried.Values
	Snapshot   = carried.Snapshot
	Result     = carried.Result
	State      = carried.State
	Change     = carried.Change
	ChangeKind = carried.ChangeKind
	Source     = carried.Source
	ResultSet  = carried.ResultSet
	Consumer   = carried.Consumer
)

// Change kinds.
const (
	Changed = carried.Changed
	Added   = carried.Added
	Removed = carried.Removed
)

// Require returns a reader's state, or an error when it is unavailable.
func Require(ctx sdk.DetectionContext, id string) (State, error) { return carried.Require(ctx, id) }

// View is the narrow read-only view a reader gets. It is not program.ReadView,
// whose Observe can exec commands and send authenticated HTTP.
type View interface {
	sdk.DetectionContext
	sdk.ObservationClock
	// Read is a store read (rows, at most 16 fields and 32 rows, read-only
	// selector) or a credential-free probe GET.
	Read(program.Query) (program.Observation, error)
	// KubernetesRead is a GET or HEAD of the Kubernetes API; Secret endpoints
	// are refused.
	KubernetesRead(program.KubernetesRequest) (json.RawMessage, error)
}

// Reader is implemented by a health-class detector package that exposes
// application state. ReadState is deterministic for a given set of reads.
type Reader interface {
	ReadState(ctx context.Context, view View) (Values, error)
}

// NoisyDeclarer is optionally implemented by a Reader to declare keys (exact
// or glob) that change on their own while the application is healthy.
type NoisyDeclarer interface{ NoisyKeys() []string }

// Run calls reader.ReadState over a budgeted narrow view of base. A failed
// read fails the call even when the reader ignores the error, so a partial
// answer is never taken for the application's state. A refused Values.Set
// and a panic fail the call too.
func Run(ctx context.Context, reader Reader, base program.ReadView) (values Values, err error) {
	view := &budgeted{DetectionContext: base, clock: base, base: base, ctx: ctx}
	defer func() {
		if r := recover(); r != nil {
			values, err = Values{}, fmt.Errorf("app-state reader panicked: %v", r)
		}
	}()
	values, err = reader.ReadState(ctx, view)
	if failed := view.failure(); failed != nil {
		return Values{}, failed
	}
	if err != nil {
		return Values{}, err
	}
	if err := values.Err(); err != nil {
		return Values{}, err
	}
	return values, nil
}

type budgeted struct {
	sdk.DetectionContext
	clock sdk.ObservationClock
	base  program.ReadView
	ctx   context.Context

	mu     sync.Mutex
	reads  int
	failed error
}

func (v *budgeted) ObservedAt() time.Time { return v.clock.ObservedAt() }

func (v *budgeted) failure() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.failed
}

// admit counts a read against the budget; a refusal or later failure sticks.
func (v *budgeted) admit() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.failed != nil {
		return v.failed
	}
	if err := v.ctx.Err(); err != nil {
		v.failed = err
		return err
	}
	if v.reads >= MaxReads {
		v.failed = ErrReadBudget
		return v.failed
	}
	v.reads++
	return nil
}

func (v *budgeted) fail(err error) error {
	if err == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.failed == nil {
		v.failed = err
	}
	return err
}

func (v *budgeted) Read(q program.Query) (program.Observation, error) {
	if err := q.Validate(); err != nil {
		return program.Observation{}, v.fail(err)
	}
	if err := v.admit(); err != nil {
		return program.Observation{}, err
	}
	obs, err := v.base.Read(q)
	return obs, v.fail(err)
}

func (v *budgeted) KubernetesRead(r program.KubernetesRequest) (json.RawMessage, error) {
	if err := program.ValidateKubernetesRead(r); err != nil {
		return nil, v.fail(err)
	}
	if err := v.admit(); err != nil {
		return nil, err
	}
	raw, err := v.base.KubernetesRead(r)
	return raw, v.fail(err)
}
