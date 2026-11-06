package telemetry

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

// Resolution errors. Callers match with errors.Is; the concrete error is a
// *ResolveError carrying the bound sources for the message.
var (
	// ErrUnbound: the named source is not bound, or no source serves the kind.
	ErrUnbound = errors.New("telemetry source is not bound")
	// ErrAmbiguous: no source was named, no default is marked, and several
	// sources serve the kind.
	ErrAmbiguous = errors.New("telemetry source is ambiguous")
	// ErrKind: the named source does not serve the query's kind.
	ErrKind = errors.New("telemetry source does not serve this kind")
)

// ResolveError says which query failed to resolve and what is bound.
type ResolveError struct {
	Err    error
	Source string
	Kind   Kind
	Bound  []string
}

func (e *ResolveError) Error() string {
	bound := strings.Join(e.Bound, ", ")
	if bound == "" {
		bound = "none"
	}
	switch {
	case errors.Is(e.Err, ErrKind):
		return fmt.Sprintf("telemetry source %q does not serve kind %q; bound: %s", e.Source, e.Kind, bound)
	case errors.Is(e.Err, ErrAmbiguous):
		return fmt.Sprintf("telemetry query of kind %q names no source and no default is set; bound: %s", e.Kind, bound)
	case e.Source == "":
		return fmt.Sprintf("telemetry query of kind %q names no source and none serves it; bound: %s", e.Kind, bound)
	}
	return fmt.Sprintf("telemetry source %q is not bound; bound: %s", e.Source, bound)
}

func (e *ResolveError) Unwrap() error { return e.Err }

// Resolver maps (source id, kind) to a Client. It holds no I/O of its own.
type Resolver struct {
	sources []Source
	byID    map[string]int
	clients []Client
}

// Bound pairs a source with the client that answers it. Client may be nil for
// a Resolver used only to resolve names (compile-time checks).
type Bound struct {
	Source Source
	Client Client
}

// NewResolver validates the effective source set: unique valid ids, valid
// kinds and capabilities, and at most one default per kind.
func NewResolver(bound []Bound) (*Resolver, error) {
	r := &Resolver{byID: map[string]int{}}
	defaults := map[Kind]string{}
	for _, b := range bound {
		s := b.Source
		if !SourceIDPattern.MatchString(s.ID) {
			return nil, fmt.Errorf("telemetry source id %q must match %s", s.ID, SourceIDPattern)
		}
		if _, dup := r.byID[s.ID]; dup {
			return nil, fmt.Errorf("telemetry source %q is bound twice", s.ID)
		}
		if len(s.Kinds) == 0 {
			return nil, fmt.Errorf("telemetry source %q serves no kinds", s.ID)
		}
		for _, k := range s.Kinds {
			if !k.Valid() {
				return nil, fmt.Errorf("telemetry source %q has unknown kind %q", s.ID, k)
			}
			if s.Default {
				if other, dup := defaults[k]; dup {
					return nil, fmt.Errorf("telemetry sources %q and %q are both the default for %q", other, s.ID, k)
				}
				defaults[k] = s.ID
			}
		}
		r.byID[s.ID] = len(r.sources)
		r.sources = append(r.sources, s)
		r.clients = append(r.clients, b.Client)
	}
	return r, nil
}

// Sources returns the bound sources sorted by id.
func (r *Resolver) Sources() []Source {
	out := slices.Clone(r.sources)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *Resolver) boundIDs() []string {
	ids := make([]string, 0, len(r.sources))
	for _, s := range r.sources {
		ids = append(ids, s.ID)
	}
	sort.Strings(ids)
	return ids
}

// Resolve applies the default-source rule. A named source must be bound and
// serve kind. An empty name selects the default for kind; with no default, the
// sole source serving kind; otherwise it is ErrAmbiguous (several) or
// ErrUnbound (none).
func (r *Resolver) Resolve(sourceID string, kind Kind) (Source, error) {
	i, err := r.index(sourceID, kind)
	if err != nil {
		return Source{}, err
	}
	return r.sources[i], nil
}

func (r *Resolver) index(sourceID string, kind Kind) (int, error) {
	fail := func(err error) (int, error) {
		return 0, &ResolveError{Err: err, Source: sourceID, Kind: kind, Bound: r.boundIDs()}
	}
	if sourceID != "" {
		i, ok := r.byID[sourceID]
		if !ok {
			return fail(ErrUnbound)
		}
		if !r.sources[i].Serves(kind) {
			return fail(ErrKind)
		}
		return i, nil
	}
	var serving []int
	for i, s := range r.sources {
		if !s.Serves(kind) {
			continue
		}
		if s.Default {
			return i, nil
		}
		serving = append(serving, i)
	}
	switch len(serving) {
	case 0:
		return fail(ErrUnbound)
	case 1:
		return serving[0], nil
	}
	return fail(ErrAmbiguous)
}

// Query validates q, resolves its source, runs it and checks the result
// against the contract for q.Kind. A client that returns a contract-violating
// result is an error, never a silently accepted answer.
func (r *Resolver) Query(ctx context.Context, q Query, at time.Time) (Result, error) {
	if err := q.Validate(); err != nil {
		return Result{}, err
	}
	i, err := r.index(q.SourceID, q.Kind)
	if err != nil {
		return Result{}, err
	}
	s := r.sources[i]
	if q.Range > 0 && !s.Has(CapRange) {
		return Result{}, fmt.Errorf("telemetry source %q has no range capability", s.ID)
	}
	if r.clients[i] == nil {
		return Result{}, fmt.Errorf("telemetry source %q has no client", s.ID)
	}
	q.SourceID = s.ID
	res, err := r.clients[i].Query(ctx, q, at)
	if err != nil {
		return Result{}, fmt.Errorf("telemetry source %q unavailable: %w", s.ID, err)
	}
	if err := res.Validate(q.Kind); err != nil {
		return Result{}, fmt.Errorf("telemetry source %q returned an invalid result: %w", s.ID, err)
	}
	return res, nil
}
