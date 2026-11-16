package onboardcheck

import (
	"context"
	"fmt"
	"strings"
	"time"

	"sdo.dev/operator/memory/appmap"
)

// Fault is a break that keeps every component up: the application answers, but
// something it depends on is silently wrong. It is judged like a component
// break: a raise must name an operation it hurts, or one of that operation's
// test scenarios.
type Fault interface {
	// Name identifies the fault in the report.
	Name() string
	// Describe says what Break does.
	Describe() string
	// Operations are the operations the fault hurts.
	Operations() []string
	// NotApplicable says, when the environment cannot carry out the fault (a
	// CNI that does not enforce NetworkPolicy, a store without SQL access), why;
	// empty means it can.
	NotApplicable(ctx context.Context) string
	// Break applies the fault.
	Break(ctx context.Context) error
	// Restore undoes it and verifies that. It is safe to call when Break failed
	// or never ran. An error means the restore is uncertain.
	Restore(ctx context.Context) error
}

// faultBreaker lets a fault run through the same wave machinery as a component.
type faultBreaker struct{ f Fault }

func (b faultBreaker) Mode() string                                          { return b.f.Describe() }
func (b faultBreaker) Break(ctx context.Context, _ appmap.Component) error   { return b.f.Break(ctx) }
func (b faultBreaker) Restore(ctx context.Context, _ appmap.Component) error { return b.f.Restore(ctx) }

// FaultTargets finds what the generic faults act on: the workers (components
// that do background work: they sit on the path of an internal operation, or
// serve nothing and hold no data, and are not an operation's entry) and the
// authoritative store.
func FaultTargets(m appmap.Map) (workers []appmap.Component, store *appmap.Component) {
	entryOf := map[string]bool{}
	onInternal := map[string]bool{}
	onPath := map[string]bool{}
	for _, op := range m.Operations {
		for i, name := range op.Path {
			onPath[name] = true
			if i == 0 && !m.Internal(op) {
				entryOf[name] = true
			}
			if m.Internal(op) {
				onInternal[name] = true
			}
		}
	}
	for _, c := range m.Components {
		if c.Workload == "" || !onPath[c.Name] || c.HoldsData || c.Data != nil || entryOf[c.Name] {
			continue
		}
		if onInternal[c.Name] || len(c.Services) == 0 {
			workers = append(workers, c)
		}
	}
	// The store is the first authoritative postgres component on a path (the
	// only engine the data-wrong break can act on); another authoritative
	// store is reported as not applicable only when no postgres one exists.
	for i, c := range m.Components {
		if c.Workload == "" || !onPath[c.Name] || c.Data == nil || c.Data.Role != appmap.RoleAuthoritative {
			continue
		}
		if c.Data.Engine == appmap.EnginePostgres {
			return workers, &m.Components[i]
		}
		if store == nil {
			store = &m.Components[i]
		}
	}
	return workers, store
}

// OperationsThrough lists the operations whose path crosses the component.
func OperationsThrough(m appmap.Map, component string) []string {
	var ids []string
	for _, op := range m.Operations {
		for _, name := range op.Path {
			if name == component {
				ids = append(ids, op.ID)
				break
			}
		}
	}
	return ids
}

// reportFault judges a fault's outcome like a component break's: green when a
// raise named an operation it hurts or one of that operation's test scenarios.
func reportFault(report *Report, f Fault, out *breakOutcome, ids []string, bound time.Duration) {
	subject := f.Name()
	switch {
	case out.err != "":
		report.add(ItemBreak, subject, Red, out.err)
	case len(out.opHit) > 0:
		var first *foundRaise
		var op string
		for _, id := range ids {
			if h := out.opHit[id]; h != nil && (first == nil || h.after < first.after) {
				first, op = h, id
			}
		}
		report.add(ItemBreak, subject, Green, fmt.Sprintf("%s: %s noticed after %s, naming %s: %s%s", out.how, first.raise.DetectorID, first.after.Round(time.Second), op, clip(first.raise.Summary, 200), signalNote(out, op)))
	default:
		report.add(ItemBreak, subject, Red, fmt.Sprintf("%s: no detector named %s or its test scenarios within %s%s; every component stayed up, so only a test or request metric of the operation can catch this", out.how, strings.Join(ids, ", "), bound, otherRaises(out)))
	}
}
