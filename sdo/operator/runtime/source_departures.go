package runtime

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// DepartureKind says how a live object departs from accepted source.
type DepartureKind string

const (
	// DepartureChanged: declared fields differ, or fields were added after acceptance.
	DepartureChanged DepartureKind = "changed"
	// DepartureMissing: accepted source declares the object and it is gone.
	DepartureMissing DepartureKind = "missing"
	// DepartureExtra: the object exists live and accepted source does not declare it.
	DepartureExtra DepartureKind = "extra"
	// DepartureSecretData: a non-SDO manager wrote a Secret's data after a reader started.
	// Secrets are compared by metadata only, so no values or diff are ever read.
	DepartureSecretData DepartureKind = "secret-data"
)

// SourceDeparture is one object that differs from accepted source because a
// manager other than SDO changed it. It is information for the responder, never
// a verdict: whether it explains the failure, and whether to restore it, is the
// responder's call.
type SourceDeparture struct {
	Ref     sdk.ObjectRef
	Kind    DepartureKind
	Fields  []string
	Manager string
	// At is when the manager wrote; zero when the object is gone and nothing records it.
	At time.Time
}

// Command is the responder command that puts the object back to accepted
// source, from the built-in actions `sdo mitigate` already offers. It is empty
// for a Secret: a restore needs values, which are never read here.
func (d SourceDeparture) Command() string {
	ref := fmt.Sprintf("%s/%s/%s", d.Ref.Kind, d.Ref.Namespace, d.Ref.Name)
	switch d.Kind {
	case DepartureChanged:
		return "sdo mitigate ReconcileFromSource --bind ref=" + ref
	case DepartureMissing:
		return "sdo mitigate CreateFromSource --bind ref=" + ref
	case DepartureExtra:
		return "sdo mitigate DeleteObject --bind ref=" + ref
	}
	return ""
}

// Text is the one-line description of the departure.
func (d SourceDeparture) Text() string {
	name := d.Ref.Kind + "/" + d.Ref.Name
	switch d.Kind {
	case DepartureMissing:
		return name + " is missing; accepted source declares it"
	case DepartureSecretData:
		return fmt.Sprintf("%s data changed at %s by a non-sdo manager; values not compared", name, hms(d.At))
	}
	what := "changed"
	if len(d.Fields) > 0 {
		what = strings.Join(d.Fields, ",") + " changed"
	}
	end := "differs from accepted source"
	if d.Kind == DepartureExtra {
		what, end = "present", "absent from accepted source"
	}
	return fmt.Sprintf("%s %s at %s by %s (not sdo); %s", name, what, hms(d.At), orQuestion(d.Manager), end)
}

// DepartureInput is what the classifier reads. The same input serves every
// reader, so what `sdo detect` shows is what a responder would see anywhere.
type DepartureInput struct {
	// View is the frozen snapshot, including DiffAgainstSource. A view that is
	// also an sdk.ObjectReader lets the classifier tell a missing object from a changed one.
	View sdk.DetectionContext
	// Events are the drift events with the manager that wrote each change.
	Events []sdk.DriftEvent
	// Onset bounds the window: later changes are consequences, not causes. Zero is unbounded.
	Onset time.Time
	// DeploymentInFlight is true while SDO is deploying or advancing the source;
	// differences then are SDO's own work in progress.
	DeploymentInFlight bool
}

// departureInput is the classifier input every reader builds the same way: the
// request's drift events, and a deployment in flight while the accepted source
// is not the deployed commit.
func departureInput(request IncidentRequest, view sdk.DetectionContext, onset time.Time) DepartureInput {
	return DepartureInput{
		View: view, Events: request.DriftEvents, Onset: onset,
		DeploymentInFlight: request.SourceCommit != "" && request.DeployedCommit != "" && request.SourceCommit != request.DeployedCommit,
	}
}

// componentObjects are the objects whose departure from source can explain a
// component: its workload and everything workloadObjects relates to it, plus its
// declared Services even when one is gone.
func componentObjects(view sdk.DetectionContext, component program.AppComponent, namespace string) []sdk.ObjectRef {
	objects := workloadObjects(view, component.WorkloadRef(namespace))
	for _, service := range component.Services {
		objects = append(objects, sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: service})
	}
	return objects
}

const departureOnsetSlack = 30 * time.Second

// SourceDepartures lists, for the given objects, the departures from accepted
// source made by a manager other than SDO. Only changes the controller
// observed qualify (a drift event names the manager and the time), and the
// source comparison must still show the departure now, so an acceptance
// record, a later SDO deployment or a repair that already restored the object
// removes it. An unreadable comparison yields nothing rather than a guess.
func SourceDepartures(in DepartureInput, objects []sdk.ObjectRef) []SourceDeparture {
	if in.DeploymentInFlight || in.View == nil {
		return nil
	}
	late := func(at time.Time) bool { return !in.Onset.IsZero() && at.After(in.Onset.Add(departureOnsetSlack)) }
	var result []SourceDeparture
	seen := map[string]bool{}
	for _, ref := range objects {
		key := ref.Kind + "/" + ref.Name
		if seen[key] {
			continue
		}
		seen[key] = true
		if ref.Kind == "Secret" {
			if departure, ok := secretDeparture(in, ref); ok && !late(departure.At) {
				result = append(result, departure)
			}
			continue
		}
		diff, err := in.View.DiffAgainstSource(ref)
		if err != nil || !diff.Changed {
			continue
		}
		departure := SourceDeparture{Ref: ref, Kind: DepartureChanged}
		if reader, ok := in.View.(sdk.ObjectReader); ok && !diff.SourceAbsent {
			if object, err := reader.Object(ref); err == nil && object == nil {
				departure.Kind = DepartureMissing
				result = append(result, departure)
				continue
			}
		}
		if diff.SourceAbsent {
			departure.Kind = DepartureExtra
		}
		event := foreignDriftEvent(in.Events, ref)
		if event == nil {
			continue
		}
		departure.Manager = event.Manager
		departure.At = event.ObservedAt
		if event.ManagerAt != nil {
			departure.At = *event.ManagerAt
		}
		if late(departure.At) {
			continue
		}
		for _, field := range event.Fields {
			if len(departure.Fields) < 2 {
				departure.Fields = append(departure.Fields, redactDetectText(field.Path, 40))
			}
		}
		result = append(result, departure)
	}
	sort.SliceStable(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if (a.Kind == DepartureMissing) != (b.Kind == DepartureMissing) {
			return a.Kind == DepartureMissing
		}
		return a.At.After(b.At)
	})
	return result
}

// foreignDriftEvent is the latest event for ref written by a manager that is not SDO.
func foreignDriftEvent(events []sdk.DriftEvent, ref sdk.ObjectRef) *sdk.DriftEvent {
	var best *sdk.DriftEvent
	when := func(e *sdk.DriftEvent) time.Time {
		if e.ManagerAt != nil {
			return *e.ManagerAt
		}
		return e.ObservedAt
	}
	for index := range events {
		event := &events[index]
		if event.Ref.Kind != ref.Kind || event.Ref.Name != ref.Name || sdk.IsSDOFieldManager(event.Manager) {
			continue
		}
		if best == nil || when(event).After(when(best)) {
			best = event
		}
	}
	return best
}

// secretDeparture reports a Secret whose data a non-SDO manager changed after
// a Pod started, so a running reader may hold the old value. It uses
// SecretMetadata only; a Secret never has its values read or compared.
func secretDeparture(in DepartureInput, ref sdk.ObjectRef) (SourceDeparture, bool) {
	for _, secret := range in.View.Secrets() {
		if secret.Name != ref.Name || secret.DataChangedAt.IsZero() {
			continue
		}
		for _, pod := range in.View.Pods() {
			if pod.CreationTimestamp.Time.Before(secret.DataChangedAt) {
				return SourceDeparture{Ref: ref, Kind: DepartureSecretData, At: secret.DataChangedAt, Manager: "a non-sdo manager"}, true
			}
		}
	}
	return SourceDeparture{}, false
}
