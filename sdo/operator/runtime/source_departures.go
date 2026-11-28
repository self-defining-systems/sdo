package runtime

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

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
	// DepartureNotReloaded: the ConfigMap matches accepted source again, but
	// workloads that read it only at container start (subPath, env, envFrom)
	// still run Pods that loaded the departed content (Consumers).
	DepartureNotReloaded DepartureKind = "not_reloaded"
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
	// SeenHealthy: what departs now is what the application stayed healthy
	// with for a settle window (seen_healthy.go).
	SeenHealthy bool
	// Unattributed is a missing object no drift event explains: shown as plain
	// information, with no claim about who removed it and no restore command.
	Unattributed bool
	// Consumers are the workloads that read this ConfigMap only at container
	// start and run a Pod that loaded the departed content: restoring the
	// object alone does not change what they run, a rollout restart does.
	Consumers []sdk.ObjectRef
	// DataConsumers are such workloads of a data store (holds_data or a data
	// role). They get no command and never make the built-in restore match:
	// restarting a data store is the responder's call.
	DataConsumers []sdk.ObjectRef
}

// Command is the responder command that puts the object back to accepted
// source, from the built-in actions `sdo mitigate` already offers.
func (d SourceDeparture) Command() string {
	ref := fmt.Sprintf("%s/%s/%s", d.Ref.Kind, d.Ref.Namespace, d.Ref.Name)
	switch d.Kind {
	case DepartureChanged:
		return "sdo mitigate ReconcileFromSource --bind ref=" + ref
	case DepartureMissing:
		if d.Unattributed {
			return ""
		}
		return "sdo mitigate CreateFromSource --bind ref=" + ref
	case DepartureExtra:
		return "sdo mitigate DeleteObject --bind ref=" + ref
	case DepartureNotReloaded:
		var commands []string
		for _, consumer := range d.Consumers {
			commands = append(commands, fmt.Sprintf("sdo mitigate RestartRollout --bind ref=%s/%s/%s", consumer.Kind, consumer.Namespace, consumer.Name))
		}
		return strings.Join(commands, "; ")
	}
	return ""
}

// dataConsumersNote says that restarting a data store still on the departed
// content is the responder's decision; SDO gives no command for it.
func dataConsumersNote(refs []sdk.ObjectRef) string {
	if len(refs) == 0 {
		return ""
	}
	return "; " + refNames(refs) + " hold data: restarting a data store is the responder's call"
}

func refNames(refs []sdk.ObjectRef) string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.Kind+"/"+ref.Name)
	}
	return strings.Join(names, ",")
}

// Text is the one-line description of the departure.
func (d SourceDeparture) Text() string {
	name := d.Ref.Kind + "/" + d.Ref.Name
	switch d.Kind {
	case DepartureMissing:
		if d.Unattributed {
			return name + " is missing; accepted source declares it"
		}
		return fmt.Sprintf("%s was deleted at %s by %s (not sdo); accepted source declares it", name, hms(d.At), orQuestion(d.Manager))
	case DepartureNotReloaded:
		readers := refNames(append(slices.Clone(d.Consumers), d.DataConsumers...))
		text := fmt.Sprintf("%s changed at %s by %s (not sdo) and matches accepted source again, but %s still run Pods started with the changed content (read only at start: subPath, env or envFrom)", name, hms(d.At), orQuestion(d.Manager), readers)
		return text + dataConsumersNote(d.DataConsumers)
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
	// DeploymentInFlight is true while the controller has an accepted-source
	// rollout or advance that has not converged (IncidentRequest.DeploymentInFlight);
	// differences then are SDO's own work in progress.
	DeploymentInFlight bool
}

// departureInput is the classifier input every reader builds the same way: the
// request's drift events, and the controller's own rollout state for the guard.
func departureInput(request IncidentRequest, view sdk.DetectionContext, onset time.Time) DepartureInput {
	return DepartureInput{View: view, Events: request.DriftEvents, Onset: onset, DeploymentInFlight: request.DeploymentInFlight}
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
			// Source comparisons exclude Secret data. Field ownership alone
			// cannot establish a departure from source.
			continue
		}
		diff, err := in.View.DiffAgainstSource(ref)
		if err != nil {
			continue
		}
		if !diff.Changed {
			if departure, ok := notReloaded(in, ref, late); ok {
				result = append(result, departure)
			}
			continue
		}
		departure := SourceDeparture{Ref: ref, Kind: DepartureChanged}
		missing := false
		if reader, ok := in.View.(sdk.ObjectReader); ok && !diff.SourceAbsent {
			// The ObjectReader contract reports an absent identity as
			// ErrObjectAbsent, never as a nil object; a nil object without an
			// error is tolerated as absent too.
			object, err := reader.Object(ref)
			if errors.Is(err, sdk.ErrObjectAbsent) || err == nil && object == nil {
				missing = true
				departure.Kind = DepartureMissing
			} else if err == nil && sdoWroteLast(object) {
				continue
			}
		}
		if diff.SourceAbsent {
			departure.Kind = DepartureExtra
		}
		event := latestDriftEvent(in.Events, ref)
		if event != nil && sdk.IsSDOFieldManager(event.Manager) {
			continue
		}
		if event == nil {
			if missing {
				departure.Unattributed = true
				result = append(result, departure)
			}
			continue
		}
		departure.Manager, departure.SeenHealthy = event.Manager, seenHealthy(in.View, ref, event)
		departure.At = event.ObservedAt
		if event.ManagerAt != nil {
			departure.At = *event.ManagerAt
		}
		if late(departure.At) {
			continue
		}
		if missing {
			result = append(result, departure)
			continue
		}
		for _, field := range event.Fields {
			if len(departure.Fields) < 2 {
				departure.Fields = append(departure.Fields, redactDetectText(field.Path, 40))
			}
		}
		if ref.Kind == "ConfigMap" && departure.Kind == DepartureChanged && len(dataPaths(event)) > 0 {
			departure.Consumers, departure.DataConsumers = splitDataStores(in.View, staleConsumers(in.View, ref, departure.At, time.Time{}))
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

// notReloaded is the departure of a ConfigMap that matches accepted source
// again while a workload that reads it only at start still runs a Pod started
// after the foreign change and before its content was written back
// (contentRestoredAt). Without a readable object or a recorded content write
// it claims nothing.
func notReloaded(in DepartureInput, ref sdk.ObjectRef, late func(time.Time) bool) (SourceDeparture, bool) {
	if ref.Kind != "ConfigMap" {
		return SourceDeparture{}, false
	}
	event := latestForeignDriftEvent(in.Events, ref)
	reader, ok := in.View.(sdk.ObjectReader)
	if event == nil || !ok {
		return SourceDeparture{}, false
	}
	at := driftEventTime(event)
	if late(at) {
		return SourceDeparture{}, false
	}
	object, err := reader.Object(ref)
	if err != nil || object == nil {
		return SourceDeparture{}, false
	}
	restored := contentRestoredAt(object, event)
	if restored.IsZero() || !restored.After(at) {
		return SourceDeparture{}, false
	}
	stale := staleConsumers(in.View, ref, at, restored)
	if len(stale) == 0 {
		return SourceDeparture{}, false
	}
	consumers, data := splitDataStores(in.View, stale)
	// The stale Pods were seen healthy only if the content was already written
	// back, and has not changed since, for a whole healthy window.
	healthy := seenHealthy(in.View, ref, event) && !restored.After(*event.HealthyContentSince)
	return SourceDeparture{Ref: ref, Kind: DepartureNotReloaded, Manager: event.Manager, At: at, SeenHealthy: healthy, Consumers: consumers, DataConsumers: data}, true
}

// driftEventTime is when the manager wrote, else when the controller observed it.
func driftEventTime(e *sdk.DriftEvent) time.Time {
	if e.ManagerAt != nil {
		return *e.ManagerAt
	}
	return e.ObservedAt
}

// latestDriftEvent is the latest event for ref across all managers. Whether its
// writer is SDO decides if the object counts: an older foreign event never
// explains a difference a later SDO write made.
func latestDriftEvent(events []sdk.DriftEvent, ref sdk.ObjectRef) *sdk.DriftEvent {
	var best *sdk.DriftEvent
	for index := range events {
		event := &events[index]
		if event.Ref.Kind != ref.Kind || event.Ref.Name != ref.Name {
			continue
		}
		if best == nil || driftEventTime(event).After(driftEventTime(best)) {
			best = event
		}
	}
	return best
}

// sdoWroteLast reports that, by managedFields time across all managers, the
// object's latest writer is one of SDO's. Status-subresource entries are
// controllers reporting state, not writers of the declared object. An object
// with no field ownership records says nothing, so it is not SDO's.
func sdoWroteLast(object *unstructured.Unstructured) bool {
	var latest string
	var at time.Time
	for _, entry := range object.GetManagedFields() {
		if entry.Time == nil || entry.Subresource == "status" {
			continue
		}
		if latest == "" || entry.Time.Time.After(at) {
			latest, at = entry.Manager, entry.Time.Time
		}
	}
	return latest != "" && sdk.IsSDOFieldManager(latest)
}
