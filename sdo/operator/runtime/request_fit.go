package runtime

import (
	"bytes"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/worker_registry"
)

// transportRung is one lossy step of the degrade ladder. apply reports whether it removed anything.
type transportRung struct {
	name  string
	apply func() bool
}

// fitStep is one rung the degrade ladder tried, with the request's transport size before and after it.
type fitStep struct {
	name          string
	before, after int
	applied       bool
}

// degradeToFit applies rungs in their fixed order until the request fits the transport limit, and returns every
// rung it tried with the size it left. A request that still does not fit after the last rung is the caller's to
// skip; requestTooLargeDetail then says what each rung removed and what remains largest.
func degradeToFit(size func() (int, error), rungs []transportRung) (steps []fitStep, fits bool, err error) {
	current, err := size()
	if err != nil {
		return nil, false, err
	}
	for _, rung := range rungs {
		if current <= worker_registry.MaxRequestBytes {
			return steps, true, nil
		}
		applied := rung.apply()
		after, err := size()
		if err != nil {
			return steps, false, err
		}
		steps = append(steps, fitStep{name: rung.name, before: current, after: after, applied: applied})
		current = after
	}
	return steps, current <= worker_registry.MaxRequestBytes, nil
}

// appliedSteps names the rungs that removed something: the request's recorded compaction.
func appliedSteps(steps []fitStep) []string {
	var names []string
	for _, step := range steps {
		if step.applied {
			names = append(names, step.name)
		}
	}
	return names
}

// describeFit renders every rung tried with its effect on the transport size.
func describeFit(steps []fitStep) string {
	parts := make([]string, 0, len(steps))
	for _, step := range steps {
		if step.applied {
			parts = append(parts, fmt.Sprintf("%s %d->%d bytes", step.name, step.before, step.after))
		} else {
			parts = append(parts, fmt.Sprintf("%s nothing to remove at %d bytes", step.name, step.before))
		}
	}
	return strings.Join(parts, ", ")
}

// requestTooLargeDetail is the record of a request that does not fit after every rung: what each rung removed
// and the request fields that remain largest.
func requestTooLargeDetail(steps []fitStep, task *contracts.AgentTask) string {
	return describeFit(steps) + "; largest remaining: " + strings.Join(worker_registry.RequestComponents(task), ", ")
}

// wireSize is the size the Job transport measures: the canonical task payload.
func wireSize(task *contracts.AgentTask) func() (int, error) {
	return func() (int, error) {
		payload, err := wire.Canonical(task)
		return len(payload), err
	}
}

const (
	keptEdgeCalls     = 8
	keptCallBytes     = 512
	keptDetectorRuns  = 64
	keptDocumentBytes = 16 << 10
	keptEvidenceBytes = 1 << 10
)

const omittedForTransport = "omitted: the request exceeded the transport limit; open the file in the checkout"

// trimSnapshotToTouched keeps the full text only of playbooks and detectors the rest of the request mentions
// (directly, or as a possible playbook of a mentioned detector). Every id stays listed, so the index is complete.
func trimSnapshotToTouched(snapshot *contracts.OperationalMemorySnapshot, mentions []byte) bool {
	if snapshot == nil {
		return false
	}
	mentioned := func(id string) bool { return id != "" && bytes.Contains(mentions, []byte(id)) }
	touchedPlaybooks := map[string]bool{}
	for _, detector := range snapshot.GetDetectors() {
		if mentioned(detector.GetId()) {
			for _, id := range detector.GetPossiblePlaybookIds() {
				touchedPlaybooks[id] = true
			}
		}
	}
	changed := false
	for _, detector := range snapshot.GetDetectors() {
		if mentioned(detector.GetId()) || (detector.GetWatches() == nil && detector.GetPredicateExcerpt() == "" && detector.GetDescription() == "") {
			continue
		}
		detector.Watches, detector.PredicateExcerpt, detector.Description = nil, "", ""
		changed = true
	}
	for _, playbook := range snapshot.GetPlaybooks() {
		if mentioned(playbook.GetId()) || touchedPlaybooks[playbook.GetId()] || (playbook.GetMarkdown() == "" && len(playbook.GetScripts()) == 0) {
			continue
		}
		playbook.Markdown, playbook.Scripts = omittedForTransport, nil
		changed = true
	}
	return changed
}

// clipSnapshotDocuments bounds the free-text documents of the snapshot; the checkout still holds them whole.
func clipSnapshotDocuments(snapshot *contracts.OperationalMemorySnapshot) bool {
	if snapshot == nil {
		return false
	}
	changed := false
	for _, document := range []*string{&snapshot.GoalMarkdown, &snapshot.ArchitectureMarkdown, &snapshot.PlaybookIndexMarkdown} {
		if len(*document) > keptDocumentBytes {
			*document, _ = clipUTF8(*document, keptDocumentBytes, false)
			changed = true
		}
	}
	return changed
}

// clipProgramEvidence bounds the observed and expected text of every program verdict in message. A Diagnose may
// quote a whole object diff as evidence, so each run's size otherwise grows with the object, not with the run
// count. The incident outcome keeps the full text.
func clipProgramEvidence(message proto.Message) bool {
	changed := false
	var walk func(protoreflect.Message)
	walk = func(m protoreflect.Message) {
		if evidence, ok := m.Interface().(*contracts.Evidence); ok {
			evidence.Observed, changed = clipUTF8(evidence.Observed, keptEvidenceBytes, changed)
			evidence.Expected, changed = clipUTF8(evidence.Expected, keptEvidenceBytes, changed)
			return
		}
		m.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			switch {
			case field.IsMap():
				if field.MapValue().Message() != nil {
					value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool { walk(item.Message()); return true })
				}
			case field.Message() == nil:
			case field.IsList():
				for index := range value.List().Len() {
					walk(value.List().Get(index).Message())
				}
			default:
				walk(value.Message())
			}
			return true
		})
	}
	walk(message.ProtoReflect())
	return changed
}

// capProgramRuns keeps the first and last runs; the incident outcome stays the full record.
func capProgramRuns(runs []*contracts.ProgramRun) ([]*contracts.ProgramRun, bool) {
	if len(runs) <= 2*keptEdgeCalls {
		return runs, false
	}
	return append(append([]*contracts.ProgramRun(nil), runs[:keptEdgeCalls]...), runs[len(runs)-keptEdgeCalls:]...), true
}

// capToolCalls keeps the first and last calls with bounded payloads and one summary call for the omitted middle.
func capToolCalls(calls []*contracts.ProgramToolCall) ([]*contracts.ProgramToolCall, bool) {
	changed := false
	shorten := func(call *contracts.ProgramToolCall) *contracts.ProgramToolCall {
		short := proto.Clone(call).(*contracts.ProgramToolCall)
		short.RequestJson, changed = clipUTF8(short.RequestJson, keptCallBytes, changed)
		short.ResponseJson, changed = clipUTF8(short.ResponseJson, keptCallBytes, changed)
		return short
	}
	var kept []*contracts.ProgramToolCall
	for index, call := range calls {
		if len(calls) > 2*keptEdgeCalls && index == keptEdgeCalls {
			kept = append(kept, &contracts.ProgramToolCall{Command: "omitted", Summary: fmt.Sprintf("%d tool calls omitted for the transport limit; the incident outcome has the full record", len(calls)-2*keptEdgeCalls)})
			changed = true
		}
		if len(calls) > 2*keptEdgeCalls && index >= keptEdgeCalls && index < len(calls)-keptEdgeCalls {
			continue
		}
		kept = append(kept, shorten(call))
	}
	return kept, changed
}

// clipUTF8 cuts value to at most limit bytes without splitting a rune.
func clipUTF8(value string, limit int, changed bool) (string, bool) {
	if len(value) <= limit {
		return value, changed
	}
	return strings.ToValidUTF8(value[:limit], "") + "…", true
}
