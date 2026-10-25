package lifecyclecore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type reflectionNode struct {
	state            *contracts.BrokerLedger
	faults, restarts int
	now              time.Time
	payloads         map[string][]byte
}

func reflectionBytes(effect ReflectionEffect) []byte {
	payload, err := json.Marshal(effect)
	if err != nil {
		panic(err)
	}
	return payload
}
func reflectionKey(node reflectionNode) string {
	var result bytes.Buffer
	var frame [8]byte
	for _, value := range []uint64{uint64(node.faults), uint64(node.restarts), uint64(node.now.UnixNano())} {
		binary.BigEndian.PutUint64(frame[:], value)
		result.Write(frame[:])
	}
	appendFrame := func(payload []byte) {
		binary.BigEndian.PutUint64(frame[:], uint64(len(payload)))
		result.Write(frame[:])
		result.Write(payload)
	}
	// StepReflection stores dispatch/rejection timestamps but never branches on
	// their values. Quotient only those historical clock values out of visited
	// state; the concrete node still checks exact payloads and restart bytes on
	// every edge. Fault/restart budgets and the current clock remain in the key.
	state := proto.Clone(node.state).(*contracts.BrokerLedger)
	if state.ReflectionDispatchAt != nil {
		state.ReflectionDispatchAt = timestamppb.New(time.Unix(0, 0))
	}
	for _, rejection := range state.ReflectionRejections {
		if rejection.RejectedAt != nil {
			rejection.RejectedAt = timestamppb.New(time.Unix(0, 0))
		}
	}
	appendFrame(modelBytes(state))
	keys := make([]string, 0, len(node.payloads))
	for id := range node.payloads {
		keys = append(keys, id)
	}
	slices.Sort(keys)
	for _, id := range keys {
		appendFrame([]byte(id))
		var effect ReflectionEffect
		if err := json.Unmarshal(node.payloads[id], &effect); err != nil {
			panic(err)
		}
		if effect.DispatchAt != nil {
			effect.DispatchAt = timestamppb.New(time.Unix(0, 0))
		}
		appendFrame(reflectionBytes(effect))
	}
	return result.String()
}
func TestExhaustiveReflectionLifecycle(t *testing.T) {
	closure := modelClosure(1)
	closure.VerifiedAt = timestamppb.New(time.Unix(100, 0))
	initial, _ := StepReflection(&contracts.BrokerLedger{IncidentId: "incident-1", OutcomeCommit: "accepted", Closure: closure, OutcomeDocument: &contracts.OutcomeRecord{IncidentId: "incident-1"}}, ReflectionEvent{Kind: ReflectionBegin, MaxRetries: 2})
	nodes := []reflectionNode{{state: initial, now: time.Unix(100, 0), payloads: map[string][]byte{}}}
	seen := map[string]int{reflectionKey(nodes[0]): 0}
	reverse := [][]int{nil}
	terminals := []int{}
	edges := 0
	deadline := time.Now().Add(4 * time.Minute)
	for cursor := 0; cursor < len(nodes); cursor++ {
		if cursor%1024 == 0 && time.Now().After(deadline) {
			t.Fatal("reflection explorer exceeded 4 minute budget")
		}
		if len(nodes) > 100000 {
			t.Fatal("reflection state bound exceeded")
		}
		node := nodes[cursor]
		drive := ReflectionEvent{Kind: ReflectionDrive}
		before := modelBytes(node.state)
		next, effects := StepReflection(node.state, drive)
		again, repeated := StepReflection(node.state, drive)
		left, _ := json.Marshal(effects)
		right, _ := json.Marshal(repeated)
		if !bytes.Equal(modelBytes(next), modelBytes(again)) || !bytes.Equal(left, right) || !bytes.Equal(before, modelBytes(node.state)) {
			t.Fatal("reflection drive is not deterministic/pure")
		}
		if node.state.ReflectionPhase == contracts.ReflectionPhase_REFLECTION_PHASE_DONE {
			terminals = append(terminals, cursor)
		}
		events := []ReflectionEvent{}
		if len(effects) > 0 {
			effect := effects[0]
			payload := reflectionBytes(effect)
			if prior, ok := node.payloads[effect.ID]; ok && !bytes.Equal(prior, payload) {
				t.Fatalf("reflection retry payload changed: %d", effect.Kind)
			}
			node.payloads = copyEvidence(node.payloads)
			node.payloads[effect.ID] = payload
			event := ReflectionEvent{EffectID: effect.ID, At: node.now}
			switch effect.Kind {
			case InspectReflection:
				event.Kind = ReflectionInspected
				event.Workspace = &contracts.ProposalWorkspace{ProposalId: "reflection", CheckoutPath: "/proposal", BaseCommit: "accepted"}
				if node.state.GetReflectionDecision().GetLearningDecision() == "updated" {
					event.Paths = []string{"playbook"}
				}
				events = append(events, event)
			case DispatchReflection:
				event.Kind = ReflectionCompleted
				for _, decision := range []string{"updated", "no_change", "failed"} {
					variant := event
					variant.Decision = &contracts.ReflectionDecision{LearningDecision: decision}
					if decision == "failed" {
						variant.Decision.Error = "agent timeout"
					}
					events = append(events, variant)
				}
			case ReviewReflection:
				event.Kind = ReflectionReviewed
				events = append(events, event)
				event.Kind = ReflectionRejected
				event.Diagnostics = []*contracts.MemoryDiagnostic{{Rule: "typed.rejection", Message: "correct the proposal"}}
				events = append(events, event)
			case PublishReflection:
				event.Kind = ReflectionPublished
				event.Commit = "reflection-commit"
				event.Paths = []string{"playbook"}
				events = append(events, event)
				event.Kind = ReflectionRejected
				event.Commit = ""
				event.Diagnostics = []*contracts.MemoryDiagnostic{{Rule: "typed.rejection", Message: "validator rejected"}}
				events = append(events, event)
			case ResetReflection:
				event.Kind = ReflectionReset
				events = append(events, event)
			case CleanupReflection:
				event.Kind = ReflectionCleaned
				events = append(events, event)
			}
			if node.faults < 3 {
				events = append(events, ReflectionEvent{Kind: ReflectionFault, EffectID: effect.ID, At: node.now.Add(time.Second)})
			}
			events = append(events, ReflectionEvent{Kind: ReflectionCompleted, EffectID: "stale", At: node.now, Decision: &contracts.ReflectionDecision{LearningDecision: "failed"}})
		}
		for _, event := range events {
			before = modelBytes(node.state)
			eventBefore, _ := json.Marshal(event)
			after, _ := StepReflection(node.state, event)
			duplicate, _ := StepReflection(node.state, event)
			if !bytes.Equal(modelBytes(after), modelBytes(duplicate)) || !bytes.Equal(before, modelBytes(node.state)) {
				t.Fatal("reflection event is not deterministic/pure")
			}
			eventAfter, _ := json.Marshal(event)
			if !bytes.Equal(eventBefore, eventAfter) {
				t.Fatal("reflection event payload mutated")
			}
			if len(after.ReflectionResults) < len(node.state.ReflectionResults) {
				t.Fatal("completed reflection result discarded")
			}
			for i, result := range node.state.ReflectionResults {
				if !proto.Equal(result, after.ReflectionResults[i]) {
					t.Fatal("completed reflection result changed")
				}
			}
			if event.Kind == ReflectionFault && (!proto.Equal(after, node.state)) {
				t.Fatal("API fault changed proposal/result state")
			}
			if event.Kind == ReflectionInspected && node.state.ReflectionPhase == contracts.ReflectionPhase_REFLECTION_PHASE_INSPECT && after.ReflectionPhase == contracts.ReflectionPhase_REFLECTION_PHASE_DISPATCH && !after.ReflectionDispatchAt.AsTime().Equal(event.At) {
				t.Fatal("dispatch timestamp did not preserve the observation clock")
			}
			if event.Kind == ReflectionRejected && len(after.ReflectionRejections) > len(node.state.ReflectionRejections) && !after.ReflectionRejections[len(after.ReflectionRejections)-1].RejectedAt.AsTime().Equal(event.At) {
				t.Fatal("rejection timestamp did not preserve the observation clock")
			}
			if after.ReflectionPhase == contracts.ReflectionPhase_REFLECTION_PHASE_RESET && len(after.ReflectionRejections) == 0 {
				t.Fatal("checkout reset without a durable typed rejection")
			}
			if len(after.ReflectionRejections) > 3 {
				t.Fatal("correction budget exceeded")
			}
			child := reflectionNode{state: after, faults: node.faults, restarts: node.restarts, now: event.At, payloads: copyEvidence(node.payloads)}
			// Only the current step can retry. Completed step IDs include the
			// durable phase and attempt, so earlier payload maps cannot affect
			// any future transition and need not inflate the BFS state key.
			if after.ReflectionPhase != node.state.ReflectionPhase {
				child.payloads = map[string][]byte{}
			}
			if event.Kind == ReflectionFault {
				child.faults++
			}
			key := reflectionKey(child)
			target, ok := seen[key]
			if !ok {
				target = len(nodes)
				seen[key] = target
				nodes = append(nodes, child)
				reverse = append(reverse, nil)
			}
			if target != cursor {
				reverse[target] = append(reverse[target], cursor)
				edges++
			}
		}
		if node.restarts < 1 {
			raw, err := protojson.Marshal(node.state)
			if err != nil {
				t.Fatal(err)
			}
			restored := &contracts.BrokerLedger{}
			if err := protojson.Unmarshal(raw, restored); err != nil {
				t.Fatal(err)
			}
			restored, _ = StepReflection(restored, ReflectionEvent{Kind: ReflectionBegin, MaxRetries: 99})
			if !proto.Equal(restored, node.state) {
				t.Fatal("restart changed persisted reflection state or reset its budget")
			}
			child := reflectionNode{state: restored, faults: node.faults, restarts: node.restarts + 1, now: node.now.Add(time.Second), payloads: copyEvidence(node.payloads)}
			key := reflectionKey(child)
			target, ok := seen[key]
			if !ok {
				target = len(nodes)
				seen[key] = target
				nodes = append(nodes, child)
				reverse = append(reverse, nil)
			}
			reverse[target] = append(reverse[target], cursor)
			edges++
		}
	}
	live := make([]bool, len(nodes))
	queue := append([]int(nil), terminals...)
	for _, index := range queue {
		live[index] = true
	}
	for cursor := 0; cursor < len(queue); cursor++ {
		for _, prior := range reverse[queue[cursor]] {
			if !live[prior] {
				live[prior] = true
				queue = append(queue, prior)
			}
		}
	}
	for index, ok := range live {
		if !ok {
			t.Fatalf("stuck reflection state %d: %v", index, nodes[index].state)
		}
	}
	t.Logf("%d reflection states, %d transitions; 3 API faults, 1 restart, 3 bounded worker attempts", len(nodes), edges)
}
