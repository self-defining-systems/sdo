package lifecyclecore

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type transition func(*contracts.ControllerState, Event) (*contracts.ControllerState, []Effect)
type modelNode struct {
	state                   *contracts.ControllerState
	begun, faults, restarts int
	now                     time.Time
	committed, archived     map[string][]byte
}

func modelBytes(message proto.Message) []byte {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		panic(err)
	}
	return data
}
func modelEffects(effects []Effect) []byte {
	var result []byte
	for _, effect := range effects {
		result = append(result, []byte(fmt.Sprintf("%d:%s:", effect.Kind, effect.ID))...)
		if effect.Closure != nil {
			result = append(result, modelBytes(effect.Closure)...)
		}
		if effect.Receipt != nil {
			result = append(result, modelBytes(effect.Receipt)...)
		}
	}
	return result
}
func modelClosure(index int) *contracts.IncidentClosure {
	return &contracts.IncidentClosure{Request: &contracts.IncidentRequest{IncidentId: fmt.Sprintf("incident-%d", index), RepositoryBaseCommit: "accepted", ResponseDeadline: timestamppb.New(time.Unix(300, 0))}, Result: &contracts.IncidentResult{IncidentId: fmt.Sprintf("incident-%d", index), Status: "completed"}}
}
func modelReceipt(id string) *contracts.ClosureReceipt {
	return &contracts.ClosureReceipt{IncidentId: id, OutcomeCommit: "outcome-" + id, AckToken: "ack-" + id}
}
func copyEvidence(original map[string][]byte) map[string][]byte {
	cloned := map[string][]byte{}
	for id, value := range original {
		cloned[id] = append([]byte(nil), value...)
	}
	return cloned
}
func nodeKey(node modelNode) string {
	key := fmt.Sprintf("%d/%d/%d/%d/%x", node.begun, node.faults, node.restarts, node.now.UnixNano(), modelBytes(node.state))
	for i := 1; i <= 2; i++ {
		id := fmt.Sprintf("incident-%d", i)
		key += fmt.Sprintf("/%x/%x", node.committed[id], node.archived[id])
	}
	return key
}

// explore checks the production function at every reachable edge, not an
// independently authored model. Bounded faults/restarts make fairness explicit:
// after the budget is spent, a functioning edge must complete the pending work.
func explore(step transition) (int, int, error) {
	root := modelNode{state: &contracts.ControllerState{}, now: time.Unix(100, 0), committed: map[string][]byte{}, archived: map[string][]byte{}}
	nodes := []modelNode{root}
	seen := map[string]int{nodeKey(root): 0}
	reverse := [][]int{nil}
	terminals := []int{}
	effectPayloads := map[string][]byte{}
	edges := 0
	deadline := time.Now().Add(2 * time.Minute)
	for cursor := 0; cursor < len(nodes); cursor++ {
		if cursor%1024 == 0 && time.Now().After(deadline) {
			return len(nodes), edges, fmt.Errorf("explorer exceeded 2 minute budget")
		}
		if len(nodes) > 200000 {
			return len(nodes), edges, fmt.Errorf("explorer exceeded state bound")
		}
		node := nodes[cursor]
		phase := node.state.GetClosureState()
		pending := node.state.GetPendingClosure()
		id := pending.GetRequest().GetIncidentId()
		processID := Identity(id, pending.GetRequest().GetRepositoryBaseCommit(), ProcessClosure)
		receipt := node.state.GetClosureReceipt()
		ackID := Identity(id, receipt.GetOutcomeCommit()+"\x00"+receipt.GetReflectionCommit(), AcknowledgeClosure)
		terminal := node.begun == 2 && (phase == contracts.ClosureState_CLOSURE_STATE_FAILED || pending == nil) && len(node.state.GetFailedClosures()) == 0
		if terminal {
			terminals = append(terminals, cursor)
		}
		events := []Event{{Kind: Drive, At: node.now}, {Kind: Committed, At: node.now, EffectID: "stale", Receipt: modelReceipt("old")}, {Kind: Rejected, At: node.now, EffectID: "stale", Error: "old failure"}, {Kind: Acknowledged, At: node.now, EffectID: "stale"}}
		if node.begun < 2 && !BlocksNext(phase, pending != nil) {
			events = append(events, Event{Kind: Begin, Closure: modelClosure(node.begun + 1), At: node.now})
		}
		if node.restarts < 1 {
			events = append(events, Event{Kind: Restart, At: node.now})
		}
		if len(node.state.GetFailedClosures()) > 0 {
			archivedIDs := []string{}
			for _, record := range node.state.FailedClosures {
				archivedIDs = append(archivedIDs, record.GetFailure().GetIncidentId())
			}
			events = append(events, Event{Kind: ArchivesPersisted, ArchivedIncidentIDs: archivedIDs, At: node.now})
		}
		if phase == contracts.ClosureState_CLOSURE_STATE_PROCESSING {
			events = append(events, Event{Kind: Committed, EffectID: processID, Receipt: modelReceipt(id), At: node.now})
			if node.faults < 3 {
				for _, kind := range []EventKind{Rejected, Canceled, AgentTimeout} {
					events = append(events, Event{Kind: kind, EffectID: processID, Error: "API unavailable", At: node.now})
				}
				events = append(events, Event{Kind: Committed, EffectID: processID, Receipt: modelReceipt("wrong"), At: node.now})
			}
		}
		if phase == contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING {
			events = append(events, Event{Kind: Acknowledged, EffectID: ackID, At: node.now})
			if node.faults < 3 {
				events = append(events, Event{Kind: AcknowledgmentRejected, EffectID: ackID, At: node.now})
			}
		}
		// Advance exactly to the next virtual deadline rather than enumerating
		// infinitely many irrelevant wall-clock ticks.
		if due := node.state.GetClosureFailure().GetNextAttemptAt(); due != nil && node.now.Before(due.AsTime()) {
			events = append(events, Event{Kind: Drive, At: due.AsTime()})
		}
		for _, event := range events {
			event.Policy = RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: 4 * time.Second}
			before := modelBytes(node.state)
			eventBefore := fmt.Sprintf("%v/%x/%x", event, modelBytes(event.Closure), modelBytes(event.Receipt))
			next, effects := step(node.state, event)
			again, repeated := step(node.state, event)
			if !bytes.Equal(modelBytes(next), modelBytes(again)) || !bytes.Equal(modelEffects(effects), modelEffects(repeated)) {
				return len(nodes), edges, fmt.Errorf("non-deterministic transition: event %d", event.Kind)
			}
			if !bytes.Equal(before, modelBytes(node.state)) || eventBefore != fmt.Sprintf("%v/%x/%x", event, modelBytes(event.Closure), modelBytes(event.Receipt)) {
				return len(nodes), edges, fmt.Errorf("transition mutated an input")
			}
			for _, effect := range effects {
				expected := Identity(effect.Closure.GetRequest().GetIncidentId(), effect.Closure.GetRequest().GetRepositoryBaseCommit(), ProcessClosure)
				if effect.Kind == AcknowledgeClosure {
					expected = Identity(effect.Receipt.GetIncidentId(), effect.Receipt.GetOutcomeCommit()+"\x00"+effect.Receipt.GetReflectionCommit(), AcknowledgeClosure)
				}
				if effect.ID != expected {
					return len(nodes), edges, fmt.Errorf("unstable effect identity")
				}
				payload := modelEffects([]Effect{effect})
				if prior, ok := effectPayloads[effect.ID]; ok && !bytes.Equal(prior, payload) {
					return len(nodes), edges, fmt.Errorf("retry payload changed for effect %s", effect.ID)
				}
				effectPayloads[effect.ID] = payload
			}
			child := modelNode{state: next, begun: node.begun, faults: node.faults, restarts: node.restarts, now: event.At, committed: copyEvidence(node.committed), archived: copyEvidence(node.archived)}
			if event.Kind == Begin && !proto.Equal(node.state, next) {
				child.begun++
			}
			if event.Kind == Restart {
				child.restarts++
				raw, err := protojson.Marshal(next)
				if err != nil {
					return len(nodes), edges, err
				}
				child.state = &contracts.ControllerState{}
				if err := protojson.Unmarshal(raw, child.state); err != nil {
					return len(nodes), edges, err
				}
			}
			if event.Kind == Rejected || event.Kind == Canceled || event.Kind == AgentTimeout || event.Kind == AcknowledgmentRejected || (event.Kind == Committed && event.Receipt.GetIncidentId() != id && event.EffectID == processID) {
				if event.EffectID != "stale" {
					child.faults++
				}
			}
			if event.Kind == Committed && next.GetClosureState() == contracts.ClosureState_CLOSURE_STATE_COMMITTED && event.EffectID == processID {
				child.committed[id] = modelBytes(pending)
			}
			if event.Kind == ArchivesPersisted {
				for _, record := range node.state.FailedClosures {
					child.archived[record.GetFailure().GetIncidentId()] = modelBytes(record.GetClosure())
				}
			}
			// Completed agent evidence must remain pending, committed, or in the
			// failure outbox/archive. The next incident cannot erase its predecessor.
			for i := 1; i <= child.begun; i++ {
				incident := fmt.Sprintf("incident-%d", i)
				want := modelBytes(modelClosure(i))
				found := false
				if p := child.state.GetPendingClosure(); p.GetRequest().GetIncidentId() == incident && bytes.Equal(modelBytes(p), want) {
					found = true
				}
				if bytes.Equal(child.committed[incident], want) || bytes.Equal(child.archived[incident], want) {
					found = true
				}
				for _, record := range child.state.FailedClosures {
					if record.GetFailure().GetIncidentId() == incident && bytes.Equal(modelBytes(record.GetClosure()), want) {
						found = true
					}
				}
				if !found {
					return len(nodes), edges, fmt.Errorf("completed result discarded: %s", incident)
				}
			}
			key := nodeKey(child)
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
	}
	// Every reachable state must have a path to two terminal incidents once
	// faults/restarts stop. Self-loops and API-fault loops are not liveness proof.
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
			return len(nodes), edges, fmt.Errorf("stuck state %d: %s", index, nodeKey(nodes[index]))
		}
	}
	if len(terminals) == 0 {
		return len(nodes), edges, fmt.Errorf("no terminal states explored")
	}
	return len(nodes), edges, nil
}

func TestExhaustiveClosureLifecycle(t *testing.T) {
	states, edges, err := explore(Step)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d states, %d transitions; 2 incidents, 3 transient faults, 1 restart", states, edges)
}

func TestExplorerRejectsF18TimeDependentRetryPayload(t *testing.T) {
	mutant := func(state *contracts.ControllerState, event Event) (*contracts.ControllerState, []Effect) {
		next, effects := Step(state, event)
		for i := range effects {
			if effects[i].Closure != nil {
				effects[i].Closure.Request.ResponseDeadline = timestamppb.New(event.At.Add(time.Minute))
			}
		}
		return next, effects
	}
	_, _, err := explore(mutant)
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("retry payload changed")) {
		t.Fatalf("F18-shaped mutation escaped: %v", err)
	}
}

func TestExplorerRejectsNonDeterministicTransitions(t *testing.T) {
	calls := 0
	mutant := func(state *contracts.ControllerState, event Event) (*contracts.ControllerState, []Effect) {
		next, effects := Step(state, event)
		calls++
		next.Version = fmt.Sprint(calls)
		return next, effects
	}
	_, _, err := explore(mutant)
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("non-deterministic")) {
		t.Fatalf("non-deterministic mutation escaped: %v", err)
	}
}

func TestExplorerRejectsDiscardedCompletedResults(t *testing.T) {
	mutant := func(state *contracts.ControllerState, event Event) (*contracts.ControllerState, []Effect) {
		next, effects := Step(state, event)
		if event.Kind == Rejected && event.EffectID != "stale" {
			next.PendingClosure = nil
		}
		return next, effects
	}
	_, _, err := explore(mutant)
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("completed result discarded")) {
		t.Fatalf("lost-result mutation escaped: %v", err)
	}
}

func TestExplorerRejectsFailureBlockingLaterIncidents(t *testing.T) {
	mutant := func(state *contracts.ControllerState, event Event) (*contracts.ControllerState, []Effect) {
		if state.GetClosureState() == contracts.ClosureState_CLOSURE_STATE_FAILED && event.Kind == Begin {
			return proto.Clone(state).(*contracts.ControllerState), nil
		}
		return Step(state, event)
	}
	_, _, err := explore(mutant)
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("stuck state")) {
		t.Fatalf("blocking mutation escaped: %v", err)
	}
}
