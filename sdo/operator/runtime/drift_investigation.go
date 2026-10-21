package runtime

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

const DefaultDriftInvestigationWindow = 20 * time.Second
const maxDriftBatchEvents = 8
const maxDriftEvidence = 8

type DriftInvestigator interface {
	Investigate(context.Context, *contracts.DriftInvestigationTask) (*contracts.DriftInvestigationResult, error)
}

type DriftInvestigationConfig struct {
	Worker            DriftInvestigator
	Async             bool
	BatchWindow       time.Duration
	DispatchesPerHour int
	TokensPerHour     int64
	BuildTask         func(context.Context, string, []sdk.DriftEvent) (*contracts.DriftInvestigationTask, error)
	OnVerdict         func(sdk.DriftEvent, *contracts.DriftEventInvestigation)
	OnPrepared        func(context.Context) error
	OnDeferred        func(DriftDeferral)
}

type DriftInvestigationState struct {
	Pending     []sdk.DriftEvent          `json:"pending,omitempty"`
	Deferred    []sdk.DriftEvent          `json:"deferred,omitempty"`
	RequestJSON string                    `json:"request_json,omitempty"`
	DispatchAt  []time.Time               `json:"dispatch_at,omitempty"`
	TokenUsage  []DriftTokenUse           `json:"token_usage,omitempty"`
	Verdicts    map[string]sdk.DriftEvent `json:"verdicts,omitempty"`
	Deferral    *DriftDeferral            `json:"deferral,omitempty"`
}

type DriftDeferralReason string

const (
	DriftDispatchBudget DriftDeferralReason = "dispatch_budget"
	DriftTokenBudget    DriftDeferralReason = "token_budget"
)

type DriftDeferral struct {
	Reason DriftDeferralReason `json:"reason"`
	Until  time.Time           `json:"until"`
}

type DriftTokenUse struct {
	At           time.Time `json:"at"`
	Tokens       int64     `json:"tokens"`
	InputTokens  int64     `json:"input_tokens,omitempty"`
	OutputTokens int64     `json:"output_tokens,omitempty"`
}

type driftInvestigationQueue struct {
	config DriftInvestigationConfig
	state  DriftInvestigationState
}

func newDriftInvestigationQueue(config DriftInvestigationConfig) *driftInvestigationQueue {
	if config.BatchWindow <= 0 {
		config.BatchWindow = DefaultDriftInvestigationWindow
	}
	return &driftInvestigationQueue{config: config}
}

func driftEventKey(event sdk.DriftEvent) string {
	paths := make([]string, 0, len(event.Fields))
	for _, field := range event.Fields {
		paths = append(paths, field.Path)
	}
	sort.Strings(paths)
	return strings.Join([]string{event.Ref.APIVersion, event.Ref.Kind, event.Ref.Namespace, event.Ref.Name, strings.Join(paths, "\x00")}, "\x00")
}

func (queue *driftInvestigationQueue) Add(event sdk.DriftEvent) {
	key := driftEventKey(event)
	if queue.state.RequestJSON != "" {
		var task contracts.DriftInvestigationTask
		if protojson.Unmarshal([]byte(queue.state.RequestJSON), &task) == nil {
			for _, old := range queue.state.Pending[:min(len(queue.state.Pending), len(task.GetEvents()))] {
				if driftEventKey(old) == key && sameDriftObservation(old, event) {
					return
				}
			}
		}
		for index, old := range queue.state.Deferred {
			if driftEventKey(old) == key {
				queue.state.Deferred[index] = event
				queue.state.Deferred[index].ObservedAt = old.ObservedAt
				return
			}
		}
		queue.state.Deferred = append(queue.state.Deferred, event)
		return
	}
	for index, old := range queue.state.Pending {
		if driftEventKey(old) == key {
			queue.state.Pending[index] = event
			queue.state.Pending[index].ObservedAt = old.ObservedAt
			return
		}
	}
	queue.state.Pending = append(queue.state.Pending, event)
	queue.prioritizeNew()
}

func sameDriftObservation(left, right sdk.DriftEvent) bool {
	if left.Manager != right.Manager || len(left.Fields) != len(right.Fields) {
		return false
	}
	fields := make(map[string]sdk.DriftField, len(left.Fields))
	for _, field := range left.Fields {
		fields[field.Path] = field
	}
	for _, field := range right.Fields {
		previous, found := fields[field.Path]
		if !found || previous.New != field.New || previous.Source != field.Source {
			return false
		}
	}
	return true
}

func (queue *driftInvestigationQueue) isNew(event sdk.DriftEvent) bool {
	_, investigated := queue.state.Verdicts[driftEventKey(event)]
	return !investigated
}

func (queue *driftInvestigationQueue) prioritizeNew() {
	sort.SliceStable(queue.state.Pending, func(i, j int) bool {
		return queue.isNew(queue.state.Pending[i]) && !queue.isNew(queue.state.Pending[j])
	})
}

func (queue *driftInvestigationQueue) Pending() int {
	return len(queue.state.Pending) + len(queue.state.Deferred)
}
func (queue *driftInvestigationQueue) LastRequestJSON() string              { return queue.state.RequestJSON }
func (queue *driftInvestigationQueue) ExportState() DriftInvestigationState { return queue.state }
func (queue *driftInvestigationQueue) RestoreState(state DriftInvestigationState) {
	queue.state = state
	if state.RequestJSON == "" {
		queue.prioritizeNew()
	}
}

func (queue *driftInvestigationQueue) Deadline() time.Time {
	if len(queue.state.Pending) == 0 {
		return time.Time{}
	}
	deadline := queue.state.Pending[0].ObservedAt.Add(queue.config.BatchWindow)
	if queue.state.RequestJSON == "" && !queue.isNew(queue.state.Pending[0]) && queue.config.DispatchesPerHour > 0 && len(queue.state.DispatchAt) >= queue.config.DispatchesPerHour {
		allowed := queue.state.DispatchAt[0].Add(time.Hour)
		if allowed.After(deadline) {
			deadline = allowed
		}
	}
	if queue.state.RequestJSON == "" && !queue.isNew(queue.state.Pending[0]) && queue.config.TokensPerHour > 0 {
		var used int64
		for _, item := range queue.state.TokenUsage {
			used += item.Tokens
		}
		if used >= queue.config.TokensPerHour && len(queue.state.TokenUsage) > 0 {
			allowed := queue.state.TokenUsage[0].At.Add(time.Hour)
			if allowed.After(deadline) {
				deadline = allowed
			}
		}
	}
	return deadline
}

func (queue *driftInvestigationQueue) DispatchReady(ctx context.Context, now time.Time) error {
	task, err := queue.PrepareReady(ctx, now)
	if err != nil || task == nil {
		return err
	}
	result, err := queue.config.Worker.Investigate(ctx, task)
	return queue.Complete(now, task, result, err)
}

func (queue *driftInvestigationQueue) PrepareReady(ctx context.Context, now time.Time) (*contracts.DriftInvestigationTask, error) {
	if queue.config.Worker == nil || len(queue.state.Pending) == 0 {
		return nil, nil
	}
	if now.Before(queue.Deadline()) {
		queue.recordDeferral(now)
		return nil, nil
	}
	cutoff := now.Add(-time.Hour)
	active := queue.state.DispatchAt[:0]
	for _, at := range queue.state.DispatchAt {
		if at.After(cutoff) {
			active = append(active, at)
		}
	}
	queue.state.DispatchAt = active
	usage := queue.state.TokenUsage[:0]
	var tokens int64
	for _, item := range queue.state.TokenUsage {
		if item.At.After(cutoff) {
			usage = append(usage, item)
			tokens += item.Tokens
		}
	}
	queue.state.TokenUsage = usage
	if queue.state.RequestJSON == "" && !queue.isNew(queue.state.Pending[0]) && (queue.config.DispatchesPerHour > 0 && len(active) >= queue.config.DispatchesPerHour ||
		queue.config.TokensPerHour > 0 && tokens >= queue.config.TokensPerHour) {
		queue.recordDeferral(now)
		return nil, nil
	}
	queue.state.Deferral = nil
	var task contracts.DriftInvestigationTask
	if queue.state.RequestJSON != "" {
		if err := protojson.Unmarshal([]byte(queue.state.RequestJSON), &task); err != nil {
			return nil, fmt.Errorf("restore canonical drift investigation: %w", err)
		}
	} else {
		batchID := fmt.Sprintf("drift-%x", sha256.Sum256([]byte(driftEventKey(queue.state.Pending[0])+queue.state.Pending[0].ObservedAt.Format(time.RFC3339Nano))))[:30]
		batch := queue.state.Pending[:min(len(queue.state.Pending), maxDriftBatchEvents)]
		if queue.isNew(batch[0]) && queue.overBudget(active, tokens) {
			for index, event := range batch {
				if !queue.isNew(event) {
					batch = batch[:index]
					break
				}
			}
		}
		if queue.config.BuildTask != nil {
			built, err := queue.config.BuildTask(ctx, batchID, batch)
			if err != nil {
				return nil, err
			}
			if built == nil || built.GetBatchId() != batchID || len(built.GetEvents()) != len(batch) {
				return nil, fmt.Errorf("drift task builder returned a mismatched batch")
			}
			task = *built
		} else {
			task = contracts.DriftInvestigationTask{SchemaVersion: ProtocolSchemaVersion, BatchId: batchID, Events: driftEventsContract(batch)}
		}
		wire, err := canonicalProtoJSON(&task)
		if err != nil {
			return nil, err
		}
		queue.state.RequestJSON = string(wire)
		queue.state.DispatchAt = append(queue.state.DispatchAt, now)
	}
	if queue.config.OnPrepared != nil {
		if err := queue.config.OnPrepared(ctx); err != nil {
			return nil, err
		}
	}
	return &task, nil
}

func (queue *driftInvestigationQueue) Complete(now time.Time, task *contracts.DriftInvestigationTask, result *contracts.DriftInvestigationResult, err error) error {
	if err != nil {
		return err
	}
	if task == nil || len(task.GetEvents()) == 0 || len(task.GetEvents()) > len(queue.state.Pending) {
		return fmt.Errorf("drift investigation result does not match pending batch")
	}
	if result == nil {
		return fmt.Errorf("drift investigation returned no result")
	}
	queue.state.TokenUsage = append(queue.state.TokenUsage, DriftTokenUse{
		At: now, Tokens: result.GetInputTokens() + result.GetOutputTokens(),
		InputTokens: result.GetInputTokens(), OutputTokens: result.GetOutputTokens(),
	})
	byIndex := make(map[int32]*contracts.DriftEventInvestigation, len(result.GetEvents()))
	for _, verdict := range result.GetEvents() {
		if verdict != nil && verdict.GetEventIndex() >= 0 && int(verdict.GetEventIndex()) < len(task.GetEvents()) {
			byIndex[verdict.GetEventIndex()] = validateDriftVerdict(verdict)
		}
	}
	for index, event := range queue.state.Pending[:len(task.GetEvents())] {
		event.InvestigationCaptureID = task.GetCaptureId()
		verdict := byIndex[int32(index)]
		if verdict == nil {
			verdict = &contracts.DriftEventInvestigation{EventIndex: int32(index), Verdict: "unsure", Reason: "investigator omitted event"}
		}
		if queue.state.Verdicts == nil {
			queue.state.Verdicts = make(map[string]sdk.DriftEvent)
		}
		event.InvestigationStatus = "completed"
		event.InvestigationVerdict = verdict.GetVerdict()
		event.InvestigationReason = safeDriftText(verdict.GetReason())
		queue.state.Verdicts[driftEventKey(event)] = event
		if queue.config.OnVerdict != nil {
			queue.config.OnVerdict(event, verdict)
		}
	}
	queue.state.Pending = append(queue.state.Pending[len(task.GetEvents()):], queue.state.Deferred...)
	queue.state.Deferred = nil
	queue.state.RequestJSON = ""
	queue.prioritizeNew()
	return nil
}

func (queue *driftInvestigationQueue) overBudget(dispatches []time.Time, tokens int64) bool {
	return queue.config.DispatchesPerHour > 0 && len(dispatches) >= queue.config.DispatchesPerHour ||
		queue.config.TokensPerHour > 0 && tokens >= queue.config.TokensPerHour
}

func (queue *driftInvestigationQueue) recordDeferral(now time.Time) {
	if queue.state.RequestJSON != "" || len(queue.state.Pending) == 0 || queue.isNew(queue.state.Pending[0]) {
		return
	}
	var deferral DriftDeferral
	if queue.config.DispatchesPerHour > 0 && len(queue.state.DispatchAt) >= queue.config.DispatchesPerHour {
		deferral = DriftDeferral{Reason: DriftDispatchBudget, Until: queue.state.DispatchAt[0].Add(time.Hour)}
	}
	var tokens int64
	for _, item := range queue.state.TokenUsage {
		tokens += item.Tokens
	}
	if queue.config.TokensPerHour > 0 && tokens >= queue.config.TokensPerHour && len(queue.state.TokenUsage) > 0 {
		until := queue.state.TokenUsage[0].At.Add(time.Hour)
		if until.After(deferral.Until) {
			deferral = DriftDeferral{Reason: DriftTokenBudget, Until: until}
		}
	}
	if deferral.Reason == "" || !deferral.Until.After(now) || reflect.DeepEqual(queue.state.Deferral, &deferral) {
		return
	}
	queue.state.Deferral = &deferral
	if queue.config.OnDeferred != nil {
		queue.config.OnDeferred(deferral)
	}
}

func validateDriftVerdict(verdict *contracts.DriftEventInvestigation) *contracts.DriftEventInvestigation {
	if verdict == nil {
		return &contracts.DriftEventInvestigation{Verdict: "unsure", Reason: "missing verdict"}
	}
	copy := *verdict
	copy.Reason = safeDriftText(copy.Reason)
	copy.MitigationRequest = safeDriftText(copy.MitigationRequest)
	copy.Evidence = make([]*contracts.DriftEvidence, 0, min(len(verdict.GetEvidence()), maxDriftEvidence))
	for _, item := range verdict.GetEvidence()[:min(len(verdict.GetEvidence()), maxDriftEvidence)] {
		if item != nil && (item.GetKind() == "source_contradiction" || item.GetKind() == "violated_relation" || item.GetKind() == "observed_impact") {
			copy.Evidence = append(copy.Evidence, &contracts.DriftEvidence{Kind: item.GetKind(), Observation: safeDriftText(item.GetObservation())})
		}
	}
	if copy.Verdict != "bug" && copy.Verdict != "benign" && copy.Verdict != "unsure" {
		copy.Verdict = "unsure"
		copy.Reason = "invalid investigator verdict"
	}
	source, relation := false, false
	for _, item := range copy.Evidence {
		if item == nil || strings.TrimSpace(item.GetObservation()) == "" {
			continue
		}
		source = source || item.GetKind() == "source_contradiction"
		relation = relation || item.GetKind() == "violated_relation" || item.GetKind() == "observed_impact"
	}
	if copy.Verdict == "bug" && (!source || !relation) {
		copy.Verdict = "unsure"
		copy.Reason = "bug verdict lacked source contradiction and violated relation or impact evidence"
		copy.MitigationRequest = ""
	}
	return &copy
}
