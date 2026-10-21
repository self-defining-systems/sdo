package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"log"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

const DefaultDriftSweepInterval = 60 * time.Second
const maxDriftFields = 12

type DriftClassifier struct {
	Programs program.ProgramRegistry
	Deadline time.Duration
}

type DriftControllerConfig struct {
	Events interface{ TakeObjectChanges() []ObjectChange }
	Source interface {
		AcceptedManifests(context.Context, string, string) ([]*unstructured.Unstructured, error)
	}
	Client        kubernetes.Interface
	Programs      program.ProgramRegistry
	View          func(sdk.DetectionContext) program.ReadView
	Deadline      time.Duration
	SweepInterval time.Duration
	Investigation DriftInvestigationConfig
}

func (c *Controller) ConfigureDrift(config DriftControllerConfig) {
	if config.SweepInterval <= 0 {
		config.SweepInterval = DefaultDriftSweepInterval
	}
	if config.Deadline <= 0 {
		config.Deadline = DefaultDiagnosisDeadline
	}
	c.drift = &config
	c.adoptionNeeded = config.Source != nil && config.Client != nil
	if config.Investigation.Worker != nil {
		config.Investigation.OnDeferred = func(deferral DriftDeferral) {
			log.Printf("drift investigation deferred: reason=%s until=%s", deferral.Reason, deferral.Until.Format(time.RFC3339))
		}
		config.Investigation.OnVerdict = func(event sdk.DriftEvent, verdict *contracts.DriftEventInvestigation) {
			c.recordDriftInvestigation(event, verdict)
		}
		config.Investigation.OnPrepared = c.PersistState
		c.driftInvestigation = newDriftInvestigationQueue(config.Investigation)
		c.driftResults = make(chan driftInvestigationCompletion, 1)
	}
	c.nextDriftSweep = c.startedAt.Add(config.SweepInterval)
}

func (c *Controller) driftInvestigationState() DriftInvestigationState {
	if c.driftInvestigation == nil {
		return DriftInvestigationState{}
	}
	return c.driftInvestigation.ExportState()
}

func (c *Controller) dispatchDriftInvestigation(ctx context.Context, now time.Time) error {
	if c.driftInvestigation == nil {
		return nil
	}
	if c.driftRunning || now.Before(c.driftRetryAt) {
		return nil
	}
	if c.driftInvestigation.config.Async {
		task, err := c.driftInvestigation.PrepareReady(ctx, now)
		if err != nil {
			c.driftRetryAt = now.Add(2 * time.Second)
			return err
		}
		if task == nil {
			return err
		}
		c.driftRunning = true
		go func() {
			result, err := c.driftInvestigation.config.Worker.Investigate(ctx, task)
			select {
			case c.driftResults <- driftInvestigationCompletion{task: task, result: result, err: err}:
			case <-ctx.Done():
			}
		}()
		return nil
	}
	return c.driftInvestigation.DispatchReady(ctx, now)
}

type driftInvestigationCompletion struct {
	task   *contracts.DriftInvestigationTask
	result *contracts.DriftInvestigationResult
	err    error
}

func (c *Controller) processDriftCompletions(now time.Time) {
	if c.driftResults == nil {
		return
	}
	for {
		select {
		case completion := <-c.driftResults:
			c.handleDriftCompletion(completion, now)
		default:
			return
		}
	}
}

func (c *Controller) handleDriftCompletion(completion driftInvestigationCompletion, now time.Time) {
	c.driftRunning = false
	if err := c.driftInvestigation.Complete(now, completion.task, completion.result, completion.err); err != nil {
		c.driftRetryAt = now.Add(2 * time.Second)
		if c.OnError != nil {
			c.OnError(fmt.Errorf("drift investigation: %w", err))
		}
	} else {
		c.driftRetryAt = time.Time{}
	}
}

func (c *Controller) recordDriftInvestigation(event sdk.DriftEvent, verdict *contracts.DriftEventInvestigation) {
	event.InvestigationStatus = "completed"
	event.InvestigationVerdict = verdict.GetVerdict()
	event.InvestigationReason = safeDriftText(verdict.GetReason())
	if verdict.GetVerdict() == "bug" {
		event.Classification = sdk.DriftFault
	}
	for _, evidence := range verdict.GetEvidence() {
		if evidence != nil {
			event.InvestigationEvidence = append(event.InvestigationEvidence, sdk.DriftEvidence{Kind: evidence.GetKind(), Observation: safeDriftText(evidence.GetObservation())})
		}
	}
	if c.driftInvestigation != nil {
		if c.driftInvestigation.state.Verdicts == nil {
			c.driftInvestigation.state.Verdicts = make(map[string]sdk.DriftEvent)
		}
		c.driftInvestigation.state.Verdicts[driftEventKey(event)] = event
	}
	c.mu.Lock()
	found := false
	for index := range c.driftEvents {
		item := &c.driftEvents[index]
		if driftEventKey(*item) != driftEventKey(event) || !item.ObservedAt.Equal(event.ObservedAt) {
			continue
		}
		*item = event
		found = true
		break
	}
	if !found {
		c.driftEvents = append(c.driftEvents, event)
	}
	if len(c.driftEvents) > 40 {
		c.driftEvents = append([]sdk.DriftEvent(nil), c.driftEvents[len(c.driftEvents)-40:]...)
	}
	c.mu.Unlock()
	if verdict.GetVerdict() != "bug" {
		return
	}
	encoded, _ := json.Marshal(map[string]any{"drift": event, "investigation": verdict})
	finding := sdk.Finding{DetectorID: "drift/investigator", RuleID: "investigated-drift", Status: sdk.FindingActive,
		Severity: sdk.SeverityLatent, Summary: "investigated drift contradicts source and violates a relation or has impact",
		Evidence: bounded(string(encoded)), PrimaryResource: event.Ref,
		Metadata: map[string]any{"drift_event": event, "investigator_evidence": event.InvestigationEvidence}}
	finding.Fingerprint = FindingFingerprint(finding)
	c.recordEvaluation(finding.DetectorID, event.ObservedAt, []sdk.Finding{finding})
	if c.mergeIntoOpenIncident(finding) || c.attachBeforeLaunch([]sdk.Finding{finding}) {
		return
	}
	c.batcher.AddAtWithDebounce(finding, event.ObservedAt, 0)
}

func (c *Controller) clearInvestigatedDrift(now time.Time, change ObjectChange, view program.ReadView) {
	if change.New == nil {
		return
	}
	ref := sdk.ObjectRef{APIVersion: change.New.GetAPIVersion(), Kind: change.New.GetKind(), Namespace: change.New.GetNamespace(), Name: change.New.GetName()}
	c.mu.Lock()
	active := false
	if c.currentIncidentRequest != nil {
		for _, finding := range c.currentIncidentRequest.Findings {
			if finding.DetectorID == "drift/investigator" && finding.PrimaryResource == ref {
				active = true
				break
			}
		}
	}
	c.mu.Unlock()
	if !active {
		return
	}
	diff, err := view.DiffAgainstSource(ref)
	if err == nil && !diff.Changed {
		c.recordEvaluation("drift/investigator", now, nil)
	}
}

func (c *Controller) processDrift(ctx context.Context, now time.Time, snapshot sdk.DetectionContext, changes []ObjectChange, sweep bool) {
	if c.drift == nil || c.drift.View == nil {
		return
	}
	view := c.drift.View(snapshot)
	classifier := DriftClassifier{Programs: c.drift.Programs, Deadline: c.drift.Deadline}
	for _, change := range changes {
		event, runs, err := classifier.Classify(ctx, change, view, now)
		if err != nil {
			if c.OnError != nil {
				c.OnError(err)
			}
			continue
		}
		if event == nil {
			c.clearInvestigatedDrift(now, change, view)
			continue
		}
		if event.Classification == sdk.DriftUnexplained && c.driftInvestigation != nil {
			event.InvestigationStatus = "pending"
		}
		c.mu.Lock()
		unchanged := false
		if c.driftInvestigation != nil {
			previous, found := c.driftInvestigation.state.Verdicts[driftEventKey(*event)]
			unchanged = found && sameDriftObservation(previous, *event)
			if c.driftInvestigation.state.RequestJSON != "" {
				var task contracts.DriftInvestigationTask
				if protojson.Unmarshal([]byte(c.driftInvestigation.state.RequestJSON), &task) == nil {
					for _, pending := range c.driftInvestigation.state.Pending[:min(len(c.driftInvestigation.state.Pending), len(task.GetEvents()))] {
						unchanged = unchanged || driftEventKey(pending) == driftEventKey(*event) && sameDriftObservation(pending, *event)
					}
				}
			}
		}
		if c.driftInvestigation == nil {
			for _, previous := range c.driftEvents {
				if previous.InvestigationStatus == "completed" && driftEventKey(previous) == driftEventKey(*event) &&
					sameDriftObservation(previous, *event) {
					unchanged = true
					break
				}
			}
		}
		if unchanged {
			c.mu.Unlock()
			continue
		}
		replaced := false
		if event.Classification == sdk.DriftUnexplained && c.driftInvestigation != nil && c.driftInvestigation.state.RequestJSON == "" {
			for index := range c.driftEvents {
				if driftEventKey(c.driftEvents[index]) == driftEventKey(*event) && c.driftEvents[index].InvestigationStatus == "pending" {
					event.ObservedAt = c.driftEvents[index].ObservedAt
					c.driftEvents[index] = *event
					replaced = true
					break
				}
			}
		}
		if !replaced {
			c.driftEvents = append(c.driftEvents, *event)
		}
		if len(c.driftEvents) > 40 {
			c.driftEvents = append([]sdk.DriftEvent(nil), c.driftEvents[len(c.driftEvents)-40:]...)
		}
		c.mu.Unlock()
		if event.Classification == sdk.DriftUnexplained && c.driftInvestigation != nil {
			c.driftInvestigation.Add(*event)
		}
		if event.Classification == sdk.DriftFault {
			for _, run := range runs {
				if run.Verdict.Result == program.Match {
					c.queueLatentFinding(now, *event, run)
				}
			}
		}
		c.recordDriftClears(now, runs)
	}
	if sweep && c.drift.Programs != nil {
		runs := SelectProgramCandidatesWithOptions(ctx, c.drift.Programs, view, nil, CandidateOptions{Deadline: c.drift.Deadline})
		for _, run := range runs {
			if run.Verdict.Result != program.Match {
				continue
			}
			var ref sdk.ObjectRef
			for _, bound := range run.Bindings {
				ref = bound
				break
			}
			if ref.Kind == "" {
				continue
			}
			c.queueLatentFinding(now, sdk.DriftEvent{Ref: ref, Source: "periodic program sweep", ObservedAt: now.UTC(), Classification: sdk.DriftFault, ProgramID: run.ProgramID}, run)
		}
		c.recordDriftClears(now, runs)
	}
}

func (c *Controller) recordDriftClears(now time.Time, runs []ProgramRun) {
	c.mu.Lock()
	active := map[string]bool{}
	if c.currentIncidentRequest != nil {
		for _, finding := range c.currentIncidentRequest.Findings {
			if strings.HasPrefix(finding.DetectorID, "drift/") {
				active[finding.DetectorID] = true
			}
		}
	}
	c.mu.Unlock()
	seen := map[string]bool{}
	matched := map[string]bool{}
	for _, run := range runs {
		id := "drift/" + run.ProgramID
		seen[id] = true
		if run.Verdict.Result == program.Match {
			matched[id] = true
		}
	}
	for id := range seen {
		if active[id] && !matched[id] {
			c.recordEvaluation(id, now, nil)
		}
	}
}

func (c *Controller) queueLatentFinding(now time.Time, event sdk.DriftEvent, run ProgramRun) {
	verdict := boundedDriftVerdict(run.Verdict)
	encoded, _ := json.Marshal(map[string]any{"drift": event, "verdict": verdict})
	evidence := bounded(string(encoded))
	finding := sdk.Finding{DetectorID: "drift/" + run.ProgramID, RuleID: run.ProgramID, Status: sdk.FindingActive,
		Severity: sdk.SeverityLatent, Summary: "learned invariant matched after drift", Evidence: evidence,
		PrimaryResource: event.Ref, ParameterBindings: run.Bindings,
		Metadata: map[string]any{"drift_event": event, "program_verdict": verdict}}
	finding.Fingerprint = FindingFingerprint(finding)
	c.recordEvaluation(finding.DetectorID, now, []sdk.Finding{finding})
	if c.mergeIntoOpenIncident(finding) || c.attachBeforeLaunch([]sdk.Finding{finding}) {
		return
	}
	c.batcher.AddAtWithDebounce(finding, now, 0)
}

func boundedDriftVerdict(verdict program.Verdict) program.Verdict {
	verdict.Question = safeDriftText(verdict.Question)
	if len(verdict.Evidence) > 6 {
		verdict.Evidence = verdict.Evidence[:6]
	}
	for index := range verdict.Evidence {
		item := &verdict.Evidence[index]
		item.Fact = safeDriftText(item.Fact)
		item.Observed = safeDriftText(item.Observed)
		item.Expected = safeDriftText(item.Expected)
	}
	return verdict
}

func safeDriftText(value string) string {
	words := strings.Fields(value)
	for index, word := range words {
		path := strings.Trim(word, `"'(),;`)
		if filepath.IsAbs(path) {
			words[index] = "[path]"
		}
	}
	return bounded(strings.Join(words, " "))
}

// Classify returns nil when the change did not touch declared SDO fields or
// the accepted source already describes the observed object.
func (c DriftClassifier) Classify(ctx context.Context, change ObjectChange, view program.ReadView, now time.Time) (*sdk.DriftEvent, []ProgramRun, error) {
	object := change.New
	if object == nil {
		object = change.Old
	}
	if object == nil || change.Initial || view == nil {
		return nil, nil, nil
	}
	ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}
	if ref.Namespace != view.Namespace() || ref.Name == "" {
		return nil, nil, nil
	}
	owner := change.Old
	if owner == nil {
		owner = change.New
	}
	owned := ownedDriftPaths(owner, "sdo-deployer")
	if change.Adoption {
		owned = flattenDriftPaths(change.Old)
	}
	if len(owned) == 0 {
		return nil, nil, nil
	}
	before := flattenDriftObject(change.Old)
	after := flattenDriftObject(change.New)
	paths := make([]string, 0)
	for path := range before {
		if !reflect.DeepEqual(before[path], after[path]) && ownsDriftPath(owned, path) {
			paths = append(paths, path)
		}
	}
	for path := range after {
		if _, exists := before[path]; !exists && ownsDriftPath(owned, path) {
			paths = append(paths, path)
		}
	}
	if len(paths) == 0 {
		return nil, nil, nil
	}
	sort.Strings(paths)
	diff, err := view.DiffAgainstSource(ref)
	if err != nil {
		return nil, nil, fmt.Errorf("compare %s/%s with accepted source: %w", ref.Kind, ref.Name, err)
	}
	if !diff.Changed {
		return nil, nil, nil
	}
	sourceValues := map[string]any{}
	if strings.HasPrefix(diff.Text, "accepted: ") {
		line := strings.SplitN(strings.TrimPrefix(diff.Text, "accepted: "), "\nobserved: ", 2)[0]
		var sourceObject map[string]any
		if json.Unmarshal([]byte(line), &sourceObject) == nil {
			sourceValues = flattenDriftObject(&unstructured.Unstructured{Object: sourceObject})
		}
	}
	manager, operation, managerAt := changedDriftManager(change, paths)
	event := &sdk.DriftEvent{Ref: ref, Fields: make([]sdk.DriftField, 0, min(len(paths), maxDriftFields)), Source: "differs from accepted manifest", Manager: manager, Operation: operation, ManagerAt: managerAt, ObservedAt: now.UTC(), Classification: sdk.DriftUnexplained}
	for _, path := range paths[:min(len(paths), maxDriftFields)] {
		event.Fields = append(event.Fields, sdk.DriftField{Path: path, Old: renderDriftValue(path, before[path]), Source: renderDriftValue(path, sourceValues[path]), New: renderDriftValue(path, after[path])})
	}
	if change.Adoption {
		return event, nil, nil
	}
	runs := SelectProgramCandidatesWithOptions(ctx, c.Programs, view, nil, CandidateOptions{Deadline: c.Deadline, Affected: []sdk.ObjectRef{ref}})
	for _, run := range runs {
		if run.Verdict.Result == program.Match {
			event.Classification = sdk.DriftFault
			event.ProgramID = run.ProgramID
			break
		}
	}
	return event, runs, nil
}

func flattenDriftObject(object *unstructured.Unstructured) map[string]any {
	result := map[string]any{}
	if object == nil {
		return result
	}
	var walk func(string, any)
	walk = func(path string, value any) {
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				next := key
				if path != "" {
					next = path + "." + key
				}
				walk(next, child)
			}
		case []any:
			for index, child := range typed {
				walk(fmt.Sprintf("%s[%d]", path, index), child)
			}
		default:
			result[path] = typed
		}
	}
	for _, key := range []string{"spec", "data", "binaryData"} {
		if value, ok := object.Object[key]; ok {
			walk(key, value)
		}
	}
	metadata, _ := object.Object["metadata"].(map[string]any)
	for _, key := range []string{"labels", "annotations"} {
		if value, ok := metadata[key]; ok {
			walk("metadata."+key, value)
		}
	}
	return result
}

func flattenDriftPaths(object *unstructured.Unstructured) map[string]bool {
	paths := map[string]bool{}
	for path := range flattenDriftObject(object) {
		paths[path] = true
	}
	return paths
}

func renderDriftValue(path string, value any) string {
	if value == nil {
		return ""
	}
	if strings.HasPrefix(path, "data.") || strings.HasPrefix(path, "binaryData.") {
		return digestJSON(value)
	}
	if value, ok := value.(string); ok {
		return safeDriftText(value)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return bounded(string(encoded))
}

func ownedDriftPaths(object *unstructured.Unstructured, manager string) map[string]bool {
	owned := map[string]bool{}
	if object == nil {
		return owned
	}
	for _, entry := range object.GetManagedFields() {
		if entry.Manager != manager || entry.Operation != metav1.ManagedFieldsOperationApply || entry.FieldsV1 == nil {
			continue
		}
		var fields map[string]any
		if json.Unmarshal(entry.FieldsV1.Raw, &fields) == nil {
			walkOwnedFields(fields, object.Object, "", owned)
		}
	}
	return owned
}

func walkOwnedFields(fields map[string]any, object any, path string, owned map[string]bool) {
	if len(fields) == 0 && path != "" {
		owned[path] = true
		return
	}
	for key, raw := range fields {
		child, _ := raw.(map[string]any)
		switch {
		case strings.HasPrefix(key, "f:"):
			name := strings.TrimPrefix(key, "f:")
			next := name
			if path != "" {
				next = path + "." + name
			}
			current, _ := object.(map[string]any)
			walkOwnedFields(child, current[name], next, owned)
		case strings.HasPrefix(key, "k:"):
			var selector map[string]any
			if json.Unmarshal([]byte(strings.TrimPrefix(key, "k:")), &selector) != nil {
				continue
			}
			items, _ := object.([]any)
			for index, item := range items {
				candidate, _ := item.(map[string]any)
				matches := true
				for name, expected := range selector {
					if fmt.Sprint(candidate[name]) != fmt.Sprint(expected) {
						matches = false
					}
				}
				if matches {
					walkOwnedFields(child, item, fmt.Sprintf("%s[%d]", path, index), owned)
				}
			}
		}
	}
}

func ownsDriftPath(owned map[string]bool, path string) bool {
	for candidate := range owned {
		if path == candidate || strings.HasPrefix(path, candidate+".") || strings.HasPrefix(path, candidate+"[") {
			return true
		}
	}
	return false
}

func changedDriftManager(change ObjectChange, paths []string) (string, string, *time.Time) {
	if change.New == nil {
		return "unknown", "delete", nil
	}
	old := map[string]metav1.ManagedFieldsEntry{}
	if change.Old != nil {
		for _, entry := range change.Old.GetManagedFields() {
			if entry.FieldsV1 != nil {
				old[entry.Manager+"/"+string(entry.Operation)] = entry
			}
		}
	}
	for _, entry := range change.New.GetManagedFields() {
		prior := old[entry.Manager+"/"+string(entry.Operation)]
		if entry.FieldsV1 == nil || entry.Manager == "sdo-deployer" || prior.FieldsV1 != nil && string(prior.FieldsV1.Raw) == string(entry.FieldsV1.Raw) && reflect.DeepEqual(prior.Time, entry.Time) {
			continue
		}
		owned := ownedDriftPathsForEntry(change.New, entry)
		for _, path := range paths {
			if ownsDriftPath(owned, path) {
				if entry.Time != nil {
					at := entry.Time.Time.UTC()
					return entry.Manager, string(entry.Operation), &at
				}
				return entry.Manager, string(entry.Operation), nil
			}
		}
	}
	for _, entry := range change.New.GetManagedFields() {
		if entry.Manager == "sdo-deployer" && entry.Time != nil {
			at := entry.Time.Time.UTC()
			return entry.Manager, string(entry.Operation), &at
		}
	}
	return "sdo-deployer", string(metav1.ManagedFieldsOperationApply), nil
}

func ownedDriftPathsForEntry(object *unstructured.Unstructured, entry metav1.ManagedFieldsEntry) map[string]bool {
	owned := map[string]bool{}
	var fields map[string]any
	if entry.FieldsV1 != nil && json.Unmarshal(entry.FieldsV1.Raw, &fields) == nil {
		walkOwnedFields(fields, object.Object, "", owned)
	}
	return owned
}
