package runtime

import (
	"context"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/traffic"
)

// automaticRecoveryLocked is the controller's only automatic-stage dispatch
// decision. The completed stage report and dispatch state are saved together
// before ordinary dispatch preparation publishes a worker task.
func (c *Controller) automaticRecoveryLocked(now time.Time, report *contracts.StageReport, stabilityOnly bool, check recoveryCheck, rolloutBlockers []sdk.Finding) {
	// A pass can start while recovery observations run with c.mu released.
	// Let it publish before selecting and freezing a handoff.
	if c.midStage.running || c.pendingLearnedPassReadyLocked() {
		return
	}
	observedAt := laterObservationTime(now, c.clock())
	request := c.currentIncidentRequest
	applied := playbookWrote(report)
	refused := stagePlanRefused(report)
	scoped := applied || refused
	deadline := c.automaticDeadlineLocked(report)
	if applied && report.RecoveryDeadline == nil {
		report.RecoveryDeadline = contractTimestamp(deadline)
	}
	agentFollows := applied && report.StabilityDeadline == nil && agentMayFollowPlaybook(c.config.AfterPlaybook, request.PriorStages)
	gaveUp := agentFollows && c.stageGaveUp && c.stageUnsettled
	if agentFollows && c.stageUnsettled && !request.ResponseDeadline.IsZero() {
		deadline = request.ResponseDeadline // Kubernetes owns the rollout until it gives up
	} else if agentFollows && !c.stageSettledAt.IsZero() {
		deadline = deadline.Add(c.stageRollout) // the estimate counts from the settle
		for _, bound := range []time.Time{c.stageSettledAt.Add(stageSettledBound), request.ResponseDeadline} {
			if !bound.IsZero() && bound.Before(deadline) {
				deadline = bound
			}
		}
	}
	// A failed script has nothing to wait for: the agent takes over now. Only
	// its responder can verify a page, so a page never waits either.
	failed := report.StabilityDeadline == nil && stageScriptsFailed(report) && agentMayFollowPlaybook(c.config.AfterPlaybook, request.PriorStages)
	page := pageOpened(request)
	canDispatch := c.codePath != nil && slices.Contains(c.codePath.Selection.ResponseStages, "agent") &&
		(request.ResponseDeadline.IsZero() || observedAt.Before(request.ResponseDeadline))
	// Nor do the writes have anything left to do for an alert they did not clear.
	// Nor for a cause its settled writes provably did not address, nor for a
	// joined alert none of its writes or plans reaches.
	beyond := canDispatch && report.StabilityDeadline == nil && agentMayFollowPlaybook(c.config.AfterPlaybook, request.PriorStages) &&
		(c.alertBeyondStageLocked(now, *request, check.beyondStage) || len(check.uncoveredCauses) > 0 || len(check.untouchedJoiners) > 0)
	// A stage that bound no learned plan wrote only SDO's built-in restore:
	// there is no learned repair to wait for, so the agent starts as its apply
	// returns, beside the restore's rollout.
	builtinOnly := canDispatch && applied && report.StabilityDeadline == nil && !learnedPlanBound(report) && agentMayFollowPlaybook(c.config.AfterPlaybook, request.PriorStages)
	if !failed && !gaveUp && !beyond && !builtinOnly && !page && (applied || refused && report.StabilityDeadline != nil) && observedAt.Before(deadline) && !stageEscalatedIn(request.PriorStages) {
		return
	}
	if stabilityOnly && scoped {
		if report.StabilityDeadline == nil {
			interval := time.Second
			for _, id := range c.recoveryDetectorIDs(*request) {
				interval = max(interval, c.detectorSpecs[id].Interval)
			}
			report.StabilityDeadline = contractTimestamp(c.closureStableSince.Add(c.config.ClosureStabilityWindow + interval))
		}
		return
	}

	// A fresh scoped clear can remove an alert from the handoff. Missing or
	// unconfirmed health cannot establish that users recovered.
	keys := alertKeys(request)
	if scoped {
		keys = c.unresolvedAlertKeysLocked(now, *request)
	} else if !stageEscalatedIn(request.PriorStages) {
		keys = findingKeys(request.Findings)
	}
	if scoped && len(keys) == 0 && report.StabilityDeadline == nil && !page {
		interval := time.Second
		for _, id := range c.recoveryDetectorIDs(*request) {
			interval = max(interval, c.detectorSpecs[id].Interval)
		}
		stabilityDeadline := observedAt.Add(c.config.ClosureStabilityWindow + interval)
		if response := request.ResponseDeadline; !response.IsZero() && response.Before(stabilityDeadline) {
			stabilityDeadline = response
		}
		report.StabilityDeadline = contractTimestamp(stabilityDeadline)
		return
	}
	if applied {
		exit := contracts.RecoveryWait_EXIT_REASON_DEADLINE
		switch {
		case page || beyond:
			exit = contracts.RecoveryWait_EXIT_REASON_UNCOVERED
		case builtinOnly:
			exit = contracts.RecoveryWait_EXIT_REASON_BUILTIN_ONLY
		}
		markAutomaticRecovery(report, observedAt, exit)
		markUnsettledRollouts(report, check.record, rolloutBlockers)
	}
	if !canDispatch || stageEscalatedIn(request.PriorStages) || !agentMayFollowPlaybook(c.config.AfterPlaybook, request.PriorStages) || scoped && len(keys) == 0 && !page {
		c.escalateLocked(observedAt)
		return
	}

	// The causes the stage did not cover go to the agent with the alerts.
	for _, key := range append(slices.Clone(check.uncoveredCauses), check.untouchedJoiners...) {
		if !slices.Contains(keys, key) {
			keys = append(keys, key)
		}
	}
	report.HandoffSelected = true
	report.HandoffKeys = slices.Clone(keys)
	if applied {
		report.HandoffState = buildHandoffState(*request, report)
	}
	c.responderDone = false
	c.responderCompletedAt = time.Time{}
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
	c.resetDispatchRetryLocked()
}

// handOffUnappliedStageLocked dispatches the agent as soon as a stage that
// wrote nothing, or bound no learned plan, ends. Its decision reads the frozen
// request alone, and once the pipeline ends only evidence already queued can
// start another automatic pass, so a further observation could change neither.
func (c *Controller) handOffUnappliedStageLocked(now time.Time) {
	report := c.awaitingAutomaticRecoveryLocked()
	if report == nil || c.config.ExternalIncidentsOnly || report.GetStabilityDeadline() != nil ||
		learnedPlanBound(report) && (playbookWrote(report) || stagePlanRefused(report)) {
		return
	}
	c.automaticRecoveryLocked(now, report, false, recoveryCheck{}, nil)
}

// learnedPlanBound reports that the stage bound a learned playbook's plan: a
// playbook other than SDO's built-in restore whose diagnosis matched and that
// the stage applied, refused or escalated. Only then is there a learned
// repair whose recovery the agent waits for (stage [deterministic, agent]).
func learnedPlanBound(report *contracts.StageReport) bool {
	return slices.ContainsFunc(report.GetMitigations(), func(attempt *contracts.StageMitigation) bool {
		return attempt.GetPlaybookId() != SourceRestoreID && attempt.GetResult() != "skipped"
	})
}

// stageSettledBound is how long settled writes wait for alerts that have no
// failing evidence, such as a user test that runs every few minutes.
const stageSettledBound = 10 * time.Minute

// workloadRollouts is each workload's ("Kind/name") rollout: whether it is
// complete, and whether Kubernetes' progress deadline gave up on it.
func workloadRollouts(view sdk.DetectionContext) (complete, gaveUp map[string]bool) {
	complete, gaveUp = map[string]bool{}, map[string]bool{}
	for _, d := range view.Deployments() {
		complete["Deployment/"+d.Name] = deployment.DeploymentRolloutComplete(&d)
		gaveUp["Deployment/"+d.Name] = slices.ContainsFunc(d.Status.Conditions, func(c appsv1.DeploymentCondition) bool {
			return c.Reason == "ProgressDeadlineExceeded" && c.Status == corev1.ConditionFalse && d.Status.ObservedGeneration >= d.Generation
		})
	}
	for _, s := range view.StatefulSets() {
		complete["StatefulSet/"+s.Name] = deployment.StatefulSetRolloutComplete(&s)
	}
	for _, d := range view.DaemonSets() {
		complete["DaemonSet/"+d.Name] = deployment.DaemonSetRolloutComplete(&d)
	}
	return complete, gaveUp
}

// observeStageRollouts reports whether every workload whose pods the stage's
// writes replace is rolled out and Ready, when its last pod became Ready, and
// whether Kubernetes gave up on one that is not: its progress deadline, or a
// condition past its self-healing with no pod still starting.
func observeStageRollouts(view sdk.DetectionContext, report *contracts.StageReport, conditions []sdk.Finding) (ready bool, lastReady time.Time, gaveUp bool) {
	complete, exceeded := workloadRollouts(view)
	starting, pods, written := map[string]bool{}, map[string]corev1.Pod{}, map[string]bool{}
	readyAt := map[string]time.Time{}
	for _, pod := range view.Pods() {
		workload := sdk.PodWorkload(view, pod)
		pods[pod.Name] = pod
		if pod.Namespace != view.Namespace() || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		at, ok := podReadySince(pod)
		complete[workload] = complete[workload] && ok
		starting[workload] = starting[workload] || sdk.PodStarting(view, pod)
		readyAt[workload] = laterObservationTime(readyAt[workload], at)
	}
	for _, attempt := range report.GetMitigations() {
		receipt := attempt.GetApplyReceipt()
		for _, ref := range attempt.GetTouchedObjects() {
			workload := ref.GetKind() + "/" + ref.GetName()
			if pod, exists := pods[ref.GetName()]; ref.GetKind() == "Pod" && exists {
				if pod.DeletionTimestamp == nil && pod.CreationTimestamp.Time.Before(receipt.GetStartedAt().AsTime()) {
					continue // written into, not replaced
				}
				workload = sdk.PodWorkload(view, pod)
			} else if ref.GetKind() == "Pod" {
				workload = ""
				for candidate := range complete {
					_, name, _ := strings.Cut(candidate, "/")
					if _, best, _ := strings.Cut(workload, "/"); strings.HasPrefix(ref.GetName(), name+"-") && len(name) > len(best) {
						workload = candidate
					}
				}
			}
			if _, known := complete[workload]; known && stageReceiptWrote(receipt) {
				written[workload] = true
			}
		}
	}
	// A condition from before the write stands until its pods had time to start.
	held := !sdk.ObservationTime(view).Before(lastStageReceipt(report).Add(sdk.SelfHealHold(view)))
	ready = true
	for workload := range written {
		lastReady = laterObservationTime(lastReady, readyAt[workload])
		if !complete[workload] {
			ready = false
			gaveUp = gaveUp || exceeded[workload] || held && !starting[workload] && slices.ContainsFunc(conditions, func(f sdk.Finding) bool {
				return f.PrimaryResource.Kind+"/"+f.PrimaryResource.Name == workload
			})
		}
	}
	return ready, lastReady, gaveUp
}

func podReadySince(pod corev1.Pod) (time.Time, bool) {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.LastTransitionTime.Time, condition.Status == corev1.ConditionTrue && pod.DeletionTimestamp == nil
		}
	}
	return time.Time{}, false
}

// observeStageSettled keeps when the stage's writes settled: the later of the
// last write and the last time one of its pods became Ready, once that held
// for a stability window. From then on only a new write changes it, so a late
// readiness blip neither ends nor restarts the wait.
func (c *Controller) observeStageSettled(now time.Time, snapshot sdk.DetectionContext, request IncidentRequest, check recoveryCheck) {
	report := deterministicStageReport(request.PriorStages)
	if !playbookWrote(report) || !check.observed {
		return
	}
	written := check.record.GetSince().AsTime()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stageSettledAt.IsZero() && !written.After(c.stageSettledAt) {
		return
	}
	var conditions []sdk.Finding
	for _, state := range c.tracker.Snapshot() {
		if state.Active && slices.Contains(memory.KubernetesConditionDetectorIDs(), state.DetectorID) {
			conditions = append(conditions, state.Finding)
		}
	}
	ready, lastReady, gaveUp := observeStageRollouts(snapshot, report, conditions)
	c.stageSettledAt, c.stageGaveUp = time.Time{}, gaveUp
	unsettled := !ready || !check.record.GetWritesSettled()
	settled := laterObservationTime(written, lastReady)
	c.stageWritesReadyAt = time.Time{}
	if !unsettled {
		c.stageWritesReadyAt = settled
	}
	c.stageUnsettled = c.stageUnsettled || unsettled
	if !unsettled && !now.Before(settled.Add(c.config.ClosureStabilityWindow)) {
		c.stageSettledAt, c.stageRollout, c.stageUnsettled = settled, settled.Sub(written), false
	}
}

// userTestsStillFailing are the alert set's user tests that violate their SLO
// over a full evaluation window of their own made only of samples after settled
// and a stability window. A test without enough such samples, or whose newest
// sample passed, is not among them: no new evidence is not failing evidence.
func userTestsStillFailing(view sdk.DetectionContext, request IncidentRequest, settled time.Time, stability time.Duration) []string {
	set, _ := view.(traffic.WindowSet)
	if set == nil || settled.IsZero() {
		return nil
	}
	windows, since := set.TrafficWindows(), settled.Add(stability)
	var failing []string
	for _, key := range alertKeys(&request) {
		workload, scenario, ok := openingUserTest(request, key)
		window := windows[workload]
		if ok && !window.ObservedAt.Before(since.Add(window.Workload.ScenarioHorizon(scenario))) && memberFailing(window, scenario) {
			failing = append(failing, key)
		}
	}
	return failing
}

func memberFailing(window traffic.Window, scenario string) bool {
	if window.LoadError != "" {
		return false
	}
	for _, observed := range window.Scenarios {
		if observed.Scenario.ID != scenario || !observed.Qualified {
			continue
		}
		var usable []traffic.Sample
		for _, sample := range observed.Samples {
			if sample.Outcome != traffic.OutcomeInvalid {
				usable = append(usable, sample)
			}
		}
		verdict := traffic.Evaluate(scenario, window.Workload.ScenarioSLO(scenario), usable, window.ObservedAt)
		return !verdict.Healthy && usable[len(usable)-1].Outcome != traffic.OutcomeOK
	}
	return false
}

// stageWrites are the map components the stage's applied writes reached and
// the finding fingerprints its applied programs targeted. Nothing is written
// when a script wrote (its writes are not all known) or a write reached no
// component the map places.
func stageWrites(view sdk.DetectionContext, pm program.AppMap, report *contracts.StageReport) (written, targeted map[string]bool) {
	written, targeted = map[string]bool{}, map[string]bool{}
	for _, attempt := range report.GetMitigations() {
		receipt := attempt.GetApplyReceipt()
		if !stageReceiptWrote(receipt) {
			continue
		}
		// A script's writes are not all known, nor is the reach of a write to
		// an object the map does not place.
		if len(attempt.GetTouchedObjects()) == 0 || slices.ContainsFunc(receipt.GetPlan().GetActions(), func(a *contracts.Action) bool { return a.GetRunScript() != nil }) {
			return nil, nil
		}
		for _, ref := range attempt.GetTouchedObjects() {
			components := writtenComponents(view, pm, objectRefState(ref))
			if len(components) == 0 {
				return nil, nil
			}
			for _, name := range components {
				written[name] = true
			}
		}
		for _, fp := range append(slices.Clone(attempt.GetClearedFindingFingerprints()), attempt.GetRemainingFindingFingerprints()...) {
			targeted[fp] = true
		}
	}
	return written, targeted
}

// writtenComponents are the map components a write to ref reaches: the
// component whose workload controls it, the components a Service fronts, or
// the components whose pod template consumes a ConfigMap, Secret, claim or
// service account. A ConfigMap or Secret also reaches every pod that may read
// it through the Kubernetes API, one that runs as its own service account
// with a token (the default account reads nothing unless bound). None when
// the map places it nowhere, or when a consumer is a workload the map does
// not place: the write's reach is then unknown.
func writtenComponents(view sdk.DetectionContext, pm program.AppMap, ref sdk.ObjectRef) []string {
	if component, ok := pm.ComponentOf(view, ref); ok {
		return []string{component.Name}
	}
	if ref.Namespace != "" && view.Namespace() != "" && ref.Namespace != view.Namespace() {
		return nil
	}
	var names []string
	if ref.Kind == "Service" {
		for _, component := range pm.Components {
			if slices.Contains(component.Services, ref.Name) {
				names = append(names, component.Name)
			}
		}
		return names
	}
	templates := map[string]corev1.PodTemplateSpec{}
	for _, d := range view.Deployments() {
		templates["Deployment/"+d.Name] = d.Spec.Template
	}
	for _, s := range view.StatefulSets() {
		templates["StatefulSet/"+s.Name] = s.Spec.Template
	}
	for _, d := range view.DaemonSets() {
		templates["DaemonSet/"+d.Name] = d.Spec.Template
	}
	for _, j := range view.Jobs() {
		templates["Job/"+j.Name] = j.Spec.Template
	}
	for _, j := range view.CronJobs() {
		templates["CronJob/"+j.Name] = j.Spec.JobTemplate.Spec.Template
	}
	readable := ref.Kind == "ConfigMap" || ref.Kind == "Secret"
	for workload, template := range templates {
		consumes := slices.ContainsFunc(sdk.PodTemplateReferences(template), func(r sdk.PodTemplateReference) bool {
			return r.Kind == ref.Kind && r.Name == ref.Name
		})
		if !consumes && !(readable && mayReadAPI(view, template.Spec)) {
			continue
		}
		index := slices.IndexFunc(pm.Components, func(component program.AppComponent) bool { return component.Workload == workload })
		if index < 0 {
			return nil
		}
		names = append(names, pm.Components[index].Name)
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// mayReadAPI reports a pod that runs as its own service account with a token
// mounted, so it may read objects through the Kubernetes API.
func mayReadAPI(view sdk.DetectionContext, spec corev1.PodSpec) bool {
	account := spec.ServiceAccountName
	if account == "" || account == "default" {
		return false
	}
	if spec.AutomountServiceAccountToken != nil {
		return *spec.AutomountServiceAccountToken
	}
	for _, sa := range view.ServiceAccounts() {
		if sa.Name == account && sa.AutomountServiceAccountToken != nil {
			return *sa.AutomountServiceAccountToken
		}
	}
	return true
}

// alertsBeyondStage are the alert set's alerts that the settled writes of the
// automatic stage cannot clear: no applied program targeted the alert, and no
// component it reports on depends, over the accepted map, on a component the
// stage wrote. A Kubernetes condition reports its object's component. A user
// test reports the components of the operations its scenario exercises, and a
// request-error alert those of its operation (see operationComponents), so a repair that is still
// propagating to users along a written component's callers waits; a user test
// also needs its newest sample failing. When any of that cannot be
// established there is none, and the stage waits its bound.
func alertsBeyondStage(view sdk.DetectionContext, m appmap.Map, request IncidentRequest, report *contracts.StageReport) []string {
	pm := m.Program()
	written, targeted := stageWrites(view, pm, report)
	if len(written) == 0 {
		return nil
	}
	var beyond []string
	for _, finding := range newRecoveryScope(alertKeys(&request)).blocking(request.Findings) {
		if targeted[FindingFingerprint(finding)] {
			continue
		}
		components, ok := alertComponents(view, m, pm, request, finding, true)
		if ok && !slices.ContainsFunc(components, func(name string) bool { return dependsOnAny(pm, name, written) }) {
			beyond = append(beyond, findingKey(finding))
		}
	}
	return beyond
}

// alertComponents are the map components an alert reports on, or false when
// the map does not tell: a Kubernetes condition's object, a user test's
// operations (failing now when failing is set), a request-error alert's
// operation.
func alertComponents(view sdk.DetectionContext, m appmap.Map, pm program.AppMap, request IncidentRequest, finding sdk.Finding, failing bool) ([]string, bool) {
	if slices.Contains(memory.KubernetesConditionDetectorIDs(), finding.DetectorID) {
		component, ok := pm.ComponentOf(view, finding.PrimaryResource)
		return []string{component.Name}, ok
	}
	var operations []string
	if workload, scenario, ok := openingUserTest(request, findingKey(finding)); ok {
		window, observed := traffic.WindowFrom(view, workload)
		if failing && (!observed || !newestSampleFailed(window, scenario)) {
			return nil, false
		}
		for _, probe := range m.Probes {
			if probe.Scenario == scenario {
				operations = append(operations, probe.Operations...)
			}
		}
	} else if finding.DetectorID == memory.RequestMetricsDetectorID && finding.RuleID == requestmetrics.RuleErrors && finding.Metadata["undeclared_route"] != true {
		if id, _ := finding.Metadata["operation"].(string); id != "" {
			operations = append(operations, id)
		}
	}
	return operationComponents(m, pm, operations)
}

// operationComponents are the components of the operations' paths and,
// transitively, of the background work they trigger: the operations entering
// internally at a triggered job class. No call edge reaches a queue worker
// from the entry that enqueues for it, so a path alone leaves it out. False
// when there are none, or the map does not hold an operation, a trigger or a
// path component.
func operationComponents(m appmap.Map, pm program.AppMap, ids []string) ([]string, bool) {
	var queue []appmap.Operation
	for _, id := range ids {
		i := slices.IndexFunc(m.Operations, func(op appmap.Operation) bool { return op.ID == id })
		if i < 0 {
			return nil, false
		}
		queue = append(queue, m.Operations[i])
	}
	var components []string
	resolved := map[string]bool{}
	for next := 0; next < len(queue); next++ {
		op := queue[next]
		if len(op.Path) == 0 {
			return nil, false
		}
		for _, name := range op.Path {
			if _, ok := pm.Component(name); !ok {
				return nil, false
			}
			components = append(components, name)
		}
		for _, class := range op.Triggers {
			if resolved[class] {
				continue
			}
			resolved[class] = true
			known := len(queue)
			for _, job := range m.Operations {
				if m.Internal(job) && job.RouteValue() == class {
					queue = append(queue, job)
				}
			}
			if len(queue) == known {
				return nil, false
			}
		}
	}
	return components, len(components) > 0
}

// causesBeyondStage are the incident's active non-alert findings (store
// state, drift, a learned detector's finding) that the stage provably did not
// address, once a user test on their path failed again after its writes
// settled at settled. No applied program targeted one, and its component is
// on that test's path and neither is, calls nor is called by a component the
// stage wrote, directly or indirectly: a misconfigured caller shows up in its
// store's state, and a callee's fault in its callers' findings. A clue already
// standing when the incident opened (StandingKeys) is baseline and never
// counts; active decides which of the others do. Whether one appeared before
// or after the stage applied does not matter: a fault that lands later is as
// far from the writes as one that was there before. Each is then positive
// evidence of a cause the writes cannot clear, so waiting for a stability
// window or the test's full window gains nothing. Unknown writes, a component
// the map does not place, or no fresh failure after the settle (the alerts
// may still be clearing) give none.
func causesBeyondStage(view sdk.DetectionContext, m appmap.Map, request IncidentRequest, report *contracts.StageReport, settled time.Time, active func(key string) bool) []string {
	pm := m.Program()
	written, targeted := stageWrites(view, pm, report)
	if len(written) == 0 || settled.IsZero() {
		return nil
	}
	alerts := alertKeys(&request)
	onPath := map[string]bool{}
	for _, finding := range newRecoveryScope(alerts).blocking(request.Findings) {
		if !failedSince(view, request, findingKey(finding), settled) {
			continue
		}
		components, _ := alertComponents(view, m, pm, request, finding, false)
		for _, name := range components {
			onPath[name] = true
		}
	}
	var uncovered []string
	for _, finding := range request.Findings {
		key := findingKey(finding)
		if slices.Contains(alerts, key) || slices.Contains(uncovered, key) || slices.Contains(request.StandingKeys, key) ||
			targeted[FindingFingerprint(finding)] {
			continue
		}
		if !active(key) {
			continue
		}
		component, ok := pm.ComponentOf(view, finding.PrimaryResource)
		if ok && onPath[component.Name] && !dependsOnAny(pm, component.Name, written) && !calledByAny(pm, component.Name, written) {
			uncovered = append(uncovered, key)
		}
	}
	return uncovered
}

// failedSince reports a user test of the alert set whose newest usable
// sample failed and was taken after since.
func failedSince(view sdk.DetectionContext, request IncidentRequest, key string, since time.Time) bool {
	workload, scenario, ok := openingUserTest(request, key)
	set, isSet := view.(traffic.WindowSet)
	if !ok || !isSet {
		return false
	}
	window := set.TrafficWindows()[workload]
	if window.LoadError != "" {
		return false
	}
	for _, observed := range window.Scenarios {
		if observed.Scenario.ID != scenario || !observed.Qualified {
			continue
		}
		for i := len(observed.Samples) - 1; i >= 0; i-- {
			if sample := observed.Samples[i]; sample.Outcome != traffic.OutcomeInvalid {
				return sample.Outcome != traffic.OutcomeOK && sample.At.After(since)
			}
		}
	}
	return false
}

// uncoveredStageCauses are causesBeyondStage once the stage's writes settled
// (applied, rolled out and Ready), with no stability window: the writes
// cannot clear them, however long they are given. A clue first seen after
// AlertJoinWindow of detection is not this incident's cause.
func (c *Controller) uncoveredStageCauses(ctx context.Context, snapshot sdk.DetectionContext, request IncidentRequest) []string {
	report := deterministicStageReport(request.PriorStages)
	c.mu.Lock()
	settled := c.stageWritesReadyAt
	if !c.stageSettledAt.IsZero() {
		settled = c.stageSettledAt
	}
	c.mu.Unlock()
	if !playbookWrote(report) || settled.IsZero() {
		return nil
	}
	m, _ := acceptedVerificationMap(ctx, c.gitRead, request)
	states := c.tracker.Snapshot()
	joinBy := request.DetectedAt.Add(AlertJoinWindow)
	return causesBeyondStage(snapshot, m, request, report, settled, func(key string) bool {
		return states[key].Active && !states[key].FirstSeenAt.After(joinBy)
	})
}

// untouchedJoiners are the joined alerts (user tests that started failing
// while the stage owned the incident) that no plan of the stage targeted and
// whose components neither are nor depend on a component the stage wrote: a
// new cause the stage is not repairing. The agent starts for one at once; it
// is the agent's first start, not a re-prompt. Unknown writes, an alert the
// map does not place, or a joiner whose newest sample passed give none.
func untouchedJoiners(view sdk.DetectionContext, m appmap.Map, request IncidentRequest, report *contracts.StageReport) []string {
	if len(request.JoinedKeys) == 0 || !playbookWrote(report) {
		return nil
	}
	return slices.DeleteFunc(alertsBeyondStage(view, m, request, report), func(key string) bool {
		return !slices.Contains(request.JoinedKeys, key)
	})
}

func (c *Controller) untouchedStageJoiners(ctx context.Context, snapshot sdk.DetectionContext, request IncidentRequest) []string {
	report := deterministicStageReport(request.PriorStages)
	if len(request.JoinedKeys) == 0 || !playbookWrote(report) {
		return nil
	}
	m, _ := acceptedVerificationMap(ctx, c.gitRead, request)
	return untouchedJoiners(snapshot, m, request, report)
}

// newestSampleFailed reports a qualified scenario whose newest usable sample
// failed.
func newestSampleFailed(window traffic.Window, scenario string) bool {
	if window.LoadError != "" {
		return false
	}
	for _, observed := range window.Scenarios {
		if observed.Scenario.ID != scenario || !observed.Qualified {
			continue
		}
		for i := len(observed.Samples) - 1; i >= 0; i-- {
			if outcome := observed.Samples[i].Outcome; outcome != traffic.OutcomeInvalid {
				return outcome != traffic.OutcomeOK
			}
		}
	}
	return false
}

// dependsOnAny reports whether the component is, or calls directly or
// indirectly, one of the targets.
func dependsOnAny(m program.AppMap, component string, targets map[string]bool) bool {
	seen := map[string]bool{component: true}
	for queue := []string{component}; len(queue) > 0; queue = queue[1:] {
		if targets[queue[0]] {
			return true
		}
		for _, edge := range m.Edges {
			if edge.From == queue[0] && !seen[edge.To] {
				seen[edge.To] = true
				queue = append(queue, edge.To)
			}
		}
	}
	return false
}

// calledByAny reports whether one of the callers calls the component,
// directly or indirectly.
func calledByAny(m program.AppMap, component string, callers map[string]bool) bool {
	for caller := range callers {
		if dependsOnAny(m, caller, map[string]bool{component: true}) {
			return true
		}
	}
	return false
}

// beyondStageAlerts are the alerts the stage's settled writes did not clear:
// alertsBeyondStage once the writes had the stability window to show an effect
// the map does not tell of, such as freed capacity, and the user tests still
// failing after it.
func (c *Controller) beyondStageAlerts(ctx context.Context, now time.Time, snapshot sdk.DetectionContext, request IncidentRequest, check recoveryCheck) []string {
	report := deterministicStageReport(request.PriorStages)
	if !playbookWrote(report) ||
		now.Before(check.record.GetSince().AsTime().Add(c.config.ClosureStabilityWindow)) {
		return nil
	}
	m, _ := acceptedVerificationMap(ctx, c.gitRead, request)
	c.mu.Lock()
	settled := c.stageSettledAt
	c.mu.Unlock()
	return append(alertsBeyondStage(snapshot, m, request, report),
		userTestsStillFailing(snapshot, request, settled, c.config.ClosureStabilityWindow)...)
}

// alertBeyondStageLocked reports one of the keys firing in this
// observation's own evaluation, and continuously for a stability window: a
// transient, a missing or an older observation starts no agent.
func (c *Controller) alertBeyondStageLocked(now time.Time, request IncidentRequest, keys []string) bool {
	if len(keys) == 0 {
		return false
	}
	_, _, firing := c.sampledAlertsLocked(now, request)
	states := c.tracker.Snapshot()
	return slices.ContainsFunc(keys, func(key string) bool {
		state, seen := states[key]
		return firing[key] && seen && !now.Before(state.FirstSeenAt.Add(c.config.ClosureStabilityWindow))
	})
}

func (c *Controller) unresolvedAlertKeysLocked(now time.Time, request IncidentRequest) []string {
	clear, sampledFiring, firing := c.sampledAlertsLocked(now, request)
	var unresolved []string
	for _, key := range alertKeys(&request) {
		id, _, _ := strings.Cut(key, "\x00")
		if firing[key] || !clear[id] && !sampledFiring[id] {
			unresolved = append(unresolved, key)
		}
	}
	return unresolved
}

// sampledAlertsLocked reads this observation's evaluations: the detectors that
// are clear, those that fire, and the keys that fire.
func (c *Controller) sampledAlertsLocked(now time.Time, request IncidentRequest) (clear, sampledFiring, firing map[string]bool) {
	scope := newRecoveryScope(alertKeys(&request))
	clear, sampledFiring, firing = map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, id := range c.recoveryDetectorIDs(request) {
		if !c.recoverySamples[id].matches(now, scope) {
			continue
		}
		states, _ := c.latestEvaluations([]string{id}, now, false)
		if len(states) == 0 || !states[0].EvaluatedAt.Equal(now) {
			continue
		}
		switch states[0].Status {
		case DetectorEvaluationClear:
			clear[id] = true
		case DetectorEvaluationFiring:
			sampledFiring[id] = true
			for _, fp := range states[0].Fingerprints {
				firing[FindingStateKey(id, fp)] = true
			}
		}
	}
	return clear, sampledFiring, firing
}

func (c *Controller) automaticDeadlineLocked(report *contracts.StageReport) time.Time {
	if at := report.GetStabilityDeadline(); at.IsValid() {
		return at.AsTime()
	}
	if at := report.GetRecoveryDeadline(); at.IsValid() {
		return at.AsTime()
	}
	// Legacy completed reports did not carry the stage's internal deadline.
	// Anchor the fallback at the original receipt, without a fresh budget.
	var wait time.Duration
	if c.config.AfterPlaybook == settings.AfterPlaybookWait {
		wait = c.config.AfterPlaybookWait
	}
	bound := automaticRecoveryDeadline(*c.currentIncidentRequest, report, lastStageReceipt(report), nil, nil, wait, c.config.AfterPlaybookWait, c.config.ClosureStabilityWindow)
	if bound.IsZero() {
		return c.responderCompletedAt
	}
	return bound
}

func markAutomaticRecovery(report *contracts.StageReport, now time.Time, exit contracts.RecoveryWait_ExitReason) {
	if report.GetRecoveryWait().GetExitReason() == contracts.RecoveryWait_EXIT_REASON_DEADLINE {
		return
	}
	completed := report.GetApplyCompletedAt()
	if completed == nil {
		completed = contractTimestamp(lastStageReceipt(report))
	}
	report.RecoveryWait = &contracts.RecoveryWait{ExitReason: exit, Duration: durationpb.New(max(0, now.Sub(completed.AsTime())))}
}

func markUnsettledRollouts(report *contracts.StageReport, recovery *contracts.RecoveryVerification, blockers []sdk.Finding) {
	for _, attempt := range report.GetMitigations() {
		id := attempt.GetApplyReceipt().GetApplicationId()
		attempt.RolloutUnsettledAtDeadline = recovery == nil && attempt.GetApplyReceipt().GetRolloutPending() != nil ||
			slices.ContainsFunc(recovery.GetWrites(), func(write *contracts.WriteSettlement) bool {
				return id != "" && write.GetApplicationId() == id && !write.GetSettled()
			})
		for _, ref := range attempt.GetTouchedObjects() {
			if slices.ContainsFunc(blockers, func(f sdk.Finding) bool { return f.PrimaryResource == objectRefState(ref) }) {
				attempt.RolloutUnsettledAtDeadline = true
			}
		}
	}
}

// stageDispatchRequest narrows only the prepared task. The controller's
// request retains the full opening and joined alert descriptions for closure.
// The task's candidate block leads with the stage's report, so the first
// prompt states what SDO already wrote.
func stageDispatchRequest(request IncidentRequest) IncidentRequest {
	report := deterministicStageReport(request.PriorStages)
	if !report.GetHandoffSelected() {
		return request
	}
	if lines := stageReportLines(request.PriorStages); len(lines) > 0 {
		request.RecentChanges, _ = boundedRecentChanges(append(lines, request.RecentChanges...))
	}
	if pageOpened(&request) {
		return request
	}
	findings := make([]sdk.Finding, 0, len(report.GetHandoffKeys()))
	for _, finding := range request.Findings {
		if slices.Contains(report.GetHandoffKeys(), findingKey(finding)) {
			findings = append(findings, finding)
		}
	}
	request.Findings = findings
	return request
}

func (c *Controller) awaitingAutomaticRecoveryLocked() *contracts.StageReport {
	if !c.incidentOpen || c.currentIncidentRequest == nil || !c.responderDone || !c.escalatedAt.IsZero() || c.dispatchError != "" {
		return nil
	}
	report := deterministicStageReport(c.currentIncidentRequest.PriorStages)
	if report.GetHandoffSelected() || !stageOnlyClosure(IncidentClosure{Request: *c.currentIncidentRequest, Result: c.currentIncidentResult}) {
		return nil
	}
	return report
}

// An observation already in progress may supply the first clear sample at
// handoff. Use ordinary read budgets and the response deadline, leaving the
// automatic deadline to govern writes and the decision after observation.
func (c *Controller) automaticObservationContext(ctx context.Context, now time.Time) (context.Context, context.CancelFunc) {
	c.mu.Lock()
	report := c.awaitingAutomaticRecoveryLocked()
	if !playbookWrote(report) {
		c.mu.Unlock()
		return ctx, func() {}
	}
	response := c.currentIncidentRequest.ResponseDeadline
	c.mu.Unlock()
	if response.IsZero() {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, response.Sub(laterObservationTime(now, c.clock())))
}

func (c *Controller) loseAutomaticObservation(now time.Time) {
	c.resetEarlyStability()
	c.mu.Lock()
	defer c.mu.Unlock()
	if report := c.awaitingAutomaticRecoveryLocked(); (playbookWrote(report) || stagePlanRefused(report)) && !c.midStage.running && !c.closurePreparingLocked() {
		c.automaticRecoveryLocked(now, report, false, recoveryCheck{}, nil)
	}
}

// A controller restart restores the already-written application's journal on
// demand. The controller state is the deadline authority; the journal is used
// to verify effects and never resumes an application here.
func (c *Controller) restoreAutomaticLoop(ctx context.Context, request IncidentRequest) (*ResponderLoop, error) {
	source, ok := c.dispatcher.(interface{ DeterministicResponseStage() ResponseStage })
	if !ok {
		return nil, nil
	}
	stage, ok := source.DeterministicResponseStage().(*DeterministicResponseStage)
	if !ok || stage.Restore == nil || stage.NewLoop == nil {
		return nil, nil
	}
	loop := stage.NewLoop(request)
	if loop == nil {
		return nil, nil
	}
	if err := stage.Restore(ctx, request, loop); err != nil {
		c.mu.Lock()
		if c.responderLoop == loop {
			c.responderLoop = nil
		}
		c.mu.Unlock()
		loop.CloseWorkingPrograms()
		return nil, err
	}
	c.mu.Lock()
	if c.currentIncidentRequest != nil && c.currentIncidentRequest.IncidentID == request.IncidentID {
		c.responderLoop = loop
	}
	c.mu.Unlock()
	return loop, nil
}

// The step time keys samples, while elapsed observation time counts against the
// response budget. An injected clock may lag the supplied step time.
func laterObservationTime(step, observed time.Time) time.Time {
	if observed.After(step) {
		return observed
	}
	return step
}
