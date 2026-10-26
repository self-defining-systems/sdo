// Package replay checks learned programs with the production executor and an
// isolated client-go fake. Its reports are offline evidence, never live trust.
package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Case keeps accepted source separate from the expected fixed state. Revisions
// must be supplied explicitly; Fixed is never used as a source or post-state.
type Case struct {
	ID        string
	Corpus    sdktest.CorpusCase
	Accepted  []*unstructured.Unstructured
	Revisions map[string][]*unstructured.Unstructured
	// RepairOnly is the learning gate; broader matrix checks remain an offline analysis.
	RepairOnly bool
}
type DetectorResult struct {
	ID       string                   `json:"id"`
	Class    sdk.DetectorClass        `json:"class"`
	Active   int                      `json:"active"`
	Error    string                   `json:"error,omitempty"`
	Bindings []program.ObjectBindings `json:"bindings,omitempty"`
}
type Result struct {
	ReplayBasis            string                      `json:"replay_basis,omitempty"`
	ProgramID              string                      `json:"program_id"`
	CaseID                 string                      `json:"case_id"`
	Status                 string                      `json:"status"`
	BoundObjects           program.ObjectBindings      `json:"bound_objects,omitempty"`
	ExpectedMatch          bool                        `json:"expected_match"`
	Healthy                program.Verdict             `json:"healthy"`
	Faulted                program.Verdict             `json:"faulted"`
	Post                   program.Verdict             `json:"post"`
	Detectors              map[string][]DetectorResult `json:"detectors"`
	Actions                []operator.PlanActionReport `json:"actions,omitempty"`
	Effects                []string                    `json:"effects,omitempty"`
	FalsePositives         []string                    `json:"false_positives,omitempty"`
	HealthFalsePositives   []string                    `json:"health_false_positives,omitempty"`
	HealthFalseNegatives   []string                    `json:"health_false_negatives,omitempty"`
	IncidentFalsePositives []string                    `json:"incident_false_positives,omitempty"`
	IncidentFalseNegatives []string                    `json:"incident_false_negatives,omitempty"`
	Overlap                []string                    `json:"overlap,omitempty"`
	Errors                 []string                    `json:"errors,omitempty"`
	DurationMS             float64                     `json:"duration_ms"`
}

// Check executes one closed loop. Detectors are the detectors linked to this
// program; Matrix separately evaluates the whole detector catalog for each case.
func Check(ctx context.Context, p program.Diagnoser, m program.Mitigator, detectors []sdk.Detector, c Case) (r Result) {
	start := time.Now()
	defer func() {
		r.DurationMS = float64(time.Since(start)) / float64(time.Millisecond)
		if v := recover(); v != nil {
			r.Status = "failed"
			r.Errors = append(r.Errors, fmt.Sprintf("panic: %v", v))
		}
	}()
	r = Result{ProgramID: p.Spec().ID, CaseID: c.ID, Status: "failed", ExpectedMatch: slices.Contains(c.Corpus.RootCauseProgramIDs, p.Spec().ID), Detectors: map[string][]DetectorResult{}}
	fail := func(err error) Result { r.Errors = append(r.Errors, err.Error()); return r }
	if c.Corpus.Open.NamespaceName == "" || c.Corpus.Healthy.NamespaceName != c.Corpus.Open.NamespaceName {
		return fail(fmt.Errorf("case namespace is missing or inconsistent"))
	}
	bindings := bindingsFor(p.Spec(), c.Corpus.Bindings)
	r.BoundObjects = bindings.Objects
	source, err := newSource(c)
	if err != nil {
		return fail(err)
	}
	healthyBindings := bindingsFor(p.Spec(), c.Corpus.PeerBindings)
	if len(c.Corpus.PeerBindings.Objects) == 0 && len(c.Corpus.PeerBindings.Values) == 0 {
		healthyBindings = bindings
	}
	r.Healthy = program.RunDiagnose(ctx, p, newReadView(ctx, c.Corpus.Healthy, c.Corpus.PhaseRecordings("healthy"), source), healthyBindings, 5*time.Second)
	r.Faulted = program.RunDiagnose(ctx, p, newReadView(ctx, c.Corpus.Open, c.Corpus.PhaseRecordings("open"), source), bindings, 5*time.Second)
	r.Detectors["healthy"] = detect(ctx, detectors, recordedPhase(c.Corpus, "healthy"))
	r.Detectors["faulted"] = detect(ctx, detectors, recordedPhase(c.Corpus, "open"))
	for _, phase := range []string{"healthy", "faulted"} {
		for i, row := range r.Detectors[phase] {
			r.scoreDetector(phase, row, detectorFamilyMatches(detectors[i], c), c)
		}
	}
	if !c.RepairOnly && r.Healthy.Result == program.Match {
		r.incidentFP("healthy:diagnose:" + p.Spec().ID)
	}
	if !c.RepairOnly && r.Healthy.Result != program.NoMatch {
		r.Errors = append(r.Errors, "healthy diagnosis is not NoMatch")
	}
	if !r.ExpectedMatch {
		if r.Faulted.Result == program.Match {
			r.incidentFP("faulted:diagnose:" + p.Spec().ID)
		}
		if r.Faulted.Result != program.Match && r.Faulted.Result != program.NoMatch {
			r.Errors = append(r.Errors, "faulted diagnosis is not NoMatch")
		}
		if !r.hasFailures() && r.Faulted.Result == program.NoMatch {
			r.Status = "negative"
		}
		return r
	}
	if r.Faulted.Result != program.Match {
		if r.Faulted.Result == program.NoMatch {
			r.IncidentFalseNegatives = append(r.IncidentFalseNegatives, "faulted:diagnose:"+p.Spec().ID)
		}
		return fail(fmt.Errorf("faulted diagnosis is %s, want Match", r.Faulted.Result))
	}
	if len(detectors) > 0 {
		for _, row := range r.Detectors["faulted"] {
			for _, actual := range row.Bindings {
				for key, want := range bindings.Objects {
					if got, ok := actual[key]; ok && got != want {
						r.Errors = append(r.Errors, "detector binding differs from labeled object: "+key)
					}
				}
			}
		}
	}
	if r.hasFailures() {
		return r
	}
	if m == nil {
		return fail(fmt.Errorf("matching program has no mitigator"))
	}
	plan, err := m.Mitigate(ctx, newReadView(ctx, c.Corpus.Open, c.Corpus.PhaseRecordings("open"), source), bindings)
	if err != nil {
		return fail(fmt.Errorf("Mitigate: %w", err))
	}
	if err = plan.Validate(); err != nil {
		return fail(err)
	}
	if containsScript(plan) {
		return observedScriptTransition(ctx, p, c, plan, source, r)
	}
	client, err := newCluster(c.Corpus.Open)
	if err != nil {
		return fail(err)
	}
	if err = client.seedDatabases(append(append([]sdktest.PrimitiveCall(nil), c.Corpus.Open.Readings...), c.Corpus.PhaseRecordings("open")...)); err != nil {
		return fail(err)
	}
	if err = client.seedExternal(append(append([]sdktest.PrimitiveCall(nil), c.Corpus.Open.Readings...), c.Corpus.PhaseRecordings("open")...)); err != nil {
		return fail(err)
	}
	r.Effects = append(r.Effects, "simulated:apiserver-metadata")
	const commit = "offline-accepted"
	incident := operator.IncidentRequest{IncidentID: c.ID, Application: c.Corpus.Open.NamespaceName, Namespace: c.Corpus.Open.NamespaceName, SourceCommit: commit, RepairPolicy: "recorded-actions"}
	// Corpus bindings and detector findings, rather than the proposed plan, name
	// the incident resources authorized by the production source policy.
	for _, ref := range bindings.Objects {
		incident.Findings = append(incident.Findings, sdk.Finding{PrimaryResource: ref})
	}
	for _, d := range detectors {
		findings, e := sdk.DetectObserved(ctx, d, recordedPhase(c.Corpus, "open"))
		if e != nil {
			return fail(e)
		}
		for _, f := range findings {
			if f.Status == sdk.FindingActive {
				incident.Findings = append(incident.Findings, f)
			}
		}
	}
	// A synthetic case without object parameters can name its fault through the
	// open inventory; no source-present object receives delete authorization.
	if len(incident.Findings) == 0 {
		for ref := range client.initial {
			incident.Findings = append(incident.Findings, sdk.Finding{PrimaryResource: ref})
		}
	}
	observedObjects, err := snapshotObjects(c.Corpus.Open)
	if err != nil {
		return fail(err)
	}
	waiter := &reaction{cluster: client, source: source, plan: plan, effects: &r.Effects}
	verifier := &postVerifier{cluster: client, source: source, p: p, bindings: bindings, detectors: detectors, result: &r, reaction: waiter, recordings: c.Corpus.PhaseRecordings("open")}
	executor := operator.PlanExecutor{Client: client, Source: source, Rollouts: waiter, Verifier: verifier, Values: program.ValueInputs{Spec: p.Spec(), Bindings: bindings, Observed: func(ref sdk.ObjectRef) (map[string]any, error) {
		o := observedObjects[ref]
		if o == nil {
			return nil, fmt.Errorf("observed object unavailable")
		}
		return o.DeepCopy().Object, nil
	}}}
	application, err := executor.Apply(ctx, incident, plan, false)
	r.Actions = application.ActionReports
	for _, a := range application.ActionReports {
		if a.Status == "applied" {
			r.Effects = append(r.Effects, fmt.Sprintf("real:%T:action:%d", plan.Actions[a.ActionIndex], a.ActionIndex))
			switch plan.Actions[a.ActionIndex].(type) {
			case program.EnsureExec, *program.EnsureExec, program.EnsureHTTP, *program.EnsureHTTP:
				r.Effects = append(r.Effects, "simulated:recorded-external-transition")
			case program.RunJob, *program.RunJob:
				r.Effects = append(r.Effects, "simulated:recorded-job-completion")
			case program.EnsureDatabaseUser, *program.EnsureDatabaseUser:
				r.Effects = append(r.Effects, "simulated:mongodb-user-catalog")
			}
		}
	}
	if err != nil {
		r.Status = "refused"
		if application.Applied {
			r.Status = "failed"
		}
		return fail(fmt.Errorf("PlanExecutor: %w", err))
	}
	if !c.RepairOnly {
		if err = checkTargetValues(ctx, plan, application, source); err != nil {
			return fail(err)
		}
	}
	if !application.VerificationCompleted || !application.VerificationPassed {
		return fail(fmt.Errorf("post-plan diagnosis or detector remains active/unknown"))
	}
	if err = checkWrites(ctx, client, application); err != nil {
		return fail(err)
	}
	r.Status = "replayed"
	return r
}
func bindingsFor(spec program.ProgramSpec, b program.Bindings) program.Bindings {
	out := program.Bindings{Objects: program.ObjectBindings{}, Values: map[string]any{}}
	for _, p := range spec.Params {
		if v, ok := b.Objects[p.Name]; ok {
			out.Objects[p.Name] = v
		}
		if v, ok := b.Values[p.Name]; ok {
			out.Values[p.Name] = v
		}
	}
	return out
}
func detect(ctx context.Context, ds []sdk.Detector, s sdk.DetectionContext) []DetectorResult {
	out := []DetectorResult{}
	for _, d := range ds {
		r := DetectorResult{ID: d.Spec().ID, Class: d.Spec().Class}
		fs, e := sdk.DetectObserved(ctx, d, s)
		if e != nil {
			r.Error = e.Error()
		}
		for _, f := range fs {
			if f.Status == sdk.FindingActive {
				r.Active++
			}
		}
		out = append(out, r)
	}
	return out
}
func Matrix(ctx context.Context, registry program.ProgramRegistry, detectors []sdk.Detector, cases []Case) []Result {
	out := []Result{}
	for _, c := range cases {
		for _, detector := range detectors {
			out = append(out, checkDetector(ctx, detector, c))
		}
		for _, spec := range registry.List() {
			p, _ := registry.Diagnoser(spec.ID)
			m, _ := registry.Mitigator(spec.ID)
			var linked []sdk.Detector
			for _, d := range detectors {
				if slices.Contains(d.Spec().Playbooks, spec.ID) {
					linked = append(linked, d)
				}
			}
			out = append(out, Check(ctx, p, m, linked, c))
		}
	}
	return out
}

type postVerifier struct {
	cluster    *cluster
	source     *source
	p          program.Diagnoser
	bindings   program.Bindings
	detectors  []sdk.Detector
	result     *Result
	reaction   *reaction
	recordings []sdktest.PrimitiveCall
}

func (v *postVerifier) Verify(ctx context.Context) (bool, error) {
	if err := v.reaction.finish(ctx); err != nil {
		return false, err
	}
	s, err := v.cluster.snapshot(ctx)
	if err != nil {
		return false, err
	}
	view := newReadView(ctx, s, unchangedPodProbes(v.cluster.initial, s, v.recordings), v.source)
	view.catalog = v.cluster
	view.external = v.cluster
	v.result.Post = program.RunDiagnose(ctx, v.p, view, v.bindings, 5*time.Second)
	v.result.Detectors["post"] = detect(ctx, v.detectors, view)
	clear := v.result.Post.Result == program.NoMatch
	for _, d := range v.result.Detectors["post"] {
		clear = clear && d.Active == 0 && d.Error == ""
	}
	return clear, nil
}

// checkWrites proves the fake state equals the production executor's prepared
// after-images and every other object is unchanged. Status/controller reactions
// are removed only on explicitly simulated objects.
func checkWrites(ctx context.Context, c *cluster, a operator.PlanApplicationResult) error {
	expected := map[sdk.ObjectRef]*unstructured.Unstructured{}
	for ref, o := range c.initial {
		expected[ref] = o.DeepCopy()
	}
	for _, patch := range a.Patches {
		if patch.Operation == "delete" || patch.Operation == "delete_pod" {
			delete(expected, patch.Ref)
		} else if patch.After != nil {
			expected[patch.Ref] = patch.After.DeepCopy()
		}
	}
	for ref := range c.simDeleted {
		delete(expected, ref)
	}
	for ref, o := range c.simCreated {
		expected[ref] = o.DeepCopy()
	}
	for ref, status := range c.simStatus {
		if o := expected[ref]; o != nil {
			o.Object["status"] = status
		}
	}
	actual, err := c.objects(ctx)
	if err != nil {
		return err
	}
	for ref, o := range expected {
		got := actual[ref]
		if got == nil {
			return fmt.Errorf("unrelated object disappeared: %s/%s", ref.Kind, ref.Name)
		}
		left, leftErr := comparable(o)
		right, rightErr := comparable(got)
		if leftErr != nil || rightErr != nil || !bytes.Equal(left, right) {
			return fmt.Errorf("unrelated fields changed: %s/%s", ref.Kind, ref.Name)
		}
		delete(actual, ref)
	}
	if len(actual) != 0 {
		return fmt.Errorf("unexpected objects appeared")
	}
	return nil
}
func comparable(o *unstructured.Unstructured) ([]byte, error) {
	x := o.DeepCopy()

	x.SetUID("")
	x.SetResourceVersion("")
	x.SetGeneration(0)
	x.SetCreationTimestamp(metav1.Time{})
	// The typed client stores optional zero structures (for example an empty
	// Service loadBalancer status) even when source omits them. Use the same
	// Kubernetes conversion on both sides; executor read-back checks still
	// independently prove all requested leaves and lifecycle effects.
	typed, err := scheme.Scheme.New(schema.FromAPIVersionAndKind(x.GetAPIVersion(), x.GetKind()))
	if kruntime.IsNotRegisteredError(err) {
		return json.Marshal(x.Object)
	}
	if err != nil {
		return nil, err
	}
	if err = kruntime.DefaultUnstructuredConverter.FromUnstructured(x.Object, typed); err != nil {
		return nil, err
	}
	return json.Marshal(typed)
}

// cluster embeds the real typed client; only API-server metadata omitted by
// client-go's tracker is synthesized. No action implementation lives here.
type cluster struct {
	operator.TypedPlanClient
	fake                  *kubefake.Clientset
	initial               map[sdk.ObjectRef]*unstructured.Unstructured
	original              sdktest.Snapshot
	catalogs              map[string]databaseCatalog
	catalogConfigurations map[string]program.DatabaseTarget
	operationState        map[string]int32
	externalEffects       []program.ExternalEffect
	effectUsed            map[int]bool
	externalCalls         []sdktest.PrimitiveCall
	jobCallsUsed          map[int]bool
	apiCallsUsed          map[int]bool
	simCreated            map[sdk.ObjectRef]*unstructured.Unstructured
	simDeleted            map[sdk.ObjectRef]bool
	simStatus             map[sdk.ObjectRef]map[string]any
}

func recordedPhase(c sdktest.CorpusCase, phase string) sdktest.Snapshot {
	s := c.Open
	if phase == "healthy" {
		s = c.Healthy
	}
	s.Readings = append(append([]sdktest.PrimitiveCall(nil), s.Readings...), c.PhaseRecordings(phase)...)
	return s
}
