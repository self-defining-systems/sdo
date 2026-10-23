// Package replay checks learned programs with the production executor and an
// isolated client-go fake. Its reports are offline evidence, never live trust.
package replay

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
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
}
type DetectorResult struct {
	ID     string `json:"id"`
	Active int    `json:"active"`
	Error  string `json:"error,omitempty"`
}
type Result struct {
	ProgramID      string                      `json:"program_id"`
	CaseID         string                      `json:"case_id"`
	Status         string                      `json:"status"`
	ExpectedMatch  bool                        `json:"expected_match"`
	Healthy        program.Verdict             `json:"healthy"`
	Faulted        program.Verdict             `json:"faulted"`
	Post           program.Verdict             `json:"post"`
	Detectors      map[string][]DetectorResult `json:"detectors"`
	Actions        []operator.PlanActionReport `json:"actions,omitempty"`
	Effects        []string                    `json:"effects,omitempty"`
	FalsePositives []string                    `json:"false_positives,omitempty"`
	Errors         []string                    `json:"errors,omitempty"`
	DurationMS     float64                     `json:"duration_ms"`
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
	healthyBindings := bindingsFor(p.Spec(), c.Corpus.PeerBindings)
	if len(c.Corpus.PeerBindings.Objects) == 0 && len(c.Corpus.PeerBindings.Values) == 0 {
		healthyBindings = bindings
	}
	r.Healthy = program.RunDiagnose(ctx, p, &sdktest.ReplayReadView{Snapshot: c.Corpus.Healthy, Calls: c.Corpus.PhaseRecordings("healthy")}, healthyBindings, 5*time.Second)
	r.Faulted = program.RunDiagnose(ctx, p, &sdktest.ReplayReadView{Snapshot: c.Corpus.Open, Calls: c.Corpus.PhaseRecordings("open")}, bindings, 5*time.Second)
	r.Detectors["healthy"] = detect(ctx, detectors, c.Corpus.Healthy)
	r.Detectors["faulted"] = detect(ctx, detectors, c.Corpus.Open)
	if r.Healthy.Result != program.NoMatch {
		r.Errors = append(r.Errors, "healthy diagnosis is not NoMatch")
	}
	for _, d := range r.Detectors["healthy"] {
		if d.Active > 0 {
			r.FalsePositives = append(r.FalsePositives, "healthy:detector:"+d.ID)
		}
		if d.Error != "" {
			r.Errors = append(r.Errors, "healthy detector "+d.ID+": "+d.Error)
		}
	}
	if !r.ExpectedMatch {
		if r.Faulted.Result == program.Match {
			r.FalsePositives = append(r.FalsePositives, "diagnose:"+p.Spec().ID)
		}
		for _, d := range r.Detectors["faulted"] {
			if d.Active > 0 {
				r.FalsePositives = append(r.FalsePositives, "detector:"+d.ID)
			}
			if d.Error != "" {
				r.Errors = append(r.Errors, "faulted detector "+d.ID+": "+d.Error)
			}
		}
		if len(r.Errors) == 0 && len(r.FalsePositives) == 0 && r.Faulted.Result == program.NoMatch {
			r.Status = "negative"
		}
		return r
	}
	if r.Faulted.Result != program.Match {
		return fail(fmt.Errorf("faulted diagnosis is %s, want Match", r.Faulted.Result))
	}
	if len(detectors) > 0 {
		active := false
		for _, d := range r.Detectors["faulted"] {
			active = active || d.Active > 0
			if d.Error != "" {
				r.Errors = append(r.Errors, "faulted detector "+d.ID+": "+d.Error)
			}
		}
		if !active {
			r.Errors = append(r.Errors, "matching detector did not fire")
		}
	}
	if len(r.Errors) > 0 || len(r.FalsePositives) > 0 {
		return r
	}
	if m == nil {
		return fail(fmt.Errorf("matching program has no mitigator"))
	}
	plan, err := m.Mitigate(ctx, &sdktest.ReplayReadView{Snapshot: c.Corpus.Open, Calls: c.Corpus.PhaseRecordings("open")}, bindings)
	if err != nil {
		return fail(fmt.Errorf("Mitigate: %w", err))
	}
	if err = plan.Validate(); err != nil {
		return fail(err)
	}
	client, err := newCluster(c.Corpus.Open)
	if err != nil {
		return fail(err)
	}
	source, err := newSource(c)
	if err != nil {
		return fail(err)
	}
	const commit = "offline-accepted"
	incident := operator.IncidentRequest{IncidentID: c.ID, Application: c.Corpus.Open.NamespaceName, Namespace: c.Corpus.Open.NamespaceName, SourceCommit: commit, RepairPolicy: "recorded-actions"}
	// Corpus bindings and detector findings, rather than the proposed plan, name
	// the incident resources authorized by the production source policy.
	for _, ref := range bindings.Objects {
		incident.Findings = append(incident.Findings, sdk.Finding{PrimaryResource: ref})
	}
	for _, d := range detectors {
		findings, e := d.Detect(ctx, c.Corpus.Open)
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
	authority := &operator.SourcePlanAuthority{Config: &operator.SourcePlanPolicy{Version: 1, Authority: "accepted-source-and-incident", Applications: map[string]string{incident.Application: incident.Namespace}, FieldRepairs: "accepted-source-only", CreateKinds: []string{"v1/ConfigMap", "v1/Service"}, DeleteKinds: []string{"networking.k8s.io/v1/NetworkPolicy", "admissionregistration.k8s.io/v1/MutatingWebhookConfiguration", "admissionregistration.k8s.io/v1/ValidatingWebhookConfiguration"}, TTLSeconds: 3600}, Source: source, UID: func(ctx context.Context, ref sdk.ObjectRef) (types.UID, error) {
		o, e := client.Get(ctx, ref)
		if e != nil {
			return "", e
		}
		return o.GetUID(), nil
	}}
	for ref := range client.initial {
		if ref.Namespace == "" {
			authority.Config.WebhookNames = append(authority.Config.WebhookNames, ref.Name)
		}
	}
	waiter := &reaction{cluster: client, plan: plan, effects: &r.Effects}
	verifier := &postVerifier{cluster: client, p: p, bindings: bindings, detectors: detectors, result: &r}
	executor := operator.PlanExecutor{Client: client, Source: source, SourceAuthority: authority, AcceptedRevision: func() string { return commit }, Rollouts: waiter, Verifier: verifier, Values: program.ValueInputs{Spec: p.Spec(), Bindings: bindings, Observed: func(ref sdk.ObjectRef) (map[string]any, error) {
		o := client.initial[ref]
		if o == nil {
			return nil, fmt.Errorf("observed object unavailable")
		}
		return o.DeepCopy().Object, nil
	}}}
	application, err := executor.Apply(ctx, incident, plan, false)
	r.Actions = application.ActionReports
	for _, a := range application.ActionReports {
		if a.Status == "applied" {
			r.Effects = append(r.Effects, fmt.Sprintf("real:action:%d", a.ActionIndex))
		}
	}
	if err != nil {
		r.Status = "refused"
		if application.Applied {
			r.Status = "failed"
		}
		return fail(fmt.Errorf("PlanExecutor: %w", err))
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
func detect(ctx context.Context, ds []sdk.Detector, s sdktest.Snapshot) []DetectorResult {
	out := []DetectorResult{}
	for _, d := range ds {
		r := DetectorResult{ID: d.Spec().ID}
		fs, e := d.Detect(ctx, s)
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
	cluster   *cluster
	p         program.Diagnoser
	bindings  program.Bindings
	detectors []sdk.Detector
	result    *Result
}

func (v *postVerifier) Verify(ctx context.Context) (bool, error) {
	s, err := v.cluster.snapshot(ctx)
	if err != nil {
		return false, err
	}
	v.result.Post = program.RunDiagnose(ctx, v.p, &sdktest.ReplayReadView{Snapshot: s}, v.bindings, 5*time.Second)
	v.result.Detectors["post"] = detect(ctx, v.detectors, s)
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
	actual, err := c.objects(ctx)
	if err != nil {
		return err
	}
	for ref, o := range expected {
		got := actual[ref]
		if got == nil {
			return fmt.Errorf("unrelated object disappeared: %s/%s", ref.Kind, ref.Name)
		}
		if !reflect.DeepEqual(comparable(o), comparable(got)) {
			return fmt.Errorf("unrelated fields changed: %s/%s", ref.Kind, ref.Name)
		}
		delete(actual, ref)
	}
	if len(actual) != 0 {
		return fmt.Errorf("unexpected objects appeared")
	}
	return nil
}
func comparable(o *unstructured.Unstructured) map[string]any {
	x := o.DeepCopy()
	delete(x.Object, "status")
	x.SetUID("")
	x.SetResourceVersion("")
	x.SetGeneration(0)
	x.SetCreationTimestamp(metav1.Time{})
	return x.Object
}

// cluster embeds the real typed client; only API-server metadata omitted by
// client-go's tracker is synthesized. No action implementation lives here.
type cluster struct {
	operator.TypedPlanClient
	fake     *kubefake.Clientset
	initial  map[sdk.ObjectRef]*unstructured.Unstructured
	original sdktest.Snapshot
}
