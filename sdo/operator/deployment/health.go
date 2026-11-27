package deployment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"sdo.dev/operator/deployment/internal/deploymentio"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// HealthProbe evaluates the compiled, accepted detectors against live state.
// The default implementation builds from the accepted repository first.
type HealthProbe interface {
	Evaluate(ctx context.Context, root, operatorRoot, namespace string, scope HealthScope) (HealthEvaluation, error)
}

// HealthScope says what a verification judges. Classes are the detector
// classes that gate (evaluation.GatingClasses; empty means every class). Pins
// are the images the deployment under verification applied: desired-state
// reads in the evaluation return them, so a detector comparing live workloads
// with the accepted source compares with the candidate's own deployment, not
// with the source's unpinned tags.
type HealthScope struct {
	Classes []sdk.DetectorClass
	Pins    rendered.ReceiptPins
}

// scopeOf is the scope of verifying the deployment source describes.
func scopeOf(source lifecycle.DeploymentReceipt) HealthScope {
	return HealthScope{
		Classes: evaluation.GatingClasses,
		Pins:    rendered.ReceiptPins{EffectiveTree: source.EffectiveTree, BaseTree: source.BaseTree, Images: source.ContainerImages},
	}
}

// HealthEvaluation is one probe result: the aggregate status and, when the
// probe reports them, the per-detector states and findings behind it.
type HealthEvaluation struct {
	Status    string
	Detectors []lifecycle.DetectorState
	// CoverageNotes describe clue findings and gaps in non-opening signals.
	CoverageNotes []lifecycle.DetectorState
	// ExistingRecordsExercised counts probe samples whose marked step
	// returned a record the probe did not write.
	ExistingRecordsExercised int
	// EnvironmentWait is the probe-user provisioning the evaluation waits for.
	EnvironmentWait time.Duration
}

type AcceptedDetectorProbe struct {
	// Settings is the operator configuration file the probe and its --evaluate-once run read (the sdo-config mount
	// or the controller's record of it); empty evaluates under settings.Default().
	Settings        string
	RequireComplete bool
	// OutsideCluster says the evaluation cannot resolve or dial in-cluster
	// Service names (it runs on a host outside the cluster network): detectors
	// that dial them are left out.
	OutsideCluster bool
	// Host says the evaluation runs on a host, not in a pod: the probes keep
	// their identity seed in a directory of the build workspace instead of
	// /workspace.
	Host       bool
	Kubeconfig string
	// TelemetryEnvironment is the telemetry environment document the probe's
	// evaluation reads; the accepted tree's own bindings come from the memory
	// host built from it. Empty provides no operator source.
	TelemetryEnvironment string
	Limit                time.Duration
}

func (probe AcceptedDetectorProbe) Evaluate(ctx context.Context, acceptedRoot, operatorRoot, namespace string, scope HealthScope) (HealthEvaluation, error) {
	limit := probe.Limit
	if limit <= 0 {
		limit = 5 * time.Minute
	}
	bounded, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	prepared, err := probe.Prepare(bounded, acceptedRoot, operatorRoot, namespace, scope)
	if err != nil {
		return HealthEvaluation{}, err
	}
	defer prepared.Close()
	stdout, stderr, err := prepared.run(bounded)
	if err != nil {
		return HealthEvaluation{}, fmt.Errorf("evaluate accepted health detectors: %w (stdout=%q stderr=%q)", err, boundedHealthOutput(stdout), boundedHealthOutput(stderr))
	}
	return decodeHealthVerdict(stdout, stderr)
}

// PreparedProbe is the accepted detectors compiled once, so the same
// evaluation can run repeatedly against a live namespace without rebuilding.
type PreparedProbe struct {
	workspace *builder.Workspace
	binary    string
	args      []string
	env       []string
	// WriteSoak is how many times per watch pass the watch re-runs the
	// memory's write scenarios, so lists the probes read keep growing while
	// the detectors judge. Zero runs the watch as the controller does.
	WriteSoak int
}

// Prepare compiles the accepted detectors and memory host of acceptedRoot.
// Close releases the build workspace.
func (probe AcceptedDetectorProbe) Prepare(ctx context.Context, acceptedRoot, operatorRoot, namespace string, scope HealthScope) (*PreparedProbe, error) {
	compiled, err := probe.Compile(ctx, acceptedRoot, operatorRoot)
	if err != nil {
		return nil, err
	}
	return compiled.Prepare(namespace, scope)
}

// CompiledProbe is the accepted detectors and memory host of one root, built:
// everything a probe needs that does not depend on the deployment it will
// judge (its namespace and the images it applied). Prepare finishes it; Close
// releases it when it was never finished.
type CompiledProbe struct {
	probe        AcceptedDetectorProbe
	acceptedRoot string
	workspace    *builder.Workspace
	binary       string
	artifact     builder.MemoryHostArtifact
	revision     string
}

// Compile builds the accepted detectors and memory host of acceptedRoot.
func (probe AcceptedDetectorProbe) Compile(ctx context.Context, acceptedRoot, operatorRoot string) (*CompiledProbe, error) {
	projected, err := memory.AcceptedDiagnostics(ctx, acceptedRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve accepted diagnostics: %w", err)
	}
	policy := settings.Default()
	if probe.Settings != "" {
		if policy, err = settings.Read(probe.Settings); err != nil {
			return nil, err
		}
	}
	workspace, err := builder.BuildWorkspace(ctx, builder.Config{Replay: policy.Learning.Replay, ApplicationRoot: acceptedRoot, DiagnosticsRoot: projected.Directory, OperatorRoot: operatorRoot})
	if err != nil {
		return nil, fmt.Errorf("stage accepted diagnostics: %w", err)
	}
	compiled := &CompiledProbe{probe: probe, acceptedRoot: acceptedRoot, workspace: workspace, revision: projected.AcceptedRevision}
	fail := func(err error) (*CompiledProbe, error) { workspace.Close(); return nil, err }
	// A host check reads no advisories: skip the informational playbook tests
	// and the program-corpus replay, which only a launch's notes consume. Every
	// gate that decides whether the detectors run still runs.
	build := workspace.ValidateAndBuild
	if probe.Host {
		build = workspace.BuildForLaunch
	}
	if compiled.binary, err = build(ctx); err != nil {
		return fail(fmt.Errorf("compile accepted health detectors: %w", err))
	}
	if compiled.artifact, err = workspace.BuildMemoryHost(ctx); err != nil {
		return fail(fmt.Errorf("compile accepted memory host: %w", err))
	}
	return compiled, nil
}

// Close releases the build workspace of a probe that was never prepared.
func (c *CompiledProbe) Close() { c.workspace.Close() }

// Prepare finishes the probe for one deployment: its namespace and image pins.
// The returned probe owns the workspace.
func (c *CompiledProbe) Prepare(namespace string, scope HealthScope) (*PreparedProbe, error) {
	probe, workspace := c.probe, c.workspace
	prepared := &PreparedProbe{workspace: workspace, binary: c.binary}
	fail := func(err error) (*PreparedProbe, error) { workspace.Close(); return nil, err }
	pinsFile := ""
	if len(scope.Pins.Images) > 0 {
		pinsFile = filepath.Join(workspace.Directory, "image-pins.json")
		encoded, err := json.Marshal(scope.Pins)
		if err == nil {
			err = os.WriteFile(pinsFile, encoded, 0o600)
		}
		if err != nil {
			return fail(fmt.Errorf("write image pins: %w", err))
		}
	}
	args := probe.evaluationArgs(c.acceptedRoot, namespace, scope, pinsFile)
	if probe.Host {
		// The probes keep their identity seed under the repository mount, which
		// is /workspace in the cluster and not writable on a host.
		mount := filepath.Join(workspace.Directory, "repository")
		if err := os.MkdirAll(mount, 0o700); err != nil {
			return fail(fmt.Errorf("create probe identity mount: %w", err))
		}
		args = append(args, "--repository-mount-path", mount)
	}
	prepared.args = append(args, "--memory-host-binary", c.artifact.Path, "--memory-host-digest", c.artifact.Digest, "--memory-commit", c.revision)
	if probe.Kubeconfig != "" {
		prepared.env = append(prepared.env, "KUBECONFIG="+probe.Kubeconfig)
	}
	return prepared, nil
}

// Close releases the build workspace.
func (p *PreparedProbe) Close() { p.workspace.Close() }

func (p *PreparedProbe) run(ctx context.Context) ([]byte, []byte, error) {
	return deploymentio.Evaluate(ctx, p.workspace.Directory, p.binary, p.args, p.env)
}

// decodeHealthVerdict keeps the per-detector evidence --evaluate-once reports:
// pending and unavailable detectors with their reasons, and each active
// finding with its summary, so the verdict can say which detector and why.
func decodeHealthVerdict(stdout, stderr []byte) (HealthEvaluation, error) {
	verdict, err := evaluation.Decode(stdout)
	if err != nil {
		return HealthEvaluation{}, fmt.Errorf("decode health detector verdict: %w (stdout=%q stderr=%q)", err, boundedHealthOutput(stdout), boundedHealthOutput(stderr))
	}
	return healthEvaluationOf(verdict), nil
}

// healthEvaluationOf is a decoded verdict as the probe result the lifecycle reads.
func healthEvaluationOf(verdict evaluation.Verdict) HealthEvaluation {
	result := HealthEvaluation{Status: string(verdict.Status), ExistingRecordsExercised: verdict.ExistingRecordsExercised, EnvironmentWait: verdict.EnvironmentWait}
	for _, state := range verdict.DetectorStates {
		if (state.Status == "clear" && state.Reason != "") || (state.Status == "pending" && state.Clue) {
			result.CoverageNotes = append(result.CoverageNotes, lifecycle.DetectorState{DetectorID: state.DetectorID, Status: state.Status, Reason: state.Reason})
			continue
		}
		if state.Status != "pending" {
			continue
		}
		result.Detectors = append(result.Detectors, lifecycle.DetectorState{DetectorID: state.DetectorID, Status: state.Status, Reason: firstNonEmpty(state.Reason, state.Error)})
	}
	for _, item := range verdict.Unavailable {
		state := lifecycle.DetectorState{DetectorID: item.DetectorID, Status: "unavailable", Reason: item.Reason}
		if item.Clue {
			result.CoverageNotes = append(result.CoverageNotes, state)
			continue
		}
		result.Detectors = append(result.Detectors, state)
	}
	for i, finding := range verdict.Findings {
		switch {
		case finding.Status != sdk.FindingActive:
			continue
		case slices.Contains(verdict.ClueFindings, i):
			result.CoverageNotes = append(result.CoverageNotes, lifecycle.DetectorState{DetectorID: finding.DetectorID, Status: "clue", Reason: firstNonEmpty(finding.Summary, finding.RuleID)})
		default:
			result.Detectors = append(result.Detectors, lifecycle.DetectorState{DetectorID: finding.DetectorID, Status: "firing", Reason: firstNonEmpty(finding.Summary, finding.RuleID) + evidenceSuffix(finding.Evidence)})
		}
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func evidenceSuffix(evidence string) string {
	if evidence == "" {
		return ""
	}
	return "; evidence: " + boundedHealthOutput([]byte(evidence))
}

// verdictOf maps a probe status to the lifecycle's typed outcome. Pending and
// unavailable are waited out; everything else that is not clear is the
// deployed source's problem.
func verdictOf(evaluation HealthEvaluation) lifecycle.HealthVerdict {
	verdict := lifecycle.HealthVerdict{Summary: "accepted health detectors report " + evaluation.Status, Detectors: evaluation.Detectors, CoverageNotes: evaluation.CoverageNotes, EnvironmentWait: evaluation.EnvironmentWait}
	switch evaluation.Status {
	case "clear":
		verdict.Outcome = lifecycle.VerificationClear
	case "pending", "unavailable":
		verdict.Outcome = lifecycle.VerificationPending
	default:
		verdict.Outcome = lifecycle.VerificationSourceFault
	}
	return verdict
}

// Watcher is one running evaluation of the accepted detectors that judges the
// live application every interval after a single warm-up, so a caller can ask
// for a fresh verdict in seconds instead of paying the warm-up per question.
type Watcher struct {
	mu      sync.Mutex
	latest  evaluation.Verdict
	have    bool
	err     error
	changed chan struct{}
	cancel  context.CancelFunc
	done    chan struct{}
}

func (p *PreparedProbe) watchArgs(interval time.Duration) []string {
	args := append(append([]string(nil), p.args...), "--watch", interval.String())
	if p.WriteSoak > 0 {
		args = append(args, "--write-soak", strconv.Itoa(p.WriteSoak))
	}
	return args
}

// Watch starts the evaluation process; Close stops it.
func (p *PreparedProbe) Watch(ctx context.Context, interval time.Duration) *Watcher {
	running, cancel := context.WithCancel(ctx)
	w := &Watcher{changed: make(chan struct{}), cancel: cancel, done: make(chan struct{})}
	args := p.watchArgs(interval)
	go func() {
		defer close(w.done)
		stderr, err := deploymentio.Lines(running, p.workspace.Directory, p.binary, args, p.env, func(line []byte) {
			verdict, derr := evaluation.Decode(line)
			w.mu.Lock()
			defer w.mu.Unlock()
			if derr != nil {
				w.err = fmt.Errorf("decode detector verdict: %w (line %q)", derr, boundedHealthOutput(line))
			} else {
				w.latest, w.have = verdict, true
			}
			w.notify()
		})
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.err == nil && running.Err() == nil {
			w.err = fmt.Errorf("detector evaluation ended: %v (stderr tail=%q)", err, boundedTail(stderr))
		}
		w.notify()
	}()
	return w
}

func (w *Watcher) notify() {
	close(w.changed)
	w.changed = make(chan struct{})
}

// Next returns the first verdict whose judging began at or after notBefore,
// waiting for it. It fails when the evaluation process has ended.
func (w *Watcher) Next(ctx context.Context, notBefore time.Time) (evaluation.Verdict, error) {
	for {
		w.mu.Lock()
		if w.err != nil {
			err := w.err
			w.mu.Unlock()
			return evaluation.Verdict{}, err
		}
		if w.have && !w.latest.StartedAt.Before(notBefore) {
			verdict := w.latest
			w.mu.Unlock()
			return verdict, nil
		}
		changed := w.changed
		w.mu.Unlock()
		select {
		case <-ctx.Done():
			return evaluation.Verdict{}, ctx.Err()
		case <-changed:
		}
	}
}

// Close stops the evaluation process and waits for it.
func (w *Watcher) Close() {
	w.cancel()
	<-w.done
}

// boundedTail keeps the end of a log, where a process says why it stopped.
func boundedTail(data []byte) string {
	const limit = 1024
	if len(data) > limit {
		return "…" + string(data[len(data)-limit:])
	}
	return string(data)
}

func boundedHealthOutput(data []byte) string {
	const limit = 2048
	if len(data) > limit {
		return string(data[:limit]) + "…"
	}
	return string(data)
}

// IndependentHealthVerifier rechecks the deployed source and accepted detector
// commit before returning a bounded live verdict to the lifecycle coordinator.
type IndependentHealthVerifier struct {
	AcceptedRoot string
	OperatorRoot string
	Readiness    *Deployer
	Probe        HealthProbe
}

// Verify judges the accepted HEAD: VerifyCandidate at the accepted repository.
func (v IndependentHealthVerifier) Verify(ctx context.Context, acceptedCommit, namespace, objective string, source lifecycle.DeploymentReceipt) (lifecycle.HealthVerdict, error) {
	return v.VerifyCandidate(ctx, v.AcceptedRoot, acceptedCommit, namespace, objective, source)
}

// VerifyCandidate judges commit, which must be the clean HEAD of root, with
// the detectors of that commit against the live deployment source: the
// accepted repository after onboarding, or a broker proposal checkout whose
// candidate is not accepted yet (an upstream advance). The deployed tree must
// be commit's tree or an ancestor's, the live workloads must still match it
// (re-verified in root, where commit lives), and neither root nor its
// detector tree may change during the evaluation. A Seeded receipt (an install
// SDO never deployed) binds no deployment: the detectors are judged against
// the live namespace as found.
func (v IndependentHealthVerifier) VerifyCandidate(ctx context.Context, root, commit, namespace, objective string, source lifecycle.DeploymentReceipt) (lifecycle.HealthVerdict, error) {
	checked, err := v.checkCandidate(ctx, root, commit, namespace, objective, source)
	if err != nil {
		return lifecycle.HealthVerdict{}, err
	}
	probe := v.Probe
	if probe == nil {
		probe = AcceptedDetectorProbe{}
	}
	evaluation, err := probe.Evaluate(ctx, root, v.OperatorRoot, namespace, scopeOf(source))
	if err != nil {
		return lifecycle.HealthVerdict{}, err
	}
	return checked.verdict(evaluation)
}

// checkedCandidate is a candidate whose commit, deployment and detector tree were verified before its
// evaluation; verdict re-checks them after it.
type checkedCandidate struct {
	root, detectorTree string
	git                func(args ...string) (string, error)
}

// checkCandidate verifies what a verdict on commit rests on: it is the clean HEAD of root, the deployed source
// is an ancestor, the diagnostics tree exists, and the deployment still is what the receipt says. A Seeded
// receipt has no deployment to check.
func (v IndependentHealthVerifier) checkCandidate(ctx context.Context, root, commit, namespace, objective string, source lifecycle.DeploymentReceipt) (checkedCandidate, error) {
	if root == "" || v.OperatorRoot == "" || v.Readiness == nil || namespace == "" || strings.TrimSpace(objective) == "" || commit == "" || !(source.Ready || source.Seeded) {
		return checkedCandidate{}, errors.New("repository, operator, observer, namespace, objective, commit, and source evidence are required")
	}
	git := func(args ...string) (string, error) {
		output, err := deploymentio.Run(ctx, "git", append([]string{"-C", root}, args...), nil)
		if err != nil {
			return "", fmt.Errorf("verified repository git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
		return strings.TrimSpace(string(output)), nil
	}
	head, err := git("rev-parse", "HEAD")
	if err != nil {
		return checkedCandidate{}, err
	}
	if head != commit {
		return checkedCandidate{}, fmt.Errorf("verified commit %s is not the HEAD of %s", commit, root)
	}
	if status, err := git("status", "--porcelain"); err != nil {
		return checkedCandidate{}, err
	} else if status != "" {
		return checkedCandidate{}, errors.New("verified repository changed before health verification")
	}
	detectorTree, err := git("rev-parse", commit+":"+layout.DiagnosticsDir)
	if err != nil {
		return checkedCandidate{}, fmt.Errorf("verified health diagnostics: %w", err)
	}
	if detectorTree == "" {
		return checkedCandidate{}, errors.New("verified detector tree is empty")
	}
	checked := checkedCandidate{root: root, detectorTree: detectorTree, git: git}
	if source.Seeded {
		return checked, nil
	}
	ancestorTrees, err := git("log", "--format=%H %T", commit)
	if err != nil {
		return checkedCandidate{}, err
	}
	sourceCommit := ""
	for _, line := range strings.Split(ancestorTrees, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 2 && parts[1] == source.BaseTree {
			sourceCommit = parts[0]
			break
		}
	}
	if sourceCommit == "" {
		return checkedCandidate{}, errors.New("deployed source tree is not an ancestor of the verified health diagnostics")
	}
	// The commit lives in root, which is not the accepted repository for a
	// candidate: re-verify the deployment against root.
	readiness := *v.Readiness
	readiness.AcceptedRoot = root
	if _, err := readiness.VerifyAccepted(ctx, sourceCommit, namespace, source); err != nil {
		return checkedCandidate{}, fmt.Errorf("deployed source changed before health verification: %w", err)
	}
	return checked, nil
}

// verdict is the lifecycle's verdict for an evaluation of the checked candidate.
func (c checkedCandidate) verdict(evaluation HealthEvaluation) (lifecycle.HealthVerdict, error) {
	verdict := verdictOf(evaluation)
	verdict.ExistingRecords = lifecycle.ExistingRecordsEvidence{Required: recordsRequired(c.root), Exercised: evaluation.ExistingRecordsExercised}
	if verdict.Outcome != lifecycle.VerificationClear {
		return verdict, nil
	}
	if got, err := c.git("rev-parse", "HEAD:"+layout.DiagnosticsDir); err != nil || got != c.detectorTree {
		return lifecycle.HealthVerdict{}, errors.New("verified health detectors changed during verification")
	}
	if status, err := c.git("status", "--porcelain"); err != nil || status != "" {
		return lifecycle.HealthVerdict{}, errors.New("verified repository changed during health verification")
	}
	return verdict, nil
}

// recordsRequired reports whether the candidate's application map needs a
// marked step to read records the probe did not write. An unreadable or
// unparsable map requires nothing here: the shared validator owns that finding.
func recordsRequired(root string) bool {
	data, err := os.ReadFile(filepath.Join(root, layout.ArchitectureFile))
	if err != nil {
		return false
	}
	region, found, err := appmap.Find(string(data))
	if err != nil || !found {
		return false
	}
	parsed, err := appmap.Parse(region.YAML)
	return err == nil && parsed.NeedsExistingRecords()
}

// Verify the configured operator root before a build can import any modules.
func (v IndependentHealthVerifier) Validate() error {
	for _, path := range []string{v.AcceptedRoot, v.OperatorRoot} {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("verification root is not a directory: %s", path)
		}
	}
	if _, err := os.Stat(filepath.Join(v.OperatorRoot, "runtime", "go.mod")); err != nil {
		return fmt.Errorf("operator runtime module: %w", err)
	}
	return nil
}

func (probe AcceptedDetectorProbe) evaluationArgs(acceptedRoot, namespace string, scope HealthScope, pinsFile string) []string {
	args := []string{"--evaluate-once", "--namespace", namespace, "--app-root", acceptedRoot}
	if len(scope.Classes) > 0 {
		args = append(args, "--classes", evaluation.FormatClasses(scope.Classes))
	}
	if pinsFile != "" {
		args = append(args, "--image-pins", pinsFile)
	}
	if probe.RequireComplete {
		args = append(args, "--accept-seed")
	}
	if probe.OutsideCluster {
		args = append(args, "--outside-cluster")
	}
	if probe.TelemetryEnvironment != "" {
		args = append(args, "--telemetry-environment", probe.TelemetryEnvironment)
	}
	if probe.Settings != "" {
		args = append(args, "--settings", probe.Settings)
	}
	return args
}
