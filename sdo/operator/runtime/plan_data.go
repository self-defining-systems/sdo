package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storeops"
	"sdo.dev/operator/sdk/storestate"
)

// Store command bounds: a backup or restore may stream up to maxBackupBytes
// for storeTransferTimeout; every other store command answers in a few lines.
const (
	storeTransferTimeout = 5 * time.Minute
	storeCommandTimeout  = time.Minute
	maxStoreOutputBytes  = 64 << 10
)

// StoreCommandClient runs one command in a store's container and streams its
// stdout, under the Pod identity uid. Nonzero exits are results.
type StoreCommandClient interface {
	StoreCommand(ctx context.Context, uid string, command program.Command, stdout io.Writer) (int32, error)
}

// StoreCommand runs a store command through the controller's exec transport.
func (c TypedPlanClient) StoreCommand(ctx context.Context, uid string, command program.Command, stdout io.Writer) (int32, error) {
	code, err := kubereads.ReadOutput(ctx, c.Get, kubereads.SPDYExec{Client: c.Client, Config: c.ExecConfig}, c.ProtectedNamespace, uid, command, stdout)
	if errors.Is(err, kubereads.ErrExecUnavailable) {
		return 0, fmt.Errorf("%w: controller exec transport unavailable", errPlanExecRefused)
	}
	return code, err
}

// StoreAccess grounds typed store actions: the accepted application map says
// which component a Pod belongs to, its engine and role; the live snapshot
// says which Pods a component has; Backups keeps what StoreBackup took.
type StoreAccess struct {
	Read     gitRead
	Snapshot SnapshotProvider
	Backups  BackupStore
	// Payloads holds the restore text captured with each promoted baseline.
	Payloads StorePayloadSource
}

// acceptedMapComponents reads the accepted application map's components.
func acceptedMapComponents(ctx context.Context, read gitRead, repository, commit string) ([]appmap.Component, error) {
	if read == nil || repository == "" || commit == "" {
		return nil, fmt.Errorf("the incident names no accepted repository commit")
	}
	document, err := read(ctx, repository, "show", commit+":"+layout.ArchitectureFile)
	if err != nil {
		return nil, fmt.Errorf("accepted architecture unreadable: %w", err)
	}
	region, found, err := appmap.Find(string(document))
	if !found {
		return nil, fmt.Errorf("accepted architecture has no application map")
	}
	var parsed appmap.Map
	if err == nil {
		parsed, err = appmap.Parse(region.YAML)
	}
	if err != nil {
		return nil, fmt.Errorf("accepted application map unreadable: %w", err)
	}
	return parsed.Components, nil
}

// storeRun is one grounded store action in progress.
type storeRun struct {
	executor  PlanExecutor
	incident  IncidentRequest
	client    StoreCommandClient
	access    StoreAccess
	uid       string
	target    program.StoreTarget
	engine    storeops.Engine
	component appmap.Component
	all       []appmap.Component
}

// groundStore checks a store action's target against the accepted map and the
// live Pod: the component declares data with this engine and the Pod belongs
// to its workload.
func (e PlanExecutor) groundStore(ctx context.Context, incident IncidentRequest, target program.StoreTarget, uid string) (storeRun, error) {
	run := storeRun{executor: e, incident: incident, uid: uid, target: target, engine: target.EngineOps()}
	if e.Stores == nil {
		return run, fmt.Errorf("store action refused: the controller has no store access configured")
	}
	run.access = *e.Stores
	client, ok := e.Client.(StoreCommandClient)
	if !ok {
		return run, fmt.Errorf("store action refused: controller exec transport unavailable")
	}
	run.client = client
	components, err := acceptedMapComponents(ctx, run.access.Read, incident.RepositoryWorktree, incident.SourceCommit)
	if err != nil {
		return run, fmt.Errorf("store action refused: %w", err)
	}
	run.all = components
	component, ok := componentNamed(components, target.Component)
	switch {
	case !ok:
		return run, fmt.Errorf("store action refused: the accepted application map has no component %q", target.Component)
	case component.Data == nil:
		return run, fmt.Errorf("store action refused: component %s declares no data role in the accepted application map; the designer adds data: {engine, role}", target.Component)
	case component.Data.Engine != target.Engine:
		return run, fmt.Errorf("store action refused: component %s is engine %q in the accepted application map, not %q", target.Component, component.Data.Engine, target.Engine)
	}
	run.component = component
	if run.access.Snapshot == nil {
		return run, fmt.Errorf("store action refused: no cluster snapshot to check the Pod's workload")
	}
	view, err := run.access.Snapshot.Snapshot(ctx)
	if err != nil {
		return run, fmt.Errorf("store action refused: cluster snapshot unavailable: %w", err)
	}
	if workload := owningWorkload(view, target.Pod); workload != component.Workload {
		return run, fmt.Errorf("store action refused: Pod %s belongs to %s, not to component %s (%s)", target.Pod.Name, workload, component.Name, component.Workload)
	}
	return run, nil
}

func componentNamed(components []appmap.Component, name string) (appmap.Component, bool) {
	for _, component := range components {
		if component.Name == name {
			return component, true
		}
	}
	return appmap.Component{}, false
}

func (r storeRun) port() int {
	if r.target.Port != 0 {
		return r.target.Port
	}
	return r.engine.DefaultPort()
}

// cappedWriter fails a stream past its limit.
type cappedWriter struct {
	buffer bytes.Buffer
	limit  int
	over   bool
}

func (w *cappedWriter) Write(p []byte) (int, error) {
	if w.buffer.Len()+len(p) > w.limit {
		w.over = true
		return 0, fmt.Errorf("output exceeds %d bytes", w.limit)
	}
	return w.buffer.Write(p)
}

// run executes one store command in the target container.
func (r storeRun) run(ctx context.Context, pod sdk.ObjectRef, container, uid string, command storeops.Command, limit int, timeout time.Duration) (string, int32, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out := &cappedWriter{limit: limit}
	code, err := r.client.StoreCommand(ctx, uid, program.Command{Pod: pod, Container: container, Args: command.Args, Stdin: command.Stdin}, out)
	if out.over {
		return "", 0, fmt.Errorf("store command output exceeded %d bytes", limit)
	}
	return out.buffer.String(), code, err
}

func (r storeRun) command(ctx context.Context, command storeops.Command) (string, int32, error) {
	return r.run(ctx, r.target.Pod, r.target.Container, r.uid, command, maxStoreOutputBytes, storeCommandTimeout)
}

// count runs the engine's count of a scope.
func (r storeRun) count(ctx context.Context, scope program.DataScope) (int, error) {
	stdout, code, err := r.command(ctx, r.engine.Count(r.port(), scope))
	if err != nil {
		return 0, fmt.Errorf("store count failed: %w", err)
	}
	n, ok := storeops.Parse(stdout, storeops.MarkerCount)
	if code != 0 || !ok {
		return 0, fmt.Errorf("store count gave no answer (exit %d)", code)
	}
	return n, nil
}

// facts reads the target store's value-free facts, when its engine has a
// store-state reader.
func (r storeRun) facts(ctx context.Context) (*storestate.Facts, string) {
	adapter, ok := storestate.AdapterFor(storestate.Engine(r.target.Engine))
	if !ok {
		return nil, fmt.Sprintf("engine %s has no store-state reader", r.target.Engine)
	}
	stdout, _, err := r.command(ctx, storeops.Command{Args: adapter.ReadCommand(storestate.Endpoint{Port: r.storeStatePort()})})
	if err != nil {
		return nil, err.Error()
	}
	facts, err := adapter.Parse(stdout)
	if err != nil {
		return nil, err.Error()
	}
	facts.Component, facts.Pod, facts.At = r.target.Component, r.target.Pod, r.executor.clock()()
	return &facts, ""
}

var dumpEnd = regexp.MustCompile(`\n` + storeops.MarkerEnd + ` ([0-9]+)\n$`)

// backup takes a StoreBackup: it streams the dump, checks it ended with the
// engine's end marker and its item count, records the store's facts, and
// stores both.
func (r storeRun) backup(ctx context.Context, a program.StoreBackup) (program.StoreBackupRecord, error) {
	if r.access.Backups == nil {
		return program.StoreBackupRecord{}, fmt.Errorf("StoreBackup refused: the controller has no backup storage")
	}
	scope := r.engine.Normalize(a.Scope)
	stdout, code, err := r.run(ctx, r.target.Pod, r.target.Container, r.uid, r.engine.Backup(r.port(), scope), maxBackupBytes, storeTransferTimeout)
	if err != nil {
		return program.StoreBackupRecord{}, fmt.Errorf("StoreBackup failed: %w", err)
	}
	if code == storeops.ExitLimit {
		return program.StoreBackupRecord{}, fmt.Errorf("StoreBackup refused: the scope holds more than its limit of %d items; narrow the selector or raise Limit", limitOf(scope))
	}
	match := dumpEnd.FindStringSubmatch("\n" + stdout)
	if code != 0 || match == nil {
		return program.StoreBackupRecord{}, fmt.Errorf("StoreBackup failed: the dump did not complete (exit %d)", code)
	}
	items := strings.Count(stdout, "\n") - 1
	if match[1] != fmt.Sprint(items) {
		return program.StoreBackupRecord{}, fmt.Errorf("StoreBackup failed: the dump reports %s items but holds %d", match[1], items)
	}
	sum := sha256.Sum256([]byte(stdout))
	record := program.StoreBackupRecord{ID: a.ID, IncidentID: r.incident.IncidentID, Component: r.target.Component, Engine: r.target.Engine, Pod: r.target.Pod,
		Scope: scope, Items: items, Bytes: int64(len(stdout)), SHA256: hex.EncodeToString(sum[:]), At: r.executor.clock()()}
	record.Facts, record.FactsError = r.facts(ctx)
	return r.access.Backups.Put(record, []byte(stdout))
}

func limitOf(scope program.DataScope) int {
	if scope.Limit == 0 {
		return storeops.DefaultLimit
	}
	return scope.Limit
}

// errStoreUnchanged marks a refusal after which the store is known unchanged.
var errStoreUnchanged = errors.New("store unchanged")

// repair runs a RepairData (a statement, a member removal or a copy, which
// counts its CopyFrom source): the count must be within the expected bounds,
// then the write, then a recount and a facts read. A named backup must be one
// of this incident; whether it covers the scope is recorded, not required.
func (r storeRun) repair(ctx context.Context, a program.RepairData) (program.DataRepairEffect, error) {
	effect := program.DataRepairEffect{Component: a.Target.Component, Backup: a.Backup}
	if a.Backup != "" {
		if r.access.Backups == nil {
			return effect, fmt.Errorf("%w: RepairData refused: backup %s named, but the controller has no backup storage", errStoreUnchanged, a.Backup)
		}
		record, _, err := r.access.Backups.Get(r.incident.IncidentID, a.Backup)
		if err != nil {
			return effect, fmt.Errorf("%w: RepairData refused: backup %s: %v; name a StoreBackup of this incident, or none", errStoreUnchanged, a.Backup, err)
		}
		covers := record.Covers(a.Target, a.Scope)
		effect.BackupCovers = &covers
	}
	counted := a.Scope
	if a.CopyFrom != nil {
		counted = *a.CopyFrom
	}
	before, err := r.count(ctx, counted)
	if err != nil {
		return effect, fmt.Errorf("%w: RepairData refused: %v", errStoreUnchanged, err)
	}
	effect.Before = before
	if before < a.ExpectMin || before > a.ExpectMax {
		return effect, fmt.Errorf("%w: RepairData refused: the scope matches %d items, outside the expected [%d, %d]; nothing was written", errStoreUnchanged, before, a.ExpectMin, a.ExpectMax)
	}
	var command storeops.Command
	switch {
	case a.MemberPredicate != nil:
		command, err = r.engine.RemoveMembers(r.port(), a.Scope, *a.MemberPredicate, a.ExpectMin, a.ExpectMax)
	case a.CopyFrom != nil:
		command, err = r.engine.Copy(r.port(), *a.CopyFrom, a.Scope, a.ExpectMin, a.ExpectMax)
	default:
		command = r.engine.Write(r.port(), a.Scope, a.Statement, a.ExpectMin, a.ExpectMax)
	}
	if err != nil {
		return effect, fmt.Errorf("%w: RepairData refused: %v", errStoreUnchanged, err)
	}
	stdout, code, err := r.command(ctx, command)
	if err != nil {
		return effect, fmt.Errorf("RepairData outcome unknown: %w", err)
	}
	if n, refused := storeops.Parse(stdout, storeops.MarkerExpect); refused && code == 0 {
		return effect, fmt.Errorf("%w: RepairData refused at write time: the scope matched %d items, outside the expected [%d, %d]", errStoreUnchanged, n, a.ExpectMin, a.ExpectMax)
	}
	affected, ok := storeops.Parse(stdout, storeops.MarkerAffected)
	if code != 0 || !ok {
		return effect, fmt.Errorf("RepairData outcome unknown: the write gave no answer (exit %d)", code)
	}
	effect.Affected = affected
	if failed := storeops.ParseFailed(stdout); failed > 0 {
		effect.Facts, effect.FactsError = r.facts(ctx)
		return effect, fmt.Errorf("RepairData applied in part: %d matched items rejected the statement (for example a key of another type) while %d changed; repair the rest by hand or restore a backup", failed, affected)
	}
	after, err := r.count(ctx, a.Scope)
	if err != nil {
		return effect, fmt.Errorf("RepairData applied; the recount failed: %w", err)
	}
	effect.After = after
	effect.Facts, effect.FactsError = r.facts(ctx)
	return effect, nil
}

// restore runs a RestoreBackup of one backup of this incident.
func (r storeRun) restore(ctx context.Context, a program.RestoreBackup) (program.DataRepairEffect, error) {
	effect := program.DataRepairEffect{Component: a.Target.Component, Backup: a.Backup}
	if r.access.Backups == nil {
		return effect, fmt.Errorf("%w: RestoreBackup refused: the controller has no backup storage", errStoreUnchanged)
	}
	record, dump, err := r.access.Backups.Get(r.incident.IncidentID, a.Backup)
	if err != nil {
		return effect, fmt.Errorf("%w: RestoreBackup refused: backup %s: %v", errStoreUnchanged, a.Backup, err)
	}
	if record.Component != a.Target.Component || record.Engine != a.Target.Engine {
		return effect, fmt.Errorf("%w: RestoreBackup refused: backup %s is of %s (%s), not %s (%s)", errStoreUnchanged, a.Backup, record.Component, record.Engine, a.Target.Component, a.Target.Engine)
	}
	command, err := r.engine.Restore(r.port(), record.Scope, string(dump))
	if err != nil {
		return effect, fmt.Errorf("%w: RestoreBackup refused: %v", errStoreUnchanged, err)
	}
	stdout, code, err := r.run(ctx, r.target.Pod, r.target.Container, r.uid, command, maxStoreOutputBytes, storeTransferTimeout)
	if err != nil {
		return effect, fmt.Errorf("RestoreBackup outcome unknown: %w", err)
	}
	restored, ok := storeops.Parse(stdout, storeops.MarkerRestored)
	if code != 0 || !ok {
		return effect, fmt.Errorf("RestoreBackup outcome unknown: the restore gave no answer (exit %d)", code)
	}
	effect.Before, effect.Affected = record.Items, restored
	if after, err := r.count(ctx, record.Scope); err == nil {
		effect.After = after
	}
	effect.Facts, effect.FactsError = r.facts(ctx)
	return effect, nil
}

// invalidate runs an InvalidateDerived and records the derivation state it ran
// under: whether the map marks the target derived and how each derived_from
// source read.
func (r storeRun) invalidate(ctx context.Context, a program.InvalidateDerived) (program.DataRepairEffect, error) {
	effect := program.DataRepairEffect{Component: a.Target.Component}
	command, err := r.engine.Invalidate(r.port(), a.Scope)
	if err != nil {
		return effect, fmt.Errorf("%w: InvalidateDerived refused: %v", errStoreUnchanged, err)
	}
	effect.SourceState = r.derivedSourceState(ctx)
	stdout, code, err := r.command(ctx, command)
	if err != nil {
		return effect, fmt.Errorf("InvalidateDerived outcome unknown: %w", err)
	}
	affected, ok := storeops.Parse(stdout, storeops.MarkerAffected)
	if code != 0 || !ok {
		return effect, fmt.Errorf("InvalidateDerived outcome unknown: the store gave no answer (exit %d)", code)
	}
	effect.Affected = affected
	return effect, nil
}

// derivedSourceState describes, as evidence, the target's derivation: whether
// the accepted map marks it data.role derived, and for every derived_from
// source whether it passed its live store-state checks.
func (r storeRun) derivedSourceState(ctx context.Context) []string {
	data := r.component.Data
	if data == nil || data.Role != appmap.RoleDerived {
		return []string{fmt.Sprintf("the accepted application map does not mark %s data.role derived", r.component.Name)}
	}
	if len(data.DerivedFrom) == 0 {
		return []string{fmt.Sprintf("%s names no derived_from source", r.component.Name)}
	}
	var state []string
	for _, name := range data.DerivedFrom {
		source, ok := componentNamed(r.all, name)
		if !ok || source.Data == nil {
			state = append(state, fmt.Sprintf("derived_from source %s is not a data component of the accepted map", name))
			continue
		}
		problems := r.sourceProblems(ctx, source)
		if len(problems) == 0 {
			state = append(state, fmt.Sprintf("source %s passed its store-state checks", name))
		}
		for _, problem := range problems {
			state = append(state, fmt.Sprintf("source %s %s", name, problem))
		}
	}
	return state
}

// podReadContext carries pod-read results into a detection context.
type podReadContext struct {
	sdk.DetectionContext
	results map[string]podread.Result
}

func (c podReadContext) PodReadResult(id string) (podread.Result, bool) {
	result, ok := c.results[id]
	return result, ok
}

// sourceProblems reads a source store's facts on each of its Pods and judges
// them with the store-state detector's rules: a write block, a refused read or
// a read with no answer is a problem.
func (r storeRun) sourceProblems(ctx context.Context, source appmap.Component) []string {
	engine := storestate.Engine(source.Data.Engine)
	adapter, ok := storestate.AdapterFor(engine)
	if !ok {
		return []string{fmt.Sprintf("(%s) has no store-state reader, so SDO cannot show it is healthy", engine)}
	}
	detector, err := storestate.NewDetector(storestate.Config{Spec: sdk.DetectorSpec{ID: "store-guard-" + source.Name}, Component: source.Name, Engine: engine, Target: podread.Target{Image: "."}})
	if err != nil {
		return []string{err.Error()}
	}
	consumer, ok := detector.(podread.Consumer)
	if !ok || len(consumer.PodReads()) != 1 {
		return []string{"cannot be read: the store-state detector declares no pod read"}
	}
	query := consumer.PodReads()[0]
	view, err := r.access.Snapshot.Snapshot(ctx)
	if err != nil {
		return []string{"cannot be read: cluster snapshot unavailable"}
	}
	image := ""
	if ops, ok := storeops.For(string(engine)); ok {
		image = ops.Image()
	}
	result := podread.Result{Query: query}
	for _, pod := range view.Pods() {
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}
		if pod.Namespace != r.incident.Namespace || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || owningWorkload(view, ref) != source.Workload {
			continue
		}
		container := storeContainer(pod, image)
		stdout, code, err := r.run(ctx, ref, container, string(pod.UID), storeops.Command{Args: adapter.ReadCommand(storestate.Endpoint{Port: adapter.DefaultPort()})}, maxStoreOutputBytes, storeCommandTimeout)
		reading := podread.Reading{Namespace: pod.Namespace, Pod: pod.Name, UID: string(pod.UID), Container: container, ObservedAt: r.executor.clock()(), ExitCode: code, Stdout: stdout}
		if err != nil {
			reading.Error = err.Error()
		}
		result.Readings = append(result.Readings, reading)
	}
	if len(result.Readings) == 0 {
		return []string{fmt.Sprintf("has no running Pod of %s to read", source.Workload)}
	}
	findings, err := detector.Detect(ctx, podReadContext{DetectionContext: view, results: map[string]podread.Result{query.ID: result}})
	if err != nil {
		return []string{"cannot be read: " + err.Error()}
	}
	var problems []string
	for _, finding := range findings {
		problems = append(problems, "fails its store-state check: "+finding.Summary)
	}
	return problems
}

// storeContainer picks the container whose image matches the engine, or the
// Pod's first container.
func storeContainer(pod corev1.Pod, image string) string {
	if image != "" {
		pattern := regexp.MustCompile(image)
		for _, container := range pod.Spec.Containers {
			if pattern.MatchString(container.Image) {
				return container.Name
			}
		}
	}
	if len(pod.Spec.Containers) == 0 {
		return ""
	}
	return pod.Spec.Containers[0].Name
}

// applyStoreAction runs one store action. wrote reports that the store may
// have changed, even when err is set.
func (e PlanExecutor) applyStoreAction(ctx context.Context, incident IncidentRequest, action program.Action, uid string, report *PlanActionReport) (wrote bool, err error) {
	target, _, _ := program.DataActionEffect(action)
	run, err := e.groundStore(ctx, incident, target, uid)
	if err != nil {
		return false, err
	}
	var effect program.DataRepairEffect
	switch a := program.DataAction(action).(type) {
	case program.StoreBackup:
		record, err := run.backup(ctx, a)
		if err != nil {
			return false, err
		}
		report.StoreBackup = &record
		return false, nil
	case program.RepairData:
		effect, err = run.repair(ctx, a)
	case program.RestoreBackup:
		effect, err = run.restore(ctx, a)
	case program.InvalidateDerived:
		effect, err = run.invalidate(ctx, a)
	case program.RestoreStoreItem:
		restored, wrote, err := run.restoreItem(ctx, a)
		if wrote || restored.Already {
			report.StoreRestore = &restored
		}
		return wrote, err
	case program.CheckStoreCredential:
		check, err := run.checkCredential(ctx, a)
		if err != nil {
			return false, err
		}
		report.StoreCredential = &check
		return false, nil
	default:
		return false, fmt.Errorf("unsupported store action %T", action)
	}
	report.DataRepair = &effect
	if errors.Is(err, errStoreUnchanged) {
		report.DataRepair = nil
		return false, errors.New(strings.TrimPrefix(err.Error(), errStoreUnchanged.Error()+": "))
	}
	return true, err
}

// storePatchOperation names a store write in the application's patch list.
func storePatchOperation(action program.Action) string {
	switch program.DataAction(action).(type) {
	case program.RepairData:
		return "repair_data"
	case program.RestoreBackup:
		return "restore_backup"
	case program.InvalidateDerived:
		return "invalidate_derived"
	case program.RestoreStoreItem:
		return "restore_store_item"
	case program.CheckStoreCredential:
		return "check_store_credential"
	}
	return "store_backup"
}
