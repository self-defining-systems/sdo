package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// dataHolders are the components the accepted application map marks
// holds_data, by workload ("Kind/name").
type dataHolders map[string]appmap.Component

// acceptedDataWorkloads reads the holds_data components of the accepted
// application map. No map marks nothing, and an unreadable or malformed one is
// the same: the shared check keeps a broken map out of accepted memory.
func acceptedDataWorkloads(ctx context.Context, read gitRead, repository, commit string) dataHolders {
	if repository == "" || commit == "" {
		return nil
	}
	document, err := read(ctx, repository, "show", commit+":"+layout.ArchitectureFile)
	if err != nil {
		return nil
	}
	region, found, err := appmap.Find(string(document))
	if !found {
		return nil
	}
	var parsed appmap.Map
	if err == nil {
		parsed, err = appmap.Parse(region.YAML)
	}
	if err != nil {
		slog.Warn("application map unreadable; no workload is treated as holding data", "commit", commit, "error", err)
		return nil
	}
	holders := dataHolders{}
	for _, component := range parsed.Components {
		if component.HoldsData {
			holders[component.Workload] = component
		}
	}
	return holders
}

// closureDataEffects records, once as the closure freezes, the data effects
// its applied actions had on holds_data components. They are evidence on the
// outcome, never a verdict.
func (c *Controller) closureDataEffects(ctx context.Context, snapshot sdk.DetectionContext, closure IncidentClosure) []string {
	if snapshot == nil || len(closure.FinalDetectorStates) == 0 || closure.Result == nil {
		return nil
	}
	read := c.gitRead
	if read == nil {
		read = lifecycleio.GitOutput
	}
	workloads := acceptedDataWorkloads(ctx, read, closure.Request.RepositoryWorktree, closure.Request.SourceCommit)
	return appliedDataEffects(snapshot, closure.Result.PlanApplications, workloads, closure.Request.Namespace)
}

// incidentDataEffects records, on each verify, the data effects the incident's
// applied actions had on holds_data components. Caller holds l.mu.
func (l *ResponderLoop) incidentDataEffects(ctx context.Context) []string {
	if !l.dataWorkloadsRead {
		l.dataWorkloads = acceptedDataWorkloads(ctx, l.gitRead(), l.Request.RepositoryWorktree, l.Request.SourceCommit)
		l.dataWorkloadsRead = true
	}
	if len(l.dataWorkloads) == 0 {
		return nil
	}
	view, err := l.snapshotView(ctx)
	if err != nil {
		return nil
	}
	return appliedDataEffects(view, l.applicationEvidenceLocked(), l.dataWorkloads, l.Request.Namespace)
}

// unverifiedRecoveryOutcome reports an outcome whose recovery SDO could not
// verify. It holds no verified repair, whatever its receipts say.
func unverifiedRecoveryOutcome(outcome *contracts.OutcomeRecord) bool {
	return validate.UnverifiedRecoveryOutcome(outcome)
}

// actionValue reads a program.Action given by pointer as its value.
func actionValue(action program.Action) program.Action {
	value := reflect.ValueOf(action)
	if value.Kind() == reflect.Pointer && !value.IsNil() {
		//assertcheck:ok reflect Elem of a pointer to a program.Action
		return value.Elem().Interface().(program.Action)
	}
	return action
}

// owningWorkload walks a Pod or ReplicaSet up its controller owners to the
// "Kind/name" the application map uses.
func owningWorkload(view sdk.DetectionContext, ref sdk.ObjectRef) string {
	kind, name := ref.Kind, ref.Name
	if kind == "Pod" {
		for _, pod := range view.Pods() {
			if pod.Namespace == ref.Namespace && pod.Name == name {
				if owner := metav1.GetControllerOf(&pod); owner != nil {
					kind, name = owner.Kind, owner.Name
				}
				break
			}
		}
	}
	if kind == "ReplicaSet" {
		for _, replicaSet := range view.ReplicaSets() {
			if replicaSet.Namespace == ref.Namespace && replicaSet.Name == name {
				if owner := metav1.GetControllerOf(&replicaSet); owner != nil {
					kind, name = owner.Kind, owner.Name
				}
				break
			}
		}
	}
	return kind + "/" + name
}

// workloads lists the holds_data workloads in a stable order.
func (h dataHolders) workloads() []string {
	workloads := make([]string, 0, len(h))
	for workload := range h {
		workloads = append(workloads, workload)
	}
	sort.Strings(workloads)
	return workloads
}

// named returns the holds_data component with this map name.
func (h dataHolders) named(name string) appmap.Component {
	for _, component := range h {
		if component.Name == name {
			return component
		}
	}
	return appmap.Component{}
}

// dataEffects is what SDO can see of one action's reach into the components
// the application map marks holds_data.
type dataEffects struct {
	verb    string
	touches []program.Touch
	// unreadInput is an input the action uses that cannot be read here, so it
	// may reach any holds_data component.
	unreadInput bool
}

// dataReach classifies actions against one view of the cluster.
type dataReach struct {
	view      sdk.DetectionContext
	holders   dataHolders
	namespace string
}

// effects assigns an external action its data effect on each holds_data
// component it reaches. SDO computes this from what it can see, and nothing an
// author writes lowers it. An opaque command, script or Job reaches the
// component it runs in and every one its inputs name (a Service, workload or
// Pod, by object or by name in its text), and its effect there is unknown. A
// typed database user change is a store setting on its target. Every other
// action has no external data effect.
func (r dataReach) effects(action program.Action) dataEffects {
	result := dataEffects{}
	reached := map[string]bool{}
	effect := program.EffectUnknown
	object := func(ref sdk.ObjectRef) {
		if component := r.component(ref); component != "" {
			reached[component] = true
		}
	}
	text := func(text string) {
		for _, component := range r.named(text) {
			reached[component] = true
		}
	}
	value := func(value program.Value) {
		if value.Origin() != program.ParameterValue {
			object(value.Ref())
		}
		resolved, _, err := program.ResolveValue(value, program.ValueInputs{})
		if err != nil {
			result.unreadInput = true
			return
		}
		text(fmt.Sprint(resolved))
	}
	if target, declared, ok := program.DataActionEffect(action); ok {
		// A typed store action declares its effect: the executor runs only
		// the engine's fixed command for the class on the component the
		// accepted map grounds. Dropping the entries of a component the map
		// does not mark derived drops records, so it is a data write.
		result.verb = storeActionVerb(action)
		effect = declared
		if holder := r.holders.named(target.Component); effect == program.EffectDerivedInvalidate && holder.HoldsData && (holder.Data == nil || holder.Data.Role != appmap.RoleDerived) {
			effect = program.EffectDataWrite
		}
		object(target.Pod)
		if r.holders.named(target.Component).HoldsData {
			reached[target.Component] = true
		}
		for _, workload := range r.holders.workloads() {
			if name := r.holders[workload].Name; reached[name] {
				result.touches = append(result.touches, program.Touch{Component: name, Effect: effect})
			}
		}
		return result
	}
	switch a := actionValue(action).(type) {
	case program.Exec:
		result.verb = "exec"
		object(a.Pod)
		for _, word := range a.Command {
			value(word)
		}
	case program.EnsureExec:
		result.verb = "exec"
		for _, command := range []program.Command{a.Observe, a.Ensure} {
			object(command.Pod)
			text(strings.Join(command.Args, " "))
			text(command.Stdin)
		}
	case program.EnsureHTTP:
		result.verb = "http request"
		for _, request := range []program.HTTPRequest{a.Observe, a.Ensure} {
			object(request.Pod)
			text(request.URL)
			text(request.Body)
			for _, header := range request.Headers {
				text(header)
			}
		}
	case program.RunScript:
		result.verb = "script"
		text(a.Script)
		for _, input := range a.Env {
			value(input)
		}
	case program.RunJob:
		result.verb = "job"
		// A round trip turns typed Go values into the plain JSON shape.
		var manifest any
		encoded, err := json.Marshal(a.Object.Object)
		if err == nil {
			err = json.Unmarshal(encoded, &manifest)
		}
		if err != nil || len(a.Object.Object) == 0 {
			result.unreadInput = true
		}
		for _, word := range manifestStrings(manifest) {
			text(word)
		}
	case program.EnsureDatabaseUser:
		result.verb = "database user change"
		effect = program.EffectStoreSetting
		object(a.Target.Pod)
	default:
		return dataEffects{}
	}
	for _, workload := range r.holders.workloads() {
		if name := r.holders[workload].Name; reached[name] {
			result.touches = append(result.touches, program.Touch{Component: name, Effect: effect})
		}
	}
	return result
}

// component names the holds_data component that ref is, or belongs to: one of
// its Services, its workload, or one of that workload's Pods or ReplicaSets.
func (r dataReach) component(ref sdk.ObjectRef) string {
	if ref.Name == "" || (ref.Namespace != "" && ref.Namespace != r.namespace) {
		return ""
	}
	ref.Namespace = r.namespace
	if ref.Kind == "Service" {
		for _, workload := range r.holders.workloads() {
			if slices.Contains(r.holders[workload].Services, ref.Name) {
				return r.holders[workload].Name
			}
		}
		return ""
	}
	return r.holders[owningWorkload(r.view, ref)].Name
}

// named lists the holds_data components whose Services, workload or Pods the
// text names as a whole DNS name, as in a host, a URL or a kubectl argument.
func (r dataReach) named(text string) []string {
	if text == "" {
		return nil
	}
	var components []string
	for _, workload := range r.holders.workloads() {
		holder := r.holders[workload]
		_, name, _ := strings.Cut(workload, "/")
		candidates := append([]string{name}, holder.Services...)
		for _, pod := range r.view.Pods() {
			if pod.Namespace == r.namespace && owningWorkload(r.view, sdk.ObjectRef{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}) == workload {
				candidates = append(candidates, pod.Name)
			}
		}
		if slices.ContainsFunc(candidates, func(candidate string) bool { return mentionsName(text, candidate) }) {
			components = append(components, holder.Name)
		}
	}
	return components
}

// mentionsName holds when name appears in text with no DNS label character
// (a lowercase letter, digit or '-') on either side.
func mentionsName(text, name string) bool {
	if name == "" {
		return false
	}
	label := func(b byte) bool { return b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '-' }
	for start := 0; start < len(text); {
		index := strings.Index(text[start:], name)
		if index < 0 {
			return false
		}
		index += start
		end := index + len(name)
		if (index == 0 || !label(text[index-1])) && (end == len(text) || !label(text[end])) {
			return true
		}
		start = index + 1
	}
	return false
}

// manifestStrings lists the string values of a manifest (its arguments,
// environment, hosts), not its field names.
func manifestStrings(value any) []string {
	switch typed := value.(type) {
	case string:
		return []string{typed}
	case map[string]any:
		var words []string
		for _, item := range typed {
			words = append(words, manifestStrings(item)...)
		}
		return words
	case []any:
		var words []string
		for _, item := range typed {
			words = append(words, manifestStrings(item)...)
		}
		return words
	}
	return nil
}

// mayChangeRecords is an effect that can change a store's records.
func mayChangeRecords(effect program.DataEffect) bool {
	return effect == program.EffectDataWrite || effect == program.EffectUnknown
}

// appliedDataEffects records, as evidence, the data effect each applied action
// had on each holds_data component it reached during the incident, and whether
// a backup of this incident held the records a write could change. Nothing
// here refuses an action or changes the outcome: the responder and learned
// programs may do anything, and the record says what SDO could see of it. Only
// inputs SDO can still see count: a recorded plan keeps its objects but not
// the text of its bound values.
func appliedDataEffects(view sdk.DetectionContext, applications []RecordedPlanApplication, holders dataHolders, namespace string) []string {
	if len(holders) == 0 || view == nil {
		return nil
	}
	reach := dataReach{view: view, holders: holders, namespace: namespace}
	seen := map[string]bool{}
	var records []string
	backups := appliedBackups(applications)
	for _, application := range applications {
		for _, action := range application.Result.ActionsApplied {
			effects := reach.effects(action)
			backed := backedStoreWrite(action, backups)
			for _, touch := range effects.touches {
				workload := holders.named(touch.Component).Workload
				record := fmt.Sprintf("data effect: a %s reached %s (holds_data) with effect %s", effects.verb, workload, touch.Effect)
				if mayChangeRecords(touch.Effect) {
					if backed {
						record += "; a backup of this incident held the records it could change"
					} else {
						record += "; no backup of this incident held the records it could change"
					}
				}
				if !seen[record] {
					seen[record] = true
					records = append(records, record)
				}
			}
			if effects.unreadInput {
				record := fmt.Sprintf("data effect: a %s used an input SDO could not read, so it may have reached any holds_data workload (%s) with effect unknown", effects.verb, strings.Join(holders.workloads(), ", "))
				if !seen[record] {
					seen[record] = true
					records = append(records, record)
				}
			}
		}
	}
	sort.Strings(records)
	return records
}

// appliedBackups lists the StoreBackup records the executor verified during
// the incident.
func appliedBackups(applications []RecordedPlanApplication) []program.StoreBackupRecord {
	var backups []program.StoreBackupRecord
	for _, application := range applications {
		for _, report := range application.Result.ActionReports {
			if report.Status == "applied" && report.StoreBackup != nil {
				backups = append(backups, *report.StoreBackup)
			}
		}
	}
	return backups
}

// backedStoreWrite holds for a typed store write whose overwritten records a
// backup of this incident holds: the backup a RepairData names, covering its
// scope; for a RestoreBackup, a backup of the restored scope taken after the
// one it restores.
func backedStoreWrite(action program.Action, backups []program.StoreBackupRecord) bool {
	switch a := program.DataAction(action).(type) {
	case program.RepairData:
		for _, backup := range backups {
			if backup.ID == a.Backup && backup.Covers(a.Target, a.Scope) {
				return true
			}
		}
	case program.RestoreBackup:
		for _, restored := range backups {
			if restored.ID != a.Backup {
				continue
			}
			for _, fresh := range backups {
				if fresh.ID != a.Backup && fresh.At.After(restored.At) && fresh.Covers(a.Target, restored.Scope) {
					return true
				}
			}
		}
	}
	return false
}

// storeActionVerb names a typed store action in a data-effect record.
func storeActionVerb(action program.Action) string {
	switch program.DataAction(action).(type) {
	case program.StoreBackup:
		return "store backup"
	case program.RepairData:
		return "data repair"
	case program.RestoreBackup:
		return "backup restore"
	case program.RestoreStoreItem:
		return "store-item restore"
	case program.CheckStoreCredential:
		return "credential check"
	}
	return "derived-store invalidation"
}
