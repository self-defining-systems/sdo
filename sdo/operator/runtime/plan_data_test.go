package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// storeArchitecture marks db the authoritative redis store and cache a redis
// copy derived from it; web holds no data and declares none.
const storeArchitecture = "# Architecture\n\n<!-- sdo:application-map:begin -->\n```yaml\nversion: 1\ncomponents:\n" +
	"  - {name: db, workload: StatefulSet/db, services: [db], holds_data: true, data: {engine: redis, role: authoritative}}\n" +
	"  - {name: cache, workload: Deployment/cache, services: [cache], holds_data: false, data: {engine: redis, role: derived, derived_from: [db]}}\n" +
	"  - {name: web, workload: Deployment/web, services: [web], holds_data: false}\n" +
	"```\n<!-- sdo:application-map:end -->\n"

// healthyRedisRead is synthetic output of the store-state redis read.
const healthyRedisRead = "== info\nrole:master\nconnected_slaves:0\nconnected_clients:3\nmaxclients:10000\nused_memory:1048576\nmaxmemory:0\nmaxmemory_policy:noeviction\nrdb_last_bgsave_status:ok\naof_last_write_status:ok\ndb0:keys=2,expires=0,avg_ttl=0\n" +
	"== dbsize\n2\n== role\nmaster\n== config\nmaxmemory\n0\nmaxmemory-policy\nnoeviction\nmaxclients\n10000\nmin-replicas-to-write\n0\nmin-replicas-max-lag\n10\nreplica-read-only\nyes\nstop-writes-on-bgsave-error\nyes\nappendonly\nno\nrequirepass\nempty\n== acl\nuser default on nopass ~* &* +@all\n"

func storePodRef(name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: name}
}

// storeView is the cluster of storeArchitecture.
func storeView() sdktest.Snapshot {
	controller := true
	owned := func(kind, name string) []metav1.OwnerReference {
		return []metav1.OwnerReference{{Kind: kind, Name: name, Controller: &controller}}
	}
	pod := func(name, uid string, owners []metav1.OwnerReference) corev1.Pod {
		return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", UID: types.UID(uid), OwnerReferences: owners},
			Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "redis", Image: "redis:7"}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	}
	return sdktest.Snapshot{
		NamespaceName:   "app",
		StatefulSetList: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "app"}}},
		DeploymentList:  []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "app"}}, {ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"}}},
		ReplicaSetList: []appsv1.ReplicaSet{
			{ObjectMeta: metav1.ObjectMeta{Name: "cache-1", Namespace: "app", OwnerReferences: owned("Deployment", "cache")}},
			{ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "app", OwnerReferences: owned("Deployment", "web")}},
		},
		PodList: []corev1.Pod{
			pod("db-0", "uid-db-0", owned("StatefulSet", "db")),
			pod("cache-1-a", "uid-cache", owned("ReplicaSet", "cache-1")),
			pod("web-1-a", "uid-web", owned("ReplicaSet", "web-1")),
		},
	}
}

// fakeRedisStores is a synthetic redis per Pod that answers SDO's store
// commands, recording each operation in order. Values are sets.
type fakeRedisStores struct {
	view   sdktest.Snapshot
	sets   map[string]map[string][]string // pod -> key -> members
	facts  map[string]string              // pod -> store-state read output
	broken map[string]bool                // pod -> exec gives no answer
	ops    []string
}

func newFakeRedisStores() *fakeRedisStores {
	return &fakeRedisStores{view: storeView(),
		sets: map[string]map[string][]string{
			"db-0":      {"item:1": {"good", "poison"}, "item:2": {"good"}, "other:1": {"x"}},
			"cache-1-a": {"item:1": {"stale"}, "item:2": {"stale"}, "item:3": {"stale"}},
		},
		facts:  map[string]string{},
		broken: map[string]bool{},
	}
}

func (f *fakeRedisStores) Get(_ context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	for _, pod := range f.view.PodList {
		if pod.Name == ref.Name && pod.Namespace == ref.Namespace && ref.Kind == "Pod" {
			object := &unstructured.Unstructured{Object: map[string]any{}}
			object.SetAPIVersion("v1")
			object.SetKind("Pod")
			object.SetNamespace(pod.Namespace)
			object.SetName(pod.Name)
			object.SetUID(pod.UID)
			object.SetResourceVersion("1")
			return object, nil
		}
	}
	return nil, fmt.Errorf("synthetic: %s not found", ref.Name)
}

func (f *fakeRedisStores) Patch(context.Context, sdk.ObjectRef, types.PatchType, []byte) error {
	return fmt.Errorf("synthetic: no patches")
}

func (f *fakeRedisStores) matching(pod, glob string) []string {
	var keys []string
	for key := range f.sets[pod] {
		if ok, _ := path.Match(glob, key); ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func (f *fakeRedisStores) StoreCommand(_ context.Context, uid string, command program.Command, stdout io.Writer) (int32, error) {
	pod := command.Pod.Name
	if f.broken[pod] {
		f.ops = append(f.ops, "unanswered:"+pod)
		return 0, fmt.Errorf("synthetic: exec transport failed")
	}
	for _, p := range f.view.PodList {
		if p.Name == pod && string(p.UID) != uid {
			return 0, fmt.Errorf("synthetic: command Pod identity changed")
		}
	}
	script := strings.Join(command.Args, "\n")
	if strings.Contains(script, "== info") {
		f.ops = append(f.ops, "facts:"+pod)
		out := healthyRedisRead
		if f.facts[pod] != "" {
			out = f.facts[pod]
		}
		_, _ = io.WriteString(stdout, out)
		return 0, nil
	}
	lua, args := command.Args[6], command.Args[7:]
	hexOf := func(s string) string { return hex.EncodeToString([]byte(s)) }
	switch {
	case strings.Contains(lua, "selected(m)"):
		f.ops = append(f.ops, "remove-members:"+pod)
		removed := 0
		for _, key := range f.matching(pod, args[0]) {
			var kept []string
			for _, member := range f.sets[pod][key] {
				if _, err := strconv.Atoi(member); err != nil && args[4] == "1" {
					removed++
					continue
				}
				kept = append(kept, member)
			}
			f.sets[pod][key] = kept
		}
		_, _ = fmt.Fprintf(stdout, "sdo-affected %d failed 0\n", removed)
	case strings.Contains(lua, "ZUNIONSTORE"):
		f.ops = append(f.ops, "copy:"+pod)
		wrote := 0
		for _, key := range f.matching(pod, args[0]) {
			target := args[6] + strings.TrimSuffix(strings.TrimPrefix(key, args[4]), args[5]) + args[7]
			if _, exists := f.sets[pod][target]; !exists {
				f.sets[pod][target] = append([]string(nil), f.sets[pod][key]...)
				wrote++
			}
		}
		_, _ = fmt.Fprintf(stdout, "sdo-affected %d failed 0\n", wrote)
	case strings.Contains(lua, "'DUMP'"):
		f.ops = append(f.ops, "backup:"+pod)
		keys := f.matching(pod, args[0])
		if limit, _ := strconv.Atoi(args[1]); len(keys) > limit {
			return 4, nil
		}
		for _, key := range keys {
			_, _ = fmt.Fprintf(stdout, "%s -1 %s\n", hexOf(key), hexOf(strings.Join(f.sets[pod][key], ",")))
		}
		_, _ = fmt.Fprintf(stdout, "#sdo-end %d\n", len(keys))
	case strings.Contains(lua, "'RESTORE'"):
		f.ops = append(f.ops, "restore:"+pod)
		n := 0
		for _, line := range strings.Split(strings.TrimSpace(command.Stdin), "\n") {
			fields := strings.Fields(line)
			if len(fields) != 3 || strings.HasPrefix(line, "#sdo-end") {
				continue
			}
			key, _ := hex.DecodeString(fields[0])
			value, _ := hex.DecodeString(fields[2])
			f.sets[pod][string(key)] = strings.Split(string(value), ",")
			n++
		}
		_, _ = fmt.Fprintf(stdout, "sdo-restored %d of %d\n", n, n)
	case strings.Contains(lua, "unpack(args)"):
		keys := f.matching(pod, args[0])
		low, _ := strconv.Atoi(args[1])
		high, _ := strconv.Atoi(args[2])
		if len(keys) < low || len(keys) > high {
			f.ops = append(f.ops, "write-refused:"+pod)
			_, _ = fmt.Fprintf(stdout, "sdo-expect-failed %d\n", len(keys))
			return 0, nil
		}
		f.ops = append(f.ops, "write:"+pod)
		changed := 0
		for _, key := range keys {
			if args[4] != "SREM" {
				continue
			}
			var kept []string
			for _, member := range f.sets[pod][key] {
				if member != args[5] {
					kept = append(kept, member)
				}
			}
			if len(kept) != len(f.sets[pod][key]) {
				changed++
			}
			f.sets[pod][key] = kept
		}
		_, _ = fmt.Fprintf(stdout, "sdo-affected %d failed 0\n", changed)
	case strings.Contains(lua, "'DEL', k"):
		f.ops = append(f.ops, "invalidate:"+pod)
		keys := f.matching(pod, args[0])
		for _, key := range keys {
			delete(f.sets[pod], key)
		}
		_, _ = fmt.Fprintf(stdout, "sdo-affected %d\n", len(keys))
	default:
		f.ops = append(f.ops, "count:"+pod)
		_, _ = fmt.Fprintf(stdout, "sdo-count %d\n", len(f.matching(pod, args[0])))
	}
	return 0, nil
}

type storeRollouts struct{}

func (storeRollouts) Wait(context.Context, []sdk.ObjectRef) error { return nil }

type storeVerifier struct{}

func (storeVerifier) Verify(context.Context) (bool, error) { return true, nil }

type storeWorld struct {
	stores   *fakeRedisStores
	backups  *memoryBackupStore
	executor PlanExecutor
	incident IncidentRequest
	now      time.Time
}

func newStoreWorld(t *testing.T) *storeWorld {
	t.Helper()
	w := &storeWorld{stores: newFakeRedisStores(), backups: &memoryBackupStore{}, now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	w.executor = PlanExecutor{Client: w.stores, Rollouts: storeRollouts{}, Verifier: storeVerifier{}, Now: func() time.Time { w.now = w.now.Add(time.Second); return w.now },
		Stores: &StoreAccess{Read: fakeArchitecture(storeArchitecture), Snapshot: riskSnapshots{view: w.stores.view}, Backups: w.backups}}
	w.incident = IncidentRequest{IncidentID: "synthetic-incident", Namespace: "app", SourceCommit: "synthetic-accepted", RepositoryWorktree: "synthetic-worktree", RepairPolicy: "recorded-actions"}
	return w
}

func (w *storeWorld) apply(t *testing.T, actions ...program.Action) (PlanApplicationResult, error) {
	t.Helper()
	w.stores.ops = nil
	return w.executor.Apply(context.Background(), w.incident, program.Plan{Actions: actions}, false)
}

var (
	dbTarget    = program.StoreTarget{Component: "db", Engine: "redis", Pod: storePodRef("db-0"), Container: "redis"}
	cacheTarget = program.StoreTarget{Component: "cache", Engine: "redis", Pod: storePodRef("cache-1-a"), Container: "redis"}
	itemScope   = program.DataScope{Selector: "item:*"}
)

func storeBackup(id string, scope program.DataScope) program.StoreBackup {
	return program.StoreBackup{ID: id, Target: dbTarget, Scope: scope, Reason: "synthetic"}
}

func removePoison(backup string) program.RepairData {
	return program.RepairData{Target: dbTarget, Scope: itemScope, Statement: "SREM poison", ExpectMin: 1, ExpectMax: 5, Backup: backup, Reason: "synthetic"}
}

// A backup records what it holds and changes nothing: its item count, size,
// digest and the store's facts at the time, kept by incident and ID.
func TestStoreBackupRecordsCountSizeDigestAndFacts(t *testing.T) {
	w := newStoreWorld(t)
	result, err := w.apply(t, storeBackup("before", itemScope))
	if err != nil {
		t.Fatal(err)
	}
	record := result.ActionReports[0].StoreBackup
	if record == nil || result.ActionReports[0].Status != "applied" {
		t.Fatalf("report = %+v", result.ActionReports[0])
	}
	stored, dump, err := w.backups.Get("synthetic-incident", "before")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(dump)
	if record.Items != 2 || record.Bytes != int64(len(dump)) || record.SHA256 != hex.EncodeToString(sum[:]) || stored.SHA256 != record.SHA256 {
		t.Fatalf("record = %+v", record)
	}
	if record.Facts == nil || record.Facts.Component != "db" || record.Component != "db" || record.Scope.Database != "0" || record.Scope.Selector != "item:*" {
		t.Fatalf("record must carry the normalized scope and the store's facts: %+v", record)
	}
	if len(result.Patches) != 0 || strings.Join(w.stores.ops, ",") != "backup:db-0,facts:db-0" {
		t.Fatalf("a backup only reads: patches %v ops %v", result.Patches, w.stores.ops)
	}
	if _, err := w.apply(t, storeBackup("before", itemScope)); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("a backup ID is used once per incident: %v", err)
	}
	if _, err := w.apply(t, storeBackup("too-big", program.DataScope{Selector: "item:*", Limit: 1})); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("a scope beyond its limit is refused: %v", err)
	}
}

// RepairData refuses only a count outside the expected bounds or a named
// backup that does not exist in this incident; any refusal writes nothing.
func TestRepairDataRefusesOnlyAnUnexpectedCountOrAMissingNamedBackup(t *testing.T) {
	for name, test := range map[string]struct {
		prepare func(*storeWorld)
		repair  program.RepairData
		refusal string
	}{
		"named backup never taken": {repair: removePoison("before"), refusal: "name a StoreBackup of this incident, or none"},
		"named backup of another incident": {prepare: func(w *storeWorld) {
			incident := w.incident
			w.incident.IncidentID = "synthetic-earlier"
			_, _ = w.apply(t, storeBackup("before", itemScope))
			w.incident = incident
		}, repair: removePoison("before"), refusal: "name a StoreBackup of this incident"},
		"count above the expected bound": {repair: func() program.RepairData { r := removePoison(""); r.ExpectMax = 1; return r }(), refusal: "outside the expected [1, 1]"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newStoreWorld(t)
			if test.prepare != nil {
				test.prepare(w)
			}
			result, err := w.apply(t, test.repair)
			if err == nil || !strings.Contains(err.Error(), test.refusal) {
				t.Fatalf("want refusal %q, got %v", test.refusal, err)
			}
			for _, op := range w.stores.ops {
				if strings.HasPrefix(op, "write") {
					t.Fatalf("a refused repair ran a write: %v", w.stores.ops)
				}
			}
			if len(result.ActionsApplied) != 0 || len(result.Patches) != 0 || result.ActionReports[0].DataRepair != nil {
				t.Fatalf("a refused repair records no effect: %+v", result)
			}
			if members := w.stores.sets["db-0"]["item:1"]; len(members) != 2 {
				t.Fatalf("the store changed: %v", members)
			}
		})
	}
}

// A backup is optional: a repair without one, or with one that does not cover
// its scope, runs and records what it named and whether it covered the scope.
func TestRepairDataRecordsItsBackupInsteadOfRequiringIt(t *testing.T) {
	covers := func(b bool) *bool { return &b }
	for name, test := range map[string]struct {
		prepare func(*storeWorld)
		repair  program.RepairData
		covers  *bool
	}{
		"no backup": {repair: removePoison("")},
		"backup of another scope": {prepare: func(w *storeWorld) {
			_, _ = w.apply(t, storeBackup("before", program.DataScope{Selector: "other:*"}))
		}, repair: removePoison("before"), covers: covers(false)},
		"items matched since the backup": {prepare: func(w *storeWorld) {
			_, _ = w.apply(t, storeBackup("before", itemScope))
			w.stores.sets["db-0"]["item:9"] = []string{"poison"}
		}, repair: removePoison("before"), covers: covers(true)},
	} {
		t.Run(name, func(t *testing.T) {
			w := newStoreWorld(t)
			if test.prepare != nil {
				test.prepare(w)
			}
			result, err := w.apply(t, test.repair)
			if err != nil {
				t.Fatal(err)
			}
			effect := result.ActionReports[0].DataRepair
			if effect == nil || effect.Affected == 0 || effect.Backup != test.repair.Backup {
				t.Fatalf("effect = %+v", effect)
			}
			if (effect.BackupCovers == nil) != (test.covers == nil) || (test.covers != nil && *effect.BackupCovers != *test.covers) {
				t.Fatalf("backup_covers = %v, want %v", effect.BackupCovers, test.covers)
			}
			if members := w.stores.sets["db-0"]["item:1"]; len(members) != 1 || members[0] != "good" {
				t.Fatalf("item:1 = %v", members)
			}
		})
	}
}

// A backed repair counts, writes, counts again and reads the store's facts,
// in that order, and reports each number.
func TestRepairDataCountsWritesAndRechecks(t *testing.T) {
	w := newStoreWorld(t)
	if _, err := w.apply(t, storeBackup("before", itemScope)); err != nil {
		t.Fatal(err)
	}
	result, err := w.apply(t, removePoison("before"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(w.stores.ops, ","); got != "count:db-0,write:db-0,count:db-0,facts:db-0" {
		t.Fatalf("order = %s", got)
	}
	effect := result.ActionReports[0].DataRepair
	if effect == nil || effect.Before != 2 || effect.Affected != 1 || effect.After != 2 || effect.Backup != "before" || effect.Facts == nil {
		t.Fatalf("effect = %+v", effect)
	}
	if len(result.Patches) != 1 || !result.Patches[0].Irreversible || result.Patches[0].Operation != "repair_data" {
		t.Fatalf("patches = %+v", result.Patches)
	}
	if members := w.stores.sets["db-0"]["item:1"]; len(members) != 1 || members[0] != "good" {
		t.Fatalf("item:1 = %v", members)
	}
	// The explicit restore puts the backed-up records back.
	if _, err := w.apply(t, program.RestoreBackup{Target: dbTarget, Backup: "before", Reason: "synthetic"}); err != nil {
		t.Fatal(err)
	}
	if members := w.stores.sets["db-0"]["item:1"]; len(members) != 2 {
		t.Fatalf("restore did not put item:1 back: %v", members)
	}
	if _, err := w.apply(t, program.RestoreBackup{Target: dbTarget, Backup: "never-taken"}); err == nil {
		t.Fatal("a restore needs an existing backup ID")
	}
	if _, err := w.apply(t, program.RestoreBackup{Target: cacheTarget, Backup: "before"}); err == nil || !strings.Contains(err.Error(), "is of db") {
		t.Fatalf("a backup restores only into its own component: %v", err)
	}
}

// A repair may remove the members a predicate selects from every matched
// collection, or copy a renamed key pattern back under its name: the count,
// write, recount and facts read are the same as for a statement, and a copy
// counts its source.
func TestRepairDataRemovesMembersAndCopiesNamespaces(t *testing.T) {
	w := newStoreWorld(t)
	w.stores.sets["db-0"]["item:1"] = []string{"1", "2", "poison"}
	result, err := w.apply(t, program.RepairData{Target: dbTarget, Scope: itemScope, MemberPredicate: &program.MemberPredicate{NotDigits: true}, ExpectMin: 1, ExpectMax: 5, Reason: "synthetic"})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(w.stores.ops, ","); got != "count:db-0,remove-members:db-0,count:db-0,facts:db-0" {
		t.Fatalf("order = %s", got)
	}
	if effect := result.ActionReports[0].DataRepair; effect == nil || effect.Before != 2 || effect.Affected != 2 {
		t.Fatalf("effect = %+v", effect)
	}
	if members := w.stores.sets["db-0"]["item:1"]; strings.Join(members, ",") != "1,2" {
		t.Fatalf("item:1 = %v", members)
	}
	delete(w.stores.sets["db-0"], "item:2")
	w.stores.sets["db-0"]["item:2:bak"] = []string{"9"}
	result, err = w.apply(t, program.RepairData{Target: dbTarget, Scope: program.DataScope{Selector: "item:*"}, CopyFrom: &program.DataScope{Selector: "item:*:bak"}, ExpectMin: 1, ExpectMax: 5, Reason: "synthetic"})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(w.stores.ops, ","); got != "count:db-0,copy:db-0,count:db-0,facts:db-0" {
		t.Fatalf("order = %s", got)
	}
	if effect := result.ActionReports[0].DataRepair; effect == nil || effect.Before != 1 || effect.Affected != 1 {
		t.Fatalf("effect = %+v", effect)
	}
	if members := w.stores.sets["db-0"]["item:2"]; strings.Join(members, ",") != "9" {
		t.Fatalf("item:2 = %v", members)
	}
}

// Dropping a cache runs whatever its source's state; the controller records
// the derivation it ran under, so a flush over a damaged source is visible as
// evidence: whether the map marks the target derived and how each source read.
func TestInvalidateDerivedRecordsTheSourceState(t *testing.T) {
	writeBlocked := strings.Replace(healthyRedisRead, "min-replicas-to-write\n0", "min-replicas-to-write\n1", 1)
	for name, test := range map[string]struct {
		prepare func(*storeWorld)
		target  program.StoreTarget
		pod     string
		state   string
	}{
		"healthy source":        {target: cacheTarget, pod: "cache-1-a", state: "source db passed its store-state checks"},
		"write-blocked source":  {prepare: func(w *storeWorld) { w.stores.facts["db-0"] = writeBlocked }, target: cacheTarget, pod: "cache-1-a", state: "source db fails its store-state check"},
		"unreadable source":     {prepare: func(w *storeWorld) { w.stores.broken["db-0"] = true }, target: cacheTarget, pod: "cache-1-a", state: "source db"},
		"authoritative target":  {target: dbTarget, pod: "db-0", state: "does not mark db data.role derived"},
		"no running source Pod": {prepare: func(w *storeWorld) { w.stores.view.PodList[0].Status.Phase = corev1.PodPending }, target: cacheTarget, pod: "cache-1-a", state: "no running Pod"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newStoreWorld(t)
			if test.prepare != nil {
				test.prepare(w)
				w.executor.Stores.Snapshot = riskSnapshots{view: w.stores.view}
			}
			result, err := w.apply(t, program.InvalidateDerived{Target: test.target, Scope: itemScope, Reason: "synthetic"})
			if err != nil {
				t.Fatal(err)
			}
			effect := result.ActionReports[0].DataRepair
			remaining := 0
			for key := range w.stores.sets[test.pod] {
				if strings.HasPrefix(key, "item:") {
					remaining++
				}
			}
			if effect == nil || effect.Affected == 0 || remaining != 0 {
				t.Fatalf("effect %+v, store %v", effect, w.stores.sets[test.pod])
			}
			if !strings.Contains(strings.Join(effect.SourceState, "; "), test.state) {
				t.Fatalf("source_state = %v, want it to record %q", effect.SourceState, test.state)
			}
		})
	}
}

// Every store action is grounded in the accepted map: the Pod belongs to the
// component, the engine is the one the map declares, and the component
// declares data at all.
func TestStoreActionsAreGroundedInTheAcceptedMap(t *testing.T) {
	for name, test := range map[string]struct {
		target   program.StoreTarget
		document string
		refusal  string
	}{
		"Pod of another component": {target: program.StoreTarget{Component: "db", Engine: "redis", Pod: storePodRef("cache-1-a"), Container: "redis"}, document: storeArchitecture, refusal: "belongs to Deployment/cache"},
		"engine mismatch":          {target: program.StoreTarget{Component: "db", Engine: "postgres", Pod: storePodRef("db-0"), Container: "redis"}, document: storeArchitecture, refusal: `is engine "redis"`},
		"component without data":   {target: program.StoreTarget{Component: "web", Engine: "redis", Pod: storePodRef("web-1-a"), Container: "redis"}, document: storeArchitecture, refusal: "declares no data role"},
		"unknown component":        {target: program.StoreTarget{Component: "nope", Engine: "redis", Pod: storePodRef("db-0"), Container: "redis"}, document: storeArchitecture, refusal: "no component"},
		"no accepted map":          {target: dbTarget, document: "# Architecture\n", refusal: "no application map"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newStoreWorld(t)
			w.executor.Stores.Read = fakeArchitecture(test.document)
			scope := program.DataScope{}
			if test.target.Engine == "postgres" {
				scope.Collection = "items"
			}
			_, err := w.apply(t, program.StoreBackup{ID: "b", Target: test.target, Scope: scope, Reason: "synthetic"})
			if err == nil || !strings.Contains(err.Error(), test.refusal) {
				t.Fatalf("want refusal %q, got %v", test.refusal, err)
			}
			if len(w.stores.ops) != 0 {
				t.Fatalf("an ungrounded action ran %v", w.stores.ops)
			}
		})
	}
	w := newStoreWorld(t)
	w.executor.Stores = nil
	if _, err := w.apply(t, storeBackup("b", itemScope)); err == nil || !strings.Contains(err.Error(), "no store access") {
		t.Fatalf("without store access every store action is refused: %v", err)
	}
}

// The automatic stage refuses no typed store action by its data effect; the
// effect is recorded instead.
func TestAutomaticStageRunsStoreActions(t *testing.T) {
	view := storeView()
	holders := dataHolders{"StatefulSet/db": {Name: "db", Workload: "StatefulSet/db", Services: []string{"db"}, HoldsData: true}}
	for name, action := range map[string]program.Action{
		"repair":                removePoison("before"),
		"restore":               program.RestoreBackup{Target: dbTarget, Backup: "before"},
		"backup":                storeBackup("before", itemScope),
		"invalidate a derived":  program.InvalidateDerived{Target: cacheTarget},
		"invalidate a data pod": program.InvalidateDerived{Target: program.StoreTarget{Component: "cache", Engine: "redis", Pod: storePodRef("db-0"), Container: "redis"}},
	} {
		t.Run(name, func(t *testing.T) {
			for _, holders := range []dataHolders{holders, nil} {
				if got := dataWorkloadWrite(view, program.Plan{Actions: []program.Action{action}}, holders, "app"); got != "" {
					t.Fatalf("dataWorkloadWrite = %q, want the action to run", got)
				}
			}
		})
	}
}

// Store actions declare their effect precisely instead of unknown.
func TestStoreActionsDeclareTheirDataEffect(t *testing.T) {
	reach := dataReach{view: storeView(), holders: dataHolders{"StatefulSet/db": {Name: "db", Workload: "StatefulSet/db", HoldsData: true}}, namespace: "app"}
	for action, want := range map[program.Action]program.DataEffect{
		storeBackup("b", itemScope):                          program.EffectRead,
		removePoison("b"):                                    program.EffectDataWrite,
		program.RestoreBackup{Target: dbTarget, Backup: "b"}: program.EffectDataWrite,
	} {
		effects := reach.effects(action)
		if len(effects.touches) != 1 || effects.touches[0] != (program.Touch{Component: "db", Effect: want}) || effects.unreadInput {
			t.Fatalf("%T effects = %+v", action, effects)
		}
	}
	if effects := reach.effects(program.InvalidateDerived{Target: cacheTarget}); len(effects.touches) != 0 {
		t.Fatalf("a derived copy is not a holds_data component: %+v", effects)
	}
	// Dropping the entries of a store the map does not mark derived drops
	// records: it is recorded as a data write.
	authoritative := dataHolders{"StatefulSet/db": {Name: "db", Workload: "StatefulSet/db", HoldsData: true, Data: &appmap.Data{Engine: "redis", Role: appmap.RoleAuthoritative}}}
	reach.holders = authoritative
	if effects := reach.effects(program.InvalidateDerived{Target: dbTarget}); len(effects.touches) != 1 || effects.touches[0].Effect != program.EffectDataWrite {
		t.Fatalf("invalidating an authoritative store = %+v, want data-write", effects)
	}
	derived := dataHolders{"StatefulSet/db": {Name: "db", Workload: "StatefulSet/db", HoldsData: true, Data: &appmap.Data{Engine: "redis", Role: appmap.RoleDerived, DerivedFrom: []string{"origin"}}}}
	reach.holders = derived
	if effects := reach.effects(program.InvalidateDerived{Target: dbTarget}); len(effects.touches) != 1 || effects.touches[0].Effect != program.EffectDerivedInvalidate {
		t.Fatalf("invalidating a derived holds_data store = %+v, want derived-invalidate", effects)
	}
}

func backupReport(id string, scope program.DataScope, at time.Time) RecordedPlanApplication {
	record := program.StoreBackupRecord{ID: id, IncidentID: "synthetic-incident", Component: "db", Engine: "redis", Pod: dbTarget.Pod, Scope: scope, At: at}
	action := storeBackup(id, scope)
	return RecordedPlanApplication{ID: "backup-" + id, Result: PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{action}}, ActionsApplied: []program.Action{action},
		ActionReports: []PlanActionReport{{ActionIndex: 0, Status: "applied", StoreBackup: &record}}}}
}

func writeApplication(action program.Action) RecordedPlanApplication {
	return RecordedPlanApplication{ID: "write", Result: PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{action}}, ActionsApplied: []program.Action{action},
		ActionReports: []PlanActionReport{{ActionIndex: 0, Status: "applied"}}}}
}

// Every applied data effect on a holds_data store is recorded, with whether a
// backup of this incident held what a write could change. The record never
// refuses anything or changes the outcome.
func TestAppliedDataEffectsRecordBackupCoverage(t *testing.T) {
	holders := dataHolders{"StatefulSet/db": {Name: "db", Workload: "StatefulSet/db", Services: []string{"db"}, HoldsData: true}}
	at := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	whole := program.DataScope{}
	restore := program.RestoreBackup{Target: dbTarget, Backup: "before"}
	opaque := program.EnsureExec{Observe: program.Command{Pod: dbTarget.Pod, Container: "redis", Args: []string{"true"}}, Ensure: program.Command{Pod: dbTarget.Pod, Container: "redis", Args: []string{"redis-cli", "FLUSHALL"}}}
	const held, notHeld = "a backup of this incident held", "no backup of this incident held"
	for name, test := range map[string]struct {
		applications []RecordedPlanApplication
		want         []string
	}{
		"repair with its covering backup":   {[]RecordedPlanApplication{backupReport("before", itemScope, at), writeApplication(removePoison("before"))}, []string{"data repair", "effect data-write", held}},
		"repair with a whole-store backup":  {[]RecordedPlanApplication{backupReport("before", whole, at), writeApplication(removePoison("before"))}, []string{"data repair", held}},
		"repair naming no recorded backup":  {[]RecordedPlanApplication{writeApplication(removePoison("before"))}, []string{"data repair", notHeld}},
		"repair naming no backup at all":    {[]RecordedPlanApplication{writeApplication(removePoison(""))}, []string{"data repair", notHeld}},
		"repair with a narrower backup":     {[]RecordedPlanApplication{backupReport("before", program.DataScope{Selector: "item:1"}, at), writeApplication(removePoison("before"))}, []string{"data repair", notHeld}},
		"restore without a fresh backup":    {[]RecordedPlanApplication{backupReport("before", itemScope, at), writeApplication(restore)}, []string{"backup restore", notHeld}},
		"restore after a fresh backup":      {[]RecordedPlanApplication{backupReport("before", itemScope, at), backupReport("pre-restore", itemScope, at.Add(time.Minute)), writeApplication(restore)}, []string{"backup restore", held}},
		"restore after an older other":      {[]RecordedPlanApplication{backupReport("before", itemScope, at), backupReport("older", itemScope, at.Add(-time.Minute)), writeApplication(restore)}, []string{"backup restore", notHeld}},
		"opaque command even with a backup": {[]RecordedPlanApplication{backupReport("before", whole, at), writeApplication(opaque)}, []string{"exec", "effect unknown", notHeld}},
		"repair whose backup was not stored": {[]RecordedPlanApplication{func() RecordedPlanApplication {
			r := backupReport("before", itemScope, at)
			r.Result.ActionReports[0].Status = "unknown"
			return r
		}(), writeApplication(removePoison("before"))}, []string{"data repair", notHeld}},
	} {
		t.Run(name, func(t *testing.T) {
			records := appliedDataEffects(storeView(), test.applications, holders, "app")
			var writes []string
			for _, record := range records {
				if !strings.Contains(record, "effect read") {
					writes = append(writes, record)
				}
			}
			requireDataEffectRecord(t, "data effects", writes, test.want)
		})
	}
	backupOnly := appliedDataEffects(storeView(), []RecordedPlanApplication{backupReport("before", itemScope, at)}, holders, "app")
	if len(backupOnly) != 1 || !strings.Contains(backupOnly[0], "store backup") || !strings.Contains(backupOnly[0], "effect read") || strings.Contains(backupOnly[0], "backup of this incident") {
		t.Fatalf("a backup alone records a read: %v", backupOnly)
	}
}

// Store actions survive the plan's persisted form.
func TestStoreActionsPersist(t *testing.T) {
	actions := []program.Action{storeBackup("b", itemScope), program.InvalidateDerived{Target: cacheTarget, Scope: itemScope}, removePoison("b"), program.RestoreBackup{Target: dbTarget, Backup: "b"}}
	encoded, err := encodePersistedActions(actions)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodePersistedActions(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !planValuesEqual(decoded, actions) {
		t.Fatalf("decoded %+v", decoded)
	}
	for _, action := range actions {
		wire, err := planActionProto(action)
		if err != nil {
			t.Fatal(err)
		}
		if back := planActionState(wire); !planValuesEqual(back, action) {
			t.Fatalf("wire round trip of %T = %+v", action, back)
		}
	}
}
