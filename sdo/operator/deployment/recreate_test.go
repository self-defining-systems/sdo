package deployment

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/render/raw"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// recreateFake is a cluster that refuses immutable changes the way the API
// server does: the dry run reports the configured conflicts of the objects in
// the documents, until the object is deleted.
type recreateFake struct {
	clusterFake
	conflicts map[string]ImmutableConflict // by Kind/name
	live      map[string]LiveObject        // by Kind/name
	// finishAfter makes a Job finish after that many reads.
	finishAfter map[string]int
	reads       map[string]int
	dependents  map[string][]LiveObject // by owner UID
	// foreign lists Kind/name objects the deployer did not apply.
	foreign map[string]bool
	// failApplyOnce makes the first Apply fail (an admission webhook).
	failApplyOnce bool
	// listedOrphans, when set, is what Orphaned returns (a stale listing).
	listedOrphans []LiveObject
}

func (c *recreateFake) Apply(ctx context.Context, documents []byte) error {
	if c.failApplyOnce {
		c.failApplyOnce = false
		c.events = append(c.events, "apply-failed")
		return errors.New("admission webhook denied the request")
	}
	before := len(c.applied)
	if err := c.clusterFake.Apply(ctx, documents); err != nil {
		return err
	}
	for _, document := range c.applied[before:] { // a created object has a new UID
		kind, _ := document["kind"].(string)
		meta, _ := document["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		if _, exists := c.live[kind+"/"+name]; !exists && c.live != nil {
			c.live[kind+"/"+name] = LiveObject{Ref: ref(kind, name), UID: "new-" + name}
		}
	}
	return nil
}

func (c *recreateFake) MarkOrphan(_ context.Context, ref sdk.ObjectRef, stamp OrphanStamp) error {
	key := ref.Kind + "/" + ref.Name
	live := c.live[key]
	live.Orphan = &stamp
	c.live[key] = live
	c.events = append(c.events, "mark "+key)
	return nil
}

func (c *recreateFake) Orphaned(context.Context, string) ([]LiveObject, error) {
	if c.listedOrphans != nil {
		return c.listedOrphans, nil
	}
	var out []LiveObject
	for _, live := range c.live {
		if live.Orphan != nil {
			out = append(out, live)
		}
	}
	slices.SortFunc(out, func(a, b LiveObject) int { return strings.Compare(a.Ref.Kind+a.Ref.Name, b.Ref.Kind+b.Ref.Name) })
	return out, nil
}

func (c *recreateFake) ImmutableConflicts(_ context.Context, documents []byte) ([]ImmutableConflict, error) {
	c.events = append(c.events, "dry-run")
	var out []ImmutableConflict
	for key, conflict := range c.conflicts {
		if _, live := c.live[key]; live && strings.Contains(string(documents), "name: "+conflict.Ref.Name+"\n") {
			out = append(out, conflict)
		}
	}
	slices.SortFunc(out, func(a, b ImmutableConflict) int { return strings.Compare(a.Ref.Kind+a.Ref.Name, b.Ref.Kind+b.Ref.Name) })
	return out, nil
}

func (c *recreateFake) Get(_ context.Context, ref sdk.ObjectRef) (LiveObject, bool, error) {
	key := ref.Kind + "/" + ref.Name
	live, ok := c.live[key]
	if c.reads == nil {
		c.reads = map[string]int{}
	}
	c.reads[key]++
	if after, set := c.finishAfter[key]; set && c.reads[key] > after {
		live.Finished = true
	}
	live.AppliedByDeployer = !c.foreign[key]
	return live, ok, nil
}

func (c *recreateFake) Dependents(_ context.Context, _ sdk.ObjectRef, uid string) ([]LiveObject, error) {
	return c.dependents[uid], nil
}

func (c *recreateFake) Delete(_ context.Context, ref sdk.ObjectRef, propagation Propagation) error {
	c.events = append(c.events, "delete "+ref.Kind+"/"+ref.Name+" "+string(propagation))
	delete(c.live, ref.Kind+"/"+ref.Name)
	return nil
}

const recJob = `apiVersion: batch/v1
kind: Job
metadata: {name: migrate}
spec:
  template:
    spec:
      restartPolicy: Never
      containers: [{name: migrate, image: "migrate:2"}]
`

const recWeb = `apiVersion: apps/v1
kind: Deployment
metadata: {name: web}
spec:
  selector: {matchLabels: {app: web, generation: two}}
  template:
    metadata: {labels: {app: web, generation: two}}
    spec:
      containers: [{name: web, image: "web:2"}]
`

func recFixture(t *testing.T, files rendertest.MapFS) (string, *contracts.DeploymentSource) {
	t.Helper()
	return depCommit(t, files), depSource(t, raw.ID, &contracts.RawRenderConfig{Paths: []string{"k8s"}})
}

func ref(kind, name string) sdk.ObjectRef {
	return sdk.ObjectRef{Kind: kind, Namespace: depNamespace, Name: name}
}

func conflict(kind, name, field string) ImmutableConflict {
	return ImmutableConflict{Ref: sdk.ObjectRef{Kind: kind, Name: name}, Fields: []string{field}}
}

func TestFinishedJobAndChangedSelectorAreRecreatedAndOrphansReplacedAfterReady(t *testing.T) {
	root, source := recFixture(t, rendertest.MapFS{"k8s/job.yaml": recJob, "k8s/web.yaml": recWeb})
	cluster := &recreateFake{
		conflicts: map[string]ImmutableConflict{
			"Job/migrate":    conflict("Job", "migrate", "spec.template"),
			"Deployment/web": conflict("Deployment", "web", "spec.selector"),
		},
		live: map[string]LiveObject{
			"Job/migrate":    {Ref: ref("Job", "migrate"), UID: "job-1", Finished: true},
			"Deployment/web": {Ref: ref("Deployment", "web"), UID: "web-1"},
			// Orphans after the delete: one the new selector cannot adopt, one it can.
			"ReplicaSet/web-old":  {Ref: ref("ReplicaSet", "web-old"), UID: "rs-old", Labels: map[string]string{"app": "web"}},
			"ReplicaSet/web-same": {Ref: ref("ReplicaSet", "web-same"), UID: "rs-same", Labels: map[string]string{"app": "web", "generation": "two"}},
		},
		dependents: map[string][]LiveObject{"web-1": {
			{Ref: ref("ReplicaSet", "web-old"), UID: "rs-old", Labels: map[string]string{"app": "web"}},
			{Ref: ref("ReplicaSet", "web-same"), UID: "rs-same", Labels: map[string]string{"app": "web", "generation": "two"}},
		}},
	}
	deployer, _ := depDeployer(t, root, &cluster.clusterFake)
	deployer.Applier = cluster
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "Deployment/demo/web", "Job/demo/migrate"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"dry-run",
		"mark ReplicaSet/web-old", "mark ReplicaSet/web-same", "delete Deployment/web Orphan", "delete Job/migrate Background",
		"apply Job/migrate,Deployment/web",
		"observe Job/demo/migrate", "observe Deployment/demo/web",
		// Only once the recreated Deployment is ready: the orphan it does not adopt.
		"delete ReplicaSet/web-old Background",
	}
	if got := strings.Join(cluster.events, "|"); got != strings.Join(want, "|") {
		t.Fatalf("events:\n%s\nwant:\n%s", strings.Join(cluster.events, "\n"), strings.Join(want, "\n"))
	}
	wantRecords := []rendered.Recreation{
		{Ref: ref("Deployment", "web"), Fields: []string{"spec.selector"}, Propagation: "Orphan", Reason: string(RecreateWorkloadOrphan), Replaced: []string{"ReplicaSet/demo/web-old"}},
		{Ref: ref("Job", "migrate"), Fields: []string{"spec.template"}, Propagation: "Background", Reason: string(RecreateFinishedJob)},
	}
	if len(receipt.Recreated) != 2 {
		t.Fatalf("receipt recreated = %+v", receipt.Recreated)
	}
	for i, w := range wantRecords {
		g := receipt.Recreated[i]
		g.Ref.APIVersion = ""
		if g.Ref != w.Ref || !slices.Equal(g.Fields, w.Fields) || g.Propagation != w.Propagation || g.Reason != w.Reason || !slices.Equal(g.Replaced, w.Replaced) {
			t.Errorf("recreated[%d] = %+v, want %+v", i, g, w)
		}
	}
	checks := LiveChecks(LiveInput{Receipt: receipt})
	if checks[0].ID != "O1.deploy" || checks[0].Status != operability.StatusPass || !strings.Contains(checks[0].Detail, "Job/demo/migrate (finished-job: spec.template, Background)") ||
		!strings.Contains(checks[0].Detail, "replaced ReplicaSet/demo/web-old") {
		t.Fatalf("O1.deploy = %+v: the report must show every recreate", checks[0])
	}
}

func TestRunningJobIsWaitedForThenRecreated(t *testing.T) {
	root, source := recFixture(t, rendertest.MapFS{"k8s/job.yaml": recJob, "k8s/web.yaml": recWeb})
	cluster := &recreateFake{
		conflicts:   map[string]ImmutableConflict{"Job/migrate": conflict("Job", "migrate", "spec.template")},
		live:        map[string]LiveObject{"Job/migrate": {Ref: ref("Job", "migrate"), UID: "job-1"}},
		finishAfter: map[string]int{"Job/migrate": 2},
	}
	deployer, _ := depDeployer(t, root, &cluster.clusterFake)
	deployer.Applier = cluster
	var slept []time.Duration
	deployer.Sleep = func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "Deployment/demo/web", "Job/demo/migrate"))
	if err != nil {
		t.Fatal(err)
	}
	if len(slept) != 2 || len(receipt.Recreated) != 1 || receipt.Recreated[0].Reason != string(RecreateJobFinishedAfterWait) {
		t.Fatalf("slept %v, recreated %+v", slept, receipt.Recreated)
	}
}

func TestRunningJobPastTheBoundIsNeverKilled(t *testing.T) {
	root, source := recFixture(t, rendertest.MapFS{"k8s/job.yaml": recJob, "k8s/web.yaml": recWeb})
	cluster := &recreateFake{
		conflicts: map[string]ImmutableConflict{
			"Job/migrate":    conflict("Job", "migrate", "spec.template"),
			"Deployment/web": conflict("Deployment", "web", "spec.selector"),
		},
		live: map[string]LiveObject{"Job/migrate": {Ref: ref("Job", "migrate"), UID: "job-1"}, "Deployment/web": {Ref: ref("Deployment", "web"), UID: "web-1"}},
	}
	deployer, _ := depDeployer(t, root, &cluster.clusterFake)
	deployer.Applier = cluster
	deployer.JobWaitLimit = 15 * time.Second
	sleeps := 0
	deployer.Sleep = func(context.Context, time.Duration) error { sleeps++; return nil }
	_, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "Deployment/demo/web", "Job/demo/migrate"))
	var immutable *ImmutableChangeError
	if !errors.As(err, &immutable) || len(immutable.Refused) != 1 || immutable.Refused[0].Reason != RefuseJobRunning {
		t.Fatalf("err = %v", err)
	}
	if sleeps != 3 {
		t.Errorf("waited %d polls, want 3 within the 15 s bound", sleeps)
	}
	if got := strings.Join(cluster.events, "|"); got != "dry-run" {
		t.Fatalf("a refused phase deleted or applied: %v", cluster.events)
	}
	cause := ClassifyDeploymentError(err)
	if cause != operability.CauseImmutableChange || operability.RouteIn(operability.ModeAdvance, cause) != operability.OwnerDeployer {
		t.Fatalf("cause %s owner %s, want immutable_change for the deployer", cause, operability.RouteIn(operability.ModeAdvance, cause))
	}
}

func TestDataBearingAndUnrecreatableChangesHoldWithTheirOwner(t *testing.T) {
	pvc := "apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata: {name: data}\nspec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}\n"
	svc := "apiVersion: v1\nkind: Service\nmetadata: {name: web}\nspec: {clusterIP: 10.0.0.9, ports: [{port: 80}]}\n"
	for _, tc := range []struct {
		name  string
		file  string
		c     ImmutableConflict
		cause operability.Cause
		owner operability.Owner
	}{
		{"pvc", pvc, conflict("PersistentVolumeClaim", "data", "spec"), operability.CauseDataBearingChange, operability.OwnerHuman},
		{"service", svc, conflict("Service", "web", "spec.clusterIPs[0]"), operability.CauseImmutableChange, operability.OwnerDeployer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, source := recFixture(t, rendertest.MapFS{"k8s/web.yaml": recWeb, "k8s/x.yaml": tc.file})
			key := tc.c.Ref.Kind + "/" + tc.c.Ref.Name
			cluster := &recreateFake{conflicts: map[string]ImmutableConflict{key: tc.c}, live: map[string]LiveObject{key: {Ref: ref(tc.c.Ref.Kind, tc.c.Ref.Name), UID: "u"}}}
			deployer, _ := depDeployer(t, root, &cluster.clusterFake)
			deployer.Applier = cluster
			_, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "Deployment/demo/web"))
			cause := ClassifyDeploymentError(err)
			if cause != tc.cause || operability.RouteIn(operability.ModeAdvance, cause) != tc.owner {
				t.Fatalf("err %v: cause %s owner %s, want %s %s", err, cause, operability.RouteIn(operability.ModeAdvance, cause), tc.cause, tc.owner)
			}
			if strings.Join(cluster.events, "|") != "dry-run" {
				t.Fatalf("a held change touched the cluster: %v", cluster.events)
			}
		})
	}
}

func TestRecreatedStatefulSetReleasesOrphanPodNamesRightAfterApply(t *testing.T) {
	sts := `apiVersion: apps/v1
kind: StatefulSet
metadata: {name: db}
spec:
  serviceName: db
  selector: {matchLabels: {app: db, generation: two}}
  template:
    metadata: {labels: {app: db, generation: two}}
    spec:
      containers: [{name: db, image: "db:2"}]
  volumeClaimTemplates: [{metadata: {name: data}, spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}}]
`
	root, source := recFixture(t, rendertest.MapFS{"k8s/db.yaml": sts})
	orphan := LiveObject{Ref: ref("Pod", "db-0"), UID: "pod-0", Labels: map[string]string{"app": "db"}}
	cluster := &recreateFake{
		conflicts:  map[string]ImmutableConflict{"StatefulSet/db": conflict("StatefulSet", "db", "spec")},
		live:       map[string]LiveObject{"StatefulSet/db": {Ref: ref("StatefulSet", "db"), UID: "db-1"}, "Pod/db-0": orphan},
		dependents: map[string][]LiveObject{"db-1": {orphan}},
	}
	deployer, _ := depDeployer(t, root, &cluster.clusterFake)
	deployer.Applier = cluster
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "StatefulSet/demo/db"))
	if err != nil {
		t.Fatal(err)
	}
	want := "dry-run|mark Pod/db-0|delete StatefulSet/db Orphan|apply StatefulSet/db|delete Pod/db-0 Background|observe StatefulSet/demo/db"
	if got := strings.Join(cluster.events, "|"); got != want {
		t.Fatalf("events = %s\nwant %s", got, want)
	}
	if len(receipt.Recreated) != 1 || !slices.Equal(receipt.Recreated[0].Replaced, []string{"Pod/demo/db-0"}) {
		t.Fatalf("recreated = %+v", receipt.Recreated)
	}
}

func TestParseImmutableConflictsReadsTheServersAnswer(t *testing.T) {
	output := `poddisruptionbudget.policy/web serverside-applied (server dry run)
Error from server (Invalid): Job.batch "app-migrations-1a2b3c4" is invalid: spec.template: Invalid value: {"labels":{"app":"migrations"},"Spec":{"Containers":[{"Name":"m"}]}}: field is immutable
Error from server (Invalid): Deployment.apps "web" is invalid: [spec.selector: Invalid value: v1.LabelSelector{MatchLabels:map[string]string{"app":"web2"}}: field is immutable, spec.template.metadata.labels: Invalid value: map[string]string{"app":"web2"}: ` + "`selector` does not match template `labels`" + `]
Error from server (Invalid): StatefulSet.apps "db" is invalid: spec: Forbidden: updates to statefulset spec for fields other than 'replicas', 'ordinals', 'template', 'updateStrategy', 'persistentVolumeClaimRetentionPolicy' and 'minReadySeconds' are forbidden
The PersistentVolumeClaim "data" is invalid: spec: Forbidden: spec is immutable after creation except resources.requests and volumeAttributesClassName for bound claims
Error from server (Invalid): ConfigMap "settings" is invalid: data.port: Invalid value: "x": must be a number
`
	got := ParseImmutableConflicts(output)
	want := map[string]string{
		"Job/app-migrations-1a2b3c4": "spec.template", "Deployment/web": "spec.selector",
		"StatefulSet/db": "spec", "PersistentVolumeClaim/data": "spec",
	}
	if len(got) != len(want) {
		t.Fatalf("conflicts = %+v", got)
	}
	for _, c := range got {
		if fields, ok := want[c.Ref.Kind+"/"+c.Ref.Name]; !ok || strings.Join(c.Fields, ",") != fields {
			t.Errorf("conflict %s/%s fields %v, want %q", c.Ref.Kind, c.Ref.Name, c.Fields, fields)
		}
	}
}

func TestCommandBackendRecreatesThroughTheAPIServer(t *testing.T) {
	var calls []string
	backend := CommandBackend{ObserveLimit: time.Minute, Run: func(_ context.Context, _ string, args []string, _ []byte) ([]byte, error) {
		calls = append(calls, strings.Join(args, " "))
		switch args[0] {
		case "apply":
			return []byte(`Error from server (Invalid): Job.batch "migrate" is invalid: spec.template: Invalid value: {}: field is immutable`), errors.New("exit status 1")
		case "get":
			if args[1] == "Job" {
				return []byte(`{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"migrate","namespace":"demo","uid":"j"},"status":{"conditions":[{"type":"Complete","status":"True"}]}}`), nil
			}
			return []byte(`{"items":[{"kind":"ReplicaSet","metadata":{"name":"web-1","namespace":"demo","uid":"r","labels":{"app":"web"},"ownerReferences":[{"uid":"d","controller":true}]}},
				{"kind":"Pod","metadata":{"name":"other","namespace":"demo","uid":"p","ownerReferences":[{"uid":"x","controller":true}]}}]}`), nil
		}
		return nil, nil
	}}
	conflicts, err := backend.ImmutableConflicts(context.Background(), []byte("---\n"))
	if err != nil || len(conflicts) != 1 || conflicts[0].Ref.Kind != "Job" {
		t.Fatalf("conflicts = %+v, %v", conflicts, err)
	}
	live, found, err := backend.Get(context.Background(), ref("Job", "migrate"))
	if err != nil || !found || !live.Finished || live.UID != "j" {
		t.Fatalf("live = %+v %v %v", live, found, err)
	}
	dependents, err := backend.Dependents(context.Background(), ref("Deployment", "web"), "d")
	if err != nil || len(dependents) != 1 || dependents[0].Ref.Name != "web-1" || dependents[0].Labels["app"] != "web" {
		t.Fatalf("dependents = %+v, %v", dependents, err)
	}
	if err := backend.Delete(context.Background(), ref("Deployment", "web"), PropagationOrphan); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"apply --server-side --force-conflicts --field-manager=sdo-deployer --dry-run=server --filename=-",
		"delete Deployment web --namespace demo --cascade=orphan --ignore-not-found --wait=true --timeout=1m0s",
	} {
		if !slices.Contains(calls, want) {
			t.Errorf("no call %q in %v", want, calls)
		}
	}
}

func deletes(events []string) []string {
	var out []string
	for _, event := range events {
		if strings.HasPrefix(event, "delete ") {
			out = append(out, event)
		}
	}
	return out
}

// A failed apply after the orphan delete must not lose the orphans: the
// retry is a new deployer with no memory and finds the workload absent, and
// the stamps the first run left still get the old ReplicaSet replaced.
func TestOrphansSurviveAFailedApplyAndAreSweptByTheRetry(t *testing.T) {
	root, source := recFixture(t, rendertest.MapFS{"k8s/web.yaml": recWeb})
	cluster := &recreateFake{
		conflicts: map[string]ImmutableConflict{"Deployment/web": conflict("Deployment", "web", "spec.selector")},
		live: map[string]LiveObject{
			"Deployment/web":     {Ref: ref("Deployment", "web"), UID: "web-1"},
			"ReplicaSet/web-old": {Ref: ref("ReplicaSet", "web-old"), UID: "rs-old", Labels: map[string]string{"app": "web"}},
		},
		dependents:    map[string][]LiveObject{"web-1": {{Ref: ref("ReplicaSet", "web-old"), UID: "rs-old", Labels: map[string]string{"app": "web"}}}},
		failApplyOnce: true,
	}
	plan := func() (Deployer, *contracts.DeploymentPlan) {
		deployer, _ := depDeployer(t, root, &cluster.clusterFake)
		deployer.Applier = cluster
		return deployer, depPlan(t, source, "Deployment/demo/web")
	}
	first, firstPlan := plan()
	if _, err := first.ApplyAndVerify(context.Background(), root, firstPlan); err == nil {
		t.Fatal("the failed apply was reported as a deploy")
	}
	stamped := cluster.live["ReplicaSet/web-old"].Orphan
	if stamped == nil || stamped.From != "web-1" || stamped.Owner != "Deployment/web" {
		t.Fatalf("orphan stamp after the failed run = %+v, want one bound to the deleted workload", stamped)
	}
	if got := deletes(cluster.events); len(got) != 1 || got[0] != "delete Deployment/web Orphan" {
		t.Fatalf("a run that never became ready deleted %v", got)
	}
	second, secondPlan := plan()
	if _, err := second.ApplyAndVerify(context.Background(), root, secondPlan); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(deletes(cluster.events), "|"); got != "delete Deployment/web Orphan|delete ReplicaSet/web-old Background" {
		t.Fatalf("deletes = %s: the retry must replace the orphan the first run stamped", got)
	}
}

// The sweep keeps an orphan whose workload has no replacement yet, one a
// controller took over, and one recreated under the same name since it was
// listed: only an object that is still the stamped, uncontrolled orphan goes.
func TestOrphanSweepKeepsWhatItCannotProveIsAnUnclaimedOrphan(t *testing.T) {
	stamp := func(from, owner string) *OrphanStamp { return &OrphanStamp{From: from, Owner: owner} }
	orphan := func(name, uid string, s *OrphanStamp) LiveObject {
		return LiveObject{Ref: ref("ReplicaSet", name), UID: uid, Orphan: s}
	}
	cases := []struct {
		name       string
		listed     LiveObject
		live       LiveObject
		wantDelete bool
	}{
		{"unclaimed orphan", orphan("rs", "u1", stamp("web-1", "Deployment/web")), orphan("rs", "u1", stamp("web-1", "Deployment/web")), true},
		{"adopted by a controller", orphan("rs", "u1", stamp("web-1", "Deployment/web")), LiveObject{Ref: ref("ReplicaSet", "rs"), UID: "u1", Controlled: true, Orphan: stamp("web-1", "Deployment/web")}, false},
		{"recreated under the same name", orphan("rs", "u1", stamp("web-1", "Deployment/web")), orphan("rs", "u2", stamp("web-1", "Deployment/web")), false},
		{"no replacement of the workload", orphan("rs", "u1", stamp("web-1", "Deployment/api")), orphan("rs", "u1", stamp("web-1", "Deployment/api")), false},
		{"workload not yet replaced", orphan("rs", "u1", stamp("new-web", "Deployment/web")), orphan("rs", "u1", stamp("new-web", "Deployment/web")), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root, source := recFixture(t, rendertest.MapFS{"k8s/web.yaml": recWeb})
			cluster := &recreateFake{
				live:          map[string]LiveObject{"Deployment/web": {Ref: ref("Deployment", "web"), UID: "new-web"}, "ReplicaSet/rs": tc.live},
				listedOrphans: []LiveObject{tc.listed},
			}
			deployer, _ := depDeployer(t, root, &cluster.clusterFake)
			deployer.Applier = cluster
			if _, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "Deployment/demo/web")); err != nil {
				t.Fatal(err)
			}
			got := deletes(cluster.events)
			if tc.wantDelete != (len(got) == 1 && got[0] == "delete ReplicaSet/rs Background") || (!tc.wantDelete && len(got) != 0) {
				t.Fatalf("deletes = %v, want delete=%v", got, tc.wantDelete)
			}
		})
	}
}

// SDO recreates only what its deployer applied. A same-named object someone
// else created is a typed hold, and nothing is stamped or deleted.
func TestRecreateRefusesObjectsTheDeployerDidNotCreate(t *testing.T) {
	root, source := recFixture(t, rendertest.MapFS{"k8s/job.yaml": recJob, "k8s/web.yaml": recWeb})
	cluster := &recreateFake{
		conflicts: map[string]ImmutableConflict{
			"Job/migrate":    conflict("Job", "migrate", "spec.template"),
			"Deployment/web": conflict("Deployment", "web", "spec.selector"),
		},
		live: map[string]LiveObject{
			"Job/migrate":    {Ref: ref("Job", "migrate"), UID: "job-1", Finished: true},
			"Deployment/web": {Ref: ref("Deployment", "web"), UID: "web-1"},
		},
		foreign:    map[string]bool{"Job/migrate": true, "Deployment/web": true},
		dependents: map[string][]LiveObject{"web-1": {{Ref: ref("ReplicaSet", "web-old"), UID: "rs-old"}}},
	}
	deployer, _ := depDeployer(t, root, &cluster.clusterFake)
	deployer.Applier = cluster
	_, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "Deployment/demo/web", "Job/demo/migrate"))
	var immutable *ImmutableChangeError
	if !errors.As(err, &immutable) || len(immutable.Refused) != 2 {
		t.Fatalf("err = %v, want both foreign objects refused", err)
	}
	for _, refused := range immutable.Refused {
		if refused.Reason != RefuseNotOurs {
			t.Errorf("%s refused as %s, want %s", refused.Conflict.Ref.Name, refused.Reason, RefuseNotOurs)
		}
	}
	if got := strings.Join(cluster.events, "|"); got != "dry-run" {
		t.Fatalf("a refused phase touched the cluster: %s", got)
	}
	if cause := ClassifyDeploymentError(err); cause != operability.CauseImmutableChange || operability.RouteIn(operability.ModeAdvance, cause) != operability.OwnerDeployer {
		t.Fatalf("cause %s, want immutable_change for the deployer", cause)
	}
}

func TestCommandBackendReadsOwnershipAndOrphanStamps(t *testing.T) {
	var calls []string
	backend := CommandBackend{Run: func(_ context.Context, _ string, args []string, _ []byte) ([]byte, error) {
		calls = append(calls, strings.Join(args, " "))
		switch args[0] {
		case "get":
			if args[1] == "Deployment" {
				return []byte(`{"kind":"Deployment","metadata":{"name":"web","uid":"d","managedFields":[{"manager":"sdo-deployer","operation":"Apply"}]}}`), nil
			}
			return []byte(`{"items":[{"kind":"ReplicaSet","metadata":{"name":"web-old","namespace":"demo","uid":"r","labels":{"sdo.dev/orphaned-from":"d"},"annotations":{"sdo.dev/orphaned-owner":"Deployment/web","sdo.dev/orphaned-selector":"app=web"}}}]}`), nil
		}
		return nil, nil
	}}
	live, found, err := backend.Get(context.Background(), ref("Deployment", "web"))
	if err != nil || !found || !live.AppliedByDeployer {
		t.Fatalf("live = %+v %v %v", live, found, err)
	}
	if err := backend.MarkOrphan(context.Background(), ref("ReplicaSet", "web-old"), OrphanStamp{From: "d", Owner: "Deployment/web", Selector: "app=web"}); err != nil {
		t.Fatal(err)
	}
	orphans, err := backend.Orphaned(context.Background(), "demo")
	if err != nil || len(orphans) != 1 || orphans[0].Orphan == nil || *orphans[0].Orphan != (OrphanStamp{From: "d", Owner: "Deployment/web", Selector: "app=web"}) {
		t.Fatalf("orphans = %+v, %v", orphans, err)
	}
	joined := strings.Join(calls, "\n")
	for _, want := range []string{"--show-managed-fields", "patch ReplicaSet web-old --namespace demo --type=merge", "--selector=sdo.dev/orphaned-from"} {
		if !strings.Contains(joined, want) {
			t.Errorf("no kubectl call containing %q in\n%s", want, joined)
		}
	}
}
