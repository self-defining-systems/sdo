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
		"delete Deployment/web Orphan", "delete Job/migrate Background",
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
	want := "dry-run|delete StatefulSet/db Orphan|apply StatefulSet/db|delete Pod/db-0 Background|observe StatefulSet/demo/db"
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
