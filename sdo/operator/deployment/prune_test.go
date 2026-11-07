package deployment

import (
	"context"
	"strings"
	"testing"

	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/sdk"
)

// pruneFake is a cluster that also lists and deletes the Jobs the deployer
// applied.
type pruneFake struct {
	clusterFake
	jobs []LiveObject
}

func (c *pruneFake) AppliedJobs(context.Context, string) ([]LiveObject, error) {
	return append([]LiveObject(nil), c.jobs...), nil
}

func (c *pruneFake) Delete(_ context.Context, ref sdk.ObjectRef, propagation Propagation) error {
	c.events = append(c.events, "delete "+ref.Kind+"/"+ref.Name+" "+string(propagation))
	return nil
}

func pruneJob(name string) string {
	return "apiVersion: batch/v1\nkind: Job\nmetadata: {name: " + name + "}\nspec:\n  template:\n    spec:\n      restartPolicy: Never\n      containers: [{name: migrate, image: \"migrate:2\"}]\n"
}

func appliedJob(name string, finished bool) LiveObject {
	return LiveObject{Ref: ref("Job", name), UID: name, Finished: finished}
}

// A release that renames its generated-name Job (a chart helper hashing the
// images) leaves the previous name behind. After the new revision is ready the
// deployer deletes the finished predecessor it applied itself, and nothing
// else.
func TestSupersededGeneratedJobsAreDeletedAfterTheRevisionIsReady(t *testing.T) {
	root, source := recFixture(t, rendertest.MapFS{"k8s/job.yaml": pruneJob("migrate-b2c3d4e"), "k8s/web.yaml": recWeb})
	cluster := &pruneFake{jobs: []LiveObject{
		appliedJob("migrate-a1b2c3d", true),  // superseded and finished: pruned
		appliedJob("migrate-b2c3d4e", true),  // this revision's: kept
		appliedJob("migrate-c9d8e7f", false), // superseded but still running: kept
		appliedJob("other-a1b2c3d", true),    // a Job this revision does not render: kept
	}}
	deployer, _ := depDeployer(t, root, &cluster.clusterFake)
	deployer.Applier = cluster
	if _, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "Deployment/demo/web", "Job/demo/migrate-*")); err != nil {
		t.Fatal(err)
	}
	var deletes []string
	for _, event := range cluster.events {
		if strings.HasPrefix(event, "delete ") {
			deletes = append(deletes, event)
		}
	}
	if got := strings.Join(deletes, "|"); got != "delete Job/migrate-a1b2c3d Background" {
		t.Fatalf("deletes = %q, want only the finished superseded Job", got)
	}
	if last := cluster.events[len(cluster.events)-1]; !strings.HasPrefix(last, "delete ") {
		t.Fatalf("the prune must follow the rollout observation: %v", cluster.events)
	}
}

// A failed deploy never prunes: the previous revision may still be needed.
func TestFailedDeployDoesNotPruneSupersededJobs(t *testing.T) {
	root, source := recFixture(t, rendertest.MapFS{"k8s/job.yaml": pruneJob("migrate-b2c3d4e"), "k8s/web.yaml": recWeb})
	cluster := &pruneFake{jobs: []LiveObject{appliedJob("migrate-a1b2c3d", true)}}
	cluster.notReady = map[string]bool{"Deployment/demo/web": true}
	deployer, _ := depDeployer(t, root, &cluster.clusterFake)
	deployer.Applier = cluster
	if _, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "Deployment/demo/web", "Job/demo/migrate-*")); err == nil {
		t.Fatal("deploy of a workload that is not ready succeeded")
	}
	for _, event := range cluster.events {
		if strings.HasPrefix(event, "delete ") {
			t.Fatalf("a failed deploy deleted %s", event)
		}
	}
}

// Only Jobs the deployer applied (its field manager and its stamp) are
// listed; a Job someone else created is never a prune candidate.
func TestCommandBackendListsOnlyJobsTheDeployerApplied(t *testing.T) {
	backend := CommandBackend{Run: func(_ context.Context, _ string, args []string, _ []byte) ([]byte, error) {
		if !strings.Contains(strings.Join(args, " "), "get jobs") || !strings.Contains(strings.Join(args, " "), "--show-managed-fields") {
			t.Fatalf("unexpected kubectl call %v", args)
		}
		return []byte(`{"items":[
{"kind":"Job","metadata":{"name":"migrate-a1b2c3d","namespace":"demo","annotations":{"sdo.dev/source-tree":"t"},"managedFields":[{"manager":"sdo-deployer","operation":"Apply"}]},"status":{"conditions":[{"type":"Complete","status":"True"}]}},
{"kind":"Job","metadata":{"name":"migrate-b1b2c3d","namespace":"demo","managedFields":[{"manager":"sdo-deployer","operation":"Apply"}]}},
{"kind":"Job","metadata":{"name":"migrate-c1b2c3d","namespace":"demo","annotations":{"sdo.dev/source-tree":"t"},"managedFields":[{"manager":"kubectl","operation":"Update"}]}}]}`), nil
	}}
	jobs, err := backend.AppliedJobs(context.Background(), "demo")
	if err != nil || len(jobs) != 1 || jobs[0].Ref.Name != "migrate-a1b2c3d" || !jobs[0].Finished {
		t.Fatalf("jobs = %+v, %v", jobs, err)
	}
}
