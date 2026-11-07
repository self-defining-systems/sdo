package deployment

import (
	"context"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render/helm"
	"sdo.dev/operator/render/rendertest"
)

func hookJob(name, events string) string {
	return "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: " + name + "\n  annotations: {\"helm.sh/hook\": \"" + events + "\"}\n" +
		"spec: {template: {spec: {restartPolicy: Never, containers: [{name: j, image: j:1}]}}}\n---\n"
}

func hookChart(t *testing.T) (string, *contracts.DeploymentSource) {
	t.Helper()
	files := rendertest.MapFS{
		"h/Chart.yaml":          "apiVersion: v2\nname: h\nversion: 0.1.0\n",
		"h/templates/web.yaml":  depDeployment,
		"h/templates/jobs.yaml": hookJob("install-only", "pre-install") + hookJob("guard", "pre-upgrade") + hookJob("both", "pre-install,pre-upgrade") + hookJob("after-up", "post-upgrade"),
	}
	return depCommit(t, files), depSource(t, helm.ID, &contracts.HelmRenderConfig{ChartPath: "h"})
}

func appliedJobs(events []string) string {
	var jobs []string
	for _, e := range events {
		if strings.HasPrefix(e, "apply ") {
			for _, name := range strings.Split(strings.TrimPrefix(e, "apply "), ",") {
				if strings.HasPrefix(name, "Job/") {
					jobs = append(jobs, name)
				}
			}
		}
	}
	return strings.Join(jobs, " ")
}

// An install runs the chart's install hooks. An upgrade of a running
// deployment runs its upgrade hooks, which is where charts keep their upgrade
// guards; a hook annotated for both runs in either.
func TestHooksFollowTheOperationTheDeployPerforms(t *testing.T) {
	root, source := hookChart(t)
	plan := depPlan(t, source, "Deployment/demo/web")

	install := &clusterFake{}
	deployer, _ := depDeployer(t, root, install)
	if _, err := deployer.ApplyAndVerify(context.Background(), root, plan); err != nil {
		t.Fatal(err)
	}
	if got := appliedJobs(install.events); got != "Job/install-only Job/both" {
		t.Fatalf("install applied %q", got)
	}

	upgrade := &recreateFake{live: map[string]LiveObject{"Deployment/web": {Ref: ref("Deployment", "web"), UID: "web-1"}}}
	deployer, _ = depDeployer(t, root, &upgrade.clusterFake)
	deployer.Applier = upgrade
	if _, err := deployer.ApplyAndVerify(context.Background(), root, plan); err != nil {
		t.Fatal(err)
	}
	if got := appliedJobs(upgrade.events); got != "Job/guard Job/both Job/after-up" {
		t.Fatalf("upgrade applied %q", got)
	}
}

// A render that fails its chart's own guard stops the deploy before anything
// is applied, and the error carries the chart's message.
func TestChartGuardStopsTheDeployBeforeAnyApply(t *testing.T) {
	files := rendertest.MapFS{
		"h/Chart.yaml":          "apiVersion: v2\nname: h\nversion: 0.1.0\n",
		"h/templates/web.yaml":  depDeployment,
		"h/templates/NOTES.txt": `{{ fail "unsupported configuration" }}`,
	}
	root := depCommit(t, files)
	cluster := &clusterFake{}
	deployer, _ := depDeployer(t, root, cluster)
	_, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, depSource(t, helm.ID, &contracts.HelmRenderConfig{ChartPath: "h"}), "Deployment/demo/web"))
	if err == nil || !strings.Contains(err.Error(), "unsupported configuration") || len(cluster.events) != 0 {
		t.Fatalf("err %v, events %v", err, cluster.events)
	}
}
