//go:build integration

package deploy_test

import (
	"os/exec"
	"strings"
	"testing"
)

// values renders vcluster-lane.sh's vcluster values with the given environment.
func values(t *testing.T, env ...string) string {
	t.Helper()
	cmd := exec.Command("bash", "-c", `set -e; source <(sed -n '/^write_values()/,/^}/p' vcluster-lane.sh); write_values chelan4 /dev/stdout`)
	cmd.Env = append([]string{"PATH=/usr/bin:/bin", "FUZZOPS_ENTRY_HOST=192.168.1.108"}, env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	return string(out)
}

// The lane's values keep what the first lane prototype had to fix by hand.
func TestVclusterLaneValues(t *testing.T) {
	v := values(t)
	for _, want := range []string{
		"kubernetes.io/hostname: chelan4",           // control plane pinned to the chosen node
		"type: NodePort",                            // the API server is reached at ENTRY:NODEPORT
		"extraSANs: [192.168.1.108]",                // ... and its certificate names ENTRY
		`services.nodeports: "10"`,                  // room for an app with several NodePorts
		"limits.cpu: null",                          // no limits.* in the quota: pods without limits are admitted
		"limits.memory: null",                       //
		"default: null",                             // LimitRange: no default limit (it OOM-kills and throttles)
		"defaultRequest:",                           // ... only a default request
		"podSecurityStandard: privileged",           // agent workers run with an unconfined seccomp profile
		"networkPolicy:",                            // isolation on (the host policy allow-node-entry lets nodes in)
		`resources: ["nodes/proxy", "nodes/stats"]`, // node usage through the virtual API
	} {
		if !strings.Contains(v, want) {
			t.Errorf("values lack %q:\n%s", want, v)
		}
	}
}

func TestVclusterLaneValuesTakeSettings(t *testing.T) {
	v := values(t, "VCLUSTER_NODEPORTS=24", "VCLUSTER_POD_SECURITY=baseline", "VCLUSTER_QUOTA_CPU=6")
	for _, want := range []string{`services.nodeports: "24"`, "podSecurityStandard: baseline", `requests.cpu: "6"`} {
		if !strings.Contains(v, want) {
			t.Errorf("values lack %q:\n%s", want, v)
		}
	}
}
