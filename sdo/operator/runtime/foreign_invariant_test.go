package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

const foreignInvariantRunner = `package main
import (
 "context"
 "encoding/json"
 "os"
 candidate "foreign-invariant/candidate"
 "sdo.dev/operator/sdk/program"
 "sdo.dev/operator/sdk/sdktest"
)
func main() {
 var input struct {Snapshot sdktest.Snapshot; Calls []sdktest.PrimitiveCall; Bindings program.Bindings}
 if err:=json.NewDecoder(os.Stdin).Decode(&input);err!=nil {panic(err)}
 view:=&sdktest.ReplayReadView{Snapshot:input.Snapshot,Calls:input.Calls}
 verdict,err:=(candidate.Program{}).Diagnose(context.Background(),view,input.Bindings)
 if err!=nil {panic(err)}
 if err:=json.NewEncoder(os.Stdout).Encode(verdict);err!=nil {panic(err)}
}
`

// Execute the actual scripted Go artifact, not a second implementation of its
// predicate. Only primitive reads are recorded at the process boundary.
type compiledForeignInvariant struct{ binary string }

func (compiledForeignInvariant) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "networkpolicy-service-isolation", Params: []program.ParamSpec{{Name: "policy", Kind: "NetworkPolicy", Required: true}}}
}
func (p compiledForeignInvariant) Diagnose(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	snapshot, err := sdktest.CaptureSnapshot(view, view.Namespace())
	if err != nil {
		return program.Verdict{}, err
	}
	recording := &sdktest.RecordingReadView{ReadView: view}
	if _, err := recording.DiffAgainstSource(bindings.Objects["policy"]); err != nil {
		return program.Verdict{}, err
	}
	for _, service := range view.Services() {
		if _, err := recording.DiffAgainstSource(sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: service.Namespace, Name: service.Name}); err != nil {
			return program.Verdict{}, err
		}
	}
	data, err := json.Marshal(struct {
		Snapshot sdktest.Snapshot
		Calls    []sdktest.PrimitiveCall
		Bindings program.Bindings
	}{snapshot, recording.Calls, bindings})
	if err != nil {
		return program.Verdict{}, err
	}
	cmd := exec.CommandContext(ctx, p.binary)
	cmd.Stdin = bytes.NewReader(data)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return program.Verdict{}, fmt.Errorf("scripted invariant: %w: %s", err, output)
	}
	var verdict program.Verdict
	if err := json.Unmarshal(output, &verdict); err != nil {
		return program.Verdict{}, err
	}
	return verdict, nil
}
func compileForeignInvariant(t *testing.T) compiledForeignInvariant {
	t.Helper()
	root := t.TempDir()
	sdkRoot, err := filepath.Abs("../sdk")
	if err != nil {
		t.Fatal(err)
	}
	module := fmt.Sprintf("module foreign-invariant\n\ngo 1.26.0\n\nrequire sdo.dev/operator/sdk v0.0.0\nreplace sdo.dev/operator/sdk => %s\n", sdkRoot)
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "candidate"), 0700); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("../../../benchmarks/sregym/fastloop/scripted_networkpolicy_program/diagnose.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "candidate", "diagnose.go"), source, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte(foreignInvariantRunner), 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "diagnose")
	cmd := exec.Command("go", "build", "-p", "8", "-mod=mod", "-o", binary, ".")
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("compile scripted learned invariant: %v: %s", err, output)
	}
	return compiledForeignInvariant{binary: binary}
}

func policyWorkloadSnapshot(policies ...networkingv1.NetworkPolicy) sdktest.Snapshot {
	return sdktest.Snapshot{NamespaceName: "app", NetworkPolicyList: policies,
		ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api"}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: driftService(8080).Spec.Ports[0].TargetPort}}}}},
		PodList:     []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "api-pod", Namespace: "app", Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Ports: []corev1.ContainerPort{{ContainerPort: 8080}}}}}}},
	}
}

func TestForeignNetworkPolicyInvariantNearMisses(t *testing.T) {
	source, commit := foreignSource(t)
	learned := compileForeignInvariant(t)
	for _, tc := range []struct {
		name     string
		policies []networkingv1.NetworkPolicy
		want     program.Result
	}{
		{"deny ingress", []networkingv1.NetworkPolicy{*foreignPolicy("deny")}, program.Match},
		{"no policy", nil, program.NoMatch},
		{"unrelated selector", func() []networkingv1.NetworkPolicy {
			p := foreignPolicy("deny")
			p.Spec.PodSelector.MatchLabels = map[string]string{"app": "elsewhere"}
			return []networkingv1.NetworkPolicy{*p}
		}(), program.NoMatch},
		{"additive allow", func() []networkingv1.NetworkPolicy {
			allow := foreignPolicy("allow")
			allow.Spec.Ingress = []networkingv1.NetworkPolicyIngressRule{{}}
			return []networkingv1.NetworkPolicy{*foreignPolicy("deny"), *allow}
		}(), program.NoMatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := LiveReadView{DetectionContext: policyWorkloadSnapshot(tc.policies...), Source: source, SourceCommit: commit}
			verdict, err := learned.Diagnose(context.Background(), view, program.Bindings{Objects: program.ObjectBindings{"policy": {APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "deny"}}})
			if err != nil || verdict.Result != tc.want {
				t.Fatalf("verdict=%+v error=%v", verdict, err)
			}
		})
	}
}
