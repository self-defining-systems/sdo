//archcheck:ratchet R5 count=3 until=S3-11c
package runtime

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/rendered"
)

func flagTestSource(t *testing.T) *contracts.DeploymentSource {
	t.Helper()
	config, err := anypb.New(&contracts.KustomizeRenderConfig{Path: "kubernetes"})
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.DeploymentSource{RendererId: "kustomize", Config: config}
}

func flagValue(args []string, name string) (string, bool) {
	for i, arg := range args {
		if arg == name && i+1 < len(args) {
			return args[i+1], true
		}
	}
	return "", false
}

func TestControllerConfigExpandsDeploymentSourceToFlag(t *testing.T) {
	want := flagTestSource(t)
	args, err := controllerConfigToArgs(&contracts.ControllerConfig{DeploymentSource: want})
	if err != nil {
		t.Fatal(err)
	}
	value, ok := flagValue(args, "--deployment-source")
	if !ok {
		t.Fatalf("no --deployment-source in %v", args)
	}
	got, err := rendered.ParseDeploymentSource(value)
	if err != nil || !proto.Equal(got, want) {
		t.Fatalf("flag value decodes to %v, %v; want %v", got, err, want)
	}
	for _, arg := range args {
		if strings.HasPrefix(arg, "--deployed-manifest-path") {
			t.Fatalf("path flag emitted beside the source: %v", args)
		}
	}
}

func TestControllerConfigWithoutDeploymentSourceOmitsFlag(t *testing.T) {
	args, err := controllerConfigToArgs(&contracts.ControllerConfig{DeployedManifestPaths: []string{"k8s"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := flagValue(args, "--deployment-source"); ok {
		t.Fatalf("unexpected --deployment-source in %v", args)
	}
}

func TestControllerConfigRejectsSourceAndPaths(t *testing.T) {
	_, err := controllerConfigToArgs(&contracts.ControllerConfig{DeploymentSource: flagTestSource(t), DeployedManifestPaths: []string{"k8s"}})
	if err == nil {
		t.Fatal("both deployment_source and deployed_manifest_paths accepted")
	}
}
