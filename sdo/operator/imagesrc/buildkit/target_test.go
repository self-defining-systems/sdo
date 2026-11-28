package buildkit

import (
	"context"
	"strings"
	"testing"

	"k8s.io/client-go/kubernetes/fake"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestValidateRegistryHostAcceptsAPathPrefix(t *testing.T) {
	for _, good := range []string{"registry:5000", "registry.example", "123456789012.dkr.ecr.us-west-2.amazonaws.com/sdo/q2-deploy", "reg.example/a_b/c-d"} {
		if err := ValidateRegistryHost(good); err != nil {
			t.Errorf("%q refused: %v", good, err)
		}
	}
	for _, bad := range []string{"", "https://reg.example", "user@reg.example", "reg.example/", "reg.example//x", "reg.example/UPPER", "Reg.example", "reg.example:0"} {
		if err := ValidateRegistryHost(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if got := registryHostOf("reg.example:5000/sdo/app"); got != "reg.example:5000" {
		t.Fatalf("registry host = %q", got)
	}
}

func TestBuilderRefusesATargetOutsideThePrefix(t *testing.T) {
	b := Builder{Client: fake.NewSimpleClientset(), AllowedRegistryHost: "reg.example/sdo/app"}
	b.Namespace, b.RepositoryPVC, b.CheckoutRoot, b.RegistrySecret, b.ServiceAccount = "ns", "pvc", "/tmp", "s", "sa"
	b.BuildKitImage = "moby/buildkit@sha256:" + strings.Repeat("a", 64)
	_, err := b.buildImage(context.Background(), "/tmp/x", &contracts.ImageBuild{Image: "reg.example/other/web:v1"})
	if err == nil || !strings.Contains(err.Error(), "outside trusted registry") {
		t.Fatalf("err = %v, want a refusal outside the prefix", err)
	}
}
