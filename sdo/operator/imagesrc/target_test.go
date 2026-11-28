package imagesrc_test

import (
	"testing"

	"sdo.dev/operator/imagesrc"
)

func TestRegistryTargetIsAHostWithAnOptionalPathPrefix(t *testing.T) {
	const ecr = "123456789012.dkr.ecr.us-west-2.amazonaws.com/sdo/q2"
	for _, tc := range []struct {
		image, target string
		under         bool
	}{
		{"registry:5000/app/web:v1", "registry:5000", true},
		{"registry:5000", "registry:5000", false},
		{"other:5000/app/web", "registry:5000", false},
		{ecr + "/web:v1", ecr, true},
		{ecr + "/team/web@sha256:" + "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f9", ecr, true},
		{"123456789012.dkr.ecr.us-west-2.amazonaws.com/other/web", ecr, false},
		{ecr + "x/web", ecr, false},
		{ecr, ecr, false},
	} {
		if got := imagesrc.UnderTarget(tc.image, tc.target); got != tc.under {
			t.Errorf("UnderTarget(%q, %q) = %v, want %v", tc.image, tc.target, got, tc.under)
		}
	}
	if host, prefix := imagesrc.SplitRegistryTarget(ecr); host != "123456789012.dkr.ecr.us-west-2.amazonaws.com" || prefix != "sdo/q2" {
		t.Fatalf("split = %q %q", host, prefix)
	}
	if host, prefix := imagesrc.SplitRegistryTarget("registry:5000"); host != "registry:5000" || prefix != "" {
		t.Fatalf("split = %q %q", host, prefix)
	}
}

func TestDeriveBuildsImagesUnderAPrefixedTarget(t *testing.T) {
	const target = "reg.example/sdo/q2"
	c := imagesrc.NewCatalog()
	if err := c.SetTrustedPushHost(target); err != nil {
		t.Fatal(err)
	}
	c.SetBuildDerivation("builder", nil)
	got, err := c.Derive([]string{target + "/web:v1", "reg.example/elsewhere/web:v1"}, nil, tree("web/Dockerfile"))
	if err == nil && len(got) == 1 && got[0].Image == target+"/web" {
		return
	}
	t.Fatalf("declared = %+v, err = %v; want one build for the prefixed image only", got, err)
}
