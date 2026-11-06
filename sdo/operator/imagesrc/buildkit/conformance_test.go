package buildkit

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/imagesrctest"
)

var _ imagesrc.Source[*contracts.BuildKitImageConfig] = Builder{}

func TestBuildKitResolvesThroughTheCatalog(t *testing.T) {
	root, checkout := proposalCheckout(t)
	b, _ := completedBuild(root)
	c := imagesrc.NewCatalog()
	if err := imagesrc.Register[*contracts.BuildKitImageConfig](c, "buildkit", b, imagesrc.Options{SameRepository: true}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetTrustedPushHost("reg.example"); err != nil {
		t.Fatal(err)
	}
	config, err := anypb.New(&contracts.BuildKitImageConfig{ContextPath: "svc", DockerfilePath: "svc/Dockerfile"})
	if err != nil {
		t.Fatal(err)
	}
	image := "reg.example/app/api:1"
	declared := []imagesrc.Declared{{Image: image, SourceID: "buildkit", Config: config}}
	base := imagesrc.Request{Tree: "tree", FS: fstest.MapFS{}, Checkout: checkout}
	got, err := c.ResolveAll(context.Background(), []string{image}, declared, base)
	if err != nil {
		t.Fatal(err)
	}
	want := imagesrc.Resolved{Ref: "reg.example/app/api@" + synthDigest, Inputs: imagesrc.Inputs{SourceImage: image, ContextPath: "svc", DockerfilePath: "svc/Dockerfile"}}
	if got[image] != want {
		t.Fatalf("resolved %+v, want %+v", got[image], want)
	}
	// A path that is not in the checkout is rejected before any Job is created.
	bad, _ := anypb.New(&contracts.BuildKitImageConfig{ContextPath: "nowhere", DockerfilePath: filepath.Join("nowhere", "Dockerfile")})
	declared[0].Config = bad
	if _, err := c.ResolveAll(context.Background(), []string{image}, declared, base); err == nil {
		t.Fatal("a context outside the checkout must be rejected")
	}
	imagesrctest.TestCatalogContract(t, c, "buildkit")
}
