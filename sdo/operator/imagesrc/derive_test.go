package imagesrc_test

import (
	"strings"
	"testing"
	"testing/fstest"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"sdo.dev/operator/imagesrc"
)

func tree(paths ...string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for _, p := range paths {
		fsys[p] = &fstest.MapFile{Data: []byte("FROM scratch\n")}
	}
	return fsys
}

func TestBuildForFindsTheImagesDockerfileByName(t *testing.T) {
	for _, tc := range []struct {
		name, image     string
		fsys            fstest.MapFS
		context, docker string
	}{
		{"Dockerfile.N at the root builds from the root", "reg:5000/app/frontend:v1",
			tree("Dockerfile.frontend", "Dockerfile.geo", "cmd/frontend/main.go"), ".", "Dockerfile.frontend"},
		{"N.Dockerfile in a directory builds from that directory", "reg:5000/app/api",
			tree("deploy/api.Dockerfile", "deploy/web.Dockerfile"), "deploy", "deploy/api.Dockerfile"},
		{"N/Dockerfile builds from N", "reg:5000/api:latest",
			tree("services/api/Dockerfile", "services/web/Dockerfile"), "services/api", "services/api/Dockerfile"},
		{"the only Dockerfile of the tree builds the image", "reg:5000/shop",
			tree("Dockerfile", "src/x.go"), ".", "Dockerfile"},
		{".sdo and hidden directories hold no build", "reg:5000/api",
			tree(".sdo/deployment/build/api/Dockerfile", ".git/api/Dockerfile", "api/Dockerfile"), "api", "api/Dockerfile"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			context, dockerfile, err := imagesrc.BuildFor(tc.fsys, tc.image)
			if err != nil {
				t.Fatal(err)
			}
			if context != tc.context || dockerfile != tc.docker {
				t.Fatalf("build = (%q, %q), want (%q, %q)", context, dockerfile, tc.context, tc.docker)
			}
		})
	}
}

func TestBuildForRefusesWhatTheNamesDoNotSettle(t *testing.T) {
	for _, tc := range []struct {
		name, image string
		fsys        fstest.MapFS
		want        []string
	}{
		{"no Dockerfile for the image", "reg:5000/api", tree("web/Dockerfile", "worker/Dockerfile"),
			[]string{"api", "Dockerfile.api", "api/Dockerfile", ".sdo/deployment/plan.yaml"}},
		{"two Dockerfiles for the image", "reg:5000/api", tree("a/api/Dockerfile", "Dockerfile.api"),
			[]string{"Dockerfile.api", "a/api/Dockerfile"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := imagesrc.BuildFor(tc.fsys, tc.image)
			if err == nil {
				t.Fatal("want an error")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want it to name %q", err, want)
				}
			}
		})
	}
}

func TestDeriveDeclaresABuildOnlyForUndeclaredImagesUnderThePushHost(t *testing.T) {
	c := imagesrc.NewCatalog()
	if err := c.SetTrustedPushHost("reg:5000"); err != nil {
		t.Fatal(err)
	}
	c.SetBuildDerivation("builder", func(context, dockerfile string) (*anypb.Any, error) {
		return anypb.New(wrapperspb.String(context + "|" + dockerfile))
	})
	declared := []imagesrc.Declared{{Image: "reg:5000/web", SourceID: "pinned"}}
	got, err := c.Derive([]string{"reg:5000/api:v1", "reg:5000/web:v2", "redis:7"}, declared, tree("api/Dockerfile", "web/Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != declared[0] || got[1].Image != "reg:5000/api" || got[1].SourceID != "builder" {
		t.Fatalf("declared = %+v, want web unchanged and one api build", got)
	}
	var config wrapperspb.StringValue
	if err := got[1].Config.UnmarshalTo(&config); err != nil || config.GetValue() != "api|api/Dockerfile" {
		t.Fatalf("config = %v (%v), want api|api/Dockerfile", config.GetValue(), err)
	}
}
