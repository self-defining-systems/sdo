package rendered

import (
	"testing"
)

func TestImageInputsRejectUnsafeDeclarations(t *testing.T) {
	for _, inputs := range []ImageInputs{
		{ContextPath: "src", DockerfilePath: "Dockerfile"},
		{SourceImage: "app", ContextPath: "../src", DockerfilePath: "Dockerfile"},
		{SourceImage: "app", ContextPath: "src", DockerfilePath: "/Dockerfile"},
		{SourceImage: "app", ContextPath: "src", DockerfilePath: "."},
		{SourceImage: "app", ContextPath: "src/../other", DockerfilePath: "Dockerfile"},
		{SourceImage: "app", ContextPath: "src"},
	} {
		if err := inputs.Validate(); err == nil {
			t.Fatalf("accepted unsafe declaration: %+v", inputs)
		}
	}
}
