package buildkit

import (
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestDerivedConfigIsTheBuildOfTheMatchedDockerfile(t *testing.T) {
	packed, err := DerivedConfig("services/api", "services/api/Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	var config contracts.BuildKitImageConfig
	if err := packed.UnmarshalTo(&config); err != nil || config.GetContextPath() != "services/api" || config.GetDockerfilePath() != "services/api/Dockerfile" {
		t.Fatalf("config = %v (%v)", &config, err)
	}
}
