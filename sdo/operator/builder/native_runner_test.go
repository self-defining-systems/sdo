package builder

import (
	"context"
	"testing"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestControllerLaunchPreservesTypedConfigAndInjectsBuildPaths(t *testing.T) {
	original := &contractsv1alpha1.ControllerConfig{Namespace: "app", ResponderAgentId: "custom-worker", DispatcherMode: "job", RepairPolicy: "commit", BrokerMode: "native", WorkerMountMode: "proposal-only", ValidatorImage: "validator:test", SchemaIdentity: "schema", ResponderBackend: "codex", ResponderModel: "model"}
	data, err := controllerConfigJSON(RunConfig{Build: Config{ApplicationRoot: "/accepted/repo"}, Controller: original}, "/shared/.sdo-prober/digest/sdo-prober", provenance{Fingerprint: "tree", ArtifactDigest: "sha256:artifact", Revision: "head"})
	if err != nil {
		t.Fatal(err)
	}
	var decoded contractsv1alpha1.ControllerConfig
	if err := protojson.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.AppRoot != "/accepted/repo" || decoded.ProberBinary != "/shared/.sdo-prober/digest/sdo-prober" || decoded.ResponderAgentId != "custom-worker" || decoded.DiagnosticsFingerprint != "tree" || decoded.ControllerArtifactDigest != "sha256:artifact" || decoded.ControllerRevision != "head" || decoded.BrokerMode != "native" || decoded.WorkerMountMode != "proposal-only" || decoded.ValidatorImage != "validator:test" || decoded.ResponderModel != "model" {
		t.Fatalf("launch contract lost fields: %s", decoded.String())
	}
	if original.AppRoot != "" || original.ProberBinary != "" {
		t.Fatal("launch mutated caller's config")
	}
}

func TestSuperviseRejectsBoundedControllerModes(t *testing.T) {
	for _, config := range []*contractsv1alpha1.ControllerConfig{{ExitAfterClosure: true}, {Duration: durationpb.New(1)}} {
		if err := Supervise(context.Background(), RunConfig{Controller: config}); err == nil {
			t.Fatal("bounded controller mode was accepted for supervision")
		}
	}
}
