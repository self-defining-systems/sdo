package builder

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

	"google.golang.org/protobuf/encoding/protojson"
)

func TestControllerLaunchPreservesTypedConfigAndInjectsBuildPaths(t *testing.T) {
	original := &contractsv1alpha1.ControllerConfig{Namespace: "app", ResponderAgentId: "custom-worker", DispatcherMode: "job", RepairPolicy: "commit", BrokerMode: "native", WorkerMountMode: "proposal-only", ResponderBackend: "codex", ResponderModel: "model"}
	data, err := controllerConfigJSON(RunConfig{Build: Config{ApplicationRoot: "/accepted/repo"}, Controller: original}, provenance{Fingerprint: "tree", ArtifactDigest: "sha256:artifact", Revision: "head"})
	if err != nil {
		t.Fatal(err)
	}
	var decoded contractsv1alpha1.ControllerConfig
	if err := protojson.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.AppRoot != "/accepted/repo" || decoded.ResponderAgentId != "custom-worker" || decoded.DiagnosticsFingerprint != "tree" || decoded.ControllerArtifactDigest != "sha256:artifact" || decoded.ControllerRevision != "head" || decoded.BrokerMode != "native" || decoded.WorkerMountMode != "proposal-only" || decoded.ResponderModel != "model" {
		t.Fatalf("launch contract lost fields: %s", decoded.String())
	}
	if original.AppRoot != "" {
		t.Fatal("launch mutated caller's config")
	}
}

func TestControllerConfigHasNoMemoryExecutionSwitch(t *testing.T) {
	fields := (&contractsv1alpha1.ControllerConfig{}).ProtoReflect().Descriptor().Fields()
	for _, name := range []protoreflect.Name{"memory_host", "restart_after_closure", "prober_binary", "prober_image", "prober_url"} {
		if fields.ByName(name) != nil {
			t.Errorf("obsolete execution switch %s remains", name)
		}
	}
}
