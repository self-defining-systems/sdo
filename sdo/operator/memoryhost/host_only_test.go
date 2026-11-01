package memoryhost

import (
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
)

func TestObservationRequiresMirrorProtocol(t *testing.T) {
	if (&pb.MemoryObservation{}).ProtoReflect().Descriptor().Fields().ByName("snapshot") != nil {
		t.Fatal("memory observations must not carry a snapshot fallback")
	}
}
