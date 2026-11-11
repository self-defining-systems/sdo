package memoryhost

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func secretMirrorFrame(t *testing.T, secret sdk.SecretMetadata) *pb.MirrorRequest {
	t.Helper()
	objects, err := ProjectSnapshot(sdktest.Snapshot{NamespaceName: "app", SecretList: []sdk.SecretMetadata{secret}})
	if err != nil {
		t.Fatal(err)
	}
	frame := &pb.MirrorRequest{Namespace: "app", Sequence: 1, ResetMirror: true, Complete: true}
	for _, item := range objects {
		frame.Upserts = append(frame.Upserts, item)
	}
	return frame
}

// The host accepts every field the value-free SecretMetadata projection
// carries (a field added to it must not make every mirror frame fail), and
// refuses any other Secret field.
func TestSecretMirrorAcceptsEverySecretMetadataFieldAndNothingElse(t *testing.T) {
	full := sdk.SecretMetadata{Name: "creds", Namespace: "app", Type: "Opaque", Labels: map[string]string{"app": "demo"},
		DataKeys: []string{"password"}, DataChangedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	if err := newMirrorTestHost(t, "accepted").Apply(secretMirrorFrame(t, full)); err != nil {
		t.Fatalf("a full value-free Secret was refused: %v", err)
	}
	frame := secretMirrorFrame(t, full)
	for _, item := range frame.Upserts {
		if item.GetRef().GetKind() == "Secret" {
			item.Object.Fields["data"] = structpb.NewStringValue("c2VjcmV0")
		}
	}
	if err := newMirrorTestHost(t, "accepted").Apply(frame); err == nil {
		t.Fatal("a Secret carrying data was accepted")
	}
}
