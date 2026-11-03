package memoryhost

import (
	"context"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

type copyProbeProgram struct{}

func (copyProbeProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "copy-probe"} }
func (copyProbeProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.NoMatch}, nil
}
func TestMirrorDecodingKeepsRPCCopiesAndHistoricalVersionsPrivate(t *testing.T) {
	ctx := context.Background()
	host := newMirrorTestHost(t, "accepted")
	registry, err := program.NewRegistry([]program.Diagnoser{copyProbeProgram{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	host.WithPrograms(registry)
	expected := "original"
	host.WithReadFactory(func(_ context.Context, s sdktest.Snapshot) (program.ReadView, error) {
		if s.PodList[0].Labels["value"] != expected || s.SecretList[0].Labels["value"] != expected || s.JobList[0].Labels["value"] != expected {
			t.Error("snapshot mutation crossed RPC/version boundary")
		}
		s.PodList[0].Labels["value"] = "mutated"
		s.SecretList[0].Labels["value"] = "mutated"
		s.JobList[0].Labels["value"] = "mutated"
		return program.ReadFrom(s), nil
	})
	manager := NewManager(0)
	defer manager.Close()
	publish := func(value string) {
		s := sdktest.Snapshot{NamespaceName: "app", JobList: []batchv1.Job{{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "app", Labels: map[string]string{"value": value}}}}, PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", Labels: map[string]string{"value": value}}}}, SecretList: []sdk.SecretMetadata{{Name: "metadata-only", Namespace: "app", Labels: map[string]string{"value": value}}}}
		if err := manager.Publish(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	publish(expected)
	if err := manager.Prepare(ctx, host); err != nil {
		t.Fatal(err)
	}
	if err := manager.Activate(ctx, host, "accepted", "digest"); err != nil {
		t.Fatal(err)
	}
	for _, sequence := range []uint64{1, 1, 2, 1, 2} {
		if sequence == 2 {
			expected = "updated"
			publish(expected)
		} else {
			expected = "original"
		}
		response, err := host.Diagnose(ctx, &pb.DiagnoseRequest{Input: &pb.MemoryProgramInput{ProgramId: "copy-probe", MirrorSequence: sequence}})
		if err != nil || response.Error != "" {
			t.Fatalf("diagnose: %v %v", response, err)
		}
	}
}
