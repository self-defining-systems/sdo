package sdktest

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type failingReads struct {
	program.ReadView
	err error
}

func (v failingReads) Observe(program.ExternalOperation) (int32, error) { return 0, v.err }
func (v failingReads) KubernetesRead(program.KubernetesRequest) (json.RawMessage, error) {
	return nil, v.err
}

// A recorded read failure keeps its cause: "unavailable" alone cannot tell a
// read that ran out of time from a replaced Pod, so an Unknown diagnosis at
// incident open could not be explained from its evidence.
func TestRecordingKeepsTheCauseOfAFailedObservation(t *testing.T) {
	cause := errors.New("exec transport failed: context deadline exceeded")
	view := &RecordingReadView{ReadView: failingReads{ReadView: &ReplayReadView{Snapshot: Snapshot{NamespaceName: "app"}}, err: cause}}
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "store-0"}
	_, err := view.Observe(program.ExternalOperation{Exec: &program.Command{Pod: pod, Container: "store", Args: []string{"true"}}})
	if err == nil || !strings.HasPrefix(err.Error(), "external observation unavailable") || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("Observe error = %v; want the unavailable class and its cause", err)
	}
	_, err = view.KubernetesRead(program.KubernetesRequest{Method: "GET", Path: "/api/v1/namespaces/app/pods"})
	if err == nil || !strings.HasPrefix(err.Error(), "Kubernetes API observation unavailable") || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("KubernetesRead error = %v; want the unavailable class and its cause", err)
	}
	calls := view.SnapshotCalls()
	if len(calls) != 2 || !strings.Contains(calls[0].Error, "context deadline exceeded") {
		t.Fatalf("recorded calls = %+v; want the cause in the recording", calls)
	}
}
