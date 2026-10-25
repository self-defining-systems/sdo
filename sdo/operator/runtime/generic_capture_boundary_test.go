package runtime

import (
	"context"
	"encoding/json"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

type requestedCaptureScope struct {
	sdktest.Snapshot
	wanted sdk.ObjectRef
	seen   *bool
}

func (s requestedCaptureScope) CaptureObjects(refs []sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	for _, ref := range refs {
		if ref == s.wanted {
			*s.seen = true
			return []unstructured.Unstructured{{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}, "webhooks": []any{}}}}, nil
		}
	}
	return nil, nil
}
func TestPreApplyRequestIncludesTargetBeforeReceiptExists(t *testing.T) {
	root := t.TempDir()
	seen := false
	ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy"}
	snapshot := requestedCaptureScope{Snapshot: sdktest.Snapshot{NamespaceName: "app"}, wanted: ref, seen: &seen}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "generic", Namespace: "app"}, nil, staticProvider{snapshot: snapshot}, nil, PlanExecutor{}, 1)
	loop.CorpusSessionRoot = root
	response := loop.Handle(context.Background(), ToolRequest{Command: "pre-apply", Refs: []sdk.ObjectRef{ref}})
	if response.Error != "" || !seen {
		t.Fatalf("target missing at capture: %+v", response)
	}
	dir, err := memory.IncidentCorpusDir(root, "generic")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "open@pre-apply.json"))
	if err != nil {
		t.Fatal(err)
	}
	var captured sdktest.Snapshot
	if err = json.Unmarshal(raw, &captured); err != nil {
		t.Fatal(err)
	}
	if _, err := captured.Object(ref); err != nil {
		t.Fatal(err)
	}
}
