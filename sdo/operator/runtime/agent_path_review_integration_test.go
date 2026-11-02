//go:build integration

package runtime

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestResponderVerifyConcurrentControllerObservations(t *testing.T) {
	now := time.Now().UTC()
	finding := sdk.Finding{DetectorID: "availability", RuleID: "persistent", Fingerprint: "persistent", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: "persistent failure", Evidence: "synthetic sample", PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}, Playbooks: []string{"base"}}
	c, err := NewController(testControllerConfig(), []sdk.Detector{reviewScopeDetector{finding}}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &verificationRetryDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	request := IncidentRequest{IncidentID: "concurrent", Namespace: "app", Findings: []sdk.Finding{finding}, ResponseDeadline: now.Add(200 * time.Hour)}
	c.currentIncidentRequest = &request
	c.incidentOpen = true
	loop := NewResponderLoop(request, nil, c.provider, nil, PlanExecutor{}, 3)
	loop.VerifyClosure = func(ctx context.Context) ToolResponse { return c.responderVerification(ctx, request.IncidentID) }
	server := SessionToolServer{Directory: filepath.Join(t.TempDir(), "tools"), Handle: loop.Handle}
	if err = server.Prepare(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if err := c.StepEvents(context.Background(), now.Add(time.Duration(i+1)*time.Hour), []sdk.WatchKind{sdk.WatchKind{APIVersion: "v1", Kind: "Service"}}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 100; i++ {
		exchangeSelfCheck(t, &server, `{"command":"verify"}`)
	}
	wg.Wait()
}

