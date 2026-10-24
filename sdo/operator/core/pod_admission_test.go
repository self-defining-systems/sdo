package core

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/workloadhealth"
)

func TestPodAdmissionRejectingAPIIsNotClear(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/namespaces/demo/pods" || r.URL.Query().Get("dryRun") != metav1.DryRunAll {
			t.Errorf("expected only a dry-run Pod create, got %s %s", r.Method, r.URL)
		}
		var pod corev1.Pod
		if err := json.NewDecoder(r.Body).Decode(&pod); err != nil {
			t.Error(err)
		}
		if pod.Name != "" || pod.GenerateName == "" || pod.ResourceVersion != "" || pod.UID != "" || pod.Spec.NodeName != "" || pod.Status.Phase != "" {
			t.Errorf("allocated fields retained: %+v", pod)
		}
		if pod.Labels["app"] != "api" || pod.Spec.Containers[0].Image != "example/api:1" {
			t.Error("lost app admission inputs")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: "Failure", Code: 500, Reason: metav1.StatusReasonInternalError, Message: "admission rejected: x509 certificate signed by unknown authority"})
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	source := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "demo", ResourceVersion: "123", UID: "allocated", Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "api", Image: "example/api:1"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	result := ObservePodAdmission(context.Background(), client, "demo", []corev1.Pod{source})
	snapshot := DetectionSnapshot{NamespaceName: "demo", PodList: []corev1.Pod{source}, Admission: &result}
	detector := workloadhealth.NewPodAdmissionDetector(sdk.DetectorSpec{ID: "pod-admission", Playbooks: []string{"health-objective"}})
	findings, err := sdk.DetectObserved(context.Background(), detector, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(findings) != 1 || findings[0].Status != sdk.FindingActive || !strings.Contains(findings[0].Evidence, "x509") {
		t.Fatalf("rejected admission must not clear: calls=%d findings=%+v", calls, findings)
	}
	if source.Spec.NodeName != "node-a" || source.Name != "api-1" {
		t.Fatal("probe mutated original pod")
	}
}

func TestPodAdmissionTransportErrorsAndRecovery(t *testing.T) {
	for _, tc := range []struct {
		name      string
		probeErr  error
		rejection bool
	}{
		{name: "success"},
		{name: "forbidden", probeErr: apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", errors.New("policy denies Pod create")), rejection: true},
		{name: "timeout", probeErr: context.DeadlineExceeded},
		{name: "API timeout", probeErr: apierrors.NewTimeoutError("try later", 1)},
		{name: "cancelled", probeErr: context.Canceled},
		{name: "connection", probeErr: errors.New("connection refused")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			calls := 0
			client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
				calls++
				return true, action.(ktesting.CreateAction).GetObject(), tc.probeErr
			})
			provider := KubernetesSnapshotProvider{Namespace: "demo", Client: client, PodAdmission: true}
			snapshot, err := provider.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			result, ok := snapshot.PodAdmission()
			if !ok || calls != 1 || result.Admitted != (tc.probeErr == nil) || (result.Rejection != "") != tc.rejection {
				t.Fatalf("result=%+v calls=%d", result, calls)
			}
			detector := workloadhealth.NewPodAdmissionDetector(sdk.DetectorSpec{ID: "pod-admission"})
			findings, err := sdk.DetectObserved(context.Background(), detector, snapshot)
			if tc.probeErr != nil && !tc.rejection && err == nil {
				t.Fatalf("observation failure cleared: %+v", result)
			}
			if tc.rejection && len(findings) != 1 {
				t.Fatal("API rejection did not fire")
			}
			tc.probeErr = nil
			recovered, err := provider.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			findings, err = sdk.DetectObserved(context.Background(), detector, recovered)
			if err != nil || len(findings) != 0 {
				t.Fatalf("recovery findings=%v err=%v", findings, err)
			}
		})
	}
}
