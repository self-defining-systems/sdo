package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type watchingDetector struct{ watches []sdk.WatchKind }

func (d watchingDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "webhook-health", Watches: d.watches}
}

func (watchingDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

// Only a real RBAC refusal or a dependency outside the declared scope fails a
// closure as denied. A timeout or server error on the same detector is an
// unavailable observer, which waits within the recheck bound.
func TestObserverDeniedOnlyForPermanentRefusals(t *testing.T) {
	resource := schema.GroupResource{Group: "admissionregistration.k8s.io", Resource: "mutatingwebhookconfigurations"}
	cluster := watchingDetector{watches: []sdk.WatchKind{{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration"}}}
	foreign := watchingDetector{watches: []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "other"}}}
	for _, test := range []struct {
		name     string
		detector sdk.Detector
		err      error
		denied   bool
	}{
		{"forbidden", cluster, apierrors.NewForbidden(resource, "", errors.New("RBAC")), true},
		{"unauthorized", foreign, apierrors.NewUnauthorized("token expired"), true},
		{"undeclared scope", cluster, errors.New("scoped observation undeclared"), true},
		{"deadline", cluster, fmt.Errorf("list webhooks: %w", context.DeadlineExceeded), false},
		{"server timeout", foreign, apierrors.NewServerTimeout(resource, "list", 1), false},
		{"timeout", cluster, apierrors.NewTimeoutError("watch", 1), false},
		{"unavailable", foreign, apierrors.NewServiceUnavailable("apiserver restarting"), false},
		{"internal", cluster, apierrors.NewInternalError(errors.New("etcd")), false},
		{"throttled", foreign, apierrors.NewTooManyRequests("slow down", 1), false},
	} {
		if got := observerDenied(test.detector, sdktest.Snapshot{}, test.err); got != test.denied {
			t.Errorf("%s: denied=%v, want %v", test.name, got, test.denied)
		}
	}
}
