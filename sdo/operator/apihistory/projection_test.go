package apihistory

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestProjectionRejectsUnreviewedShapes(t *testing.T) {
	for _, tc := range []struct{ name, raw, want string }{
		{
			"scalar_object_smuggling",
			`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":{"containers":[{"name":"web","image":{"message":"fake-private"},"ports":[{"containerPort":{"value":"fake-private"},"protocol":["fake-private"]}],"readinessProbe":{"exec":{"command":["fake-private"]},"httpGet":{"port":{"value":"fake-private"}}}}]},"status":{"phase":["fake-private"],"containerStatuses":[{"ready":{"value":"fake-private"},"restartCount":"fake-private","state":{"running":{"message":"fake-private"}}}]}}`,
			`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"redacted":true,"spec":{"containers":[{"name":"web","ports":[{}],"readinessProbe":{"exec":{},"httpGet":{}}}]},"status":{"containerStatuses":[{"state":{"running":{}}}]}}`,
		},
		{
			"presence_empty_ips",
			`{"apiVersion":"v1","kind":"Pod","status":{"podIP":"","podIPPresent":true,"podIPs":[{"ip":"fake-private"}]}}`,
			`{"apiVersion":"v1","kind":"Pod","metadata":{},"redacted":true,"status":{"podIPPresent":false}}`,
		},
		{
			"headless_service_presence",
			`{"apiVersion":"v1","kind":"Service","spec":{"clusterIP":"None","clusterIPs":["fake-private"],"clusterIPPresent":false}}`,
			`{"apiVersion":"v1","kind":"Service","metadata":{},"redacted":true,"spec":{"clusterIPPresent":true}}`,
		},
		{
			"empty_endpoint_lists",
			`{"apiVersion":"v1","kind":"Endpoints","subsets":[{"addresses":[],"notReadyAddresses":[],"readyAddressCount":99}]}`,
			`{"apiVersion":"v1","kind":"Endpoints","metadata":{},"redacted":true,"subsets":[{"readyAddressCount":0,"notReadyAddressCount":0}]}`,
		},
		{
			"unspecified_slice_readiness",
			`{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","endpoints":[{"addresses":["192.0.2.1"]},{"addresses":["192.0.2.2"],"conditions":{"ready":null}},{"addresses":["192.0.2.3"],"conditions":{"ready":false}}]}`,
			`{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","metadata":{},"redacted":true,"readyAddressCount":2,"notReadyAddressCount":1}`,
		},
		{
			"list_uses_enclosing_kind",
			`{"apiVersion":"apps/v1","kind":"DeploymentList","metadata":{"continue":"fake-private"},"items":[{"kind":"Secret","apiVersion":"fake-private","spec":{"template":{"spec":{"containers":[{"image":"example.invalid/web:v1"}]}}},"data":{"token":"fake-private"}}]}`,
			`{"apiVersion":"apps/v1","kind":"DeploymentList","metadata":{"continue":"redacted"},"redacted":true,"items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{},"redacted":true,"spec":{"template":{"spec":{"containers":[{"image":"example.invalid/web:v1"}]}}}}]}`,
		},
		{
			"hpa_v1_spec",
			`{"apiVersion":"autoscaling/v1","kind":"HorizontalPodAutoscaler","spec":{"scaleTargetRef":{"kind":"Deployment","name":"web"},"minReplicas":1,"maxReplicas":3,"targetCPUUtilizationPercentage":70},"status":{"message":"fake-private"}}`,
			`{"apiVersion":"autoscaling/v1","kind":"HorizontalPodAutoscaler","metadata":{},"redacted":true,"spec":{"scaleTargetRef":{"kind":"Deployment","name":"web"},"minReplicas":1,"maxReplicas":3,"targetCPUUtilizationPercentage":70}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, _, err := projectResponse([]byte(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			var got, want any
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(tc.want), &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("projection = %s; want %s", body, tc.want)
			}
		})
	}
}

func TestMalformedIdentityNeverBecomesEmptyList(t *testing.T) {
	for _, raw := range []string{
		`{"apiVersion":"v1","kind":"PodList","items":{"message":"fake-private"}}`,
		`{"apiVersion":"v1","kind":"PodList","metadata":{"continue":{"message":"fake-private"}},"items":[]}`,
		`{"apiVersion":"v1","kind":"PodList","metadata":"fake-private","items":[]}`,
		`{"apiVersion":"v1","kind":"PodList","items":[{"metadata":{"name":{"message":"fake-private"}}}]}`,
	} {
		if _, _, err := projectResponse([]byte(raw)); err == nil {
			t.Errorf("malformed identity/list could be read as absence: %s", raw)
		}
	}
}

func TestImageReferenceProjection(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, tc := range []struct{ raw, want string }{
		{"busybox:v1", "docker.io/library/busybox:v1"},
		{"example.invalid:5000/team/web:v1", "example.invalid:5000/team/web:v1"},
		{"example.invalid/web@" + digest, "example.invalid/web@" + digest},
		{"example.invalid/web:v1@" + digest, "example.invalid/web:v1@" + digest},
		// Valid identifiers are disclosed deliberately; syntax is not a secret detector.
		{"example.invalid/web:synthetic-token-shaped-tag", "example.invalid/web:synthetic-token-shaped-tag"},
		{"https://user:synthetic-password@example.invalid/web:v1", ""},
		{"user:synthetic-password@example.invalid/web:v1", ""},
		{"example.invalid/web:v1?token=synthetic-password", ""},
		{"example.invalid/web:v1#synthetic-password", ""},
		{"Bearer synthetic-password", ""},
		{"example.invalid/web@sha256:bad", ""},
		{"example.invalid/web:v1\n", ""},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			for _, list := range []bool{false, true} {
				container := map[string]any{"name": "web", "image": tc.raw}
				obj := map[string]any{"apiVersion": "v1", "kind": "Pod", "spec": map[string]any{"containers": []any{container}, "initContainers": []any{container}}, "status": map[string]any{"containerStatuses": []any{container}, "initContainerStatuses": []any{container}}}
				if list {
					obj = map[string]any{"apiVersion": "v1", "kind": "PodList", "items": []any{obj}}
				}
				b, _ := json.Marshal(obj)
				out, _, err := projectResponse(b)
				if err != nil {
					t.Fatal(err)
				}
				var projected map[string]any
				if err := json.Unmarshal(out, &projected); err != nil {
					t.Fatal(err)
				}
				if list {
					projected = projected["items"].([]any)[0].(map[string]any)
				}
				for parent, fields := range map[string][]string{"spec": {"containers", "initContainers"}, "status": {"containerStatuses", "initContainerStatuses"}} {
					for _, field := range fields {
						got := projected[parent].(map[string]any)[field].([]any)[0].(map[string]any)["image"]
						if (tc.want == "" && got != nil) || (tc.want != "" && got != tc.want) {
							t.Errorf("%s: image=%v want=%q", field, got, tc.want)
						}
					}
				}
			}
		})
	}
}

func TestConstrainedOperationalLeavesAreCanonical(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{`{"apiVersion":"v1","kind":"Pod","spec":{"containers":[{"resources":{"requests":{"cpu":"0.1","memory":"1024Mi"}},"env":[{"valueFrom":{"fieldRef":{"apiVersion":"v1","fieldPath":"metadata.annotations['example.invalid/key']"},"resourceFieldRef":{"resource":"limits.cpu","divisor":"0.1"}}}],"envFrom":[{"secretRef":{"name":"settings","key":"invented","optional":false}}]}]}}`,
			`{"apiVersion":"v1","kind":"Pod","metadata":{},"redacted":true,"spec":{"containers":[{"resources":{"requests":{"cpu":"100m","memory":"1Gi"}},"env":[{"valueFrom":{"fieldRef":{"apiVersion":"v1","fieldPath":"metadata.annotations['example.invalid/key']"},"resourceFieldRef":{"resource":"limits.cpu","divisor":"100m"}}}],"envFrom":[{"secretRef":{"name":"settings","optional":false}}]}]}}`},
		{`{"apiVersion":"v1","kind":"Event","reason":"CustomOpaqueReason","eventTime":"2026-01-01T01:02:03.123+01:00"}`,
			`{"apiVersion":"v1","kind":"Event","metadata":{},"redacted":true,"reason":"redacted","eventTime":"2026-01-01T00:02:03.123Z"}`},
		{`{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","spec":{"ingress":[{"from":[{"ipBlock":{"cidr":"192.0.2.5/24","except":["192.0.2.7/32"]}}],"ports":[{"port":"http","protocol":"TCP"}]}]}}`,
			`{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{},"redacted":true,"spec":{"ingress":[{"from":[{"ipBlock":{"cidr":"192.0.2.0/24","except":["192.0.2.7/32"]}}],"ports":[{"port":"http","protocol":"TCP"}]}]}}`},
	} {
		out, _, err := projectResponse([]byte(tc.raw))
		if err != nil {
			t.Fatal(err)
		}
		var got, want any
		json.Unmarshal(out, &got)
		json.Unmarshal([]byte(tc.want), &want)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("projection=%s want=%s", out, tc.want)
		}
	}
}
