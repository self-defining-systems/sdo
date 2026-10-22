package apihistory

import (
	"encoding/json"
	"reflect"
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
