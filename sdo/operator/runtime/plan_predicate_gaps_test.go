package runtime

import (
	"sdo.dev/operator/sdk"
	"testing"
)

func TestPlanPredicateRejectsRV13ChannelAndRoutingGaps(t *testing.T) {
	pod := func(extra map[string]any) map[string]any {
		p := map[string]any{"automountServiceAccountToken": false, "containers": []any{map[string]any{"name": "api"}}}
		for k, v := range extra {
			p[k] = v
		}
		return p
	}
	for _, key := range []string{"serviceAccount", "nfs", "flexVolume"} {
		t.Run(key, func(t *testing.T) {
			spec := pod(map[string]any{key: map[string]any{"placeholder": "placeholder"}})
			if key == "serviceAccount" {
				spec[key] = "placeholder"
			} else {
				delete(spec, key)
				spec["volumes"] = []any{map[string]any{"name": "fixture", key: map[string]any{"placeholder": "placeholder"}}}
			}
			object := planObject(sdk.ObjectRef{Kind: "Deployment"}, map[string]any{"template": map[string]any{"spec": spec}})
			if planSecretPredicate(object) == nil {
				t.Fatal("unconfined channel accepted")
			}
		})
	}
	for _, kind := range []string{"Pod", "CronJob"} {
		t.Run(kind, func(t *testing.T) {
			spec := pod(nil)
			delete(spec, "automountServiceAccountToken")
			if kind == "CronJob" {
				spec = map[string]any{"jobTemplate": map[string]any{"spec": map[string]any{"template": map[string]any{"spec": spec}}}}
			}
			object := planObject(sdk.ObjectRef{Kind: kind}, spec)
			if planSecretPredicate(object) == nil {
				t.Fatal("implicit token automount accepted")
			}
		})
	}
	for _, direction := range []string{"ingress", "egress"} {
		for _, peers := range []any{nil, []any{}, []any{map[string]any{}}, []any{map[string]any{"namespaceSelector": map[string]any{}}}, []any{map[string]any{"podSelector": map[string]any{}}}} {
			t.Run(direction, func(t *testing.T) {
				rule := map[string]any{}
				key := "from"
				if direction == "egress" {
					key = "to"
				}
				if peers != nil {
					rule[key] = peers
				}
				object := planObject(sdk.ObjectRef{Kind: "NetworkPolicy"}, map[string]any{"podSelector": map[string]any{}, direction: []any{rule}})
				if planSecretPredicate(object) == nil {
					t.Fatal("allow-all or cross-namespace route accepted")
				}
			})
		}
	}
	safe := planObject(sdk.ObjectRef{Kind: "NetworkPolicy"}, map[string]any{"podSelector": map[string]any{"matchLabels": map[string]any{"app": "api"}}, "egress": []any{map[string]any{"to": []any{map[string]any{"podSelector": map[string]any{"matchLabels": map[string]any{"app": "db"}}}}}}})
	if err := planSecretPredicate(safe); err != nil {
		t.Fatalf("bounded namespace-local route refused: %v", err)
	}
}

func TestPlanCredentialDeltaRejectsNewImplicitTokenChannel(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	before := planObject(ref, map[string]any{"replicas": int64(1)})
	after := planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "api"}}}}})
	if validateCredentialChannelDelta(before, after) == nil {
		t.Fatal("new implicit service account token channel accepted")
	}
	if err := validateCredentialChannelDelta(after, after.DeepCopy()); err != nil {
		t.Fatal("unchanged legacy token channel freezes unrelated repair")
	}
}
