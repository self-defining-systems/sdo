package apihistory

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func FuzzRV3bOpaqueLeaves(f *testing.F) {
	for _, s := range []string{"", "normal", "\x00\n\r\t", "https://u:p@registry.invalid/web:v1", "../x?token=x#x", "\\\"'[]{}", "κλειδί", strings.Repeat("x", 256)} {
		for n := uint8(0); n < 12; n++ {
			f.Add(n, s)
		}
	}
	f.Fuzz(func(t *testing.T, n uint8, s string) {
		if len(s) > 4096 {
			return
		}
		secret := "Authorization: Bearer rv3bcanary " + s
		c := map[string]any{"name": "web", "image": "example.invalid/web:v1"}
		status := map[string]any{"name": "web"}
		spec := map[string]any{"containers": []any{c}, "initContainers": []any{c}}
		obj := map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "p", "namespace": "app"}, "spec": spec, "status": map[string]any{"containerStatuses": []any{status}, "initContainerStatuses": []any{status}}}
		switch n % 12 {
		case 0:
			for _, p := range []string{"readinessProbe", "livenessProbe", "startupProbe"} {
				c[p] = map[string]any{"httpGet": map[string]any{"path": "/" + secret, "httpHeaders": []any{map[string]any{"name": "Authorization", "value": secret}}, "host": secret, "port": 8080}, "exec": map[string]any{"command": []any{secret}}, "grpc": map[string]any{"port": 8080, "service": secret}, "tcpSocket": map[string]any{"port": 8080, "host": secret}}
			}
		case 1:
			c["image"] = secret
			status["image"] = secret
		case 2:
			for _, p := range []string{"state", "lastState"} {
				status[p] = map[string]any{"waiting": map[string]any{"reason": secret, "message": secret}, "terminated": map[string]any{"reason": secret, "message": secret, "exitCode": 1}}
			}
		case 3:
			c["command"] = []any{secret}
			c["args"] = []any{secret}
			c["env"] = []any{map[string]any{"name": "TOKEN", "value": secret}}
			obj["metadata"].(map[string]any)["annotations"] = map[string]any{"safe": secret}
			obj["metadata"].(map[string]any)["labels"] = map[string]any{"safe": secret}
		case 4:
			c["resources"] = map[string]any{"requests": map[string]any{"cpu": secret, "memory": secret, "ephemeral-storage": secret, "storage": secret}, "limits": map[string]any{"cpu": secret}}
		case 5:
			c["env"] = []any{map[string]any{"name": "MODE", "valueFrom": map[string]any{"fieldRef": map[string]any{"apiVersion": secret, "fieldPath": secret}, "resourceFieldRef": map[string]any{"resource": secret, "divisor": secret}}}}
		case 6:
			c["envFrom"] = []any{map[string]any{"prefix": secret, "configMapRef": map[string]any{"name": "settings", "key": secret}, "secretRef": map[string]any{"name": "settings", "key": secret}}}
		case 7:
			obj["status"].(map[string]any)["phase"] = secret
			obj["status"].(map[string]any)["conditions"] = []any{map[string]any{"type": "Ready", "status": secret, "reason": secret, "message": secret}}
		case 8:
			c["ports"] = []any{map[string]any{"name": "http", "containerPort": secret, "hostPort": secret, "protocol": secret}}
			c["readinessProbe"] = map[string]any{"httpGet": map[string]any{"port": secret, "scheme": secret}, "periodSeconds": secret}
		case 9:
			obj["apiVersion"] = "apps/v1"
			obj["kind"] = "Deployment"
			obj["spec"] = map[string]any{"template": map[string]any{"metadata": map[string]any{"annotations": map[string]any{"safe": secret}}, "spec": spec}}
			c["image"] = secret
		case 10:
			obj["apiVersion"] = "networking.k8s.io/v1"
			obj["kind"] = "NetworkPolicy"
			obj["spec"] = map[string]any{"policyTypes": []any{secret}, "ingress": []any{map[string]any{"from": []any{map[string]any{"ipBlock": map[string]any{"cidr": secret, "except": []any{secret}}}}, "ports": []any{map[string]any{"port": secret, "protocol": secret, "endPort": secret}}}}}
		case 11:
			obj["kind"] = "Event"
			obj["reason"] = secret
			obj["type"] = secret
			obj["message"] = secret
			obj["eventTime"] = secret
			obj["firstTimestamp"] = secret
			obj["series"] = map[string]any{"lastObservedTime": secret, "count": secret}
		}
		if n&16 != 0 {
			obj = map[string]any{"apiVersion": obj["apiVersion"], "kind": obj["kind"].(string) + "List", "items": []any{obj}}
		}
		raw, _ := json.Marshal(obj)
		got, _, err := projectResponse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(got, []byte("rv3bcanary")) {
			t.Fatalf("canary leaked: %s", got)
		}
		again, _, err := projectResponse(raw)
		if err != nil || !bytes.Equal(got, again) {
			t.Fatal("nondeterministic projection")
		}
	})
}
