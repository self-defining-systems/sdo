package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
)

// validateCredentialChannelDelta compares object images, never path spellings.
// Existing channels do not freeze unrelated repairs; new or changed channels
// require refusal. No channel is dereferenced and errors contain no values.
func validateCredentialChannelDelta(before, after *unstructured.Unstructured) error {
	prior := credentialChannels(before)
	for channel := range credentialChannels(after) {
		if !prior[channel] {
			return fmt.Errorf("plan safety: repair introduces a credential or unconfined content channel")
		}
	}
	return nil
}

func credentialChannels(object *unstructured.Unstructured) map[string]bool {
	result := map[string]bool{}
	if object == nil {
		return result
	}
	var visit func(any, string)
	visit = func(value any, path string) {
		switch v := value.(type) {
		case map[string]any:
			if _, pod := v["containers"]; pod && v["automountServiceAccountToken"] != false {
				result[path+"/implicit-service-account-token"] = true
			}
			for key, child := range v {
				blocked := false
				switch key {
				case "envFrom", "secretKeyRef", "imagePullSecrets", "secret", "secretRef", "secretName", "projected", "csi", "hostPath", "persistentVolumeClaim", "nfs", "flexVolume", "serviceAccountName", "serviceAccount", "externalName", "externalIPs", "ipBlock":
					blocked = true
				case "hostNetwork", "hostPID", "hostIPC", "privileged", "allowPrivilegeEscalation", "automountServiceAccountToken":
					blocked = child != false
				case "type":
					blocked = child == "ExternalName" || child == "LoadBalancer" || child == "NodePort"
				}
				next := path + "/" + key
				if blocked {
					encoded, _ := json.Marshal(child)
					result[next+":"+string(encoded)] = true
				}
				visit(child, next)
			}
		case []any:
			for i, child := range v {
				visit(child, fmt.Sprintf("%s/%d", path, i))
			}
		}
	}
	visit(object.Object, "")
	// Absence of a denial defaults to a token mount, including historical
	// templates with no explicit account name or minimal source representations.
	var podPath []string
	switch object.GetKind() {
	case "Pod":
		podPath = []string{"spec"}
	case "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "ReplicationController", "Job":
		podPath = []string{"spec", "template", "spec"}
	case "CronJob":
		podPath = []string{"spec", "jobTemplate", "spec", "template", "spec"}
	}
	if podPath != nil {
		pod, _, _ := unstructured.NestedMap(object.Object, podPath...)
		if pod["automountServiceAccountToken"] != false {
			account, _ := pod["serviceAccountName"].(string)
			if account == "" {
				account = "default"
			}
			result["pod-token/default-true/"+account] = true
		}
	}
	return result
}

// A pinned, independently inspected tokenless account is not a credential
// channel. No other channel can be exempted by the action or its path spelling.
func (e PlanExecutor) validateCredentialDelta(ctx context.Context, before, after *unstructured.Unstructured) error {
	if after == nil {
		return fmt.Errorf("plan safety: missing post-image")
	}
	previous, _, _ := unstructured.NestedString(before.Object, "spec", "template", "spec", "serviceAccountName")
	next, _, _ := unstructured.NestedString(after.Object, "spec", "template", "spec", "serviceAccountName")
	if previous != next {
		ref := sdk.ObjectRef{APIVersion: after.GetAPIVersion(), Kind: after.GetKind(), Namespace: after.GetNamespace(), Name: after.GetName()}
		if e.Safety == nil {
			return fmt.Errorf("plan safety: identity change requires independent authorization")
		}
		if err := e.validateServiceAccount(ctx, ref, after); err != nil {
			return err
		}
		before = before.DeepCopy()
		_ = unstructured.SetNestedField(before.Object, next, "spec", "template", "spec", "serviceAccountName")
	}
	return validateCredentialChannelDelta(before, after)
}
