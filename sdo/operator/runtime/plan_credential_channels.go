package runtime

import (
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
