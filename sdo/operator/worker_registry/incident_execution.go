package worker_registry

import (
	"context"
	"encoding/json"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sort"
	"strings"
)

// IncidentExecutionNames returns every durable execution of this incident,
// including unpublished reservations and continuation workers. API/read errors
// are uncertainty, never proof of quiescence.
func IncidentExecutionNames(ctx context.Context, client kubernetes.Interface, namespace, root, incidentID string, initialOnly bool) ([]string, error) {
	names := map[string]bool{JobName(incidentID): true}
	if root != "" {
		entries, err := os.ReadDir(filepath.Join(root, standbyRoot, "bindings"))
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			data, err := os.ReadFile(filepath.Join(root, standbyRoot, "bindings", entry.Name()))
			if err != nil {
				return nil, err
			}
			var b WarmBinding
			if err := json.Unmarshal(data, &b); err != nil {
				return nil, err
			}
			if b.IncidentID == incidentID {
				if !strings.HasPrefix(b.JobName, "sdo-standby-") {
					return nil, fmt.Errorf("invalid incident binding Job")
				}
				names[b.JobName] = true
			}
		}
	}
	maps, err := client.CoreV1().ConfigMaps(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("inspect incident execution requests: %w", err)
	}
	for _, cm := range maps.Items {
		if strings.HasSuffix(cm.Name, "-binding") && cm.Annotations["sdo.dev/incident-id"] == incidentID && (!initialOnly || cm.Annotations["sdo.dev/task-id"] == incidentID) {
			name := cm.Annotations["sdo.dev/job-name"]
			if strings.HasPrefix(name, "sdo-standby-") && cm.Name == name+"-binding" {
				job, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
				if err == nil && string(job.UID) == cm.Annotations["sdo.dev/job-uid"] {
					names[name] = true
				} else if err != nil && !apierrors.IsNotFound(err) {
					return nil, err
				}
			}
		}
		if !strings.HasSuffix(cm.Name, "-request") {
			continue
		}
		task := new(contracts.AgentTask)
		if wire.Unmarshal([]byte(cm.Data[TaskDataKey]), task) != nil {
			continue
		}
		request := new(contracts.IncidentRequest)
		if task.GetRequest().UnmarshalTo(request) != nil || request.GetIncidentId() != incidentID {
			continue
		}
		if initialOnly && task.GetTaskId() != incidentID {
			continue
		}
		names[strings.TrimSuffix(cm.Name, "-request")] = true
	}
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}

// IncidentResponderJobName exports the initial responder's actual execution
// identity; benchmark receipt construction remains outside the production edge.
func IncidentResponderJobName(ctx context.Context, client kubernetes.Interface, namespace, incidentID string) (string, error) {
	names, err := IncidentExecutionNames(ctx, client, namespace, "", incidentID, true)
	if err != nil {
		return "", err
	}
	found := ""
	for _, name := range names {
		_, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		if found != "" {
			return "", fmt.Errorf("multiple initial responder Jobs for incident %q", incidentID)
		}
		found = name
	}
	if found == "" {
		return JobName(incidentID), nil
	}
	return found, nil
}
