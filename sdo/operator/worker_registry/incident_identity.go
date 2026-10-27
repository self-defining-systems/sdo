package worker_registry

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const incidentExecutionLabel = "sdo.dev/incident-execution"
const incidentExecutionDataKey = "execution.json"

// Each immutable attempt has one transport-independent identity, published
// before a cold Job or warm inbox can release work. It outlives Job TTL and PVC
// request cleanup so orphan Pods remain attributable after controller restart.
type incidentExecutionIdentity struct {
	Version    int    `json:"version"`
	IncidentID string `json:"incident_id"`
	TaskID     string `json:"task_id"`
	JobName    string `json:"job_name"`
	Digest     string `json:"request_sha256"`
	JobUID     string `json:"job_uid,omitempty"`
}

func incidentExecutionName(digest string) string { return "sdo-exec-" + digest[:52] }

func decodeIncidentExecution(cm *corev1.ConfigMap) (*incidentExecutionIdentity, error) {
	var identity incidentExecutionIdentity
	decoder := json.NewDecoder(bytes.NewBufferString(cm.Data[incidentExecutionDataKey]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&identity); err != nil {
		return nil, fmt.Errorf("decode incident execution %s: %w", cm.Name, err)
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("incident execution has trailing data")
	}
	digest, err := hex.DecodeString(identity.Digest)
	if err != nil || len(digest) != 32 || identity.Version != 1 || identity.IncidentID == "" || identity.TaskID == "" || len(validation.IsDNS1123Subdomain(identity.JobName)) != 0 || cm.Labels[incidentExecutionLabel] != "1" {
		return nil, fmt.Errorf("invalid incident execution %s", cm.Name)
	}
	if cm.Name != incidentExecutionName(identity.Digest) || cm.Annotations["sdo.dev/incident-id"] != identity.IncidentID ||
		(identity.JobName != JobName(identity.TaskID) && identity.JobName != "sdo-task-"+identity.Digest[:52] && !strings.HasPrefix(identity.JobName, "sdo-standby-")) {
		return nil, fmt.Errorf("conflicting incident execution identity %s", cm.Name)
	}
	return &identity, nil
}

func (e KubernetesExecutor) loadIncidentExecution(ctx context.Context, task *contracts.AgentTask) (*incidentExecutionIdentity, error) {
	if !task.GetRequest().MessageIs(&contracts.IncidentRequest{}) {
		return nil, nil
	}
	cm, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Get(ctx, incidentExecutionName(taskRequestDigest(task)), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read incident execution identity: %w", err)
	}
	identity, err := decodeIncidentExecution(cm)
	if err != nil {
		return nil, err
	}
	request := new(contracts.IncidentRequest)
	if err := task.Request.UnmarshalTo(request); err != nil {
		return nil, err
	}
	if identity.Digest != taskRequestDigest(task) || identity.TaskID != task.TaskId || identity.IncidentID != request.IncidentId {
		return nil, errors.New("incident execution differs from immutable task")
	}
	return identity, nil
}

func (e KubernetesExecutor) ensureIncidentExecution(ctx context.Context, task *contracts.AgentTask, jobName, jobUID string) error {
	if !task.GetRequest().MessageIs(&contracts.IncidentRequest{}) {
		return nil
	}
	request := new(contracts.IncidentRequest)
	if err := task.Request.UnmarshalTo(request); err != nil {
		return err
	}
	identity := incidentExecutionIdentity{Version: 1, IncidentID: request.IncidentId, TaskID: task.TaskId, JobName: jobName, Digest: taskRequestDigest(task), JobUID: jobUID}
	data, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	immutable := true
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: incidentExecutionName(identity.Digest), Labels: map[string]string{incidentExecutionLabel: "1", "app.kubernetes.io/managed-by": "sdo-operator"}, Annotations: map[string]string{"sdo.dev/incident-id": identity.IncidentID}}, Immutable: &immutable, Data: map[string]string{incidentExecutionDataKey: string(data)}}
	if _, err := decodeIncidentExecution(cm); err != nil {
		return err
	}
	_, err = e.Client.CoreV1().ConfigMaps(e.Namespace).Create(ctx, cm, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		prior, err := e.loadIncidentExecution(ctx, task)
		if err != nil {
			return err
		}
		if prior == nil || *prior != identity {
			return errors.New("immutable incident execution already belongs to another Job")
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("publish incident execution identity: %w", err)
	}
	return nil
}

// Only proven-unpublished warm claims may discard an execution identity and
// fall back cold. Published or uncertain work keeps its original identity.
func (e KubernetesExecutor) removeUnpublishedIncidentExecution(ctx context.Context, task *contracts.AgentTask) error {
	identity, err := e.loadIncidentExecution(ctx, task)
	if err != nil || identity == nil {
		return err
	}
	if e.WarmBinding == nil || identity.JobName != e.WarmBinding.JobName {
		return errors.New("cannot remove another incident execution")
	}
	err = e.Client.CoreV1().ConfigMaps(e.Namespace).Delete(ctx, incidentExecutionName(identity.Digest), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
