package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sort"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/program"
)

const ScriptServiceAccount = "sdo-script"

func (c TypedPlanClient) ScriptReady() error {
	if c.Client == nil || c.ScriptImage == "" || c.ScriptNamespace == "" || c.ScriptRepositoryMount == "" || c.ScriptRepositoryPVC == "" {
		return fmt.Errorf("script Job transport requires tools, Kubernetes and shared incident storage")
	}
	return nil
}

// A terminal Job and its terminated pods cannot admit another mutation.
func scriptJobStopped(ctx context.Context, client kubernetes.Interface, namespace, name string) bool {
	job, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		if err != nil {
			return false
		}
		terminal := false
		for _, condition := range job.Status.Conditions {
			terminal = terminal || condition.Status == corev1.ConditionTrue && (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed)
		}
		if !terminal {
			return false
		}
	}
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + name})
	if err != nil {
		return false
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return false
		}
	}
	return true
}

// RunScript reserves a durable application before creating a nonretrying Job.
// A reservation with a missing Job is uncertain and can never launch a replacement.
func (c TypedPlanClient) RunScript(ctx context.Context, r ScriptExecutionRequest) (out ScriptExecutionResult, err error) {
	out = ScriptExecutionResult{ApplicationID: r.ApplicationID, ScriptDigest: program.ScriptDigest(r.Script)}
	if err := c.ScriptReady(); err != nil {
		return out, err
	}
	if r.ApplicationID == "" || r.IncidentID == "" || r.Namespace == "" {
		return out, fmt.Errorf("script Job requires configured image and application/incident/namespace identity")
	}
	if err = c.checkPlanNamespace(r.Namespace); err != nil {
		return out, err
	}
	if len(r.Script) == 0 || len(r.Script) > program.MaxScriptBytes || r.Timeout <= 0 || r.Timeout > program.MaxScriptTimeout {
		return out, fmt.Errorf("script Job request exceeds execution bounds")
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(r.IncidentID+"\x00"+r.ApplicationID)))
	out.JobName = "sdo-script-" + digest[:32]
	out.ResultRef = "ConfigMap/" + c.ScriptNamespace + "/" + out.JobName
	maps := c.Client.CoreV1().ConfigMaps(c.ScriptNamespace)
	jobs := c.Client.BatchV1().Jobs(c.ScriptNamespace)
	index, getErr := maps.Get(ctx, out.JobName, metav1.GetOptions{})
	provenance, _ := json.Marshal(r.Inputs)
	fresh := false
	if apierrors.IsNotFound(getErr) {
		deadline := lifecycleio.Now().Add(r.Timeout)
		if !r.Deadline.IsZero() && r.Deadline.Before(deadline) {
			deadline = r.Deadline
		}
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		index, err = maps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: out.JobName, Namespace: c.ScriptNamespace, Labels: map[string]string{"sdo.dev/script-application": digest[:32], "app.kubernetes.io/managed-by": "sdo"}}, Data: map[string]string{"application_id": r.ApplicationID, "incident_id": r.IncidentID, "script_digest": out.ScriptDigest, "deadline": deadline.UTC().Format(time.RFC3339Nano), "phase": "creating", "inputs": string(provenance), "namespace": r.Namespace}}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			index, err = maps.Get(ctx, out.JobName, metav1.GetOptions{})
		} else {
			fresh = err == nil
		}
	} else {
		err = getErr
	}
	if err != nil {
		return out, err
	}
	if index.Data["application_id"] != r.ApplicationID || index.Data["incident_id"] != r.IncidentID || index.Data["namespace"] != r.Namespace || index.Data["script_digest"] != out.ScriptDigest {
		return out, fmt.Errorf("script application identity conflict")
	}
	out.JobUID = index.Data["job_uid"]
	_ = json.Unmarshal([]byte(index.Data["inputs"]), &out.Inputs)
	if raw := index.Data["result.json"]; raw != "" {
		return decodeScriptResult(out, raw)
	}
	// A result is published only after the runner drains. On other exits wait
	// for the Job's active deadline, including when the caller was cancelled.
	defer func() {
		if err == nil {
			return
		}
		wait := context.WithoutCancel(ctx)
		ticker := lifecycleio.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			probe, cancel := context.WithTimeout(wait, 2*time.Second)
			stopped := scriptJobStopped(probe, c.Client, c.ScriptNamespace, out.JobName)
			cancel()
			if stopped {
				return
			}
			<-ticker.C
		}
	}()
	job, getErr := jobs.Get(ctx, out.JobName, metav1.GetOptions{})
	if apierrors.IsNotFound(getErr) && fresh {
		deadline, parseErr := time.Parse(time.RFC3339Nano, index.Data["deadline"])
		if parseErr != nil {
			return out, parseErr
		}
		seconds := int64(math.Ceil(lifecycleio.Until(deadline).Seconds()))
		if seconds < 1 {
			return out, fmt.Errorf("script deadline expired before Job creation")
		}
		zero := int32(0)
		env := []corev1.EnvVar{{Name: "SDO_SCRIPT_TEXT", Value: r.Script}, {Name: "SDO_SCRIPT_INDEX", Value: out.JobName}, {Name: "SDO_SCRIPT_NAMESPACE", Value: r.Namespace}, {Name: "SDO_SCRIPT_STORAGE_NAMESPACE", Value: c.ScriptNamespace}}
		keys := make([]string, 0, len(r.Env))
		for key := range r.Env {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			env = append(env, corev1.EnvVar{Name: key, Value: r.Env[key]})
		}
		sub, prepareErr := memory.PrepareIncidentSession(c.ScriptRepositoryMount, c.ScriptRepositorySubPath, r.IncidentID)
		if prepareErr != nil {
			return out, prepareErr
		}
		job, err = jobs.Create(ctx, scriptJob(c, out.JobName, r.ApplicationID, out.ScriptDigest, digest[:32], seconds, zero, env, sub), metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			job, err = jobs.Get(ctx, out.JobName, metav1.GetOptions{})
		}
	} else {
		err = getErr
	}
	if err != nil {
		return out, fmt.Errorf("recover script Job (no replacement permitted): %w", err)
	}
	if job.Annotations["sdo.dev/script-application"] != r.ApplicationID || job.Annotations["sdo.dev/script-digest"] != out.ScriptDigest || job.UID == "" {
		return out, fmt.Errorf("script Job identity unavailable or changed")
	}
	if out.JobUID != "" && out.JobUID != string(job.UID) {
		return out, fmt.Errorf("script Job UID changed; refusing replacement")
	}
	out.JobUID = string(job.UID)
	// Update only our identity field, retaining a concurrently published result.
	for attempt := 0; attempt < 4; attempt++ {
		index, err = maps.Get(ctx, out.JobName, metav1.GetOptions{})
		if err != nil {
			return out, err
		}
		if previous := index.Data["job_uid"]; previous != "" && previous != out.JobUID {
			return out, fmt.Errorf("script application Job UID conflict")
		}
		index.OwnerReferences = []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID}}
		index.Data["job_uid"] = out.JobUID
		index.Data["phase"] = "started"
		_, err = maps.Update(ctx, index, metav1.UpdateOptions{})
		if !apierrors.IsConflict(err) {
			break
		}
	}
	if err != nil {
		return out, err
	}
	interval := c.ScriptPollInterval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	ticker := lifecycleio.NewTicker(interval)
	defer ticker.Stop()
	for {
		index, err = maps.Get(ctx, out.JobName, metav1.GetOptions{})
		if err != nil {
			return out, err
		}
		if raw := index.Data["result.json"]; raw != "" {
			return decodeScriptResult(out, raw)
		}
		if raw := index.Data["capture.json"]; raw != "" {
			captured, captureErr := decodeScriptResult(out, raw)
			if captureErr != nil {
				return out, captureErr
			}
			out = captured
		}
		current, jobErr := jobs.Get(ctx, out.JobName, metav1.GetOptions{})
		if jobErr != nil {
			return out, fmt.Errorf("script Job outcome unknown: %w", jobErr)
		}
		if current.UID != job.UID {
			return out, fmt.Errorf("script Job UID changed")
		}
		for _, condition := range current.Status.Conditions {
			if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
				return out, fmt.Errorf("script Job failed without result: %s", condition.Reason)
			}
		}
		deadline, parseErr := time.Parse(time.RFC3339Nano, index.Data["deadline"])
		if parseErr != nil {
			return out, parseErr
		}
		if !lifecycleio.Now().Before(deadline) {
			out.TimedOut = true
			return out, fmt.Errorf("script Job deadline exceeded after %s", strconv.FormatFloat(r.Timeout.Seconds(), 'f', 3, 64))
		}
		select {
		case <-ctx.Done():
			out.TimedOut = ctx.Err() == context.DeadlineExceeded
			return out, ctx.Err()
		case <-ticker.C:
		}
	}
}

// Only the trusted runner mounts the incident fence and service account token.
// The runner drops credentials before executing authored shell code.
func scriptJob(c TypedPlanClient, name, application, scriptDigest, label string, seconds int64, zero int32, env []corev1.EnvVar, session string) *batchv1.Job {
	no, root, mode := false, int64(0), int32(0600)
	replace := batchv1.Failed
	ttl := int32(24 * 60 * 60)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.ScriptNamespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}, Annotations: map[string]string{"sdo.dev/script-application": application, "sdo.dev/script-digest": scriptDigest}},
		Spec: batchv1.JobSpec{TTLSecondsAfterFinished: &ttl, BackoffLimit: &zero, ActiveDeadlineSeconds: &seconds, PodReplacementPolicy: &replace,
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"sdo.dev/script-application": label, "app.kubernetes.io/managed-by": "sdo"}}, Spec: corev1.PodSpec{
				ServiceAccountName: ScriptServiceAccount, AutomountServiceAccountToken: &no, NodeName: c.ScriptNode, RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{Name: "script", Image: c.ScriptImage, ImagePullPolicy: corev1.PullIfNotPresent,
					Command: []string{"/usr/local/bin/sdo-operator", "script-runner"}, Env: env,
					SecurityContext: &corev1.SecurityContext{RunAsUser: &root, AllowPrivilegeEscalation: &no},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "incident", MountPath: "/sdo/history", SubPath: path.Join(session, "api-history")},
						{Name: "identity", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true},
					},
				}},
				Volumes: []corev1.Volume{
					{Name: "incident", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: c.ScriptRepositoryPVC}}},
					{Name: "identity", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{DefaultMode: &mode, Sources: []corev1.VolumeProjection{
						{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}},
						{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
					}}}},
				},
			}},
		},
	}
}

func decodeScriptResult(identity ScriptExecutionResult, raw string) (ScriptExecutionResult, error) {
	var out ScriptExecutionResult
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return identity, fmt.Errorf("invalid script result: %w", err)
	}
	if out.ApplicationID != identity.ApplicationID || out.JobName != identity.JobName || out.JobUID == "" || identity.JobUID != "" && out.JobUID != identity.JobUID || out.ScriptDigest != identity.ScriptDigest {
		return identity, fmt.Errorf("script result identity mismatch")
	}
	out.ResultRef = identity.ResultRef
	return out, nil
}
