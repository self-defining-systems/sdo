package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk/program"
)

const ScriptServiceAccount = "sdo-script"

func (c TypedPlanClient) ScriptReady() error {
	if c.Client == nil || c.ScriptImage == "" {
		return fmt.Errorf("script Job transport requires a configured tool image and Kubernetes client")
	}
	return nil
}

// RunScript reserves a durable application before creating a nonretrying Job.
// A reservation with a missing Job is uncertain and can never launch a replacement.
func (c TypedPlanClient) RunScript(ctx context.Context, r ScriptExecutionRequest) (out ScriptExecutionResult, err error) {
	out = ScriptExecutionResult{ApplicationID: r.ApplicationID, ScriptDigest: program.ScriptDigest(r.Script)}
	if c.Client == nil || c.ScriptImage == "" || r.ApplicationID == "" || r.IncidentID == "" || r.Namespace == "" {
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
	maps := c.Client.CoreV1().ConfigMaps(r.Namespace)
	jobs := c.Client.BatchV1().Jobs(r.Namespace)
	index, getErr := maps.Get(ctx, out.JobName, metav1.GetOptions{})
	provenance, _ := json.Marshal(r.Inputs)
	fresh := false
	if apierrors.IsNotFound(getErr) {
		deadline := time.Now().Add(r.Timeout)
		if !r.Deadline.IsZero() && r.Deadline.Before(deadline) {
			deadline = r.Deadline
		}
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		index, err = maps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: out.JobName, Namespace: r.Namespace, Labels: map[string]string{"sdo.dev/script-application": digest[:32], "app.kubernetes.io/managed-by": "sdo"}}, Data: map[string]string{"application_id": r.ApplicationID, "incident_id": r.IncidentID, "script_digest": out.ScriptDigest, "deadline": deadline.UTC().Format(time.RFC3339Nano), "phase": "creating", "inputs": string(provenance)}}, metav1.CreateOptions{})
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
	if index.Data["application_id"] != r.ApplicationID || index.Data["incident_id"] != r.IncidentID || index.Data["script_digest"] != out.ScriptDigest {
		return out, fmt.Errorf("script application identity conflict")
	}
	out.JobUID = index.Data["job_uid"]
	_ = json.Unmarshal([]byte(index.Data["inputs"]), &out.Inputs)
	if raw := index.Data["result.json"]; raw != "" {
		return decodeScriptResult(out, raw)
	}
	job, getErr := jobs.Get(ctx, out.JobName, metav1.GetOptions{})
	if apierrors.IsNotFound(getErr) && fresh {
		deadline, parseErr := time.Parse(time.RFC3339Nano, index.Data["deadline"])
		if parseErr != nil {
			return out, parseErr
		}
		seconds := int64(math.Ceil(time.Until(deadline).Seconds()))
		if seconds < 1 {
			return out, fmt.Errorf("script deadline expired before Job creation")
		}
		zero := int32(0)
		env := []corev1.EnvVar{{Name: "SDO_SCRIPT_CONTROL_NAMESPACE", Value: c.ProtectedNamespace}, {Name: "SDO_SCRIPT_TEXT", Value: r.Script}, {Name: "SDO_SCRIPT_INDEX", Value: out.JobName}, {Name: "SDO_SCRIPT_NAMESPACE", Value: r.Namespace}}
		keys := make([]string, 0, len(r.Env))
		for key := range r.Env {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			env = append(env, corev1.EnvVar{Name: key, Value: r.Env[key]})
		}
		job, err = jobs.Create(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: out.JobName, Namespace: r.Namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}, Annotations: map[string]string{"sdo.dev/script-application": r.ApplicationID, "sdo.dev/script-digest": out.ScriptDigest}}, Spec: batchv1.JobSpec{BackoffLimit: &zero, ActiveDeadlineSeconds: &seconds, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"sdo.dev/script-application": digest[:32], "app.kubernetes.io/managed-by": "sdo"}}, Spec: corev1.PodSpec{ServiceAccountName: ScriptServiceAccount, RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "script", Image: c.ScriptImage, ImagePullPolicy: corev1.PullIfNotPresent, Command: []string{"/usr/local/bin/sdo-operator", "script-runner"}, Env: env}}}}}}, metav1.CreateOptions{})
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
	ticker := time.NewTicker(interval)
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
		if !time.Now().Before(deadline) {
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
func decodeScriptResult(identity ScriptExecutionResult, raw string) (ScriptExecutionResult, error) {
	var out ScriptExecutionResult
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return identity, fmt.Errorf("invalid script result: %w", err)
	}
	if out.ApplicationID != identity.ApplicationID || out.JobName != identity.JobName || out.JobUID == "" || identity.JobUID != "" && out.JobUID != identity.JobUID || out.ScriptDigest != identity.ScriptDigest {
		return identity, fmt.Errorf("script result identity mismatch")
	}
	if len(out.Stdout) > program.MaxScriptOutputBytes {
		out.Stdout = out.Stdout[:program.MaxScriptOutputBytes]
		out.StdoutTruncated = true
	}
	if len(out.Stderr) > program.MaxScriptOutputBytes {
		out.Stderr = out.Stderr[:program.MaxScriptOutputBytes]
		out.StderrTruncated = true
	}
	return out, nil
}
