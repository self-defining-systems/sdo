package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
)

// dataAtRiskEvaluation is the pending evaluation that carries this reason into
// the outcome's detector history, the way any unconfirmed closure says why. It
// is not a detector; the name is the contract reflection's view reads.
const dataAtRiskEvaluation = "controller.data-at-risk"

// dataAtRisk names the holds_data workloads whose Pods were replaced or whose
// containers restarted since the incident opened, with nothing persistent to
// bring their data back. The Pod's own timestamps say so, so this needs no
// record of what happened or who did it, and it covers a program, a hand repair
// and a crash alike. Health cannot rule the loss out: probes see only the data
// they wrote. An emptyDir outlives a container restart but not its Pod.
func dataAtRisk(view sdk.DetectionContext, workloads map[string]bool, namespace string, since time.Time) []string {
	if len(workloads) == 0 || since.IsZero() {
		return nil
	}
	since = since.Truncate(time.Second)
	risks := map[string]string{}
	for _, pod := range view.Pods() {
		if pod.Namespace != namespace {
			continue
		}
		workload := owningWorkload(view, sdk.ObjectRef{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name})
		if !workloads[workload] || podHasPersistentStorage(pod) {
			continue
		}
		if !pod.CreationTimestamp.Time.Before(since) {
			risks[workload] = fmt.Sprintf("%s holds data and has no persistent storage, and its Pod %s was replaced during the incident; health probes cannot show whether its data survived", workload, pod.Name)
		} else if containerRestartedSince(pod, since) && risks[workload] == "" {
			risks[workload] = fmt.Sprintf("%s holds data and has no persistent storage, and a container of its Pod %s restarted during the incident; health probes cannot show whether its data survived", workload, pod.Name)
		}
	}
	result := make([]string, 0, len(risks))
	for _, risk := range risks {
		result = append(result, risk)
	}
	sort.Strings(result)
	return result
}

// Only these volume types disappear with the Pod. Anything else (a claim, a
// hostPath, a cloud volume) is taken as persistent.
func podHasPersistentStorage(pod corev1.Pod) bool {
	for _, volume := range pod.Spec.Volumes {
		source := volume.VolumeSource
		if source.ConfigMap == nil && source.Secret == nil && source.EmptyDir == nil && source.Projected == nil && source.DownwardAPI == nil && source.Ephemeral == nil {
			return true
		}
	}
	return false
}

func containerRestartedSince(pod corev1.Pod, since time.Time) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.EmptyDir != nil {
			return false
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if running := status.State.Running; running != nil && !running.StartedAt.Time.Before(since) {
			return true
		}
	}
	return false
}

// closureDataRisk decides whether a health-clear closure may stand as verified.
// It runs once, as the closure freezes: a frozen closure keeps its identity.
func (c *Controller) closureDataRisk(ctx context.Context, snapshot sdk.DetectionContext, closure IncidentClosure) string {
	if snapshot == nil || len(closure.FinalDetectorStates) == 0 {
		return ""
	}
	read := c.gitRead
	if read == nil {
		read = lifecycleio.GitOutput
	}
	workloads := acceptedDataWorkloads(ctx, read, closure.Request.RepositoryWorktree, closure.Request.SourceCommit)
	risks := dataAtRisk(snapshot, workloads, closure.Request.Namespace, closure.DetectedAt)
	if len(risks) == 0 {
		return ""
	}
	slog.Warn("closure_data_at_risk", "incident_id", closure.Request.IncidentID, "risks", risks)
	detail := risks[0]
	for _, risk := range risks[1:] {
		detail += "; " + risk
	}
	return detail
}

// incidentDataAtRisk is the same fact for the responder, on each verify.
func (l *ResponderLoop) incidentDataAtRisk(ctx context.Context) []string {
	if l.Request.DetectedAt.IsZero() {
		return nil
	}
	if !l.dataWorkloadsRead {
		l.dataWorkloads = acceptedDataWorkloads(ctx, l.gitRead(), l.Request.RepositoryWorktree, l.Request.SourceCommit)
		l.dataWorkloadsRead = true
	}
	if len(l.dataWorkloads) == 0 {
		return nil
	}
	view, err := l.snapshotView(ctx)
	if err != nil {
		return nil
	}
	return dataAtRisk(view, l.dataWorkloads, l.Request.Namespace, l.Request.DetectedAt)
}

// dataLossOutcome reports an outcome that may have lost data. It holds no
// verified repair, whatever its receipts say.
func dataLossOutcome(outcome *contracts.OutcomeRecord) bool {
	for _, evaluation := range outcome.GetDetectorHistory() {
		if evaluation.GetDetectorId() == dataAtRiskEvaluation {
			return true
		}
	}
	return false
}
