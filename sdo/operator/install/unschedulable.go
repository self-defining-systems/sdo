package install

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// defaultUnschedulableBound is how long the scheduler must have refused a Pod for want of a resource before an
// install wait gives up on it. Kubernetes' own retry cadence is far shorter, so a refusal that stood this long is
// not a pending race: nothing on the cluster is going to make room.
const defaultUnschedulableBound = time.Minute

// PodUnschedulableError reports a Pod the scheduler has refused for want of a node resource (the message names an
// "Insufficient <resource>") for at least the bound. An install that waits on such a Pod fails with it at once instead
// of waiting out its whole bound.
type PodUnschedulableError struct {
	Pod      string
	Refusal  string
	Standing time.Duration
}

func (e *PodUnschedulableError) Error() string {
	return fmt.Sprintf("Pod %s cannot be scheduled: the scheduler has refused it for lack of a node resource for %ds: %s", e.Pod, int(e.Standing.Seconds()), e.Refusal)
}

// resourceRefusal reports the standing refusal of pod, nil when it is scheduled, pending for another reason (an image
// pull, a volume, a taint) or refused for less than the bound. The age is the PodScheduled condition's own transition
// time, so a wait that began late does not restart the count.
func (i Installer) resourceRefusal(pod *corev1.Pod) *PodUnschedulableError {
	if pod == nil || pod.Status.Phase != corev1.PodPending {
		return nil
	}
	bound := i.UnschedulableBound
	if bound <= 0 {
		bound = defaultUnschedulableBound
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type != corev1.PodScheduled || condition.Status != corev1.ConditionFalse || condition.Reason != corev1.PodReasonUnschedulable {
			continue
		}
		if !strings.Contains(condition.Message, "Insufficient ") || condition.LastTransitionTime.IsZero() {
			return nil
		}
		if standing := i.now().Sub(condition.LastTransitionTime.Time); standing >= bound {
			return &PodUnschedulableError{Pod: pod.Name, Refusal: condition.Message, Standing: standing}
		}
	}
	return nil
}
