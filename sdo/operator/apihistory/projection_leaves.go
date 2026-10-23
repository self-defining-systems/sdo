package apihistory

import (
	_ "crypto/sha256" // Enable supported image digest algorithms for reference validation.
	_ "crypto/sha512"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/distribution/reference"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Identifiers (including valid image repository/tag/digest identifiers) are an
// accepted disclosure boundary, not a guarantee that user-chosen names cannot
// encode secrets. Free text is never accepted merely because its JSON type is
// string. Validators never expose parse errors, which can contain raw input.
func enumField(values ...string) projection {
	return func(v any) (any, bool) {
		s, ok := v.(string)
		if ok {
			for _, allowed := range values {
				if s == allowed {
					return s, true
				}
			}
		}
		return nil, false
	}
}

// This finite vocabulary is shared by conditions, Events and current/previous
// container states. Custom producer reasons are opaque, including single tokens.
var reviewedReason = enumField(
	"BackOff", "CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "InvalidImageName",
	"CreateContainerConfigError", "CreateContainerError", "RunContainerError", "ContainerCreating",
	"PodInitializing", "Completed", "Error", "OOMKilled", "ContainerCannotRun", "StartError",
	"Evicted", "DeadlineExceeded", "NodeLost", "Shutdown", "Terminated", "UnexpectedAdmissionError",
	"ContainersReady", "ContainersNotReady", "PodCompleted", "PodFailed", "ReadinessGatesNotReady",
	"SchedulingGated", "Unschedulable", "SchedulingDisabled", "KubeletNotReady", "KubeletReady",
	"MinimumReplicasAvailable", "MinimumReplicasUnavailable", "NewReplicaSetAvailable", "NewReplicaSetCreated",
	"ReplicaSetUpdated", "ReplicaSetCreateError", "ProgressDeadlineExceeded", "FailedCreate", "SuccessfulCreate",
	"FailedDelete", "SuccessfulDelete", "ScalingReplicaSet", "FailedScheduling", "Scheduled",
	"Pulling", "Pulled", "Created", "Started", "Killing", "Unhealthy", "Failed", "FailedMount",
	"FailedAttachVolume", "FailedMapVolume", "FailedSync", "FailedKillPod", "FailedPostStartHook",
	"FailedPreStopHook", "FailedValidation", "FailedBinding", "SuccessfulAttachVolume", "SandboxChanged",
	"FailedCreatePodSandBox", "FailedPodSandBoxStatus", "FailedToRetrieveImagePullSecret",
)

func reasonField(v any) (any, bool) {
	if value, ok := reviewedReason(v); ok {
		return value, true
	}
	if _, ok := v.(string); ok {
		return "redacted", true
	}
	return nil, false
}

func imageField(v any) (any, bool) {
	s, ok := v.(string)
	if !ok {
		return nil, false
	}
	// ParseNormalizedNamed rejects URL schemes, userinfo, queries, fragments,
	// whitespace and malformed digests. Unlike ParseDockerRef it keeps both a
	// tag and digest when both are present. Invalid images are omitted entirely.
	ref, err := reference.ParseNormalizedNamed(s)
	if err != nil {
		return nil, false
	}
	return ref.String(), true
}

func portField(v any) (any, bool) {
	if s, ok := v.(string); ok {
		return s, len(validation.IsValidPortName(s)) == 0
	}
	return numberField(v)
}

func quantityField(v any) (any, bool) {
	s, ok := v.(string)
	if n, numeric := v.(float64); numeric {
		s, ok = strconv.FormatFloat(n, 'g', -1, 64), true
	}
	if !ok {
		return nil, false
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return nil, false
	}
	return q.String(), true
}

func percentOrNumber(v any) (any, bool) {
	if s, ok := v.(string); ok {
		if !strings.HasSuffix(s, "%") {
			return nil, false
		}
		digits := strings.TrimSuffix(s, "%")
		if digits == "" || strings.IndexFunc(digits, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
			return nil, false
		}
		n, err := strconv.ParseUint(digits, 10, 8)
		if err != nil || n > 100 {
			return nil, false
		}
		return strconv.FormatUint(n, 10) + "%", true
	}
	return numberField(v)
}

func cidrField(v any) (any, bool) {
	s, ok := v.(string)
	if !ok {
		return nil, false
	}
	prefix, err := netip.ParsePrefix(s)
	if err != nil {
		return nil, false
	}
	return prefix.Masked().String(), true
}

func timestampField(v any) (any, bool) {
	s, ok := v.(string)
	if !ok {
		return nil, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil, false
	}
	return t.UTC().Format(time.RFC3339Nano), true
}

func fieldPathField(v any) (any, bool) {
	s, ok := v.(string)
	if !ok {
		return nil, false
	}
	if value, ok := enumField("metadata.name", "metadata.namespace", "metadata.uid", "metadata.labels", "metadata.annotations", "spec.nodeName", "spec.serviceAccountName", "status.hostIP", "status.hostIPs", "status.podIP", "status.podIPs")(s); ok {
		return value, true
	}
	for _, prefix := range []string{"metadata.labels['", "metadata.annotations['"} {
		if strings.HasPrefix(s, prefix) && strings.HasSuffix(s, "']") {
			key := strings.TrimSuffix(strings.TrimPrefix(s, prefix), "']")
			return s, len(validation.IsQualifiedName(key)) == 0
		}
	}
	return nil, false
}

var protocolField = enumField("TCP", "UDP", "SCTP")
var resourceField = enumField("limits.cpu", "limits.memory", "limits.ephemeral-storage", "requests.cpu", "requests.memory", "requests.ephemeral-storage")
