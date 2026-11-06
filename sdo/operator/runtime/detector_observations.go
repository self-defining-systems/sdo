package runtime

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"

	controllercore "sdo.dev/operator/core"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/usage"
)

// detectorObservations is the one place that derives the Kubernetes-, log-
// and metrics-backed observations from a detector bundle's declarations. The
// controller loop, each memory-host swap and --evaluate-once compose through
// it (each with the revision's telemetry sources), so no evaluation path can drop an input a detector declared.
type detectorObservations struct {
	Client kubernetes.Interface
	Clock  func() time.Time
	// Telemetry opens the metrics source of each accepted revision.
	Telemetry TelemetrySources
	// RequireMetrics refuses metric-consuming bundles without a metrics source.
	RequireMetrics bool
}

// compose wraps base with every observation the detectors declare. A
// Kubernetes-backed declaration without a client is refused here, naming the
// detector, instead of surfacing later as an unavailable observation.
func (o detectorObservations) compose(base SnapshotProvider, detectors []sdk.Detector, designerSources []byte) (SnapshotProvider, *MetricsObserver, error) {
	if o.Client == nil {
		for _, detector := range detectors {
			if needs := kubernetesObservations(detector); needs != "" {
				return nil, nil, fmt.Errorf("detector %q declares the %s observation, but this evaluation path has no Kubernetes client", detector.Spec().ID, needs)
			}
		}
	}
	clock := o.Clock
	if clock == nil {
		clock = lifecycleio.Now
	}
	provider := base
	if sdk.ConsumesPodAdmission(detectors) {
		provider = podAdmissionSnapshotProvider{Base: provider, Client: o.Client}
	}
	provider = withUsage(provider, detectors, o.Client, clock)
	logsObserver, err := NewLogsObserver(o.Client, detectors, LogsObserverOptions{Clock: clock})
	if err != nil {
		return nil, nil, err
	}
	if logsObserver != nil {
		provider = LogsSnapshotProvider{Base: provider, Observer: logsObserver}
	}
	return withMetrics(provider, detectors, o.Telemetry, designerSources, o.RequireMetrics, clock)
}

// kubernetesObservations names the first client-backed input detector declares.
func kubernetesObservations(detector sdk.Detector) string {
	if sdk.ConsumesPodAdmission([]sdk.Detector{detector}) {
		return "pod admission"
	}
	if optional, ok := detector.(interface{ ConsumesUsage() bool }); !ok || optional.ConsumesUsage() {
		if _, ok := detector.(usage.Consumer); ok {
			return "usage"
		}
	}
	if consumer, ok := detector.(logs.Consumer); ok && len(consumer.LogQueries()) > 0 {
		return "logs"
	}
	return ""
}

// podAdmissionSnapshotProvider adds one bounded server-side dry-run Pod create
// per snapshot, sourced from the snapshot's own Pods.
type podAdmissionSnapshotProvider struct {
	Base   SnapshotProvider
	Client kubernetes.Interface
}

func (p podAdmissionSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	base, err := p.Base.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	result := controllercore.ObservePodAdmission(ctx, p.Client, base.Namespace(), base.Pods())
	return inputs.New(base).WithPodAdmission(result), nil
}
