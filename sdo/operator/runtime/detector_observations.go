package runtime

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	controllercore "sdo.dev/operator/core"
	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/usage"
	"sdo.dev/operator/sdk/vantage"
)

// detectorObservations is the one place that derives the Kubernetes-, log-,
// pod-read and metrics-backed observations from a detector bundle's declarations. The
// controller loop, each memory-host swap and --evaluate-once compose through
// it (each with the revision's telemetry sources), so no evaluation path can drop an input a detector declared.
type detectorObservations struct {
	Client kubernetes.Interface
	Clock  func() time.Time
	// Telemetry opens the metrics source of each accepted revision.
	Telemetry TelemetrySources
	// RequireMetrics refuses metric-consuming bundles without a metrics source.
	RequireMetrics bool
	// ExecConfig is the credential pod reads exec with, as for Diagnose reads.
	// A nil config leaves declared reads explicitly unavailable.
	ExecConfig *rest.Config
	// ProtectedNamespace is the control namespace, which pod reads never enter.
	ProtectedNamespace string
	// Exec replaces the API server exec transport in tests.
	Exec kubereads.PodExec
	// PodReadBaseline, when set, carries the controller's promoted healthy
	// readings with each pod read; it outlives every memory-host swap.
	PodReadBaseline *PodReadBaseline
	// VantageBaseline, when set, carries the promoted healthy vantage readings.
	VantageBaseline *VantageBaseline
	// Platform, when set, carries the source inventory reader and the
	// promoted healthy platform observation.
	Platform *PlatformState
	// VantageImage replaces DefaultVantageImage.
	VantageImage string
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
		provider = podAdmissionSnapshotProvider{Base: provider, Client: o.Client, ProtectedNamespace: o.ProtectedNamespace}
	}
	provider = withUsage(provider, detectors, o.Client, clock)
	logsObserver, err := NewLogsObserver(o.Client, detectors, LogsObserverOptions{Clock: clock})
	if err != nil {
		return nil, nil, err
	}
	if logsObserver != nil {
		provider = LogsSnapshotProvider{Base: provider, Observer: logsObserver}
	}
	podReads, err := NewPodReadObserver(o.Client, o.exec(), o.ProtectedNamespace, detectors, clock)
	if err != nil {
		return nil, nil, err
	}
	if podReads != nil {
		provider = PodReadsSnapshotProvider{Base: provider, Observer: podReads, Baseline: o.PodReadBaseline}
	}
	vantageObserver, err := NewVantageObserver(o.Client, o.exec(), o.ProtectedNamespace, o.VantageImage, detectors, clock)
	if err != nil {
		return nil, nil, err
	}
	if vantageObserver != nil {
		provider = VantageSnapshotProvider{Base: provider, Observer: vantageObserver, Baseline: o.VantageBaseline}
	}
	if sdk.ConsumesPlatform(detectors) {
		provider = platformSnapshotProvider{Base: provider, Observer: &PlatformObserver{Client: o.Client, ProtectedNamespace: o.ProtectedNamespace, State: o.Platform, Clock: clock}}
	}
	return withMetrics(provider, detectors, o.Telemetry, designerSources, o.RequireMetrics, clock)
}

func (o detectorObservations) exec() kubereads.PodExec {
	if o.Exec != nil {
		return o.Exec
	}
	return kubereads.SPDYExec{Client: o.Client, Config: o.ExecConfig}
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
	if consumer, ok := detector.(podread.Consumer); ok && len(consumer.PodReads()) > 0 {
		return "pod read"
	}
	if consumer, ok := detector.(vantage.Consumer); ok && len(consumer.VantageProbes()) > 0 {
		return "vantage probe"
	}
	if sdk.ConsumesPlatform([]sdk.Detector{detector}) {
		return "platform"
	}
	return ""
}

// podAdmissionSnapshotProvider adds one bounded server-side dry-run Pod create
// per snapshot, sourced from the snapshot's own Pods.
type podAdmissionSnapshotProvider struct {
	Base               SnapshotProvider
	Client             kubernetes.Interface
	ProtectedNamespace string
}

func (p podAdmissionSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	base, err := p.Base.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	result, submitted := controllercore.ObservePodAdmission(ctx, p.Client, base)
	if submitted != nil {
		// The entries that select the submitted Pod: with one mutating
		// candidate, Mutations is attributed exactly, and a dry run that
		// timed out names the webhooks it waited on.
		if webhooks, err := kubereads.PodAdmissionWebhooks(ctx, p.Client, p.ProtectedNamespace, base.Namespace(), submitted.Labels); err == nil {
			result.Webhooks = webhooks
		}
	}
	return inputs.New(base).WithPodAdmission(result), nil
}
