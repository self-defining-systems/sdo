package runtime

import (
	"context"
	"fmt"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/usage"
	"time"
)

type newInputsDetector struct{}

func (newInputsDetector) Spec() sdk.DetectorSpec {
	s := fastloopDetector{}.Spec()
	s.ID = "new-inputs"
	s.Watches = []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "StatefulSet"}, usage.Watch, traffic.Watch}
	return s
}

func (newInputsDetector) UsageRequirements() usage.Requirements {
	return usage.Requirements{Throttling: true}
}

func (newInputsDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	var findings []sdk.Finding
	for _, sts := range view.StatefulSets() {
		if sts.Status.ReadyReplicas < *sts.Spec.Replicas {
			findings = append(findings, sdk.Finding{RuleID: "replicas", PrimaryResource: sdk.ObjectRefFrom("StatefulSet", "apps/v1", &sts), Summary: "StatefulSet below desired", Evidence: fmt.Sprintf("ready=%d desired=%d", sts.Status.ReadyReplicas, *sts.Spec.Replicas), Status: sdk.FindingActive, Severity: sdk.SeverityCritical})
		}
	}
	r, err := usage.From(view)
	if err != nil {
		return nil, err
	}
	for _, w := range r.Containers {
		if w.Identity.Container == "" {
			continue
		}
		var points []usage.Point
		for _, s := range w.Samples {
			if s.WorkingSetBytes != nil {
				points = append(points, usage.Point{At: s.At, Value: *s.WorkingSetBytes})
			}
		}
		s := w.Samples[len(w.Samples)-1]
		if !s.ThrottlingAvailable {
			return nil, fmt.Errorf("cadvisor counters missing")
		}
		if s.MemoryLimit == nil {
			return nil, fmt.Errorf("memory limit missing")
		}
		if forecast, ok := usage.TimeToThreshold(points, *s.MemoryLimit); ok && forecast <= time.Minute {
			findings = append(findings, usage.ThresholdFinding("memory-forecast", sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: w.Identity.Pod}, sdk.SeverityCritical, *s.WorkingSetBytes, s.MemoryLimit, time.Minute, &forecast))
		}
	}
	window, ok := traffic.WindowFrom(view, "links")
	if !ok {
		return nil, fmt.Errorf("links unavailable")
	}
	for _, edge := range window.Links {
		last := edge.Samples[len(edge.Samples)-1]
		if !last.OK {
			findings = append(findings, sdk.Finding{RuleID: "mongodb-link", PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "db"}, Summary: "mongodb handshake failed", Evidence: fmt.Sprintf("protocol=%s class=%s error=%s", edge.Link.Protocol, last.Class, last.Error), Status: sdk.FindingActive, Severity: sdk.SeverityCritical})
		}
	}
	for i := range findings {
		findings[i].DetectorID = "new-inputs"
	}
	return findings, nil
}

type observedLinks struct {
	SnapshotProvider
	p *prober.Prober
}

func (p observedLinks) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	base, err := p.SnapshotProvider.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return inputs.New(base).WithTraffic(trafficWindows(p.p.Windows())), nil
}
