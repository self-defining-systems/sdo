package memoryhost

import (
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/types/known/structpb"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/usage"
)

// Registration carries optional declarations as well as scheduling metadata.
// DetectorSpec alone does not describe the inputs a controller must collect.
type Registration struct {
	sdk.DetectorSpec
	Metrics      []metrics.Query
	Logs         []logs.Query
	Usage        usage.Requirements
	UsesUsage    bool
	Reachability bool
	Alerts       bool
	PodAdmission bool
	Traffic      []string
}

func registration(d sdk.Detector) Registration {
	r := Registration{DetectorSpec: d.Spec()}
	if c, ok := d.(metrics.Consumer); ok {
		r.Metrics = c.MetricQueries()
	}
	if c, ok := d.(logs.Consumer); ok {
		r.Logs = c.LogQueries()
	}
	if c, ok := d.(usage.Consumer); ok {
		r.Usage = c.UsageRequirements()
		r.UsesUsage = true
	}
	if c, ok := d.(servicehealth.ReachabilityConsumer); ok {
		r.Reachability = c.ConsumesServiceReachability()
	}
	if c, ok := d.(metrics.AlertConsumer); ok {
		r.Alerts = c.ConsumesAlerts()
	}
	if c, ok := d.(interface{ ConsumesPodAdmission() bool }); ok {
		r.PodAdmission = c.ConsumesPodAdmission()
	}
	if c, ok := d.(traffic.Consumer); ok {
		r.Traffic = c.TrafficWorkloads()
	}
	return r
}
func DecodeRegistration(wire *pb.MemoryDetectorRegistration) (Registration, error) {
	var r Registration
	if wire == nil || wire.Specification == nil {
		return r, errors.New("missing detector registration")
	}
	raw, err := wire.Specification.MarshalJSON()
	if err != nil {
		return r, err
	}
	if err = json.Unmarshal(raw, &r); err != nil {
		return r, err
	}
	if r.ID == "" || r.ID != wire.Id {
		return r, errors.New("detector registration ID mismatch")
	}
	if err = metrics.ValidateQueries(r.Metrics); err != nil {
		return r, err
	}
	for _, q := range r.Logs {
		if err = q.Validate(); err != nil {
			return r, fmt.Errorf("detector %s: %w", r.ID, err)
		}
	}
	return r, nil
}
func encodeStruct(value any) (*structpb.Struct, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	out := &structpb.Struct{}
	err = out.UnmarshalJSON(raw)
	return out, err
}
