package runtime

import (
	"context"
	"time"

	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
)

// AdoptionPass is one ownership-adoption pass of the controller, runnable
// without a controller loop: tests and rehearsals drive the production
// adoption code against a real API server through it.
type AdoptionPass struct {
	Namespace    string
	SourceCommit string
	Source       rendered.Source
	Client       kubernetes.Interface
	// Snapshot is the detection snapshot the pass classifies differences with.
	Snapshot sdk.DetectionContext
}

// Run adopts the accepted source's objects once and returns the pass's status.
func (p AdoptionPass) Run(ctx context.Context, now time.Time) (OwnershipAdoptionStatus, error) {
	c := &Controller{config: ControllerConfig{Namespace: p.Namespace, SourceCommit: p.SourceCommit}}
	c.ConfigureDrift(DriftControllerConfig{
		Source: p.Source,
		Client: p.Client,
		View: func(snapshot sdk.DetectionContext) program.ReadView {
			return LiveReadView{Carrier: inputs.New(snapshot), Context: ctx, Client: p.Client, Source: p.Source, SourceCommit: p.SourceCommit}
		},
	})
	err := c.adoptOwnership(ctx, now, p.Snapshot)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ownershipAdoption, err
}
