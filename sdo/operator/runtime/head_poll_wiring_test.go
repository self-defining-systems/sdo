package runtime

import (
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// The controller the operator starts hands its native broker to the head poller and the source gate; a broker
// passed by value never reached them (the type assertion on the pointer failed), so no branch head was ever polled.
func TestTheStartedControllerPollsBranchHeads(t *testing.T) {
	c, err := NewController(testControllerConfig(), []sdk.Detector{}, staticProvider{}, &recordingDispatcher{}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatal(err)
	}
	native := &NativeIncidentBroker{
		SourceGate: &SourceGate{},
		HeadHolds:  ConfigMapHeadHolds{},
		Repairs:    DeploymentRepairer{},
	}
	if err := attachNativeBroker(c, native); err != nil {
		t.Fatal(err)
	}
	if c.head.poller == nil {
		t.Fatal("the controller has no head poller: a pushed branch head would never be deployed")
	}
}
