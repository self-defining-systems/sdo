package lifecycle

import "sdo.dev/operator/broker"

// The broker implements the advance's narrow interface directly.
var _ AdvanceBroker = (*broker.Broker)(nil)
