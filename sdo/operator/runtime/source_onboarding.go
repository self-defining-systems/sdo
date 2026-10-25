package runtime

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/sdk"
)

func cloneNamespaceOnboarding(record *contracts.NamespaceOnboarding) *contracts.NamespaceOnboarding {
	if record == nil {
		return nil
	}
	return proto.Clone(record).(*contracts.NamespaceOnboarding)
}

// SetNamespaceOnboarding imports only the trusted installer's create-once
// record. Incident-time state and subsequent healthy observations cannot add
// identities. The durable controller state retains the same accepted commit.
func (c *Controller) SetNamespaceOnboarding(record *contracts.NamespaceOnboarding) error {
	if record == nil {
		return nil
	}
	if err := onboarding.Validate(record, c.config.Namespace); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.namespaceOnboarding != nil && !proto.Equal(c.namespaceOnboarding, record) {
		return fmt.Errorf("namespace onboarding acknowledgement cannot be replaced")
	}
	c.namespaceOnboarding = cloneNamespaceOnboarding(record)
	c.sourceInventory = nil
	return nil
}
func (s *SourceInventory) SetNamespaceOnboarding(record *contracts.NamespaceOnboarding) error {
	if record == nil {
		return nil
	}
	if err := onboarding.Validate(record, s.Namespace); err != nil {
		return err
	}
	s.NamespaceOnboarding = cloneNamespaceOnboarding(record)
	s.acknowledged = map[sdk.ObjectRef]bool{}
	for _, ref := range record.Objects {
		s.acknowledged[objectRefState(ref)] = true
	}
	return nil
}
