package runtime

import (
	"context"
	"fmt"
	"k8s.io/client-go/kubernetes"
	"time"

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

// AttachPendingOnboarding restores the install snapshot, never live identities.
func (c *Controller) AttachPendingOnboarding(ctx context.Context, client kubernetes.Interface, control string) error {
	if c.namespaceOnboarding != nil {
		return nil
	}
	record, err := onboarding.Pending(ctx, client, control, c.config.Namespace)
	if err != nil {
		return err
	}
	c.pendingOnboarding = record
	c.onboardingClient = client
	c.onboardingControl = control
	return nil
}

func (c *Controller) beginOnboarding(ctx context.Context, detectors []sdk.Detector) *contracts.NamespaceOnboarding {
	if c.pendingOnboarding == nil || len(detectors) != len(c.detectors) {
		return nil
	}
	record := c.pendingOnboarding
	c.pendingOnboarding = nil
	claimed, err := onboarding.Claim(ctx, c.onboardingClient, c.onboardingControl)
	if err != nil && c.OnError != nil {
		c.OnError(fmt.Errorf("claim onboarding evaluation: %w", err))
	}
	if !claimed {
		return nil
	}
	return record
}

func (c *Controller) finishOnboarding(ctx context.Context, record *contracts.NamespaceOnboarding, findings []sdk.Finding, errored bool, now time.Time) {
	if record == nil {
		return
	}
	c.mu.Lock()
	eligible := !c.incidentOpen && !c.closureBlocksNextLocked() && c.lastAcknowledgedIncidentID == "" && c.config.SourceCommit == record.AcceptedCommit
	c.mu.Unlock()
	if !eligible || errored || len(findings) != 0 || len(c.healthDetectorIDs) == 0 {
		return
	}
	stored, err := onboarding.Complete(ctx, c.onboardingClient, c.onboardingControl, record)
	if err == nil {
		err = c.SetNamespaceOnboarding(stored)
	}
	if err == nil {
		err = c.prepareSourceInventory(ctx)
	}
	if c.OnError != nil {
		if err != nil {
			c.OnError(fmt.Errorf("complete onboarding: %w", err))
		} else {
			c.OnError(fmt.Errorf("namespace onboarding established: accepted_commit=%s install_to_record=%s", record.AcceptedCommit, now.Sub(record.EstablishedAt.AsTime())))
		}
	}
}
