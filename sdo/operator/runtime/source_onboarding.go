package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
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
	stored, err := onboarding.Load(ctx, client, control, c.config.Namespace)
	if err != nil {
		return err
	}
	if stored != nil {
		if err := c.SetNamespaceOnboarding(stored); err != nil {
			return err
		}
	}
	reference, note, err := onboarding.LoadReachabilityWithNote(ctx, client, control, c.config.Namespace)
	if err != nil {
		return err
	}
	if note != nil {
		slog.InfoContext(ctx, "namespace onboarding reachability reference absent; no exclusions", "note", note)
	}
	if reference != nil && reference.Authorization.ObjectiveDigest != c.reachabilityObjectiveDigest() {
		reference = nil
	}
	c.onboardingReachability = &onboardingReachabilityState{reference: reference}
	c.provider = onboardingSnapshotProvider{Base: c.provider, State: c.onboardingReachability}
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
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return nil
	}
	defer cancel()
	record := c.pendingOnboarding
	c.pendingOnboarding = nil
	claimed, err := onboarding.Claim(actionCtx, c.onboardingClient, c.onboardingControl)
	if err != nil && c.OnError != nil {
		c.OnError(fmt.Errorf("claim onboarding evaluation: %w", err))
	}
	if !claimed {
		if err == nil {
			existing, loadErr := onboarding.Load(ctx, c.onboardingClient, c.onboardingControl, c.config.Namespace)
			if loadErr == nil {
				loadErr = c.SetNamespaceOnboarding(existing)
			}
			if loadErr == nil {
				loadErr = c.prepareSourceInventory(ctx)
			}
			if loadErr != nil && c.OnError != nil {
				c.OnError(loadErr)
			}
		}
		return nil
	}
	return record
}

func (c *Controller) finishOnboarding(ctx context.Context, record *contracts.NamespaceOnboarding, findings []sdk.Finding, errored bool, now time.Time) {
	if record == nil {
		return
	}
	commitVerified := c.onboardingCommitVerified(ctx, record.AcceptedCommit)
	c.mu.Lock()
	eligible := !c.incidentOpen && !c.closureBlocksNextLocked() && c.lastAcknowledgedIncidentID == "" && c.incidentDetectedAt.IsZero() && len(c.failedClosures) == 0 && commitVerified
	c.mu.Unlock()
	if !eligible || (c.onboardingReachability != nil && c.onboardingReachability.invalid) || errored || len(findings) != 0 || len(c.healthDetectorIDs) == 0 || !c.pendingInventoryClear(record, now) {
		return
	}
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return
	}
	defer cancel()
	var reference *servicehealth.ReachabilityBaseline
	if c.onboardingReachability != nil {
		reference = c.onboardingReachability.candidate
	}
	stored, err := onboarding.CompleteWithReachability(actionCtx, c.onboardingClient, c.onboardingControl, record, reference)
	if err == nil && c.onboardingReachability != nil {
		c.onboardingReachability.candidate, err = onboarding.LoadReachability(actionCtx, c.onboardingClient, c.onboardingControl, c.config.Namespace)
	}
	if err == nil {
		err = c.SetNamespaceOnboarding(stored)
		if err == nil {
			c.publishOnboardingReachability(ctx)
		}
	}
	if err == nil {
		err = c.prepareSourceInventory(ctx)
	}
	if err != nil {
		if c.OnError != nil {
			c.OnError(fmt.Errorf("complete onboarding: %w", err))
		}
	} else {
		slog.InfoContext(ctx, "namespace onboarding established", "accepted_commit", record.AcceptedCommit, "install_to_record", time.Since(record.EstablishedAt.AsTime()))
	}
}

// Preview acknowledgement only to distinguish install identities from later
// drift. It grants no authority and never mutates the accepted inventory index.
func (c *Controller) pendingInventoryClear(record *contracts.NamespaceOnboarding, now time.Time) bool {
	findings, observed := c.pendingInventoryFindings(record, now)
	return observed && len(findings) == 0
}

// Retain contradictions observed during partial install batches even if the
// object or commit is restored before the confirmed full sweep. Unavailable
// inventory is not itself drift, but still cannot authorize completion.
func (c *Controller) observePendingOnboarding(ctx context.Context, now time.Time) {
	if c.pendingOnboarding == nil || c.onboardingReachability == nil {
		return
	}
	if !c.onboardingCommitVerified(ctx, c.pendingOnboarding.AcceptedCommit) {
		c.onboardingReachability.invalid = true
	}
	if findings, observed := c.pendingInventoryFindings(c.pendingOnboarding, now); observed && len(findings) != 0 {
		c.onboardingReachability.invalid = true
	}
}

func (c *Controller) pendingInventoryFindings(record *contracts.NamespaceOnboarding, now time.Time) ([]sdk.Finding, bool) {
	if c.drift == nil || !c.drift.Inventory {
		return nil, true
	}
	if c.sourceInventory == nil {
		return nil, false
	}
	reader, ok := c.drift.Events.(interface {
		SourceDriftObjects() []*unstructured.Unstructured
	})
	if !ok {
		return nil, false
	}
	if readiness, ok := c.drift.Events.(interface{ SourceDriftReady() bool }); ok && !readiness.SourceDriftReady() {
		return nil, false
	}
	preview := *c.sourceInventory
	if err := preview.SetNamespaceOnboarding(record); err != nil {
		return nil, false
	}
	return preview.Findings(reader.SourceDriftObjects(), now), true
}

// A startup judge may publish health memory before the first evaluation. Keep
// the install identity/commit immutable, and accept only that verified lineage.
func (c *Controller) onboardingCommitVerified(ctx context.Context, installed string) bool {
	current := c.CurrentAcceptedCommit()
	if current == installed {
		return true
	}
	ok, err := memory.HealthRefreshPreservesInstall(ctx, c.config.RepositoryWorktree, installed, current)
	if err != nil && c.OnError != nil {
		c.OnError(fmt.Errorf("verify onboarding health refresh: %w", err))
	}
	return err == nil && ok
}
