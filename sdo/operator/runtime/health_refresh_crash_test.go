package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/sdk"
)

// crashTransport persists the fake API server independently of the controller
// process. The production executor enforces retained request payload equality,
// Job/result identity and completion on both sides of a real SIGKILL.
type crashAPIState struct {
	Maps corev1.ConfigMapList
	Jobs batchv1.JobList
}

func crashAt(path, selected, point string) {
	if selected != point {
		return
	}
	if err := os.WriteFile(path+".cut", []byte(point), 0600); err != nil {
		panic(err)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		panic(err)
	}
	select {}
}

type crashProposalBroker struct {
	lifecycle.ProposalBroker
	path, cut string
}

func (b crashProposalBroker) CreateProposalForPhase(ctx context.Context, id string, phase broker.Phase) (broker.Proposal, error) {
	proposal, err := b.ProposalBroker.CreateProposalForPhase(ctx, id, phase)
	if err == nil {
		crashAt(b.path, b.cut, "proposal")
	}
	return proposal, err
}

func (b crashProposalBroker) Publish(ctx context.Context, id string, actor broker.Actor, phase broker.Phase) (broker.Receipt, error) {
	crashAt(b.path, b.cut, "before-accept")
	receipt, err := b.ProposalBroker.Publish(ctx, id, actor, phase)
	if err == nil {
		crashAt(b.path, b.cut, "after-accept")
	}
	return receipt, err
}

// crashStateStore retains the controller's actual durable contract across child
// processes. Cuts bracket the Save that makes obsolete evidence terminal.
type crashStateStore struct {
	path string
}

func (s crashStateStore) Load(context.Context) (*contracts.ControllerState, string, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	state := &contracts.ControllerState{}
	if err := wire.UnmarshalDurable(data, state); err != nil {
		return nil, "", err
	}
	return state, fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

func (s crashStateStore) Save(ctx context.Context, state *contracts.ControllerState, revision string) (string, error) {
	_, current, err := s.Load(ctx)
	if err != nil {
		return "", err
	}
	if current != revision {
		return "", ErrStateConflict
	}
	data, err := wire.Canonical(state)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(s.path+".tmp", data, 0600); err != nil {
		return "", err
	}
	if err := os.Rename(s.path+".tmp", s.path); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

func crashController(t *testing.T, root, compiled, state, cut string) *Controller {
	t.Helper()
	cfg := testControllerConfig()
	cfg.SourceCommit = compiled
	detector := controllerDetector("health", time.Second, sdk.Finding{})
	detector.spec.Class, detector.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
	detector.spec.OriginatingCommit = compiled
	c, err := NewController(cfg, []sdk.Detector{detector}, staticProvider{}, &recordingDispatcher{}, time.Unix(1000, 0))
	if err != nil {
		t.Fatal(err)
	}
	rv6Guard(c, root, compiled)
	if err := c.AttachStateStore(context.Background(), crashStateStore{path: state + ".controller"}); err != nil {
		t.Fatal(err)
	}
	c.discardRestoredHealthObservations(time.Unix(1000, 0))
	return c
}
