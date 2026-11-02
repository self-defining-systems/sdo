package runtime

import (
	"context"
	"regexp"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/worker_registry"
)

// restartClassWorld is everything about the application under test that the
// restart-class generator must not hard-code: the accepted source, what the
// scripted responder edits, the live cluster it observes, and the judge that
// adopts the repaired source. The default world is the synthetic tracing seed.
type restartClassWorld struct {
	Application, Namespace string
	ManifestPaths          []string
	// EffectPath is the accepted-source path counted to prove exactly one public effect.
	EffectPath string
	// Edit is the scripted responder's source repair in its proposal checkout.
	Edit func(t *testing.T, worktree string) error
	// Live returns the ready cluster objects that match the accepted source at commit.
	Live func(t *testing.T, root, commit string) []kruntime.Object
	// Coordinator builds the health judge that adopts repaired source.
	Coordinator func(t *testing.T, root, proposals, state string) lifecycle.Coordinator
}

// Real-seed replay: the accepted source, rendered lock and health memory come
// from a refreshed benchmark seed that is only ever read; the scripted
// responder performs the exact source edits of one hand-written replay slot.
const (
	realSeedRootEnv   = "RT1_SEED_ROOT"
	realSeedCommitEnv = "RT1_SEED_COMMIT"
	realSeedSlotEnv   = "RT1_SEED_SLOT"
)

var realSeedApplication = regexp.MustCompile(`(?m)^application:\s*"?([^"\n]+)"?\s*$`)

type realSeedLock struct {
	SourceCommit  string   `json:"source_commit"`
	Namespace     string   `json:"namespace"`
	ManifestPaths []string `json:"manifest_paths"`
}

// seedJudge adopts repaired source the way a judge that finds the accepted
// health policy still correct does: it changes nothing, so only the real broker,
// validation and provenance decide whether the refresh is accepted.
type seedJudge struct{ registry *worker_registry.Registry }

func (j seedJudge) Execute(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	request := &contracts.HealthJudgeRequest{}
	if err := task.GetRequest().UnmarshalTo(request); err != nil {
		return nil, err
	}
	// A no-op is the one publication the broker accepts for an input-bound
	// refresh, and it keeps the detector bytes that bind incidents to health policy.
	ids := []string{"health-objective"}
	response, err := anypb.New(&contracts.HealthJudgeResult{Authored: true, AgentSessionId: "scripted-seed-judge", DetectorIds: ids})
	if err != nil {
		return nil, err
	}
	return &contracts.AgentResult{SchemaVersion: worker_registry.SchemaVersion, TaskId: task.GetTaskId(), Status: "completed", ProposalId: task.GetWorkspace().GetProposalId(), Response: response}, nil
}
