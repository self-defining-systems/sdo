package runtime

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

type menuCrash struct{}
type menuCutStore struct {
	StateStore
	cut   func(*contracts.ControllerState) bool
	fired bool
}

func (s *menuCutStore) Save(ctx context.Context, state *contracts.ControllerState, revision string) (string, error) {
	next, err := s.StateStore.Save(ctx, state, revision)
	if err == nil && !s.fired && s.cut != nil && s.cut(state) {
		s.fired = true
		panic(menuCrash{})
	}
	return next, err
}
func menuThirdRound(t *testing.T) (*ResponderLoop, *menuB3Cluster) {
	loop, cluster := menuB3Loop(t)
	restarts := 0
	cluster.afterPatch = func(ref sdk.ObjectRef) {
		stamp, _, _ := unstructured.NestedString(cluster.objects[menuDeployment].Object, "spec", "template", "metadata", "annotations", "kubectl.kubernetes.io/restartedAt")
		if ref == menuDeployment && cluster.observedReady && stamp != "" {
			restarts++
			if restarts < 2 {
				_ = setMenuReady(cluster, false)
			} else {
				_ = setMenuReady(cluster, true)
			}
		}
	}
	return loop, cluster
}
func setMenuReady(cluster *menuB3Cluster, ready bool) error {
	value := int64(0)
	if ready {
		value = 1
	}
	return unstructured.SetNestedField(cluster.objects[menuDeployment].Object, value, "status", "readyReplicas")
}

func TestResponderMenuSeededCrashCutsAboveRoundTwo(t *testing.T) {
	for seed := int64(0); seed < 6; seed++ {
		for _, cut := range []string{"option_selected", "effect_applied_round_3", "report_delivered_round_3"} {
			t.Run(fmt.Sprintf("seed-%d/%s", seed, cut), func(t *testing.T) {
				ctx := context.Background()
				loop, cluster := menuThirdRound(t)
				response := menuCall(t, loop, map[string]any{"command": "menu"})
				menu := response["menu"].(map[string]any)
				request := menuSelection(t, menu, "routing-then-restart", 3, "seeded-cut")
				original := loop.selection.store
				injected := &menuCutStore{StateStore: original, cut: func(state *contracts.ControllerState) bool {
					a := state.GetSelectionLedger().GetAttempts()
					if len(a) == 0 {
						return false
					}
					if cut == "option_selected" {
						return len(a[0].Actions) == 0 && len(a[0].Barriers) == 0
					}
					if cut == "effect_applied_round_3" {
						for _, action := range a[0].Actions {
							if action.RoundIndex == 3 && action.Complete {
								return true
							}
						}
					}
					return false
				}}
				loop.selection.store = injected
				if cut == "effect_applied_round_3" && seed%2 == 1 {
					prior := cluster.afterPatch
					cluster.afterPatch = func(ref sdk.ObjectRef) {
						prior(ref)
						actions := loop.selection.snapshot().Attempts[0].Actions
						if len(actions) > 0 && actions[len(actions)-1].RoundIndex == 3 && !actions[len(actions)-1].Complete {
							injected.fired = true
							panic(menuCrash{}) // API effect landed, completion reply was lost.
						}
					}
				}
				crashed := false
				var delivered map[string]any
				func() {
					defer func() {
						if value := recover(); value != nil {
							if _, ok := value.(menuCrash); !ok {
								panic(value)
							}
							crashed = true
						}
					}()
					delivered = menuCall(t, loop, request)
				}()
				if cut != "report_delivered_round_3" && !crashed {
					t.Fatal("crash cut was not reached")
				}
				if cut == "report_delivered_round_3" {
					report, ok := delivered["report"].(map[string]any)
					if !ok || report["status"] != "STATUS_SUCCESS" || len(report["rounds"].([]any)) != 3 {
						t.Fatalf("third round did not settle: %+v", delivered)
					}
				}
				state, _, err := original.Load(ctx)
				if err != nil {
					t.Fatal(err)
				}
				beforeWrites := len(cluster.patches)
				replacement := NewResponderLoop(loop.Request, loop.Programs, cluster, nil, loop.Executor, 99)
				replacement.MenuConfig = loop.MenuConfig
				if err := replacement.attachSelectionJournal(ctx, loop.Request, original, state, false); err != nil {
					t.Fatal(err)
				}
				rng := rand.New(rand.NewSource(seed))
				for i := 0; i < 1+rng.Intn(4); i++ {
					opened := replacement.Handle(ctx, ToolRequest{Command: "session-open"})
					if opened.Error != "" || opened.Session.SessionId != loop.sessionID {
						t.Fatal("replacement lost the original session")
					}
				}
				a := replacement.selection.snapshot().Attempts[0]
				report := a.Report
				if report == nil || len(cluster.patches) != beforeWrites || !replacement.selection.snapshot().WritesFrozen || replacement.MaxBatches != 5 || !replacement.Request.ResponseDeadline.Equal(loop.Request.ResponseDeadline) {
					t.Fatal("recovery replayed writes or reset incident limits")
				}
				if cut == "report_delivered_round_3" {
					if !proto.Equal(report, state.SelectionLedger.Attempts[0].Report) {
						t.Fatal("delivered report changed on replacement")
					}
				} else if report.Status != contracts.MitigationReport_STATUS_INTERRUPTED {
					t.Fatal("crash did not settle visibly interrupted")
				}
				if cut == "effect_applied_round_3" {
					found := false
					for _, round := range report.Rounds {
						if round.RoundIndex == 3 {
							found = true
						}
					}
					if !found {
						t.Fatal("recovery dropped round above two")
					}
				}
				query := replacement.Handle(ctx, ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{Version: 1, IncidentId: loop.Request.IncidentID, SessionId: loop.sessionID, DispatchEpoch: 2, AttemptId: a.AttemptId}})
				if query.Error != "" || !proto.Equal(query.Report, report) || len(cluster.patches) != beforeWrites {
					t.Fatal("replacement report delivery mutated settlement")
				}
			})
		}
	}
}
