package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// These are REV44's two constructions. The guard must inspect returned values,
// including pointers and helper output, rather than source spellings.
func TestMitigationPlanRefusesUnprovenancedScriptValues(t *testing.T) {
	for _, construction := range []string{"field-built", "reassigned", "pointer", "helper"} {
		t.Run(construction, func(t *testing.T) {
			m := funcMitigator{id: "script", plan: func(program.ReadView, program.Bindings) (program.Plan, error) {
				var step program.RunScript
				if construction == "reassigned" {
					step = program.RunScript{Script: "echo recorded"}
				}
				step.Script = "echo unrecorded-script"
				var action program.Action = step
				if construction == "pointer" {
					action = &step
				}
				if construction == "helper" {
					action = scriptFromHelper()
				}
				return program.Plan{Actions: []program.Action{action}}, nil
			}}
			_, err := BuildMitigationPlan(context.Background(), m, &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app"}}, program.Bindings{}, time.Second)
			if err == nil || !strings.Contains(err.Error(), "script_unprovenanced") {
				t.Fatalf("unrecorded returned bytes accepted: %v", err)
			}
		})
	}
}
func scriptFromHelper() program.Action { return program.RunScript{Script: "echo unrecorded-script"} }

// Exercise the real controller receipt producer and the public stage/tool
// consumers, with immutable accepted Git memory between them.
func TestAcceptedScriptProvenanceAtPublicApplyPaths(t *testing.T) {
	for _, construction := range []string{"field-built", "reassigned", "provenanced"} {
		for _, publicPath := range []string{"stage", "mitigate-match"} {
			t.Run(construction+"/"+publicPath, func(t *testing.T) {
				loop, state := pipelineTestLoop(t, true)
				calls := 0
				p := provenancePipelineProgram{construction: construction, calls: &calls}
				registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
				if err != nil {
					t.Fatal(err)
				}
				loop.Programs = registry
				root, commit := acceptedScriptMemory(t, p.Spec().ID, "echo recorded")
				loop.Request.RepositoryWorktree, loop.Request.SourceCommit = root, commit
				state.Request = loop.Request
				state.Coverage = nil
				scriptClient := &scriptPlanFake{}
				loop.Executor.Client = scriptClient
				if publicPath == "stage" {
					stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true, ObservationCeiling: time.Millisecond}
					report, err := stage.Run(context.Background(), state)
					if err != nil {
						t.Fatal(err)
					}
					if construction != "provenanced" {
						if len(report.Mitigations) != 1 || report.Mitigations[0].SkipReason != "script_unprovenanced" {
							t.Fatalf("missing typed handover: %v", report)
						}
						before := calls
						if _, err := RunStageAgain(context.Background(), state, stage); err != nil {
							t.Fatal(err)
						}
						if calls != before {
							t.Fatal("stage retried refused program")
						}
						restarted, restored := pipelineTestLoop(t, true)
						restarted.Programs = registry
						restarted.Request.RepositoryWorktree, restarted.Request.SourceCommit = root, commit
						restored.Request = restarted.Request
						restored.PriorStages = []*contracts.StageReport{report}
						restored.Refresh = true
						afterRestart := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return restarted }, ObservationCeiling: time.Millisecond}
						if _, err := afterRestart.Run(context.Background(), restored); err != nil {
							t.Fatal(err)
						}
						if calls != before {
							t.Fatal("restart retried refused program revision")
						}

					}
				} else {
					loop.MenuConfig = productionResponderMenu()
					menu := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: p.Spec().ID, Match: true})
					options := matchOptions(menu, p.Spec().ID)
					if len(options) != 1 {
						t.Fatalf("missing matched option: %+v", menu)
					}
					response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: options[0]}})
					if construction != "provenanced" && response.Report.GetReasonCode() != "script_unprovenanced" {
						t.Fatalf("tool accepted unrecorded bytes: %+v", response)
					}
					if construction == "provenanced" && response.Error != "" {
						t.Fatalf("provenanced script refused: %+v", response)
					}
				}
				if construction == "provenanced" {
					if scriptClient.request.Script != "echo recorded" {
						t.Fatalf("provenanced bytes did not reach executor: %+v", scriptClient.request)
					}
				} else if scriptClient.request.Script != "" || len(loop.PlanApplications()) != 0 {
					t.Fatal("unprovenanced script reached execution")
				}
			})
		}
	}
}

type provenancePipelineProgram struct {
	pipelinePortProgram
	construction string
	calls        *int
}

func (p provenancePipelineProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	*p.calls++
	var step program.RunScript
	if p.construction != "field-built" {
		step = program.RunScript{Script: "echo recorded"}
	}
	if p.construction != "provenanced" {
		step.Script = "echo unrecorded-script"
	}
	return program.Plan{Actions: []program.Action{step}}, nil
}

func acceptedScriptMemory(t *testing.T, id, script string) (string, string) {
	return acceptedScriptMemoryWithExecutedScript(t, id, script, script)
}
func acceptedScriptMemoryWithExecutedScript(t *testing.T, id, script, executed string) (string, string) {
	t.Helper()
	root := nativeRepository(t)
	capture := t.TempDir()
	const incident = "script-origin"
	if err := memory.CaptureIncidentSnapshot(capture, incident, "open@detect", sdktest.Snapshot{NamespaceName: "app"}, "app"); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(100, 0)
	outcome := &contracts.OutcomeRecord{IncidentId: incident, RepairActions: []*contracts.RepairActionReceipt{{ActionId: "original", Success: true, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}}}
	if executed != "" {
		executor := PlanExecutor{Client: &scriptPlanFake{}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
		result, err := executor.Apply(context.Background(), IncidentRequest{IncidentID: incident, Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.RunScript{Script: executed}}}, false)
		if err != nil {
			t.Fatal(err)
		}
		receipts := ProgramRepairReceipts([]RecordedPlanApplication{{ID: "original", Result: result, StartedAt: at, CompletedAt: at.Add(time.Second)}}, nil)
		if len(receipts) == 0 || receipts[0].ScriptDigest == "" {
			t.Fatal("execution did not produce digest receipt")
		}
		outcome.RepairActions[0].ScriptDigest = receipts[0].ScriptDigest
	}
	if err := memory.CaptureRepairReceipts(capture, incident, outcome); err != nil {
		t.Fatal(err)
	}
	dir, err := memory.IncidentCorpusDir(capture, incident)
	if err != nil {
		t.Fatal(err)
	}
	recordings, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	labels, err := json.Marshal(map[string]any{"root_cause_program_ids": []string{id}, "script_origins": map[string]string{program.ScriptDigest(script): "original"}, "provenance": map[string]string{"incident_id": incident}})
	if err != nil {
		t.Fatal(err)
	}
	for file, data := range map[string][]byte{
		".sdo/corpus/origin/labels.json":        labels,
		".sdo/corpus/origin/recordings.json":    recordings,
		".sdo/playbooks/" + id + "/diagnose.go": []byte("package synthetic\n"),
	} {
		full := filepath.Join(root, file)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "synthetic accepted script provenance")
	return root, nativeGit(t, root, "rev-parse", "HEAD")
}

func TestScriptProvenanceUsesOnlyThisProgramsAcceptedReceipts(t *testing.T) {
	for _, mode := range []string{"no-corpus", "other-program", "failed", "untimed", "different-incident", "unlinked", "dirty-checkout", "newer-commit"} {
		t.Run(mode, func(t *testing.T) {
			loop, _ := pipelineTestLoop(t, true)
			id := (pipelinePortProgram{}).Spec().ID
			root, commit := acceptedScriptMemory(t, id, "echo recorded")
			labelPath := filepath.Join(root, ".sdo/corpus/origin/labels.json")
			recordingPath := filepath.Join(root, ".sdo/corpus/origin/recordings.json")
			labels := map[string]any{}
			raw, err := os.ReadFile(labelPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &labels); err != nil {
				t.Fatal(err)
			}
			script := "echo recorded"
			switch mode {
			case "no-corpus":
				nativeGit(t, root, "rm", "-r", ".sdo/corpus")
			case "other-program":
				labels["root_cause_program_ids"] = []string{"unrelated"}
			case "unlinked":
				labels["script_origins"] = map[string]string{}
			case "different-incident":
				labels["provenance"] = map[string]string{"incident_id": "another-incident"}
			case "dirty-checkout", "newer-commit":
				script = "echo unrecorded-script"
				labels["script_origins"] = map[string]string{program.ScriptDigest(script): "original"}
			case "failed", "untimed":
				var calls []sdktest.PrimitiveCall
				raw, err := os.ReadFile(recordingPath)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(raw, &calls); err != nil {
					t.Fatal(err)
				}
				var receipt program.ScriptOriginReceipt
				if err := json.Unmarshal(calls[0].Result, &receipt); err != nil {
					t.Fatal(err)
				}
				if mode == "failed" {
					receipt.Success = false
				} else {
					receipt.CompletedAt = time.Time{}
				}
				calls[0].Result, err = json.Marshal(receipt)
				if err != nil {
					t.Fatal(err)
				}
				raw, err = json.Marshal(calls)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(recordingPath, raw, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "no-corpus" {
				raw, err = json.Marshal(labels)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(labelPath, raw, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "dirty-checkout" {
				nativeGit(t, root, "add", ".")
				nativeGit(t, root, "commit", "-qm", "synthetic invalid lineage")
				if mode != "newer-commit" {
					commit = nativeGit(t, root, "rev-parse", "HEAD")
				}
			}
			loop.Request.RepositoryWorktree, loop.Request.SourceCommit = root, commit
			m := funcMitigator{id: id, plan: func(program.ReadView, program.Bindings) (program.Plan, error) {
				return program.Plan{Actions: []program.Action{program.RunScript{Script: script}}}, nil
			}}
			_, err = BuildMitigationPlan(context.Background(), m, &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app"}}, program.Bindings{}, time.Second, loop.scriptProvenance(id))
			var refused *ScriptUnprovenancedError
			if !errors.As(err, &refused) {
				t.Fatalf("invalid lineage accepted: %v", err)
			}
		})
	}
}

// Review negative control: the only original receipt contains no script bytes
// or digest, so relabeling it must not authorize a never-executed script.
func TestREV44BReceiptWithoutScriptBytesCannotAuthorizeScript(t *testing.T) {
	for _, executed := range []string{"", "echo actually-executed"} {
		t.Run(executed, func(t *testing.T) {
			loop, state := pipelineTestLoop(t, true)
			calls := 0
			p := provenancePipelineProgram{construction: "provenanced", calls: &calls}
			registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
			if err != nil {
				t.Fatal(err)
			}
			loop.Programs = registry
			root, commit := acceptedScriptMemoryWithExecutedScript(t, p.Spec().ID, "echo recorded", executed)
			// The label points to either a legacy receipt or different executed bytes.
			loop.Request.RepositoryWorktree, loop.Request.SourceCommit = root, commit
			state.Request = loop.Request
			state.Coverage = nil
			client := &scriptPlanFake{}
			loop.Executor.Client = client
			stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true, ObservationCeiling: time.Millisecond}
			report, err := stage.Run(context.Background(), state)
			if err != nil {
				t.Fatal(err)
			}
			if client.request.Script != "" {
				t.Fatalf("NEVER-EXECUTED SCRIPT AUTHORIZED: %q; report=%v", client.request.Script, report)
			}
		})
	}
}
