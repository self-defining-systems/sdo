//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

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
	outcome := &contracts.OutcomeRecord{IncidentId: incident, RepairActions: []*contracts.RepairActionReceipt{{ActionId: "original", Kind: "run-script", Target: "Pod/app/synthetic", Summary: "synthetic repair", Details: "synthetic original receipt", Success: true, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}}}
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
	outcome.SchemaVersion = 1
	outcome.Classification = "success"
	if _, err := memory.NewJSONStore(root).Put(context.Background(), outcome); err != nil {
		t.Fatal(err)
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
	for _, mode := range []string{"no-corpus", "other-program", "failed", "untimed", "different-incident", "unlinked", "dirty-checkout", "newer-commit", "forged-corpus"} {
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
			case "dirty-checkout", "newer-commit", "forged-corpus":
				script = "echo unrecorded-script"
				labels["script_origins"] = map[string]string{program.ScriptDigest(script): "original"}
				if mode == "forged-corpus" {
					raw, err := os.ReadFile(recordingPath)
					if err != nil {
						t.Fatal(err)
					}
					var calls []sdktest.PrimitiveCall
					if err := json.Unmarshal(raw, &calls); err != nil {
						t.Fatal(err)
					}
					for i := range calls {
						if calls[i].Method == "RepairReceipt" {
							var r program.ScriptOriginReceipt
							if err := json.Unmarshal(calls[i].Result, &r); err != nil {
								t.Fatal(err)
							}
							r.ScriptDigest = program.ScriptDigest(script)
							calls[i].Result, _ = json.Marshal(r)
						}
					}
					raw, _ = json.Marshal(calls)
					if err := os.WriteFile(recordingPath, raw, 0644); err != nil {
						t.Fatal(err)
					}
				}

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

// A hand repair recorded without script bytes (sdo act, an observed API write)
// authorizes the learned script that encodes it, by receipt identity. A receipt
// that recorded different executed bytes still refuses: it names what ran.
func TestScriptProvenanceLinksReceiptIdentityAndRecordedBytes(t *testing.T) {
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
			// The label points to either a receipt without bytes or different executed bytes.
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
			if executed == "" && client.request.Script != "echo recorded" {
				t.Fatalf("script encoding a recorded hand repair refused: %q; report=%v", client.request.Script, report)
			}
			if executed != "" && client.request.Script != "" {
				t.Fatalf("script differing from the recorded executed bytes authorized: %q; report=%v", client.request.Script, report)
			}
		})
	}
}

// Real execution receipts must remain consumable by live learned programs when
// there is no corpus at all. Changing checkout files cannot authorize bytes.
func TestReplayOffAcceptedReceiptsReachLiveProgramsWithoutCorpus(t *testing.T) {
	for _, variant := range []string{"accepted", "new origin", "legacy helper", "unimported helper", "wrong bytes", "dirty receipt"} {
		t.Run(variant, func(t *testing.T) {
			loop, _ := pipelineTestLoop(t, true)
			id := (pipelinePortProgram{}).Spec().ID
			root := nativeRepository(t)
			const incident = "receipt-only"
			at := time.Unix(100, 0)
			executor := PlanExecutor{Client: &scriptPlanFake{}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{IncidentID: incident, Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.RunScript{Script: "echo recorded"}}}, false)
			if err != nil {
				t.Fatal(err)
			}
			receipts := ProgramRepairReceipts([]RecordedPlanApplication{{ID: "original", Result: result, StartedAt: at, CompletedAt: at.Add(time.Second)}}, nil)
			projected, err := incidentResultContract(IncidentResult{RepairActions: receipts})
			if err != nil {
				t.Fatal(err)
			}
			outcome := &contracts.OutcomeRecord{SchemaVersion: 1, Classification: "success", IncidentId: incident, RepairActions: projected.GetRepairActions()}
			data, err := protojson.Marshal(outcome)
			if err != nil {
				t.Fatal(err)
			}
			for file, body := range map[string][]byte{
				layout.OutcomePath(incident):            data,
				".sdo/playbooks/" + id + "/README.md":   []byte("---\noriginating_incident: " + incident + "\n---\n# Synthetic\n"),
				".sdo/playbooks/" + id + "/diagnose.go": []byte("package synthetic\n"),
			} {
				path := filepath.Join(root, file)
				if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, body, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if variant == "new origin" {
				body := "---\noriginating_incident: older-birth\nscript_receipts:\n  " + program.ScriptDigest("echo recorded") + ":\n    incident_id: " + incident + "\n    receipt_id: " + outcome.RepairActions[0].ActionId + "\n---\n# Synthetic\n"
				if err := os.WriteFile(filepath.Join(root, ".sdo/playbooks/"+id+"/README.md"), []byte(body), 0644); err != nil {
					t.Fatal(err)
				}
			}

			if strings.Contains(variant, "helper") {
				outcome.RepairActions[0].ScriptDigest = ""
				raw, _ := protojson.Marshal(outcome)
				if err := os.WriteFile(filepath.Join(root, layout.OutcomePath(incident)), raw, 0644); err != nil {
					t.Fatal(err)
				}
				files := map[string]string{
					".sdo/diagnostics/go.mod":       "module synthetic\n\ngo 1.26.0\n",
					".sdo/lib/script/metadata.yaml": "originating_incident: " + incident + "\nscript_origins:\n  " + program.ScriptDigest("echo recorded") + ": " + outcome.RepairActions[0].ActionId + "\n",
					".sdo/lib/script/repair.go":     "package script\n",
				}
				if variant == "legacy helper" {
					files[".sdo/playbooks/"+id+"/diagnose.go"] = `package synthetic
import _ "synthetic/lib/script"
`
				}
				for file, body := range files {
					name := filepath.Join(root, file)
					if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(name, []byte(body), 0644); err != nil {
						t.Fatal(err)
					}
				}
			}

			nativeGit(t, root, "add", ".")
			nativeGit(t, root, "commit", "-qm", "accepted receipt-only memory")
			loop.Request.RepositoryWorktree = root
			loop.Request.SourceCommit = nativeGit(t, root, "rev-parse", "HEAD")
			script := "echo recorded"
			if variant == "wrong bytes" {
				script = "echo unrecorded"
			}
			if variant == "dirty receipt" {
				script = "echo forged"
				outcome.RepairActions[0].ScriptDigest = program.ScriptDigest(script)
				data, _ := protojson.Marshal(outcome)
				if err := os.WriteFile(filepath.Join(root, layout.OutcomePath(incident)), data, 0644); err != nil {
					t.Fatal(err)
				}
			}
			err = loop.scriptProvenance(id).check(context.Background(), id, script)
			if (err == nil) != (variant == "accepted" || variant == "legacy helper" || variant == "new origin") {
				t.Fatalf("%s: %v", variant, err)
			}
		})
	}
}
