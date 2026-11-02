//go:build integration

package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory/layout"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func coveredRepeatOutcome(t *testing.T) *contracts.OutcomeRecord {
	o := t1Outcome(t, "synthetic-revision")
	o.Findings = []*contracts.Finding{{Fingerprint: "routing", DetectorId: "routing", RuleId: "target-port", Status: "resolved", Severity: "warn", Summary: "synthetic routing mismatch", Evidence: "synthetic receipt", PrimaryResource: t1Ref("api")}}
	o.PlaybookCoverage = &contracts.PlaybookCoverage{Matches: []*contracts.PlaybookMatch{{PlaybookId: t1Program, ProgramRevision: "synthetic-revision", Bindings: o.ProgramRuns[0].Bindings, ExplainedFindingFingerprints: []string{"routing"}}}}
	return o
}

func TestReflectionNoveltyRequiresCompleteRecordedCoverage(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*contracts.OutcomeRecord)
	}{
		{"covered", func(*contracts.OutcomeRecord) {}},
		{"uncovered", func(o *contracts.OutcomeRecord) {
			o.Findings = append(o.Findings, &contracts.Finding{Fingerprint: "new"})
		}},
		{"failed", func(o *contracts.OutcomeRecord) { o.Classification = "failed" }},
		{"partial", func(o *contracts.OutcomeRecord) { o.PlanApplications[0].VerificationPassed = false }},
		{"unknown", func(o *contracts.OutcomeRecord) {
			o.PlanApplications[0].Verification.Result = contracts.Verdict_RESULT_UNKNOWN
		}},
		{"new transition", func(o *contracts.OutcomeRecord) { o.UndetectedCauses = []*contracts.UndetectedCause{{}} }},
		{"new detector evidence", func(o *contracts.OutcomeRecord) { o.DisabledSetupDetectors = []*contracts.DisabledSetupDetector{{}} }},
		{"new source", func(o *contracts.OutcomeRecord) { o.RepairCommit = proto.String("source-repair") }},
		{"program failure", func(o *contracts.OutcomeRecord) { o.ProgramFailureSignals = []string{"failed"} }},
		{"no findings", func(o *contracts.OutcomeRecord) { o.Findings = nil }},
		{"no application", func(o *contracts.OutcomeRecord) { o.PlanApplications = nil }},
		{"empty fingerprint", func(o *contracts.OutcomeRecord) { o.Findings[0].Fingerprint = "" }},
		{"wrong binding", func(o *contracts.OutcomeRecord) {
			o.PlaybookCoverage.Matches[0].Bindings = []*contracts.Binding{{Name: "service", Object: t1Ref("peer")}}
		}},
		{"wrong revision", func(o *contracts.OutcomeRecord) { o.PlaybookCoverage.Matches[0].ProgramRevision = "peer-revision" }},
		{"missing post-apply proof", func(o *contracts.OutcomeRecord) { o.ProgramToolCalls = nil }},
		{"unapplied coverage", func(o *contracts.OutcomeRecord) { o.PlaybookCoverage.Matches[0].PlaybookId = "peer" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := coveredRepeatOutcome(t)
			test.change(o)
			if got := coveredRepeatCandidate(o); got != (test.name == "covered") {
				t.Fatalf("covered repeat = %v", got)
			}
		})
	}
}

func TestCoveredRepeatBrokerSkipsReflectionWorker(t *testing.T) {
	for _, test := range []struct {
		name                            string
		manual, session, stage, generic bool
	}{
		{"covered deterministic stage", false, false, true, false}, {"hand repair", true, false, true, false}, {"covered watching session", false, true, false, false}, {"covered proof without session", false, false, false, false}, {"covered generic detection", false, false, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			manual := test.manual
			ctx := context.Background()
			root := nativeRepository(t)
			nativeMemoryFixture(t, root)
			t1Write(t, root, ".sdo/playbooks/contract-repair/README.md", []byte("---\nschema_version: 1\nowner: responder\nfault_class: contract-repair\noriginating_incident: initial\n---\nRepair <RESOURCE_NAME>\n"))
			nativeGit(t, root, "add", ".")
			nativeGit(t, root, "commit", "-qm", "synthetic accepted program")
			repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			workerCalls := 0
			native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), Replay: "off", ResponderBackend: "test", ResponderModel: "test",
				Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
					o := coveredRepeatOutcome(t)
					o.IncidentId = c.Request.IncidentID
					if manual {
						repair := proto.Clone(o.RepairActions[0]).(*contracts.RepairActionReceipt)
						repair.ActionId = "hand-repair"
						repair.Kind = "sdo-act"
						o.RepairActions = append(o.RepairActions, repair)
					}
					return DerivedOutcome{Record: o, RequiresReflection: true}, nil
				}),
				Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
					workerCalls++
					if test.session {
						return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "native_memory_check_no_change"}, nil
					}
					return ReflectionDecision{LearningDecision: "failed", Error: "synthetic learning worker observed new evidence"}, nil
				}),
			}
			p, err := native.PrepareIncident(ctx, "covered-repeat")
			if err != nil {
				t.Fatal(err)
			}
			c := nativeClosure(p)
			c.Request.RepairPolicy = "recorded-actions"
			if test.stage {
				c.Result.ResolutionPath = "deterministic"
				c.Request.PriorStages = []*contracts.StageReport{{Stage: "deterministic"}}
			}
			if test.generic {
				c.DetectorTimeline = []DetectorTimelineEntry{{DetectorID: "workload-health", DetectorClass: "health", Owner: "health_judge", RuleID: "pending", Fingerprint: "init", FirstActivatedAt: c.DetectedAt, LastSeenAt: c.DetectedAt, Relation: RelationBeforeDispatch, Activations: 1}}
			}
			if test.session {
				c.Result.ResponderSessionID = "synthetic-session"
			}
			if _, err := native.ProcessClosure(ctx, c); err != nil {
				t.Fatal(err)
			}
			ledger, err := native.readLedger(c.Request.IncidentID)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if manual {
				want = 1
			}
			if workerCalls != want {
				t.Fatalf("worker calls=%d want=%d", workerCalls, want)
			}
			if !manual && ledger.GetReflectionDecision().GetNoChangeReason() != "accepted_programs_no_new_evidence" {
				t.Fatalf("worker calls=%d decision=%v", workerCalls, ledger.GetReflectionDecision())
			}
		})
	}
}

func TestReflectionPeerPublicationDoesNotRepeatCleanExploration(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint("conflict=", conflict), func(t *testing.T) {
			ctx := context.Background()
			root := nativeRepository(t)
			nativeMemoryFixture(t, root)
			repository, err := broker.New(root, t.TempDir(), func(context.Context, string, []string) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), Replay: "off", ResponderBackend: "test", ResponderModel: "test",
				Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
					return testDerivedOutcome(c.Request.IncidentID, "success", true), nil
				}),
				Reflector: ReflectionWorkerFunc(func(ctx context.Context, _ IncidentClosure, w IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
					calls++
					author := filepath.Join(w.Worktree, ".sdo/playbooks/base/README.md")
					body := "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\n---\nAuthor refinement and peer accepted evidence <RESOURCE_NAME>\n"
					if err := os.WriteFile(author, []byte(body), 0644); err != nil {
						return ReflectionDecision{}, err
					}
					if calls == 1 {
						if _, err := repository.RecordOutcome(ctx, "peer", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "peer", Classification: "success"}); err != nil {
							return ReflectionDecision{}, err
						}
						peer, err := repository.CreateProposalForPhase(ctx, "peer", broker.ReflectionPhase)
						if err != nil {
							return ReflectionDecision{}, err
						}
						book := "peer"
						if conflict {
							book = "base"
						}
						path := filepath.Join(peer.Path, ".sdo/playbooks", book, "README.md")
						if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
							return ReflectionDecision{}, err
						}
						body := "---\nschema_version: 1\nowner: responder\nfault_class: " + book + "\noriginating_incident: initial\n---\nPeer accepted evidence <RESOURCE_NAME>\n"
						if err := os.WriteFile(path, []byte(body), 0644); err != nil {
							return ReflectionDecision{}, err
						}
						if _, err := repository.PublishReflection(ctx, "peer", &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{book}}); err != nil {
							return ReflectionDecision{}, err
						}
					}
					return ReflectionDecision{LearningDecision: "updated", PlaybookIDs: []string{"base"}}, nil
				}),
			}
			p, err := native.PrepareIncident(ctx, "author")
			if err != nil {
				t.Fatal(err)
			}
			c := nativeClosure(p)
			c.Request.RepairPolicy = "recorded-actions"
			if _, err := native.ProcessClosure(ctx, c); err != nil {
				t.Fatal(err)
			}
			want := 1
			if conflict {
				want = 2
			}
			if calls != want {
				t.Fatalf("explorations=%d want %d", calls, want)
			}
			accepted, err := os.ReadFile(filepath.Join(root, ".sdo/playbooks/base/README.md"))
			if err != nil || !strings.Contains(string(accepted), "Author refinement and peer accepted evidence") {
				t.Fatalf("lost author or peer intent: %s %v", accepted, err)
			}
		})
	}
}

// The real deterministic stage publishes the application/NoMatch receipts;
// the real outcome deriver and broker consume them without a model worker.
func TestCoveredRepeatStageProducerReachesBroker(t *testing.T) {
	ctx := context.Background()
	loop, state := pipelineTestLoop(t, false)
	root := nativeRepository(t)
	nativeMemoryFixture(t, root)
	for _, match := range state.Request.PlaybookCoverage.Matches {
		id := match.GetPlaybookId()
		t1Write(t, root, ".sdo/playbooks/"+id+"/README.md", []byte("---\nschema_version: 1\nowner: responder\nfault_class: "+id+"\noriginating_incident: initial\n---\nSynthetic accepted repair <RESOURCE_NAME>\n"))
	}
	if _, err := layout.WritePlaybookIndex(root); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "synthetic accepted stage memory")
	b, err := broker.New(root, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	workers := 0
	native := NativeIncidentBroker{Repository: b, LedgerRoot: t.TempDir(), Replay: "off", Deriver: ProtoOutcomeDeriver{}, Programs: loop.Programs, ResponderBackend: "test", ResponderModel: "test", Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
		workers++
		return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "worker fallback"}, nil
	})}
	workspace, err := native.PrepareIncident(ctx, state.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	state.Request.RepositoryBaseCommit = workspace.BaseCommit
	state.Request.RepositoryWorktree = workspace.Worktree
	loop.Request = state.Request
	if err := RunResponseStages(ctx, state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
		t.Fatal(err)
	}
	result, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	closure := IncidentClosure{Request: state.Request, Result: &result, VerifiedAt: time.Now(), FinalDetectorStates: []DetectorEvaluation{{DetectorID: loop.Detectors[0].Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: time.Now()}}, Frozen: true}
	closure.Request.PriorStages = cloneStageReports(state.PriorStages)
	if _, err := native.ProcessClosure(ctx, closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger(state.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if workers != 0 || ledger.GetReflectionDecision().GetNoChangeReason() != "accepted_programs_no_new_evidence" {
		t.Fatalf("workers=%d decision=%v", workers, ledger.GetReflectionDecision())
	}
}
