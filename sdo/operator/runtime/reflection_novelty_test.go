package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	for _, manual := range []bool{false, true} {
		t.Run(fmt.Sprint("hand-repair=", manual), func(t *testing.T) {
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
			modelCalls := 0
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
					modelCalls++
					return ReflectionDecision{LearningDecision: "failed", Error: "synthetic learning worker observed new evidence"}, nil
				}),
			}
			p, err := native.PrepareIncident(ctx, "covered-repeat")
			if err != nil {
				t.Fatal(err)
			}
			c := nativeClosure(p)
			c.Request.RepairPolicy = "recorded-actions"
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
			if modelCalls != want {
				t.Fatalf("model calls=%d want=%d", modelCalls, want)
			}
			if !manual && ledger.GetReflectionDecision().GetNoChangeReason() != "accepted_programs_no_new_evidence" {
				t.Fatalf("model calls=%d decision=%v", modelCalls, ledger.GetReflectionDecision())
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
