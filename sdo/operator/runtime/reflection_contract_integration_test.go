//go:build integration

package runtime

// Independent T1 oracle: expectations below come from decisions 162–176/186,
// not R6's verifiedRepeat/verifiedTrustOutcome fixtures. Only repository and
// closure setup helpers are shared with the older integration tests.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/format"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Each matrix row crosses real Git proposals, outcome persistence, registered
// protobuf task/result transport, deterministic corpus writes, broker review,
// acceptance/rejection and restart. The scripted executor never launches a model.
func TestT1ScriptedBrokerMatrix(t *testing.T) {
	for _, c := range t1Classes() {
		t.Run(c.name, func(t *testing.T) {
			// Each row owns its repositories, captures and worker state.
			t.Parallel()
			for _, reject := range []bool{false, true} {
				t.Run(fmt.Sprintf("reject=%t", reject), func(t *testing.T) {
					run := t1RunBroker(t, c, reject)
					wantModel := c.model || (reject && c.positive > 0)
					if (run.calls > 0) != wantModel {
						t.Logf("rejections=%v skipped=%s", run.ledger.ReflectionRejections, run.ledger.ReflectionSkippedReason)
						t.Errorf("reflection calls = %d; requires model = %t", run.calls, wantModel)
					}
					if c.model {
						if c.unverified {
							if len(run.work) != 0 || len(run.unassigned) != 1 || run.unassigned[0].GetDisposition() != "question" || run.unassigned[0].GetReason() != "no_recorded_repair" || len(run.unassigned[0].GetResources()) != 1 || !proto.Equal(run.unassigned[0].Resources[0], t1Ref("api")) {
								t.Fatalf("unverified cause escaped evidence-gap projection: work=%v unassigned=%v", run.work, run.unassigned)
							}
						} else {
							t1AssertWork(t, run.work, c)
						}
					}
					if run.positive != c.positive || run.negative != c.negative {
						t.Errorf("published/attempted cases = +%d/-%d; want +%d/-%d", run.positive, run.negative, c.positive, c.negative)
					}
					if got := ProgramTrustStats([]*contracts.OutcomeRecord{run.outcome})[t1Program].VerifiedSuccesses; got != c.credit {
						t.Errorf("durable trust credit = %d; want %d (reflection rejected=%t)", got, c.credit, reject)
					}
					if c.negative > 0 && !run.ledger.ReflectionRejected {
						t.Error("unchanged false-matching program escaped negative corpus replay rejection")
					}
					if !reject && !c.model && run.ledger.ReflectionCommit == "" {
						t.Error("covered case was not accepted through the broker")
					}
					wantGaps := 0
					if c.unverified {
						wantGaps = 1
					}
					if int(run.ledger.GetReflectionNoRepairCount()) != wantGaps {
						t.Errorf("durable gap count = %d; want %d", run.ledger.GetReflectionNoRepairCount(), wantGaps)
					}
				})
			}
		})
	}
}

func t1RunBroker(t *testing.T, c t1Class, reject bool) t1Run {
	t.Helper()
	ctx := context.Background()
	root := nativeRepository(t)
	nativeMemoryFixture(t, root)
	source, err := format.Source([]byte(t1ProgramSource))
	if err != nil {
		t.Fatal(err)
	}
	t1Write(t, root, ".sdo/playbooks/contract-repair/diagnose.go", source)
	detectorPath := ".sdo/diagnostics/detectors/health/health/manifest.yaml"
	detectorManifest, err := os.ReadFile(filepath.Join(root, detectorPath))
	if err != nil {
		t.Fatal(err)
	}
	t1Write(t, root, detectorPath, []byte(strings.Replace(string(detectorManifest), "  - base\n", "  - base\n  - contract-repair\n", 1)))
	t1Write(t, root, ".sdo/playbooks/contract-repair/README.md", []byte("---\nschema_version: 1\nowner: responder\nfault_class: contract-repair\noriginating_incident: seed\n---\nRestore the accepted target port for <SERVICE>.\n"))
	t1Write(t, root, ".sdo/corpus/seed/labels.json", []byte(`{"root_cause_program_ids":["contract-repair"],"bindings":{"service":{"apiVersion":"v1","kind":"Service","namespace":"app","name":"api"}},"peer_bindings":{"service":{"apiVersion":"v1","kind":"Service","namespace":"app","name":"peer"}}}`))
	seedCapture := t.TempDir()
	t1Captures(t, seedCapture, t1Class{fixed: program.NoMatch})
	for destination, origin := range map[string]string{"open.json": "open@detect.json", "fixed.json": "fixed.json", "healthy.json": "fixed.json", "recordings.json": "open@detect-recordings.json"} {
		raw, err := os.ReadFile(filepath.Join(seedCapture, origin))
		if err != nil {
			t.Fatal(err)
		}
		t1Write(t, root, ".sdo/corpus/seed/"+destination, raw)
	}
	labelPath := filepath.Join(root, ".sdo/corpus/seed/labels.json")
	raw, readErr := os.ReadFile(labelPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	var label map[string]any
	if err := json.Unmarshal(raw, &label); err != nil {
		t.Fatal(err)
	}
	snapshot := capturefixture.FromView(t, `{"namespace":"app"}`)
	digest := fmt.Sprintf("%x", sha256.Sum256(snapshot))
	label["provenance"] = map[string]string{"incident_id": "seed", "open_at": "detect", "open_sha256": digest, "fixed_sha256": digest}
	t1Write(t, root, ".sdo/corpus/seed/labels.json", t1JSON(t, label))
	t1Write(t, root, ".sdo/playbooks/README.md", []byte("# Playbooks\n\n- [base](base/README.md)\n- [contract-repair](contract-repair/README.md)\n"))
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "handwritten memory")
	base := nativeGit(t, root, "rev-parse", "HEAD")
	revision, err := programDependencyRevision(ctx, root, base, t1Program)
	if err != nil {
		t.Fatal(err)
	}
	o := t1Outcome(t, revision)
	o.SourceCommit = base
	if c.change != nil {
		c.change(o)
	}
	// Successful closure alone is not per-cause proof. These synthetic
	// controller observations explicitly bind each claimed repair to a fresh
	// clearance; rows without a mutation receipt must still remain unassigned.
	for index, cause := range o.ConfirmedRootCauses {
		detector := fmt.Sprintf("t1-health-%d", index)
		for _, ref := range cause.Resources {
			o.Findings = append(o.Findings, &contracts.Finding{DetectorId: detector, RuleId: "available", Status: "active", Severity: "warn", Summary: "service unavailable", Evidence: "synthetic controller observation before repair", PrimaryResource: proto.Clone(ref).(*contracts.ObjectRef), Fingerprint: detector})
		}
		o.FinalHealthDetectorState = append(o.FinalHealthDetectorState, &contracts.DetectorEvaluation{DetectorId: detector, Status: "clear", EvaluatedAt: timestamppb.New(time.Unix(102, 0))})
	}
	run := t1Run{}
	capture := ""
	seenCases := map[string]bool{}
	inspectCases := func(candidate string) {
		dirs, err := filepath.Glob(filepath.Join(candidate, ".sdo/corpus/*/labels.json"))
		if err != nil {
			t.Fatal(err)
		}
		for _, label := range dirs {
			name := filepath.Base(filepath.Dir(label))
			if seenCases[name] || name == "seed" {
				continue
			}
			seenCases[name] = true
			corpus, err := sdktest.LoadCorpusCase(filepath.Dir(label))
			if err != nil {
				t.Fatal(err)
			}
			if err := sdktest.ValidateCorpusProvenance(filepath.Dir(label)); err != nil {
				t.Error(err)
			}
			if corpus.Provenance.IncidentID != o.IncidentId {
				t.Errorf("uncorrelated corpus: %v", corpus.Provenance)
			}
			openSource := "open@detect.json"
			if strings.HasPrefix(name, "fixed-no-match-") {
				openSource = "fixed.json"
			}
			for file, source := range map[string]string{"open.json": openSource, "fixed.json": "fixed.json", "healthy.json": "fixed.json", "recordings.json": "open@detect-recordings.json"} {
				expected, err := os.ReadFile(filepath.Join(capture, source))
				if err != nil {
					t.Fatal(err)
				}
				actual, err := os.ReadFile(sdktest.CorpusSnapshotFile(filepath.Dir(label), file))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(actual, expected) {
					t.Errorf("case %s changed protected %s bytes", name, file)
				}
			}
			// Each distinct snapshot is stored once; healthy (and a fixed open) is fixed.json.
			copies := []string{"healthy.json"}
			if openSource == "fixed.json" {
				copies = append(copies, "open.json")
			}
			for _, copied := range copies {
				if _, err := os.Stat(filepath.Join(filepath.Dir(label), copied)); !os.IsNotExist(err) {
					t.Errorf("case %s stores %s, a copy of fixed.json", name, copied)
				}
			}
			if strings.HasPrefix(name, "repeat-") {
				run.positive++
				if len(corpus.RootCauseProgramIDs) != 1 || len(corpus.NoMatchProgramIDs) != 0 {
					t.Errorf("invalid positive labels: %+v", corpus)
				}
			} else if strings.HasPrefix(name, "fixed-no-match-") {
				run.negative++
				if len(corpus.RootCauseProgramIDs) != 0 || len(corpus.NoMatchProgramIDs) != 1 || corpus.Provenance.OpenAt != "fixed" {
					t.Errorf("negative mislabeled positive: %+v", corpus)
				}
			}
		}
	}
	// Normal broker validation calls this hermetic replay gate. It evaluates
	// real case bytes against the accepted diagnoser, including fixed negatives.
	repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), func(ctx context.Context, candidate string, paths []string) error {
		inspectCases(candidate)
		if reject {
			return validate.Err([]validate.Diagnostic{{Rule: "t1.scripted_rejection", Message: "script rejects publication"}})
		}
		seen := map[string]bool{}
		for _, path := range paths {
			dir := filepath.Dir(path)
			if !strings.HasPrefix(dir, ".sdo/corpus/") || seen[dir] {
				continue
			}
			seen[dir] = true
			corpus, err := sdktest.LoadCorpusCase(filepath.Join(candidate, dir))
			if err != nil {
				return err
			}
			verdict := program.RunDiagnose(ctx, t1Diagnoser{t1Program}, &sdktest.ReplayReadView{Snapshot: corpus.Fixed, Calls: corpus.PhaseRecordings("fixed")}, corpus.Bindings, time.Second)
			if verdict.Result != program.NoMatch {
				return validate.Err([]validate.Diagnostic{{Rule: "corpus.replay", Message: "fixed capture still matches or lacks proof"}})
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Prior replay records are persisted before the new closure, as in production.
	if c.prior {
		prior := proto.Clone(o).(*contracts.OutcomeRecord)
		prior.IncidentId = "earlier"
		if _, err := repository.RecordOutcome(ctx, prior.IncidentId, prior); err != nil {
			t.Fatal(err)
		}
	}
	session := t.TempDir()
	capture, err = memory.IncidentCorpusDir(session, o.IncidentId)
	if err != nil {
		t.Fatal(err)
	}
	t1Captures(t, capture, c)
	registry, err := program.NewRegistry([]program.Diagnoser{t1Diagnoser{t1Program}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.name == "absent_fixed_verdict" {
		registry = nil
		t1Captures(t, capture, t1Class{fixed: program.NoMatch})
	}
	worker := RegisteredReflectionWorker{AgentID: "t1-scripted", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(root), Timeout: time.Minute, SessionMode: "fresh"}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		inspectCases(task.Workspace.CheckoutPath)
		run.calls++
		request := &contracts.ReflectionRequest{}
		if err := task.Request.UnmarshalTo(request); err != nil {
			return nil, err
		}
		if c.prior && (len(request.PriorOutcomes) != 1 || request.PriorOutcomes[0].IncidentId != "earlier") {
			t.Errorf("prior accepted repair missing from reflection history: %v", request.PriorOutcomes)
		}
		run.work = request.CauseWork
		run.unassigned = request.UnassignedWork
		if request.ValidationAttempt > 1 && len(request.PreviousDiagnostics) == 0 {
			t.Error("retry lost typed rejection")
		}
		response := &contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"base"}}
		if c.decline {
			response = &contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "protected fixed observation missing"}
			if c.decline {
				response.DeclineReason = "no_recorded_repair"
			}
		} else {
			// A visible, bounded scripted edit ensures questions exercise publication.
			file := filepath.Join(task.Workspace.CheckoutPath, ".sdo/playbooks/base/README.md")
			raw, err := os.ReadFile(file)
			if err != nil {
				return nil, err
			}
			if err := os.WriteFile(file, append(raw, []byte("\nT1 scripted reflection.\n")...), 0644); err != nil {
				return nil, err
			}
		}
		packed, err := anypb.New(response)
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: packed}, err
	})
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), SessionRoot: session, Programs: registry, Reflector: worker, ResponderBackend: "scripted", ResponderModel: "scripted", Deriver: OutcomeDeriverFunc(func(context.Context, IncidentClosure, string, string, string) (DerivedOutcome, error) {
		return DerivedOutcome{Record: o, RequiresReflection: true}, nil
	})}
	workspace, err := native.PrepareIncident(ctx, o.IncidentId)
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.RepairPolicy = "recorded-actions"
	if c.decline {
		// A real reflection session must exercise publication rejection, rather
		// than the controller-owned no-session settlement.
		closure.Result.ResponderSessionID = "t1-scripted-session"
	}
	if _, err := native.ProcessClosure(ctx, closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger(o.IncidentId)
	if err != nil {
		t.Fatal(err)
	}
	run.ledger = ledger
	stored, err := memory.OpenReader(root).List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range stored {
		if record.IncidentID == o.IncidentId {
			run.outcome = record.Record
		}
	}
	if run.outcome == nil {
		t.Fatal("authoritative outcome not persisted")
	}
	inspectCases(root)
	calls := run.calls
	if _, err := native.ProcessClosure(ctx, closure); err != nil {
		t.Fatal(err)
	}
	if calls != run.calls {
		t.Error("closure replay dispatched reflection twice")
	}
	return run
}

func TestT1TrustProperty(t *testing.T) {
	// Precompute the original seed-186 choices and arrival permutations before
	// running independent broker worlds under the existing -parallel bound.
	deadline := time.Now().Add(4 * time.Minute)
	rng := rand.New(rand.NewSource(186))
	classes := t1Classes()
	type trialPlan struct {
		class   t1Class
		reject  bool
		arrival []int
		outcome *contracts.OutcomeRecord
	}
	plans := make([]trialPlan, 16)
	arrival := []int{}
	for trial := range plans {
		c := classes[trial%len(classes)]
		if trial >= len(classes) {
			c = classes[rng.Intn(len(classes))]
		}
		reject := rng.Intn(2) == 0
		arrival = append(arrival, trial)
		rng.Shuffle(len(arrival), func(i, j int) { arrival[i], arrival[j] = arrival[j], arrival[i] })
		plans[trial] = trialPlan{class: c, reject: reject, arrival: append([]int(nil), arrival...)}
	}
	t.Run("broker worlds", func(t *testing.T) {
		for trial := range plans {
			t.Run(fmt.Sprintf("trial=%d", trial), func(t *testing.T) {
				t.Parallel()
				if time.Now().After(deadline) {
					t.Fatal("property-test wall-clock budget exhausted")
				}
				plans[trial].outcome = t1RunBroker(t, plans[trial].class, plans[trial].reject).outcome
			})
		}
	})
	outcomes := []*contracts.OutcomeRecord{}
	for trial, plan := range plans {
		if time.Now().After(deadline) {
			t.Fatal("property-test wall-clock budget exhausted")
		}
		plan.outcome.Timestamps.CompletedAt = timestamppb.New(time.Unix(int64(200+trial), 0))
		before := ProgramTrustStats(outcomes)[t1Program]
		outcomes = append(outcomes, plan.outcome)
		orderedStats := ProgramTrustStats(outcomes)
		shuffled := make([]*contracts.OutcomeRecord, len(outcomes))
		for i, index := range plan.arrival {
			shuffled[i] = plans[index].outcome
		}
		outcomes = shuffled
		shuffledStats := ProgramTrustStats(outcomes)
		if !reflect.DeepEqual(orderedStats, shuffledStats) {
			t.Errorf("seed=186 trial=%d arrival order changed trust", trial)
		}
		after := shuffledStats[t1Program]
		if plan.class.credit == 0 && after.VerifiedSuccesses > before.VerifiedSuccesses {
			t.Errorf("seed=186 trial=%d class=%s non-covered instance increased trust (before=%+v after=%+v)", trial, plan.class.name, before, after)
		}
	}
}
