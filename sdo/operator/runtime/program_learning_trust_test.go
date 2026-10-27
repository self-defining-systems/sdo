package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/program"
)

func learningTrustRepository(t *testing.T) (string, string) {
	t.Helper()
	root := nativeRepository(t)
	files := map[string]string{
		".sdo/playbooks/service-targetport-mismatch/diagnose.go": "package targetport\n",
		".sdo/playbooks/service-targetport-mismatch/mitigate.go": "package targetport\n",
		".sdo/corpus/learning/open.json":                         string(capturefixture.FromView(t, `{"namespace":"app","services":[{"metadata":{"namespace":"app","name":"api"},"spec":{"ports":[{"port":80,"targetPort":9999}]}}]}`)),
		".sdo/corpus/learning/fixed.json":                        string(capturefixture.FromView(t, `{"namespace":"app","services":[{"metadata":{"namespace":"app","name":"api"},"spec":{"ports":[{"port":80,"targetPort":8080}]}}]}`)),
	}
	files[".sdo/corpus/learning/healthy.json"] = files[".sdo/corpus/learning/fixed.json"]
	files[".sdo/corpus/learning/recordings.json"] = "[]"
	labels := map[string]any{"bindings": program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}, "root_cause_program_ids": []string{"service-targetport-mismatch"}, "provenance": map[string]string{
		"incident_id": "learning", "open_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte(files[".sdo/corpus/learning/open.json"]))),
		"fixed_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte(files[".sdo/corpus/learning/fixed.json"]))),
	}}
	data, _ := json.Marshal(labels)
	files[".sdo/corpus/learning/labels.json"] = string(data)
	for name, content := range files {
		full := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "learn\n\nSDO-Incident: learning\nSDO-Actor: responder\nSDO-Phase: reflection\nSDO-Validation: passed\nSDO-Learning: executable-v1")
	return root, nativeGit(t, root, "rev-parse", "HEAD")
}

func TestAcceptedCorpusDoesNotInventTrustForAnUnmatchedOrigin(t *testing.T) {
	root, commit := learningTrustRepository(t)
	record := &contracts.OutcomeRecord{IncidentId: "learning", Classification: "success"}
	dispatcher := KubernetesJobDispatcher{Outcomes: priorOutcomeReader{outcomes: []memory.Outcome{{IncidentID: "learning", Record: record}}}}
	trust, err := dispatcher.programTrust(context.Background(), IncidentRequest{RepositoryWorktree: root, SourceCommit: commit})
	if err != nil || trust["service-targetport-mismatch"].VerifiedSuccesses != 0 || trust["service-targetport-mismatch"].Failures != 0 {
		t.Fatalf("corpus invented origin credit: %+v %v", trust, err)
	}
}

func TestNewProgramGetsOneVerifiedOriginCreditWithoutPriorProgramRun(t *testing.T) {
	root, commit := learningTrustRepository(t)
	origin := &contracts.OutcomeRecord{IncidentId: "learning", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{t1Ref("api")}}}}
	dispatcher := KubernetesJobDispatcher{Outcomes: priorOutcomeReader{outcomes: []memory.Outcome{{IncidentID: "learning", Record: origin}}}}
	request := IncidentRequest{RepositoryWorktree: root, SourceCommit: commit}
	trust, err := dispatcher.programTrust(context.Background(), request)
	if err != nil || trust["service-targetport-mismatch"].VerifiedSuccesses != 1 || trust["service-targetport-mismatch"].Failures != 0 {
		t.Fatalf("new replayed program has no first-encounter credit: %+v %v", trust, err)
	}
	failure := &contracts.OutcomeRecord{IncidentId: "repeat", SourceCommit: trust["service-targetport-mismatch"].LastChange, ProgramFailureSignals: []string{"application_verification_failure:service-targetport-mismatch"}}
	dispatcher.Outcomes = priorOutcomeReader{outcomes: []memory.Outcome{{IncidentID: "learning", Record: origin}, {IncidentID: "repeat", Record: failure}}}
	trust, err = dispatcher.programTrust(context.Background(), request)
	if err != nil || trust["service-targetport-mismatch"].VerifiedSuccesses != 0 || trust["service-targetport-mismatch"].Failures != 1 {
		t.Fatalf("origin replay erased a real failure: %+v %v", trust, err)
	}
}

func TestMissingOptionalLearningCreditDoesNotBlockResponder(t *testing.T) {
	dispatcher := KubernetesJobDispatcher{Outcomes: priorOutcomeReader{}}
	trust, err := dispatcher.programTrust(context.Background(), IncidentRequest{RepositoryWorktree: filepath.Join(t.TempDir(), "deleted-proposal"), SourceCommit: strings.Repeat("a", 40)})
	if err != nil || len(trust) != 0 {
		t.Fatalf("optional learning evidence blocked dispatch: %v %v", trust, err)
	}
}

func TestLearningEvidenceGitErrorIncludesStderr(t *testing.T) {
	_, err := memory.ReadProgramLearningEvidence(context.Background(), filepath.Join(t.TempDir(), "deleted-proposal"), strings.Repeat("a", 40))
	if err == nil || !strings.Contains(err.Error(), "cannot change to") {
		t.Fatalf("lost git stderr: %v", err)
	}
}

func TestLearningTrustUsesAcceptedRepositoryAfterProposalRetirement(t *testing.T) {
	root, commit := learningTrustRepository(t)
	origin := &contracts.OutcomeRecord{IncidentId: "learning", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{t1Ref("api")}}}}
	dispatcher := KubernetesJobDispatcher{AcceptedRepository: root, Outcomes: priorOutcomeReader{outcomes: []memory.Outcome{{IncidentID: "learning", Record: origin}}}}
	trust, err := dispatcher.programTrust(context.Background(), IncidentRequest{RepositoryWorktree: filepath.Join(t.TempDir(), "deleted-proposal"), SourceCommit: commit})
	if err != nil || trust["service-targetport-mismatch"].VerifiedSuccesses != 1 || trust["service-targetport-mismatch"].Failures != 0 {
		t.Fatalf("accepted credit depended on deleted proposal: %v %v", trust, err)
	}
}

func TestMissingLearningCommitOrCorpusIsOptional(t *testing.T) {
	root := nativeRepository(t)
	dispatcher := KubernetesJobDispatcher{AcceptedRepository: root, Outcomes: priorOutcomeReader{}}
	for _, commit := range []string{strings.Repeat("a", 40), nativeGit(t, root, "rev-parse", "HEAD")} {
		trust, err := dispatcher.programTrust(context.Background(), IncidentRequest{SourceCommit: commit})
		if err != nil || len(trust) != 0 {
			t.Fatalf("absent optional credit blocked dispatch: %v %v", trust, err)
		}
	}
}

func TestPreparedResponderRequestSurvivesMissingOptionalLearningCredit(t *testing.T) {
	root := nativeRepository(t)
	request := goldenRequest(t)
	request.SourceCommit = strings.Repeat("a", 40) // absent Git object
	request.RepositoryWorktree = root
	request.RepositoryBaseCommit = nativeGit(t, root, "rev-parse", "HEAD")
	dispatcher := KubernetesJobDispatcher{AcceptedRepository: root, Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), Outcomes: priorOutcomeReader{}, LoopFactory: func(IncidentRequest) *ResponderLoop { return &ResponderLoop{} }}
	prepared, err := dispatcher.PrepareDispatch(context.Background(), request)
	if err != nil || prepared.PreparedAgentTask == "" {
		t.Fatalf("responder request blocked by optional credit: %v", err)
	}
}
