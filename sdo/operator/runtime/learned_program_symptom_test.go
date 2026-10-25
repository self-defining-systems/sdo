package runtime

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

func TestLearnedOriginCreditIncludesObservedUnwrittenSymptoms(t *testing.T) {
	root, commit := learningTrustRepository(t)
	at := time.Unix(100, 0)
	target := t1Ref("api")
	symptom := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}
	origin := &contracts.OutcomeRecord{IncidentId: "learning", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{target, symptom}}}, RepairActions: []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{target}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}}, Findings: []*contracts.Finding{{DetectorId: "health", Status: "active", PrimaryResource: target, Fingerprint: "target"}, {DetectorId: "health", Status: "active", PrimaryResource: symptom, Fingerprint: "symptom"}}, DetectorHistory: []*contracts.DetectorEvaluation{{DetectorId: "health", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}}}
	for name, mutate := range map[string]func(*contracts.OutcomeRecord){
		"recovered symptom":       func(*contracts.OutcomeRecord) {},
		"missing symptom opening": func(o *contracts.OutcomeRecord) { o.Findings = o.Findings[:1] },
		"stale clearance":         func(o *contracts.OutcomeRecord) { o.DetectorHistory[0].EvaluatedAt = timestamppb.New(at) },
		"symptom was also written": func(o *contracts.OutcomeRecord) {
			o.RepairActions[0].Resources = append(o.RepairActions[0].Resources, symptom)
		},
	} {
		t.Run(name, func(t *testing.T) {
			record := proto.Clone(origin).(*contracts.OutcomeRecord)
			mutate(record)
			dispatcher := KubernetesJobDispatcher{Outcomes: priorOutcomeReader{outcomes: []memory.Outcome{{IncidentID: "learning", Record: record}}}}
			stats, err := dispatcher.programTrust(context.Background(), IncidentRequest{RepositoryWorktree: root, SourceCommit: commit})
			want := 0
			if name == "recovered symptom" {
				want = 1
			}
			if err != nil || stats["service-targetport-mismatch"].VerifiedSuccesses != want {
				t.Fatalf("origin credit=%+v err=%v want%d", stats, err, want)
			}
		})
	}
}
