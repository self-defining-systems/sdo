package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func applyDatabaseUser(t *testing.T, client *databaseFake) (PlanApplicationResult, error) {
	t.Helper()
	a := databaseAction()
	ref := a.Target.Pod
	object := planObject(ref, map[string]any{"containers": []any{map[string]any{"name": "mongo"}}})
	object.SetUID("one")
	object.SetResourceVersion("1")
	client.planFakeClient = &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}
	return PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{a}}, false)
}

// A verified EnsureDatabaseUser names what changed, not only "applied".
func TestEnsureDatabaseUserReportsVerifiedOutcome(t *testing.T) {
	want := []program.DatabaseRole{{Database: "app", Name: "readWrite"}}
	cases := map[string]struct {
		users   []program.DatabaseUser
		outcome program.DatabaseUserOutcome
	}{
		"created":         {nil, program.DatabaseUserCreated},
		"updated":         {[]program.DatabaseUser{{Name: "app", Roles: []program.DatabaseRole{{Database: "admin", Name: "read"}}}}, program.DatabaseUserUpdated},
		"already_correct": {[]program.DatabaseUser{{Name: "app", Roles: want}}, program.DatabaseUserAlreadyCorrect},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			result, err := applyDatabaseUser(t, &databaseFake{users: tc.users})
			if err != nil || len(result.ActionReports) != 1 {
				t.Fatalf("%+v %v", result, err)
			}
			report := result.ActionReports[0]
			effect := report.DatabaseUser
			if report.Status != "applied" || effect == nil || effect.User != "app" || effect.Outcome != tc.outcome || !effect.Verified || len(effect.Roles) != 1 || effect.Roles[0] != want[0] || report.FailureReason != "" {
				t.Fatalf("result is not definite: %+v effect=%+v", report, effect)
			}
			raw, _ := json.Marshal(report)
			if !strings.Contains(string(raw), `"outcome":"`+string(tc.outcome)+`"`) {
				t.Fatalf("responder-visible report lacks the outcome: %s", raw)
			}
		})
	}
}

// A write the read-back does not show is a typed unknown, never an outcome.
func TestEnsureDatabaseUserUnverifiedWriteIsTypedUnknown(t *testing.T) {
	result, err := applyDatabaseUser(t, &databaseFake{ignore: true})
	report := result.ActionReports[0]
	if err == nil || report.Status != "unknown" || report.DatabaseUser != nil || report.FailureReason != "database_user_unverified" {
		t.Fatalf("%+v %v", report, err)
	}
}

// Failures proven to precede the write are definite: nothing applied, typed reason.
func TestEnsureDatabaseUserPreWriteFailureIsDefinite(t *testing.T) {
	credential := program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "mongo-script", Key: "init.sh", Assignment: "ROOT_PWD"}}
	cases := map[string]struct {
		err    error
		reason string
	}{
		"authentication": {&DatabaseOperationError{Step: DatabaseAuthentication, Reason: databaseRejected}, "database_authentication_rejected"},
		"credential":     {&CredentialReferenceError{Reference: credential, Reason: "key missing"}, "database_credential_reference_unavailable"},
		"binary":         {&DatabaseOperationError{Step: DatabaseClientBinary, Reason: databaseNotFound}, "database_client_binary_not_found"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			client := &databaseFake{writeErr: tc.err}
			result, err := applyDatabaseUser(t, client)
			report := result.ActionReports[0]
			if err == nil || result.Applied || len(result.Patches) != 0 || len(result.ActionsApplied) != 0 || report.Status != "refused" || report.FailureReason != tc.reason || report.DatabaseUser != nil {
				t.Fatalf("pre-write failure not definite: %+v %v", result, err)
			}
		})
	}
}

// Transport loss may hide a write: it stays unknown with its own reason, and no
// upstream text (which can carry credential bytes) reaches the report.
func TestEnsureDatabaseUserWriteUncertaintyKeepsIrreversibleUnknown(t *testing.T) {
	for name, writeErr := range map[string]error{
		"transport": &DatabaseOperationError{Step: DatabaseExecTransport, Reason: databaseClientFailed},
		"upstream":  fmt.Errorf("canary-password"),
		"identity":  &DatabaseOperationError{Step: DatabaseContainerLookup, Reason: databaseIdentityChanged},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := applyDatabaseUser(t, &databaseFake{writeErr: writeErr})
			report := result.ActionReports[0]
			raw, _ := json.Marshal(result)
			if err == nil || !result.Applied || len(result.Patches) != 1 || !result.Patches[0].Irreversible || report.Status != "unknown" || report.FailureReason == "" || strings.Contains(string(raw)+err.Error(), "canary-password") {
				t.Fatalf("%+v %v", result, err)
			}
		})
	}
}

// The responder reads these exact keys from the mitigate report (see
// sdo/agents/responder/multi_output.py): the real executor result must carry them.
func TestEnsureDatabaseUserOutcomeReachesMenuReportJSON(t *testing.T) {
	for name, tc := range map[string]struct {
		client *databaseFake
		path   string
		want   string
	}{
		"verified": {&databaseFake{}, "action_results.0.database_user.outcome", "created"},
		"failed":   {&databaseFake{writeErr: &DatabaseOperationError{Step: DatabaseAuthentication, Reason: databaseRejected}}, "action_results.0.failure_reason", "database_authentication_rejected"},
	} {
		t.Run(name, func(t *testing.T) {
			result, _ := applyDatabaseUser(t, tc.client)
			raw, err := json.Marshal(MenuRoundExecution{OptionID: "EnsureDatabaseUser", ActionResults: result.ActionReports})
			if err != nil {
				t.Fatal(err)
			}
			var decoded map[string]any
			if err = json.Unmarshal(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			var node any = decoded
			for _, key := range strings.Split(tc.path, ".") {
				switch typed := node.(type) {
				case map[string]any:
					node = typed[key]
				case []any:
					node = typed[0]
				}
			}
			if node != tc.want {
				t.Fatalf("%s = %v in %s", tc.path, node, raw)
			}
		})
	}
}
