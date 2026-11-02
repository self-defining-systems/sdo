//go:build integration

package runtime

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// A source regression after verification is information: the closure the
// health detectors verified still commits, with the difference noted.
func TestClosureHandoffNotesSourceRegressionAfterVerification(t *testing.T) {
	f := newClosureDriftFixture(t, true, true, "app")
	f.restoreSource(t)
	f.settle(t, 1)
	if len(f.closed) != 1 || closureOutcome(t, f.closed[0]) != "success" {
		t.Fatal("fixture needs verified image and source repair")
	}
	service, err := f.client.CoreV1().Services("app").Get(context.Background(), "reservation", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service.Spec.Ports[0].TargetPort.IntVal = 9999
	if _, err := f.client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	broker := &recordingClosureBroker{}
	if err := f.c.SetIncidentBroker(broker); err != nil {
		t.Fatal(err)
	}
	effect, ok := f.c.PendingClosureEffect()
	if !ok {
		t.Fatal("missing pending closure")
	}
	if err := f.c.ExecuteClosureEffect(context.Background(), effect); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, f.c.closureResults)
	f.c.processBrokerCompletions()
	if len(broker.closures) != 1 || f.c.closureState != contracts.ClosureState_CLOSURE_STATE_COMMITTED {
		t.Fatalf("source regression retired a health-verified closure: state=%v failure=%+v closures=%d", f.c.closureState, f.c.closureFailure, len(broker.closures))
	}
	if notes := strings.Join(broker.closures[0].SourceCheckNotes, ";"); !strings.Contains(notes, "unexplained drift remains on Service/reservation") {
		t.Fatalf("regression not recorded on the outcome: %q", notes)
	}
}
