package runtime

import (
	"context"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestControllerLifecyclePhasesAreClosedEnums(t *testing.T) {
	fields := (&contracts.ControllerState{}).ProtoReflect().Descriptor().Fields()
	for _, name := range []protoreflect.Name{"dispatch_state", "closure_state"} {
		field := fields.ByName(name)
		if field.Kind() != protoreflect.EnumKind {
			t.Errorf("%s is %s; lifecycle phases must be generated enums", name, field.Kind())
		}
	}
}

func TestControllerStateRejectsUnknownLifecyclePhases(t *testing.T) {
	for _, field := range []string{"dispatch_state", "closure_state"} {
		for _, value := range []string{`"typo"`, `9999`} {
			t.Run(field+value, func(t *testing.T) {
				client := fake.NewSimpleClientset(&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: "state", Namespace: "demo", Annotations: map[string]string{stateRevisionAnnotation: "1"}},
					Data:       map[string]string{stateDataKey: `{"version":"sdo.controller/v1","` + field + `":` + value + `}`},
				})
				if _, _, err := lifecycleio.NewConfigMapStateStore(client, "demo", "state").Load(context.Background()); err == nil {
					t.Fatal("unknown lifecycle phase accepted")
				}
			})
		}
	}
}

// exportState keeps tests explicit about the fallible SDK-to-proto boundary.
func exportState(t testing.TB, controller *Controller) *contracts.ControllerState {
	t.Helper()
	state, err := controller.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func mustConvert[T any, M any](t *testing.T, convert func(T) (M, error), value T) M {
	t.Helper()
	result, err := convert(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

const (
	stateDataKey            = lifecycleio.StateDataKey
	stateRevisionAnnotation = lifecycleio.StateRevisionAnnotation
)

func encodeControllerState(state *contracts.ControllerState) ([]byte, error) {
	return wire.Canonical(state)
}
func decodeControllerState(payload []byte) (*contracts.ControllerState, error) {
	state := &contracts.ControllerState{}
	if err := wire.UnmarshalDurable(payload, state); err != nil {
		return nil, err
	}
	return state, nil
}
