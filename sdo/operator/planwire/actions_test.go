package planwire

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

// Every action kind of the wire vocabulary says whether its effect lies in
// Kubernetes object fields. A new kind must be classified here before an
// object read could ever credit it.
func TestEveryActionKindClassifiesItsEffect(t *testing.T) {
	oneof := (&contracts.Action{}).ProtoReflect().Descriptor().Oneofs().ByName("kind")
	fields := oneof.Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		action := &contracts.Action{}
		message := action.ProtoReflect()
		message.Set(field, protoreflect.ValueOfMessage(message.NewField(field).Message()))
		if _, known := classifyEffect(action); !known {
			t.Errorf("action kind %s does not classify whether its effect lies in Kubernetes objects", field.Name())
		}
	}
	if !NonObjectEffect(&contracts.Action{}) {
		t.Fatal("an unclassified action must count as a non-object effect")
	}
}

func TestNonObjectEffectKinds(t *testing.T) {
	command := program.Command{Args: []string{"true"}}
	for _, tc := range []struct {
		action    program.Action
		nonObject bool
	}{
		{program.EnsureExec{Observe: command, Ensure: command}, true},
		{&program.EnsureExec{Observe: command, Ensure: command}, true},
		{program.RunScript{Script: "true"}, true},
		{program.Exec{Container: "app"}, true},
		{program.RestartRollout{}, false},
		{program.DeletePod{}, false},
	} {
		if got := HasNonObjectEffect([]program.Action{tc.action}); got != tc.nonObject {
			t.Errorf("%T: non-object effect = %v, want %v", tc.action, got, tc.nonObject)
		}
	}
}
