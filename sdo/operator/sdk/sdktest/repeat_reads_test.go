package sdktest

import (
	"reflect"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Every read a program can make and the recorder keeps must be repeatable at
// the controller's fixed boundary; otherwise fixed and healthy recordings miss
// the observations a replay needs and programs replay Unknown.
func TestRecordPrimitiveCallsRepeatsEveryRecordedRead(t *testing.T) {
	samples := map[reflect.Type]reflect.Value{
		reflect.TypeOf(program.DatabaseTarget{}): reflect.ValueOf(program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db-0"}, Container: "db", Database: "app"}),
		reflect.TypeOf(sdk.ObjectRef{}):           reflect.ValueOf(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db-0"}),
	}
	recorder := &RecordingReadView{ReadView: program.ReadFrom(Snapshot{NamespaceName: "app"})}
	readView := reflect.TypeOf((*program.ReadView)(nil)).Elem()
	for i := range readView.NumMethod() {
		method := readView.Method(i)
		var args []reflect.Value
		for j := range method.Type.NumIn() {
			in := method.Type.In(j)
			if sample, ok := samples[in]; ok {
				args = append(args, sample)
			} else {
				args = append(args, reflect.Zero(in))
			}
		}
		func() {
			defer func() { _ = recover() }()
			reflect.ValueOf(recorder).MethodByName(method.Name).Call(args)
		}()
	}
	recorded := map[string]bool{}
	for _, call := range recorder.Calls {
		recorded[call.Method] = true
	}
	if !recorded["DatabaseUsers"] || !recorded["DiffAgainstSource"] || len(recorded) < 20 {
		t.Fatalf("recorder exercised too few reads: %v", recorded)
	}
	repeated, err := RecordPrimitiveCalls(program.ReadFrom(Snapshot{NamespaceName: "app"}), recorder.Calls)
	if err != nil {
		t.Fatalf("a recorded read cannot be repeated: %v", err)
	}
	for _, call := range repeated {
		delete(recorded, call.Method)
	}
	if len(recorded) > 0 {
		t.Fatalf("recorded reads not repeated at the fixed boundary: %v", recorded)
	}
}
