package runtime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// setField gives one ControllerConfig field a non-zero value of its kind.
func setField(config *contractsv1alpha1.ControllerConfig, field protoreflect.FieldDescriptor) {
	message := config.ProtoReflect()
	switch {
	case field.IsList():
		list := message.Mutable(field).List()
		list.Append(protoreflect.ValueOfString("x=y"))
	case field.Message() != nil:
		nested := message.Mutable(field).Message()
		// Construct the descriptor's actual message type, including future
		// structured config fields, instead of inserting a Duration into it.
		for i := 0; i < nested.Descriptor().Fields().Len(); i++ {
			child := nested.Descriptor().Fields().Get(i)
			if child.Kind() == protoreflect.StringKind && !child.IsList() {
				nested.Set(child, protoreflect.ValueOfString("value"))
				break
			}
			if child.Kind() == protoreflect.Int64Kind {
				nested.Set(child, protoreflect.ValueOfInt64(7))
				break
			}
		}
	case field.Kind() == protoreflect.StringKind:
		message.Set(field, protoreflect.ValueOfString("value"))
	case field.Kind() == protoreflect.BoolKind:
		message.Set(field, protoreflect.ValueOfBool(true))
	case field.Kind() == protoreflect.Int32Kind:
		message.Set(field, protoreflect.ValueOfInt32(3))
	default:
		panic("unhandled ControllerConfig field kind " + field.Kind().String() + " for " + string(field.Name()))
	}
}

// operatorSource returns the text of every non-test, non-generated Go file in the
// operator module that could read a ControllerConfig field.
func operatorSource(t *testing.T) string {
	t.Helper()
	var source strings.Builder
	err := filepath.WalkDir("..", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == "gen" || entry.Name() == "testdata") {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		source.Write(raw)
		return nil
	})
	if err != nil {
		t.Fatalf("walk operator source: %v", err)
	}
	return source.String()
}

func goName(name protoreflect.Name) string {
	var out strings.Builder
	upper := true
	for _, r := range string(name) {
		switch {
		case r == '_':
			upper = true
		case upper:
			out.WriteString(strings.ToUpper(string(r)))
			upper = false
		default:
			out.WriteRune(r)
		}
	}
	return out.String()
}

// TestEveryControllerConfigFieldHasARoute fails when a ControllerConfig field is
// added without a consumer: each live field must become a controller flag in
// controllerConfigToArgs, or be read directly from the config by operator code
// (native launch, installer, memory edge). A field with neither is silently
// dropped on the way to the controller, which is how a config reaches a Job
// without the setting an experiment asked for.
func TestEveryControllerConfigFieldHasARoute(t *testing.T) {
	source := operatorSource(t)
	baseline := controllerConfigToArgs(&contractsv1alpha1.ControllerConfig{})
	fields := (&contractsv1alpha1.ControllerConfig{}).ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.Options().(interface{ GetDeprecated() bool }).GetDeprecated() {
			continue
		}
		t.Run(string(field.Name()), func(t *testing.T) {
			config := &contractsv1alpha1.ControllerConfig{}
			setField(config, field)
			if len(controllerConfigToArgs(config)) > len(baseline) {
				return
			}
			accessor := goName(field.Name())
			if strings.Contains(source, "Get"+accessor+"()") || strings.Contains(source, "."+accessor+" ") {
				return
			}
			t.Fatalf("ControllerConfig.%s reaches neither a controller flag nor any operator code; "+
				"emit it in controllerConfigToArgs or read it where it is used, or deprecate the field", field.Name())
		})
	}
}
