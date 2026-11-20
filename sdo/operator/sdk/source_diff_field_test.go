package sdk

import "testing"

func envDiff(accepted, observed string) SourceDiff {
	doc := func(value string) []byte {
		return []byte(`{"spec":{"template":{"spec":{"containers":[{"name":"app","env":[{"name":"A","value":"1"},{"name":"ADDR","value":"` + value + `"}]}]}}}}`)
	}
	return SourceDiff{Changed: true, Text: SourceComparisonText(doc(accepted), doc(observed))}
}

// Field accepts the same selector paths the actions take (SetField, FieldValue): a program that reads a
// field with the path it will later restore must not see a silently absent field.
func TestFieldResolvesNamedListSelectors(t *testing.T) {
	diff := envDiff("svc:8080", "svc:8082")
	path := "spec.template.spec.containers[name=app].env[name=ADDR].value"
	f, err := diff.Field(path)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Declared || !f.Live || f.Accepted != "svc:8080" || f.Observed != "svc:8082" || !f.Differs() {
		t.Fatalf("Field(%q) = %+v, want the env value on both sides, differing", path, f)
	}
	f, err = diff.Field("spec.template.spec.containers[0].env[1].value")
	if err != nil || f.Accepted != "svc:8080" {
		t.Fatalf("index selectors = %+v, %v", f, err)
	}
	f, err = diff.Field("spec.template.spec.containers[name=app].env[name=A].value")
	if err != nil || f.Differs() {
		t.Fatalf("an unchanged entry must not differ: %+v, %v", f, err)
	}
	f, err = diff.Field("spec.template.spec.containers[name=other].env[name=A].value")
	if err != nil || f.Declared || f.Live {
		t.Fatalf("an absent named entry is an absent field, got %+v, %v", f, err)
	}
	if _, err := diff.Field("spec.template.spec.containers[name=app"); err == nil {
		t.Fatal("a malformed selector must be an error, never a silently absent field")
	}
}
