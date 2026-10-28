package layout

import "testing"

func TestLogicalizeTextNamesMemoryByLogicalReference(t *testing.T) {
	for in, want := range map[string]string{
		".sdo/playbooks/deployment-source-drift and .sdo/playbooks/source-absent-networkpolicy contain programs.": "playbook:deployment-source-drift and playbook:source-absent-networkpolicy contain programs.",
		"read .sdo/playbooks/fix/README.md.":                "read playbook:fix.",
		"ran .sdo/playbooks/fix/scripts/apply.sh":           "ran script:fix/apply.sh",
		"edit .sdo/playbooks/fix/mitigate.go":               "edit playbook:fix",
		".sdo/diagnostics/detectors/incidents/web-5xx/x.go": "detector:web-5xx",
		"helper .sdo/lib/kube/client.go":                    "helper memory:lib/kube/client.go",
		`windows .sdo\playbooks\fix\README.md`:              "windows playbook:fix",
		"no memory path here":                               "no memory path here",
	} {
		if got := LogicalizeText(in); got != want {
			t.Errorf("LogicalizeText(%q) = %q, want %q", in, got, want)
		}
	}
}

// Any agent prose stored in an outcome is shown to later workers, so the
// rewrite must be total: no input keeps a physical path, and it is stable.
func FuzzLogicalizeText(f *testing.F) {
	for _, seed := range []string{".sdo/", `.sdo\`, ".sdo/.sdo/x", ".sdo//.sdo\\.sdo/y", "a.sdo/playbooks/x/scripts/s.sh/.sdo/", ".sdo/playbooks/../../etc", "plain"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in string) {
		out := LogicalizeText(in)
		if ContainsPhysicalPath(out) {
			t.Fatalf("LogicalizeText(%q) = %q keeps a physical path", in, out)
		}
		if again := LogicalizeText(out); again != out {
			t.Fatalf("LogicalizeText is not stable: %q -> %q -> %q", in, out, again)
		}
		if !ContainsPhysicalPath(in) && out != in {
			t.Fatalf("LogicalizeText changed text without a memory path: %q -> %q", in, out)
		}
	})
}
