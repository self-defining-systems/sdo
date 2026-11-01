package testsplit

import (
	"strings"
	"testing"
)

func TestSplitPreservesEmbeddedSourceAndPureHelper(t *testing.T) {
	source := []byte("package toy\nimport (\"os/exec\";\"testing\")\nfunc helper() string {return `package main\nfunc main() {}\n`}\n// Real process.\nfunc TestProcess(t *testing.T){_ = exec.Command(\"toy\"); _ = helper()}\nfunc TestPure(t *testing.T){_ = helper()}\n")
	unit, integration, err := Split(source, []string{"TestProcess"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(unit), "os/exec") || strings.Contains(string(unit), "TestProcess") || !strings.Contains(string(unit), "func main() {}") {
		t.Fatalf("unit %s", unit)
	}
	if !strings.HasPrefix(string(integration), "//go:build integration\n") || !strings.Contains(string(integration), "// Real process.") || !strings.Contains(string(integration), "exec.Command") {
		t.Fatalf("integration %s", integration)
	}
}
func TestSplitRejectsUnknownAndPartialGroups(t *testing.T) {
	for _, selected := range [][]string{{"missing"}, {"first"}, {"first", "first"}, nil} {
		if _, _, err := Split([]byte("package toy\nvar (first=1;second=2)\n"), selected); err == nil {
			t.Fatalf("selection %v accepted", selected)
		}
	}
	if _, _, err := Split([]byte("//go:build integration\n\npackage toy\nfunc helper(){}\n"), []string{"helper"}); err == nil {
		t.Fatal("existing build constraint accepted")
	}
}
func TestSplitWholeFileAndReceiverNames(t *testing.T) {
	source := []byte("package toy\ntype worker struct{}\nfunc (*worker) Run(){}\n")
	names, err := List(source)
	if err != nil || strings.Join(names, ",") != "worker,worker.Run" {
		t.Fatalf("names %v, error %v", names, err)
	}
	unit, integration, err := Split(source, names)
	if err != nil || unit != nil || !strings.Contains(string(integration), "type worker struct") {
		t.Fatalf("unit %s integration %s error %v", unit, integration, err)
	}
}

func TestProcessClosureKeepsPureSibling(t *testing.T) {
	report, err := ProcessClosure(map[string][]byte{
		"helpers_test.go": []byte("package toy\nimport \"os/exec\"\nfunc realGit(){_ = exec.Command(\"git\")}\nfunc pure(){}\n"),
		"cases_test.go":   []byte("package toy\nfunc TestGit(){realGit()}\nfunc TestPure(){pure()}\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(report["helpers_test.go"], ",") != "realGit" || strings.Join(report["cases_test.go"], ",") != "TestGit" {
		t.Fatalf("report %v", report)
	}
}

func TestIntegrationBlankIdentifierDoesNotContaminatePureTests(t *testing.T) {
	report, err := ProcessClosure(map[string][]byte{
		"integration_test.go": []byte("//go:build integration\n\npackage toy\nvar _=1\n"),
		"unit_test.go":        []byte("package toy\nfunc TestPure(){_ = 1}\n"),
	})
	if err != nil || len(report) != 0 {
		t.Fatalf("report %v error %v", report, err)
	}
}
