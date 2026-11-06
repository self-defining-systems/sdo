package memory

import "testing"

func TestAcceptedHealthObjective(t *testing.T) {
	root := t.TempDir()
	if _, err := AcceptedHealthObjective(root); err == nil {
		t.Fatal("missing goal accepted")
	}
	if err := WriteGoal(root, "toy", "  serve every request\n", nil); err != nil {
		t.Fatal(err)
	}
	if got, err := AcceptedHealthObjective(root); err != nil || got != "serve every request" {
		t.Fatalf("objective = %q, %v", got, err)
	}
	if err := WriteGoal(root, "toy", " ", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := AcceptedHealthObjective(root); err == nil {
		t.Fatal("empty objective accepted")
	}
}
