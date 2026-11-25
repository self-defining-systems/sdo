package runtime

import (
	"strings"
	"testing"
)

const longPolicy = "NetworkPolicy/synthetic-callback-restriction-for-storage-nodes"

// Names are identity, not values: a long one is shown, whatever its length.
func TestChangesShowLongObjectNamesAndHideSecretValues(t *testing.T) {
	token := strings.Repeat("aB3/", 12)
	row := IncidentChange{Source: "kubernetes", Component: "storage-component-with-a-very-long-map-name",
		ReaderOrObject: longPolicy, Writer: "kubectl-client-side-apply-from-a-pipeline", After: token}
	csv := string(renderChanges([]IncidentChange{row}, detectOnset))
	for _, want := range []string{longPolicy, row.Component, row.Writer, "[redacted]"} {
		if !strings.Contains(csv, want) {
			t.Fatalf("missing %q:\n%s", want, csv)
		}
	}
	evidence, _ := rowEvidence(row, detectOnset)
	for _, text := range []string{csv, evidence.Text} {
		if strings.Contains(text, token) {
			t.Fatalf("secret value shown: %s", text)
		}
	}
	secret := IncidentChange{Source: "kubernetes", ReaderOrObject: "Secret/synthetic-production-database-credentials", Key: "data.password", After: "hunter2"}
	csv = string(renderChanges([]IncidentChange{secret}, detectOnset))
	if !strings.Contains(csv, secret.ReaderOrObject) || strings.Contains(csv, "hunter2") {
		t.Fatalf("a Secret shows its name and never its value:\n%s", csv)
	}
	// A recorded write without a resource is free text.
	write := IncidentChange{Source: "write", ReaderOrObject: "https://host/" + token}
	evidence, _ = rowEvidence(write, detectOnset)
	write.At = detectOnset
	elsewhere := candidateView{in: candidateInput{Onset: detectOnset}, elsewhere: []IncidentChange{write}}.elsewhereLines(true)
	for _, text := range []string{string(renderChanges([]IncidentChange{write}, detectOnset)), evidence.Text, strings.Join(elsewhere, "\n")} {
		if strings.Contains(text, token[:20]) || !strings.Contains(text, "[redacted]") {
			t.Fatalf("free text leaked:\n%s", text)
		}
	}
}

func TestFreeTextKeepsKindNameAndMasksTokens(t *testing.T) {
	for _, secret := range []string{
		strings.Repeat("f0", 20), "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", strings.Repeat("Ab1/", 10),
		"glpat-abcdefghijklmnopqrstuvwxyz012345", "Kind/" + strings.Repeat("aB", 20),
		"Token/550e8400-e29b-41d4-a716-446655440000", "Session/" + strings.Repeat("0f", 16) + "-a1", "Vault/hvs-caesiabcdefghijklmnop1234567890",
	} {
		if got := redactDetectText("value "+secret, 200); strings.Contains(got, secret) {
			t.Errorf("secret shown: %s", got)
		}
	}
	if got := redactDetectText("token="+longPolicy, 200); strings.Contains(got, longPolicy) {
		t.Errorf("a credential field was shown: %s", got)
	}
	if got := redactDetectText(longPolicy+" denies ingress", 200); !strings.Contains(got, longPolicy) {
		t.Errorf("object name hidden: %s", got)
	}
}
