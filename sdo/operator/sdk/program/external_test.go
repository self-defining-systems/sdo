package program_test

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestExternalOperationVocabulary(t *testing.T) {
	c := program.Command{Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db"}, Container: "db", Args: []string{"sh", "-c", "test -f /tmp/ready"}}
	a := program.EnsureExec{Observe: c, Ensure: program.Command{Pod: c.Pod, Container: c.Container, Args: []string{"sh", "-c", "touch /tmp/ready"}}}
	if err := (program.Plan{Actions: []program.Action{a}}).Validate(); err != nil {
		t.Fatal(err)
	}
	a.Observe.Args = nil
	if (program.Plan{Actions: []program.Action{a}}).Validate() == nil {
		t.Fatal("accepted missing observation")
	}
}
func TestExternalHTTPAuthenticationUsesReferences(t *testing.T) {
	h := program.HTTPRequest{Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}, Container: "app", Method: "GET", URL: "http://api.app.svc/", Headers: map[string]string{"Authorization": "Bearer synthetic-token"}}
	if h.Validate() == nil {
		t.Fatal("literal authentication header accepted")
	}
	h.Headers = nil
	h.HeaderEnv = map[string]program.CredentialRef{"X-API-Key": {Env: "API_KEY"}}
	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestExternalHTTPRejectsCredentialSeparatorVariants(t *testing.T) {
	h := program.HTTPRequest{Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}, Container: "app", Method: "GET", URL: "http://api.app.svc/"}
	for _, name := range []string{"api-key", "API_KEY", "api.key", "access_token"} {
		q := h
		q.URL += "?" + name + "=synthetic-token"
		if q.Validate() == nil {
			t.Errorf("accepted credential query %s", name)
		}
	}
	for _, name := range []string{"X-API_KEY", "x.api.key", "X_Authorization"} {
		q := h
		q.Headers = map[string]string{name: "synthetic-token"}
		if q.Validate() == nil {
			t.Errorf("accepted credential header %s", name)
		}
	}
}

func TestExternalHTTPRejectsMalformedCredentialQuery(t *testing.T) {
	h := program.HTTPRequest{Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}, Container: "app", Method: "GET", URL: "http://api.app.svc/?api-key=synthetic-token;"}
	if h.Validate() == nil {
		t.Fatal("malformed credential query bypassed name validation")
	}
}
