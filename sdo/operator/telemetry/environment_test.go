package telemetry

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const goodEnvironment = `{"version":1,"provided":[{"ID":"default","Adapter":"prometheus","Kinds":["metrics"],"Endpoint":{"URL":"http://prom.sdo-ctl.svc:9090"},"Default":true}],"metricsPublishUrl":"http://prom.sdo-ctl.svc:9090"}`

func TestParseEnvironmentRoundTripsAndStampsOperatorOrigin(t *testing.T) {
	env, err := ParseEnvironment([]byte(goodEnvironment))
	if err != nil {
		t.Fatal(err)
	}
	bindings := env.ProvidedBindings()
	if len(bindings) != 1 || bindings[0].ID != "default" || bindings[0].Origin != OriginOperator || !bindings[0].Default {
		t.Fatalf("bindings = %+v", bindings)
	}
	data, err := env.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	again, err := ParseEnvironment(data)
	if err != nil || again.MetricsPublishURL != env.MetricsPublishURL {
		t.Fatalf("round trip = %+v, %v", again, err)
	}
}

func TestParseEnvironmentRejectsBadDocuments(t *testing.T) {
	cases := map[string]string{
		"unknown field":        `{"version":1,"flavor":"x"}`,
		"wrong version":        `{"version":2}`,
		"no version":           `{}`,
		"bad mode":             `{"version":1,"mode":"closed"}`,
		"trailing document":    `{"version":1} {"version":1}`,
		"duplicate source":     `{"version":1,"provided":[{"ID":"a","Adapter":"p"},{"ID":"a","Adapter":"p"}]}`,
		"bad source id":        `{"version":1,"provided":[{"ID":"A","Adapter":"p"}]}`,
		"designer origin":      `{"version":1,"provided":[{"ID":"a","Adapter":"p","Origin":"app-shipped"}]}`,
		"credential no dir":    `{"version":1,"provided":[{"ID":"a","Adapter":"p","Endpoint":{"Credential":"tok"}}]}`,
		"credential bad":       `{"version":1,"credentialDir":"/c","provided":[{"ID":"a","Adapter":"p","Endpoint":{"Credential":"../x"}}]}`,
		"relative cred dir":    `{"version":1,"credentialDir":"c"}`,
		"publish not http":     `{"version":1,"metricsPublishUrl":"ftp://x"}`,
		"publish without host": `{"version":1,"metricsPublishUrl":"http://"}`,
		"unknown capability":   `{"version":1,"capabilities":["root"]}`,
	}
	for name, doc := range cases {
		if _, err := ParseEnvironment([]byte(doc)); err == nil {
			t.Errorf("%s: accepted %s", name, doc)
		}
	}
	if _, err := ParseEnvironment([]byte(strings.Repeat(" ", MaxEnvironmentBytes+1))); err == nil {
		t.Error("oversized document accepted")
	}
}

func TestFloorEnvironmentProvidesNothing(t *testing.T) {
	env, err := ParseEnvironment([]byte(`{"version":1,"mode":"floor","provided":[{"ID":"a","Adapter":"p"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := env.ProvidedBindings(); len(got) != 0 {
		t.Fatalf("floor provided %v", got)
	}
}

func TestEnvironmentReadsCredentialsPerRequest(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "tok"), 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "tok", "token")
	env := Environment{Version: 1, CredentialDir: dir}
	for _, want := range []string{"one", "two"} {
		if err := os.WriteFile(path, []byte(want+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if got, err := env.ReadCredential("tok"); err != nil || got != want {
			t.Fatalf("ReadCredential = %q, %v; want %q", got, err, want)
		}
	}
	for _, name := range []string{"missing", "../tok", ""} {
		if _, err := env.ReadCredential(name); err == nil {
			t.Errorf("credential %q read", name)
		}
	}
	if _, err := (Environment{Version: 1}).ReadCredential("tok"); err == nil {
		t.Error("credential read without a directory")
	}
}
