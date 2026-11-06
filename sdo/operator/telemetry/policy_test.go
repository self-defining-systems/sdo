package telemetry_test

import (
	"strings"
	"testing"

	"sdo.dev/operator/telemetry"
)

func testPolicy() telemetry.Policy {
	return telemetry.Policy{
		Mode:                 telemetry.ModeOpen,
		ControlNamespace:     "sdo-ctl",
		ApplicationNamespace: "shop",
		ExtraNamespaces:      []string{"monitoring"},
		ExternalHosts:        []string{"logs.example.com"},
		Credentials:          []string{"loki-token"},
	}
}

func TestPolicyEndpoint(t *testing.T) {
	cases := []struct {
		name, endpoint, want string // want is a substring of the error; empty means accepted
	}{
		{"control namespace service", "http://sdo-prometheus.sdo-ctl.svc:9090", ""},
		{"cluster domain", "https://prom.shop.svc.cluster.local:9090/prefix", ""},
		{"application namespace", "http://loki.shop.svc", ""},
		{"extra namespace", "http://prom.monitoring.svc:80", ""},
		{"external allowlisted", "https://logs.example.com/loki", ""},
		{"other namespace", "http://prom.kube-system.svc:9090", "namespace"},
		{"external not allowlisted", "https://evil.example.com", "allowlist"},
		{"suffix smuggled after cluster domain", "http://prom.sdo-ctl.svc.cluster.local.evil.com", "allowlist"},
		{"ipv4 literal", "http://10.0.0.5:9090", "IP"},
		{"ipv6 literal", "http://[::1]:9090", "IP"},
		{"decimal ip", "http://2130706433", "IP"},
		{"localhost", "http://localhost:9090", "localhost"},
		{"metadata address", "http://169.254.169.254/latest", "IP"},
		{"metadata name", "http://metadata.google.internal/computeMetadata", "metadata"},
		{"userinfo", "http://user:pw@prom.sdo-ctl.svc", "userinfo"},
		{"query", "http://prom.sdo-ctl.svc/?x=1", "query"},
		{"fragment", "http://prom.sdo-ctl.svc/#x", "fragment"},
		{"scheme", "ftp://prom.sdo-ctl.svc", "scheme"},
		{"file scheme", "file:///etc/passwd", "scheme"},
		{"no host", "http://", "host"},
		{"port zero", "http://prom.sdo-ctl.svc:0", "port"},
		{"port out of range", "http://prom.sdo-ctl.svc:70000", "port"},
		{"trailing dot", "http://prom.sdo-ctl.svc.", "host"},
		{"uppercase host", "http://PROM.SDO-CTL.SVC", ""},
		{"empty", "", "empty"},
		{"not a url", "::::", "endpoint"},
		{"backslash", "http://prom.sdo-ctl.svc\\@evil.com", "endpoint"},
	}
	p := testPolicy()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := p.CheckEndpoint(c.endpoint)
			if c.want == "" {
				if err != nil {
					t.Fatalf("CheckEndpoint(%q) = %v, want accepted", c.endpoint, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("CheckEndpoint(%q) = %v, want error containing %q", c.endpoint, err, c.want)
			}
		})
	}
}

func TestPolicyEndpointAllowlistNeverAdmitsAnIPLiteral(t *testing.T) {
	p := testPolicy()
	p.ExternalHosts = append(p.ExternalHosts, "10.0.0.5", "localhost")
	for _, e := range []string{"http://10.0.0.5", "http://localhost"} {
		if err := p.CheckEndpoint(e); err == nil {
			t.Fatalf("%s must stay rejected even when allowlisted", e)
		}
	}
}

func TestPolicyCredential(t *testing.T) {
	p := testPolicy()
	if err := p.CheckCredential(""); err != nil {
		t.Fatalf("empty credential: %v", err)
	}
	if err := p.CheckCredential("loki-token"); err != nil {
		t.Fatalf("granted credential: %v", err)
	}
	if err := p.CheckCredential("other"); err == nil || !strings.Contains(err.Error(), "loki-token") {
		t.Fatalf("ungranted credential = %v, want an error naming the granted set", err)
	}
}

func TestPolicyValidate(t *testing.T) {
	if err := testPolicy().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*telemetry.Policy){
		"mode":       func(p *telemetry.Policy) { p.Mode = "closed" },
		"namespace":  func(p *telemetry.Policy) { p.ExtraNamespaces = []string{"Bad_NS"} },
		"credential": func(p *telemetry.Policy) { p.Credentials = []string{"../x"} },
		"capability": func(p *telemetry.Policy) { p.Capabilities = []telemetry.ClusterCapability{"root"} },
		"recipe cap": func(p *telemetry.Policy) {
			p.Recipes = []telemetry.Recipe{{Name: "x", Declares: []telemetry.ClusterCapability{"root"}}}
		},
		"recipe name": func(p *telemetry.Policy) { p.Recipes = []telemetry.Recipe{{Name: "X"}} },
		"external ip": func(p *telemetry.Policy) { p.ExternalHosts = []string{"10.0.0.1"} },
	} {
		t.Run(name, func(t *testing.T) {
			p := testPolicy()
			mutate(&p)
			if err := p.Validate(); err == nil {
				t.Fatal("Validate accepted a bad policy")
			}
		})
	}
	var zero telemetry.Policy
	if zero.EffectiveMode() != telemetry.ModeOpen {
		t.Fatalf("zero policy mode = %q, want open", zero.EffectiveMode())
	}
}

func TestCheckBindingNamesWhichGrantFailed(t *testing.T) {
	policy := testPolicy()
	ok := telemetry.Binding{Endpoint: telemetry.Endpoint{URL: "http://prom.shop.svc:9090", Credential: "loki-token"}}
	if err := policy.CheckBinding(ok).Err(); err != nil {
		t.Fatal(err)
	}
	badEndpoint := policy.CheckBinding(telemetry.Binding{Endpoint: telemetry.Endpoint{URL: "http://prom.other.svc"}})
	if badEndpoint.Endpoint == nil || badEndpoint.Credential != nil || !strings.HasPrefix(badEndpoint.Err().Error(), "endpoint:") {
		t.Fatalf("endpoint check = %+v", badEndpoint)
	}
	badCredential := policy.CheckBinding(telemetry.Binding{Endpoint: telemetry.Endpoint{URL: "http://prom.shop.svc", Credential: "nope"}})
	if badCredential.Credential == nil || badCredential.Endpoint != nil || !strings.HasPrefix(badCredential.Err().Error(), "credential:") {
		t.Fatalf("credential check = %+v", badCredential)
	}
}

func TestEnvironmentPolicyCarriesItsGrants(t *testing.T) {
	env := telemetry.Environment{Version: telemetry.EnvironmentVersion, ControlNamespace: "sdo-ctl", ApplicationNamespace: "shop", ExternalHosts: []string{"logs.example.com"}, Credentials: []string{"loki-token"}}
	policy := env.Policy()
	if err := policy.CheckBinding(telemetry.Binding{Endpoint: telemetry.Endpoint{URL: "https://logs.example.com", Credential: "loki-token"}}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := (telemetry.Environment{Version: telemetry.EnvironmentVersion}).Policy().CheckBinding(telemetry.Binding{Endpoint: telemetry.Endpoint{URL: "http://prom.shop.svc"}}).Err(); err == nil {
		t.Fatal("an environment that grants nothing must admit no endpoint")
	}
}
