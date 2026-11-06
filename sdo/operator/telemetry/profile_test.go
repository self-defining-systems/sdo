package telemetry_test

import (
	"strings"
	"testing"

	"sdo.dev/operator/telemetry"
)

func TestProfileParseIsStrictAndDerivesTheEnvironment(t *testing.T) {
	good := `{"name":"lab","mode":"pinned","recipes":["prometheus"],"sources":[{"id":"prom","adapter":"prometheus","kinds":["metrics"],"endpoint":"http://p.lab.svc:9090","default":true}],"metricsPublishUrl":"http://p.lab.svc:9090"}`
	p, err := telemetry.ParseProfile([]byte(good))
	if err != nil {
		t.Fatal(err)
	}
	env := p.Environment()
	if env.Mode != telemetry.ModePinned || len(env.Provided) != 1 || env.Provided[0].Origin != telemetry.OriginOperator || env.MetricsPublishURL == "" {
		t.Fatalf("environment = %+v", env)
	}
	if err := env.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]string{
		"unknown field":    strings.Replace(good, `"mode"`, `"bogus":1,"mode"`, 1),
		"bad name":         strings.Replace(good, `"lab"`, `"Lab!"`, 1),
		"bad mode":         strings.Replace(good, `pinned`, `closed`, 1),
		"bad endpoint":     strings.Replace(good, `http://p.lab.svc:9090","default`, `ftp://p","default`, 1),
		"trailing":         good + `{}`,
		"credential field": strings.Replace(good, `"default":true`, `"default":true,"credential":"t"`, 1),
	} {
		if _, err := telemetry.ParseProfile([]byte(bad)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestWithSourceDefaultsTheFirstMetricsSourceOnly(t *testing.T) {
	p := telemetry.Profile{Name: "none"}
	metrics := []telemetry.Kind{telemetry.KindMetrics}
	p, err := p.WithSource(telemetry.ProfileSource{ID: "a", Adapter: "prometheus", Kinds: metrics, Endpoint: "http://a.svc"})
	if err != nil || !p.Sources[0].Default {
		t.Fatalf("first metrics source is the default: %+v %v", p, err)
	}
	p, err = p.WithSource(telemetry.ProfileSource{ID: "b", Adapter: "prometheus", Kinds: metrics, Endpoint: "http://b.svc"})
	if err != nil || p.Sources[1].Default {
		t.Fatalf("second source is not a default: %+v %v", p, err)
	}
	if _, err := p.WithSource(telemetry.ProfileSource{ID: "a", Adapter: "prometheus", Kinds: metrics, Endpoint: "http://c.svc"}); err == nil {
		t.Fatal("duplicate id accepted")
	}
}
