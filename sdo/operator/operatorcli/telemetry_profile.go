package operatorcli

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"sigs.k8s.io/yaml"

	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/telemetry"
)

// resolveTelemetry is one validated profile for the control namespace: the named profile plus the operator-provided
// ID=URL sources.
func resolveTelemetry(profileName string, sourceList []string, controlNamespace string) (telemetry.Profile, error) {
	profile, err := loadTelemetryProfile(profileName, controlNamespace)
	if err != nil {
		return telemetry.Profile{}, err
	}
	for _, value := range sourceList {
		id, endpoint, ok := strings.Cut(value, "=")
		if !ok || id == "" || endpoint == "" {
			return telemetry.Profile{}, fmt.Errorf("telemetry source %q must be ID=URL", value)
		}
		if parsed, err := url.Parse(endpoint); err != nil || parsed.Host == "" {
			return telemetry.Profile{}, fmt.Errorf("telemetry source %q: %q is not a URL", value, endpoint)
		}
		profile, err = profile.WithSource(telemetry.ProfileSource{ID: id, Adapter: "prometheus", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Endpoint: endpoint})
		if err != nil {
			return telemetry.Profile{}, fmt.Errorf("telemetry source %q: %w", value, err)
		}
	}
	return profile, nil
}

// loadTelemetryProfile reads a built-in name, an inline JSON document (what
// the operate Job receives) or a profile file.
func loadTelemetryProfile(value, controlNamespace string) (telemetry.Profile, error) {
	if profile, ok := telemetrysources.BuiltinProfile(value, controlNamespace); ok {
		return profile, nil
	}
	if strings.HasPrefix(strings.TrimSpace(value), "{") {
		return telemetry.ParseProfile([]byte(value))
	}
	data, err := os.ReadFile(value)
	if err != nil {
		return telemetry.Profile{}, fmt.Errorf("telemetry profile %q is not a built-in profile (%s, %s) and not a readable file: %w", value, telemetrysources.DefaultProfileName, telemetrysources.NoneProfileName, err)
	}
	document, err := yaml.YAMLToJSONStrict(data)
	if err != nil {
		return telemetry.Profile{}, fmt.Errorf("telemetry profile %s: %w", value, err)
	}
	return telemetry.ParseProfile(document)
}
