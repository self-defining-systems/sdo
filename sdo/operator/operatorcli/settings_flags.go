package operatorcli

import (
	"fmt"
	"strings"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/deploymentpolicy"
	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
)

// resolveSettings is the entry of host and install-existing: Default(), each --config file in order, then the
// SDO_<SECTION>_<KEY> environment, validated. No knob has a flag of its own. The renamed host environment names are
// refused first.
func resolveSettings(files []string, environ []string) (settings.Config, error) {
	if err := settings.RefuseRenamedEnvironment(environ); err != nil {
		return settings.Config{}, err
	}
	cfg, err := settings.Resolve(files, environ)
	if err != nil {
		return settings.Config{}, err
	}
	return cfg, cfg.Validate()
}

// applySettings carries the resolved configuration into the installer's configuration, with the install-shaped
// documents the host resolves from it (the deployment policy, the telemetry profile, which may name a host file, and
// the evidence sink). Every other value reaches the Jobs only through the mounted sdo-config.
func applySettings(c *install.Config, cfg settings.Config, controlNamespace string) error {
	policy, err := deploymentpolicy.FromFlags(deploymentpolicy.Flags{
		AllowedRegistries: strings.Join(cfg.Deployment.Policy.AllowedRegistries, ","), ScopeBlocking: cfg.Deployment.Policy.ScopeBlocking,
		GrantedNamespaces: strings.Join(cfg.Deployment.Policy.GrantedNamespaces, ","), GrantedClusterKinds: strings.Join(cfg.Deployment.Policy.GrantedClusterKinds, ","),
	})
	if err != nil {
		return err
	}
	encodedPolicy, err := deploymentpolicy.Format(policy)
	if err != nil {
		return err
	}
	profile, err := resolveTelemetry(cfg.Telemetry.Profile, cfg.Telemetry.Sources, controlNamespace)
	if err != nil {
		return err
	}
	sink, err := resolveEvidenceSink(cfg.Evidence, controlNamespace)
	if err != nil {
		return err
	}
	c.Settings = &cfg
	c.DeploymentPolicy, c.TelemetryProfile, c.EvidenceSink = encodedPolicy, profile, sink
	c.BackendRecipes = telemetrysources.Recipes()
	return nil
}

// mountedSettings is where the operate and controller Jobs mount the sdo-config ConfigMap.
var mountedSettings = install.SettingsFile()

// loadMountedSettings reads the resolved configuration a Job was started with. The installer always mounts it, so
// an absent file is an error, never a fallback to defaults.
func loadMountedSettings(path string) (settings.Config, error) {
	cfg, err := settings.Read(path)
	if err != nil {
		return settings.Config{}, fmt.Errorf("operator configuration (the sdo-config mount): %w", err)
	}
	return cfg, nil
}
