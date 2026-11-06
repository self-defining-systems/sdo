package archcheck

// Rules is the single declarative table that drives the checker. Every
// boundary the deployment plugin plan relies on is data here; the engine in
// archcheck.go only interprets it.
//
// Known violations are //archcheck: directives in the violating file (see
// directive.go) so parallel PRs never edit this table. They may only shrink: a
// directive that no longer matches anything fails the check, and ratchet
// (R5) counts must be lowered when a count drops. The Ratchet rows here are
// generated files, which cannot carry directives.
func Rules() Config {
	const module = "sdo.dev/operator"
	return Config{
		Module: module,
		Families: []Family{
			{Dir: "render", SupportDirs: []string{"rendertest"}},
			{Dir: "imagesrc", SupportDirs: []string{"imagesrctest"}},
			{Dir: "upstream", SupportDirs: []string{"upstreamtest"}},
			{Dir: "telemetry", SupportDirs: []string{"telemetrytest"}},
		},
		WiringDirs: []string{"operatorcli"},
		// The checker's own sources and fixtures name tool libraries as data.
		ExemptDirs:   []string{"internal/archcheck"},
		ToolPrefixes: []string{"helm.sh/", "sigs.k8s.io/kustomize/", "github.com/google/go-containerregistry/", "github.com/moby/buildkit/"},
		// What an implementation subpackage may import from this module besides
		// its own family interface package and support package.
		ImplMayImport: []string{module + "/contracts", module + "/sdk"},
		// The explicit registry of plugin config types. Add a type here when a
		// plugin family gains a config message; no name pattern is inferred.
		ConfigTypes: []ConfigType{
			{
				Name:     "RawManifestStrategy",
				Import:   module + "/contracts/gen/sdodev/contracts/v1alpha1",
				OwnerPkg: module + "/render/raw",
			},
			{
				Name:     "RawRenderConfig",
				Import:   module + "/contracts/gen/sdodev/contracts/v1alpha1",
				OwnerPkg: module + "/render/raw",
			},
			{
				Name:     "KustomizeRenderConfig",
				Import:   module + "/contracts/gen/sdodev/contracts/v1alpha1",
				OwnerPkg: module + "/render/kustomize",
			},
			{
				Name:     "HelmRenderConfig",
				Import:   module + "/contracts/gen/sdodev/contracts/v1alpha1",
				OwnerPkg: module + "/render/helm",
			},
			{
				Name:     "GitPollConfig",
				Import:   module + "/contracts/gen/sdodev/contracts/v1alpha1",
				OwnerPkg: module + "/upstream/gitpoll",
			},
			{
				Name:     "BuildKitImageConfig",
				Import:   module + "/contracts/gen/sdodev/contracts/v1alpha1",
				OwnerPkg: module + "/imagesrc/buildkit",
			},
			{
				Name:     "PrebuiltImageConfig",
				Import:   module + "/contracts/gen/sdodev/contracts/v1alpha1",
				OwnerPkg: module + "/imagesrc/prebuilt",
			},
		},
		RatchetTokens: []string{"ManifestPaths", "manifest_paths"},
		// Generated files cannot carry directives; only the serial proto PRs
		// (S3-01, S3-11c, S3-15) edit these entries.
		Ratchet: []RatchetEntry{
			{File: "contracts/gen/sdodev/contracts/v1alpha1/controller_config.pb.go", Count: 5, Reason: "generated wire field", Slice: "S3-11c"},
			{File: "contracts/gen/sdodev/contracts/v1alpha1/lifecycle.pb.go", Count: 5, Reason: "generated wire field", Slice: "S3-15"},
		},
		TransitionalPRs: TransitionalPRs,
	}
}

// TransitionalPRs is the closed list of render-seam PR ids a directive's
// until= may name (the PR that removes the transitional form). Only S3-00a and
// S3-15 edit it; S3-15 deletes it with R5.
var TransitionalPRs = []string{
	"S3-01", "S3-01b", "S3-03", "S3-04", "S3-05", "S3-07", "S3-08", "S3-09i", "S3-09r",
	"S3-10a", "S3-10b", "S3-10c", "S3-10d", "S3-11a", "S3-11b", "S3-11c",
	"S3-12", "S3-13a", "S3-13b", "S3-13c", "S3-14", "S3-15",
}
