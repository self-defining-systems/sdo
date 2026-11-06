package archcheck

// Rules is the single declarative table that drives the checker. Every
// boundary the deployment plugin plan relies on is data here; the engine in
// archcheck.go only interprets it.
//
// Allowlist entries are today's known violations. They may only shrink: an
// entry that no longer matches anything fails the check, and the ratchet
// (R5) counts must be lowered when a count drops.
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
		ConfigTypeSource: ConfigTypeSource{
			Dir:    "contracts/gen/sdodev/contracts/v1alpha1",
			// "Strategy" goes with RawManifestStrategy in S3-15.
			Suffixes: []string{"Strategy", "RenderConfig", "ImageConfig"},
		},
		RatchetTokens: []string{"ManifestPaths", "manifest_paths"},
		Allow: []Allow{
			{Rule: R1, File: "rendered/generators.go", Reason: "kustomize build lives in core rendered generators", Slice: "O-seam-render (render/kustomize)"},
			{Rule: R4, File: "deployment/registry.go", Reason: "generic registry switches on the raw strategy", Slice: "O-seam-render (render catalog)"},
			{Rule: R4, File: "deployment/raw.go", Reason: "raw strategy decoded in core deployment package", Slice: "O-seam-render (render/raw)"},
		},
		Ratchet: []RatchetEntry{
			{File: "broker/broker.go", Count: 2, Reason: "broker reads manifest paths from the lifecycle config", Slice: "O-contract"},
			{File: "contracts/gen/sdodev/contracts/v1alpha1/controller_config.pb.go", Count: 5, Reason: "generated wire field", Slice: "O-contract"},
			{File: "contracts/gen/sdodev/contracts/v1alpha1/lifecycle.pb.go", Count: 5, Reason: "generated wire field", Slice: "O-contract"},
			{File: "deployment/manifest_set.go", Count: 4, Reason: "manifest-set core helper", Slice: "O-seam-render"},
			{File: "deployment/raw.go", Count: 3, Reason: "raw renderer inside deployment", Slice: "O-seam-render (render/raw)"},
			{File: "deployment/raw_test.go", Count: 1, Reason: "raw renderer test", Slice: "O-seam-render (render/raw)"},
			{File: "deployment/registry.go", Count: 2, Reason: "registry reads raw strategy paths", Slice: "O-seam-render"},
			{File: "lifecycle/coordinator.go", Count: 7, Reason: "lifecycle plumbs manifest paths", Slice: "O-contract"},
			{File: "lifecycle/install_adapter.go", Count: 3, Reason: "install adapter plumbs manifest paths", Slice: "O-contract"},
			{File: "operatorcli/controller.go", Count: 4, Reason: "CLI wiring", Slice: "O-seam-render"},
			{File: "operatorcli/install_existing.go", Count: 6, Reason: "CLI wiring", Slice: "O-seam-render"},
			{File: "operatorcli/onboarding.go", Count: 1, Reason: "CLI wiring", Slice: "O-seam-render"},
			{File: "operatorcli/source_render.go", Count: 4, Reason: "CLI wiring", Slice: "O-seam-render"},
			{File: "rendered/selection.go", Count: 1, Reason: "core rendered selection", Slice: "O-seam-render"},
			{File: "rendered/source.go", Count: 5, Reason: "core rendered source", Slice: "O-seam-render"},
			{File: "rendered/source_cache.go", Count: 1, Reason: "core rendered source cache", Slice: "O-seam-render"},
			{File: "rendered/source_cache_test.go", Count: 4, Reason: "rendered cache test", Slice: "O-seam-render"},
			{File: "runtime/controller_config.go", Count: 1, Reason: "runtime controller config", Slice: "O-contract"},
			{File: "runtime/run.go", Count: 10, Reason: "runtime plumbs manifest paths", Slice: "O-contract"},
		},
	}
}
