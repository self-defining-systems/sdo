package validate

// Rule IDs are stable: agents, the CLI, and tests match on them.
const (
	RuleUpkeepMigration = "ownership.upkeep-migration"
	RulePlaybookStatus  = "playbook.status"
	// Publication content and lifecycle context.
	RuleEmptyDiff          = "diff.empty"
	RuleBootstrapClosed    = "lifecycle.bootstrap-closed"
	RuleAcceptedOutcome    = "lifecycle.accepted-outcome"
	RuleVerifiedSource     = "lifecycle.verified-source"
	RuleReflectionDecision = "reflection.decision"
	// An escalated closure the responder confirmed saves its localization as a
	// Diagnose-only playbook, so a repeat escalates without an agent.
	RuleWhitespace               = "diff.whitespace"
	RuleSnapshotBudget           = "memory.snapshot-budget"
	RuleSnapshotProjection       = "memory.snapshot"
	RuleClusterLocal             = "memory.cluster-local"
	RuleSecretData               = "memory.secret-data"
	RuleSecretMaterial           = "memory.secret-material"
	RuleLiveObjectName           = "playbook.live-object-name"
	RuleDuplicateRepair          = "playbook.duplicate-repair"
	RuleSharedRepair             = "playbook.shared-repair"
	RulePlaybookComponentCopy    = "playbook.component-copy"
	RuleDetectorComponentCopy    = "detector.component-copy"
	RulePlaybookComponentLiteral = "playbook.component-literal"
	RuleDetectorComponentLiteral = "detector.component-literal"
	RuleProgramNormalizedRead    = "program.normalized-read"
	RuleDetectorUnscopedBinding  = "detector.unscoped-binding"

	// Structural.
	RuleUnknownPath      = "layout.unknown-path"
	RuleInvalidID        = "id.pattern"
	RuleSymlink          = "layout.symlink"
	RuleNotRegular       = "layout.not-regular"
	RuleSecretPath       = "layout.secret-path"
	RuleTooLarge         = "layout.too-large"
	RuleLegacyManifest   = "layout.legacy-manifest"
	RuleSchemaVersion    = "schema.version"
	RuleYAMLParse        = "yaml.parse"
	RuleGoSyntax         = "go.syntax"
	RuleDocumentParse    = "document.parse"
	RuleManifestParse    = "manifest.parse"
	RuleManifestInvalid  = "manifest.invalid"
	RuleManifestLocation = "manifest.location"
	RuleSourcesInvalid   = "sources.invalid"

	// Deployment class (.sdo/deployment): the strict patch format, the owner
	// header, the stated upstream base, and the plan's catalog decode.
	RulePatchInvalid      = "patch.invalid"
	RulePatchOwner        = "patch.owner"
	RulePatchUpstreamBase = "patch.upstream-base"
	// RulePatchUncaptured is the broker publish rule: app files of the agent's
	// checkout that the patch series does not reproduce are never published.
	RulePatchUncaptured   = "patch.uncaptured"
	RuleDeploymentInvalid = "deployment.invalid"

	// Application source an agent committed (the broker's source publication):
	// the shape of the committed files, their size, and the history they sit on.
	RuleSourcePolicy  = "source.policy"
	RuleSourceSize    = "source.size"
	RuleSourceHistory = "source.history"

	// Telemetry bindings (.sdo/diagnostics/telemetry), the detection designer's
	// data: sources and backends are checked against the environment's policy
	// and the adapter registry without executing anything.
	RuleTelemetrySchema     = "telemetry.schema"
	RuleTelemetryAdapter    = "telemetry.adapter"
	RuleTelemetryEndpoint   = "telemetry.endpoint"
	RuleTelemetryCredential = "telemetry.credential"
	RuleTelemetryOptions    = "telemetry.options"
	RuleTelemetryDefault    = "telemetry.default"
	RuleTelemetryBackend    = "telemetry.backend"
	RuleTelemetryMode       = "telemetry.mode"
	RuleTelemetryOrigin     = "telemetry.origin"
	// Compile layer: a detector query that names (or defaults to) no bound
	// source, or a source that does not serve the query's kind.
	RuleTelemetryUnbound = "telemetry.unbound"
	RuleTelemetryKind    = "telemetry.kind"

	// Semantic.
	RuleArchitectureMissing = "memory.architecture-missing"
	RuleGoalOwner           = "goal.owner"
	RuleManifestMissing     = "manifest.missing"
	RuleDetectorNone        = "detector.none"
	RuleDetectorNoSource    = "detector.no-go-source"
	RuleDuplicateID         = "id.duplicate"
	RuleUnknownPlaybook     = "ref.unknown-playbook"
	RulePlaybookNone        = "playbook.none"
	RulePlaybookNoReadme    = "playbook.missing-readme"
	RulePlaybookOwner       = "playbook.owner"
	RulePlaybookIncident    = "playbook.originating-incident"
	RuleIndexMissing        = "playbook.index-missing"
	RuleIndexGenerated      = "playbook.index-generated"
	RuleScriptNotExecutable = "script.not-executable"
	RuleScriptUnreferenced  = "script.unreferenced"
	RuleScriptSyntax        = "script.syntax"
	RulePhysicalPath        = "text.physical-path"

	RuleCorpusImmutable = "program.corpus-immutable"

	// Ownership.
	RuleOwnershipDenied       = "ownership.denied"
	RuleUnknownActor          = "ownership.unknown-actor"
	RuleUnknownPhase          = "ownership.unknown-phase"
	RuleUnsafePath            = "ownership.unsafe-path"
	RuleProtectedDetector     = "ownership.protected-detector"
	RuleProvenanceRewrite     = "ownership.provenance-rewrite"
	RuleResponderFiring       = "ownership.responder-firing"
	RuleInitialHealthOnly     = "ownership.initial-health-only"
	RuleInitialNeedsDetectors = "ownership.initial-needs-detectors"

	// Application map (the region of .sdo/arch.md the detection designer owns).
	// Blocking for the designer's commits once a map exists, notes for other
	// actors; map.missing, map.unprobed and map.possible-edge are always
	// notes. map.coverage is a user-path component no probed user operation
	// covers and that records no unprobed reason.
	RuleMapMissing        = "map.missing"
	RuleMapSchema         = "map.schema"
	RuleMapUnknownName    = "map.unknown-name"
	RuleMapPathMismatch   = "map.path-mismatch"
	RuleMapUncoveredOp    = "map.uncovered-operation"
	RuleMapUncoveredWrite = "map.uncovered-write"
	RuleMapUncoveredData  = "map.uncovered-data"
	RuleMapCoverage       = "map.coverage"
	RuleMapUnprobed       = "map.unprobed"
	RuleMapKnownBroken    = "map.known-broken"
	RuleMapPossibleEdge   = "map.possible-edge"
	// Ownership of the map region: the designer changes nothing outside it,
	// and nobody else changes it.
	RuleMapOutsideRegion = "ownership.map-outside-region"
	RuleMapRegionOwner   = "ownership.map-region-owner"

	// Data checks (.sdo/diagnostics/data), the detection designer's declarative
	// record checks: schema, read-only allowlist and bounds, and the
	// application-map data roles of the stores they name.
	RuleDataCheckInvalid   = "data-check.invalid"
	RuleDataCheckComponent = "data-check.component"

	// Explicit Go health-bootstrap preparation.
	RuleGenericHealth = "bootstrap.generic-health"

	// Informational replay evidence, never an acceptance diagnostic.
	RuleReplayObservation = "program.replay-observation"
	// Informational traffic coverage: a health-probe scenario that keeps the
	// default latency bound. Never an acceptance diagnostic.
	RuleTrafficDefaultLatency = "traffic.default-latency"
	// RuleProgramDiffText notes (never rejects) a program deciding on a diff's
	// Text, so reflection rewrites it with typed field comparisons.
	RuleProgramDiffText = "program.diff-text-match"

	// Compile.
	RuleProgramMitigationRepair = "program.mitigation-not-repaired"
	RuleDetectorCompile         = "detector.compile"
	RuleDetectorVet             = "detector.vet"
	RuleDetectorTest            = "detector.test"
	RuleDetectorHealthyBaseline = "detector.healthy-baseline"
	// RuleDetectorControllerStart rejects detectors the controller would
	// refuse at start; it runs the controller's own startup validation.
	RuleDetectorControllerStart = "detector.controller-start"
)
