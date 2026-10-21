package validate

// Rule IDs are stable: agents, the CLI, and tests match on them.
const (
	// Publication content and lifecycle context.
	RuleEmptyDiff          = "diff.empty"
	RuleBootstrapClosed    = "lifecycle.bootstrap-closed"
	RuleAcceptedOutcome    = "lifecycle.accepted-outcome"
	RuleVerifiedSource     = "lifecycle.verified-source"
	RuleReflectionDecision = "reflection.decision"
	RuleWhitespace         = "diff.whitespace"
	RuleSnapshotBudget     = "memory.snapshot-budget"
	RuleSnapshotProjection = "memory.snapshot"

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

	// Semantic.
	RuleGoalMissing          = "memory.goal-missing"
	RuleArchitectureMissing  = "memory.architecture-missing"
	RuleGoalOwner            = "goal.owner"
	RuleManifestMissing      = "manifest.missing"
	RuleDetectorNone         = "detector.none"
	RuleDetectorNoSource     = "detector.no-go-source"
	RuleDuplicateID          = "id.duplicate"
	RuleUnknownPlaybook      = "ref.unknown-playbook"
	RulePlaybookNone         = "playbook.none"
	RulePlaybookNoReadme     = "playbook.missing-readme"
	RulePlaybookOwner        = "playbook.owner"
	RulePlaybookFaultClass   = "playbook.fault-class"
	RulePlaybookIncident     = "playbook.originating-incident"
	RulePlaybookPlaceholder  = "playbook.placeholder"
	RuleIndexMissing         = "playbook.index-missing"
	RuleIndexLink            = "playbook.index-link"
	RulePlaybookNotIndexed   = "playbook.not-indexed"
	RuleScriptNotExecutable  = "script.not-executable"
	RuleScriptUnreferenced   = "script.unreferenced"
	RuleScriptSyntax         = "script.syntax"
	RulePhysicalPath         = "text.physical-path"
	RuleArchitectureCommit   = "architecture.generated-at-commit"
	RuleArchitectureBaseline = "architecture.source-commit"

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
	RuleArchitectureAlone     = "ownership.architecture-alone"
	RuleUpkeepMigration       = "ownership.upkeep-migration"

	// Explicit Go health-bootstrap preparation.
	RuleGenericHealth = "bootstrap.generic-health"

	// Compile.
	RuleDetectorCompile = "detector.compile"
	RuleDetectorVet     = "detector.vet"
	RuleDetectorTest    = "detector.test"
)
