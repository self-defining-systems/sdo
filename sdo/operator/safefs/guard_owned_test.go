package safefs_test

// Reviewed exceptions for the structural guard in guard_test.go. Every entry
// says why the written path is never below an agent-controlled checkout.
// Writing into an agent checkout is never a valid reason: use safefs. Adding
// an entry is a review decision.

const (
	reasonExec     = "determinism edge package: starts a fixed tool (git, go, kubectl) or a caller-supplied binary; writer verbs in its arguments are listed in verbOwned"
	reasonState    = "operator-owned recorded state or evidence directory, never a checkout"
	reasonBuild    = "operator-generated build workspace under MkdirTemp (or its cache); source is copied in with O_EXCL after symlinks are rejected"
	reasonSession  = "operator session, startup or standby directories on the repository mount; control files, not checkout paths"
	reasonLedger   = "operator ledger, event stream or task store under the operator state directory"
	reasonRendered = "operator render cache and scratch index; archive extraction creates with O_EXCL under a fresh directory"
	reasonCLI      = "operator-chosen output, snapshot or scratch directory named on the command line, never an agent checkout"
	reasonScratch  = "operator scratch, socket or toolchain directory created with MkdirTemp or operator configuration"
	reasonTool     = "developer tool, test environment or test helper, not on the controller path"
	reasonSafefs   = "the safefs implementation: creates the operator-chosen root it then confines every write to"
)

// operatorOwned lists the files that call the os or ioutil writers directly.
var operatorOwned = map[string]string{
	"apihistory/identifiers.go":                      reasonState,
	"apihistory/mutation_gate.go":                    reasonState,
	"apihistory/store.go":                            reasonState,
	"apihistory/writer.go":                           reasonState,
	"evidencesink/mirror.go":                         "only reads runtime evidence, opening each file with O_NOFOLLOW",
	"evidencesink/sink.go":                           "writes only below the runner-owned evidence sink, never a checkout; paths are allowlisted relative names and symlinked directories are refused",
	"broker/advance.go":                              "removes broker-owned proposal roots (whole directories, never a path inside a checkout)",
	"broker/advance_patches.go":                      "removes a MkdirTemp scratch directory",
	"broker/attestation.go":                          "materializes an attestation tree into a fresh broker-created scratch directory",
	"broker/broker.go":                               "broker-owned workspaces and ledger; copyChange Lstats and rejects symlinks before it replaces a file",
	"broker/concurrent_publish.go":                   "writes into clones and metadata the broker itself created from the accepted base",
	"broker/rejection.go":                            "broker-owned rejection ledger",
	"broker/source_evidence.go":                      "broker-owned ledger under the operator state directory",
	"broker/stage.go":                                "broker-owned candidate staging directory",
	"broker/subprocess_validator.go":                 "removes a MkdirTemp scratch directory",
	"builder/internal/buildio/build.go":              reasonBuild,
	"builder/jit.go":                                 reasonBuild,
	"builder/memory_host.go":                         reasonBuild,
	"builder/native.go":                              reasonBuild,
	"builder/native_application_map.go":              reasonBuild,
	"builder/native_contracts.go":                    reasonBuild,
	"builder/native_runner.go":                       reasonBuild,
	"builder/native_traffic.go":                      reasonBuild,
	"builder/programs.go":                            reasonBuild,
	"builder/replay.go":                              reasonBuild,
	"cmd/configdoc/main.go":                          reasonTool,
	"cmd/sdo-go-cache/main.go":                       reasonTool,
	"deployment/health.go":                           reasonState,
	"install/internal/installio/seed.go":             reasonScratch,
	"internal/purity/determinism/cmd/main.go":        reasonTool,
	"internal/purity/testsplit/cmd/main.go":          reasonTool,
	"internal/sandboxio/sandbox_linux.go":            "writes the child process's own /proc/self/oom_score_adj, not a file tree",
	"internal/testutil/fakego.go":                    reasonTool,
	"memory/api_history.go":                          "operator-owned recovery state, not a checkout",
	"memory/commit_catalog.go":                       "scratch directory created with MkdirTemp",
	"memory/corpus_capture.go":                       "controller capture area under the repository mount, written by the controller",
	"memory/healthy_baseline.go":                     "operator state directory",
	"memory/json_repository.go":                      "outcome store: accepted-branch repository owned by the operator",
	"memory/layout/migrate.go":                       "migrates a bootstrap checkout or a base tree the operator materialized, before any agent turn",
	"memory/reachability_baseline.go":                "operator state directory",
	"memory/sdk_gaps.go":                             "outcome store: accepted-branch repository owned by the operator",
	"memory/session.go":                              "operator session storage",
	"memory/validate/git.go":                         "materializes the base tree into a MkdirTemp scratch directory",
	"memory/validate/ownership.go":                   "removes a MkdirTemp scratch directory",
	"memory/validate/preexisting.go":                 "operator cache directory",
	"memory/validate/publication.go":                 "removes a MkdirTemp scratch directory",
	"memory/validate/semantic.go":                    "removes a MkdirTemp scratch directory",
	"memory/validate/whitespace.go":                  "MkdirTemp scratch index",
	"memoryhost/hostio/process.go":                   reasonScratch,
	"memoryhost/hostio/probe_users.go":               "controller-owned probe-identity seed under .sdo-runtime/probe-identity on the repository volume, a fixed path never in a checkout and never published",
	"operatorcli/baseline_write.go":                  reasonCLI,
	"operatorcli/config_cmd.go":                      "controller copies its mounted configuration to its own runtime evidence directory on the repository mount",
	"operatorcli/operability_terminal.go":            reasonCLI,
	"operatorcli/program.go":                         reasonCLI,
	"operatorcli/runtime_artifacts.go":               reasonCLI,
	"operatorcli/script_runner.go":                   reasonCLI,
	"operatorcli/telemetry_policy.go":                reasonCLI,
	"patchset/checkout.go":                           "pruneEmpty removes only empty directories after safefs.Remove checked the parents",
	"patchset/internal/gitio/gitio.go":               reasonScratch,
	"rendered/internal/renderio/git.go":              reasonRendered,
	"rendered/internal/renderio/materialize.go":      reasonRendered,
	"rendered/materialize.go":                        reasonRendered,
	"runtime/envtestio/envtestio.go":                 reasonTool,
	"runtime/event_stream.go":                        reasonLedger,
	"runtime/internal/lifecycleio/state_evidence.go": reasonLedger,
	"runtime/internal/lifecycleio/task_store.go":     reasonLedger,
	"runtime/jitview/cmd/gen/main.go":                reasonTool,
	"runtime/native_broker.go":                       reasonLedger,
	"runtime/routing_capture.go":                     reasonLedger,
	"runtime/store_backups.go":                       "controller-owned backup state under .sdo-runtime/backups on the repository volume, keyed by incident digest; never a checkout and never published",
	"runtime/source_inventory_report.go":             reasonLedger,
	"safefs/safefs.go":                               reasonSafefs,
	"shellpath/kubernetes.go":                        reasonScratch,
	"shellpath/repository.go":                        reasonScratch,
	"toolchain/shared.go":                            reasonScratch,
	"upstream/upstreamtest/upstreamtest.go":          reasonTool,
	"worker_registry/kubernetes.go":                  reasonSession,
	"worker_registry/staged_task.go":                 reasonSession,
	"worker_registry/standby.go":                     reasonSession,
	"worker_registry/standby_daemon.go":              reasonSession,
	"worker_registry/startup.go":                     reasonSession,
	"worker_registry/startup_artifacts.go":           reasonSession,
	"worker_registry/startup_dispatch.go":            reasonSession}

// subprocessOwned lists every file that can start a subprocess. These are the
// determinism edges (internal/*io packages) plus a few tool entry points.
var subprocessOwned = map[string]string{
	"internal/sandboxio/sandbox.go":                 "re-executes the caller's own binary (/proc/self/exe) as a memory-limited render child that receives its request on stdin and writes only stdout",
	"broker/internal/processio/git.go":              reasonExec,
	"broker/internal/processio/validation.go":       reasonExec,
	"builder/internal/buildio/build.go":             reasonExec,
	"builder/internal/buildio/run.go":               reasonExec,
	"cmd/sdo-replay/internal/replayio/run.go":       reasonExec,
	"deployment/internal/deploymentio/process.go":   reasonExec,
	"install/internal/installio/adapters.go":        reasonExec,
	"install/internal/installio/seed.go":            reasonExec,
	"internal/rehearsal/rehearsalio/rehearsalio.go": reasonExec,
	"internal/testutil/gitio/git.go":                reasonExec,
	"internal/testutil/toolio/toolio.go":            reasonExec,
	"memory/internal/memoryio/process.go":           reasonExec,
	"memoryhost/hostio/process.go":                  reasonExec,
	"operatorcli/internal/processio/archive.go":     reasonExec,
	"operatorcli/internal/processio/git.go":         reasonExec,
	"operatorcli/internal/processio/pod.go":         reasonExec,
	"operatorcli/internal/processio/script.go":      reasonExec,
	"patchset/internal/gitio/gitio.go":              reasonExec,
	"rendered/internal/renderio/git.go":             reasonExec,
	"rendered/internal/renderio/materialize.go":     reasonExec,
	"runtime/envtestio/envtestio.go":                reasonExec,
	"runtime/internal/lifecycleio/git.go":           reasonExec,
	"runtime/internal/lifecycleio/process.go":       reasonExec,
	"shellpath/internal/shellio/shellio.go":         reasonExec,
	"toolchain/download.go":                         reasonExec,
	"toolchain/shared.go":                           reasonExec,
	"upstream/gitpoll/internal/gitio/gitio.go":      reasonExec,
	"worker_registry/internal/workerio/env.go":      reasonExec,
}

// verbOwned lists files whose subprocess arguments contain a writerVerb.
var verbOwned = map[string]string{
	"builder/internal/buildio/build.go":         "cp -R seeds the operator's build cache directory",
	"toolchain/shared.go":                       "cp -R seeds the operator's shared toolchain cache from the image",
	"install/internal/installio/seed.go":        "tar and git clean/checkout/sh -c operate on the operator's own accepted-checkout clone and the sync pod workspace, before any agent turn",
	"operatorcli/internal/processio/script.go":  "bash -c runs an operator script in its own scratch directory",
	"operatorcli/internal/processio/archive.go": "sh -c tars a directory inside a pod to stdout; reads only",
	"memory/internal/memoryio/process.go":       "bash -n only syntax-checks a script and writes nothing",
	"rendered/internal/renderio/git.go":         "git rm --cached edits a scratch index (GIT_INDEX_FILE), never a working tree",
	"shellpath/internal/shellio/shellio.go":     "bash -s runs the operator's shell-path script",
}
