package operatorcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/operability/offline"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/render"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/renderenv"
)

// OperabilityUsage documents "sdo-operator operability".
const OperabilityUsage = `usage: sdo-operator operability check --offline [--root .] [--namespace NS] [--stage deployment|complete]
                                      [--allow-registry HOSTS] [--scope-blocking] [--grant-foreign]
                                      [--grant-cluster-scoped] [--json]

  check --offline
        Compute, with no cluster, the operability checks Go can decide from the
        checkout: patches apply, the effective tree renders deterministically,
        expected workloads match, image declarations are consistent, scope and
        registry limits hold (O1, O2, O6). With --stage complete it also runs the
        shared memory validator over the checkout and reports application-map
        coverage (O3), memory consistency (O5) and, when SDO_BASE_COMMIT,
        SDO_ACTOR and SDO_PHASE are set, diff ownership (O6). Live checks (apply,
        readiness, detectors under traffic) are not run here.
        Prints one line per check, or the report as JSON. The report is advisory:
        its subject is not bound to a published commit.
        --allow-registry takes comma-separated registry hosts; none configured
        skips the allowlist. Foreign-namespace and cluster-scoped objects are
        notes unless --scope-blocking.
        Exit 0 when every offline item of the stage is accepted, 1 when a check
        fails or is missing, 2 for usage or infrastructure errors.`

// RunOperability runs "sdo-operator operability <args>" and returns the exit code.
func RunOperability(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if getenv == nil {
		getenv = processio.Environment
	}
	if len(args) == 0 {
		fmt.Fprintln(stderr, OperabilityUsage)
		return exitInfra
	}
	var code int
	var err error
	switch args[0] {
	case "check":
		code, err = operabilityCheck(ctx, args[1:], getenv, stdout)
	case "help", "-h", "--help":
		fmt.Fprintln(stdout, OperabilityUsage)
		return exitClean
	default:
		err = fmt.Errorf("unknown operability command %q\n%s", args[0], OperabilityUsage)
	}
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(stdout, OperabilityUsage)
		return exitClean
	}
	if err != nil {
		fmt.Fprintln(stderr, "sdo-operator operability:", err)
		return exitInfra
	}
	return code
}

func operabilityCheck(ctx context.Context, args []string, getenv func(string) string, stdout io.Writer) (int, error) {
	flags := flag.NewFlagSet("operability check", flag.ContinueOnError)
	offlineOnly := flags.Bool("offline", false, "run the checks that need no cluster (required)")
	root := flags.String("root", ".", "repository checkout")
	namespace := flags.String("namespace", "default", "namespace objects are rendered into")
	stageFlag := flags.String("stage", string(operability.StageDeployment), "report stage: deployment or complete")
	registries := flags.String("allow-registry", "", "comma-separated image registry hosts to allow")
	scopeBlocking := flags.Bool("scope-blocking", false, "fail on foreign-namespace and cluster-scoped objects")
	grantForeign := flags.Bool("grant-foreign", false, "permit foreign-namespace objects")
	grantCluster := flags.Bool("grant-cluster-scoped", false, "permit cluster-scoped objects")
	asJSON := flags.Bool("json", false, "print the report as JSON")
	rest, err := parseFlags(flags, args)
	if err != nil {
		return exitInfra, err
	}
	if len(rest) != 0 {
		return exitInfra, fmt.Errorf("check takes no arguments, got %q", rest)
	}
	if !*offlineOnly {
		return exitInfra, errors.New("only --offline is implemented: live checks run inside the operator")
	}
	stage := operability.Stage(*stageFlag)
	if stage != operability.StageDeployment && stage != operability.StageComplete {
		return exitInfra, fmt.Errorf("unknown stage %q (deployment or complete)", *stageFlag)
	}
	absolute, err := absRoot(*root)
	if err != nil {
		return exitInfra, err
	}
	policy := offline.Policy{ScopeBlocking: *scopeBlocking, GrantForeign: *grantForeign, GrantClusterScoped: *grantCluster}
	for _, host := range strings.Split(*registries, ",") {
		if host = strings.TrimSpace(host); host != "" {
			policy.AllowedRegistries = append(policy.AllowedRegistries, host)
		}
	}

	var subject operability.Subject
	subject.AcceptedCommit = processio.HeadCommit(ctx, absolute)
	checks, err := offlineDeploymentChecks(ctx, absolute, *namespace, policy, &subject)
	if err != nil {
		return exitInfra, err
	}
	if stage == operability.StageComplete {
		memory, err := offlineMemory(ctx, absolute, getenv)
		if err != nil {
			return exitInfra, err
		}
		checks = append(checks, offline.CheckMemory(memory)...)
	}
	report, err := operability.New(stage, subject.WithUnavailable()).With(checks...)
	if err != nil {
		return exitInfra, err
	}
	decision := report.Decide(offline.Items(stage)...)
	if err := printOperability(stdout, report, decision, *asJSON); err != nil {
		return exitInfra, err
	}
	if decision.Kind != operability.Accepted {
		return exitDiagnostics, nil
	}
	return exitClean, nil
}

// offlineDeploymentChecks reads the checkout the way "deployment render" does
// (working .sdo/deployment over the recorded base tree) and runs the offline
// deployment checks. A plan that does not parse is an O1.plan failure; only
// infrastructure problems are errors.
func offlineDeploymentChecks(ctx context.Context, root, namespace string, policy offline.Policy, subject *operability.Subject) ([]operability.Check, error) {
	data, err := os.ReadFile(filepath.Join(root, ".sdo", "deployment", "plan.yaml"))
	if err != nil {
		return nil, fmt.Errorf("read the deployment plan: %w (write .sdo/deployment/plan.yaml)", err)
	}
	plan, err := planfile.Parse(data, planCatalogs())
	if err != nil {
		return []operability.Check{{
			ID: "O1.plan", Phase: operability.PhaseOffline, Status: operability.StatusFail,
			Cause: operability.CauseRender, Detail: operability.Clip(err.Error()), Paths: []string{".sdo/deployment/plan.yaml"},
		}}, nil
	}
	_, patchErr := patchset.Check(ctx, root)
	var perr *patchset.PatchError
	if patchErr != nil && !errors.As(patchErr, &perr) {
		return nil, patchErr
	}
	tree, err := patchset.PublishedTree(ctx, root)
	if err != nil {
		return nil, err
	}
	subject.BaseTree = tree
	store := rendered.ObjectStore{Dir: root}
	var effective rendered.EffectiveTree
	if patchErr == nil {
		if effective, err = rendered.Effective(ctx, store, rendered.BaseTree(tree)); err != nil {
			return nil, err
		}
		defer effective.Close()
		subject.EffectiveTree, subject.PatchesDigest = effective.ID, effective.PatchesDigest
	}
	env := renderenv.For(nil, namespace, render.SecretsOmit)
	var declared []imagesrc.Declared
	for _, image := range plan.GetImages() {
		declared = append(declared, imagesrc.Declared{Image: image.GetImage(), SourceID: image.GetSourceId(), Config: image.GetConfig()})
	}
	result := offline.CheckDeployment(ctx, offline.Deployment{
		PatchesErr: patchErr,
		Render: func(ctx context.Context) (rendered.DesiredSet, error) {
			fsys, err := rendered.GitTreeFS(ctx, effective)
			if err != nil {
				return rendered.DesiredSet{}, err
			}
			return rendered.Desired(ctx, fsys, env, renderers.Catalog(), plan.GetSource())
		},
		ExpectedWorkloads: plan.GetExpectedWorkloadIds(),
		Declared:          declared,
		Policy:            policy,
	})
	if result.Set.RenderDigest != "" {
		subject.RenderDigest, subject.EnvDigest = result.Set.RenderDigest, result.Set.EnvDigest
	}
	return result.Checks, nil
}

// offlineMemory runs the static layers of the shared memory validator over the
// checkout: structure and semantics always, ownership when the broker's base,
// actor and phase are known from the environment.
func offlineMemory(ctx context.Context, root string, getenv func(string) string) (offline.Memory, error) {
	request := validate.Request{Root: root, Layers: validate.Structural | validate.Semantic, PlanCatalogs: planCatalogs(), Replay: "off"}
	base, actor, phase := getenv("SDO_BASE_COMMIT"), getenv(memoryActorEnv), getenv(memoryPhaseEnv)
	checkedOwnership := base != "" && actor != "" && phase != ""
	if checkedOwnership {
		request.Layers |= validate.Ownership
		request.Base, request.Actor, request.Phase = base, validate.Actor(actor), validate.Phase(phase)
	}
	var gaps []validate.Diagnostic
	request.CoverageGaps = &gaps
	diagnostics, err := validate.Validate(ctx, request)
	if err != nil {
		return offline.Memory{}, err
	}
	return offline.Memory{Blocking: findings(diagnostics), Notes: findings(gaps), OwnershipChecked: checkedOwnership}, nil
}

func findings(diagnostics []validate.Diagnostic) []offline.Finding {
	out := make([]offline.Finding, 0, len(diagnostics))
	for _, d := range diagnostics {
		out = append(out, offline.Finding{Rule: d.Rule, Path: d.Path, Line: d.Line, Message: d.Message})
	}
	return out
}

// operabilityOutput is the JSON form: the report plus the decision recomputed
// from it. The decision is for the reader; the report carries no verdict.
type operabilityOutput struct {
	Report   operability.Report `json:"report"`
	Decision struct {
		Kind   operability.DecisionKind `json:"kind"`
		Owner  operability.Owner        `json:"owner,omitempty"`
		Checks []string                 `json:"checks,omitempty"`
	} `json:"decision"`
}

func printOperability(out io.Writer, report operability.Report, decision operability.Decision, asJSON bool) error {
	if asJSON {
		var o operabilityOutput
		o.Report = report
		o.Decision.Kind, o.Decision.Owner = decision.Kind, decision.Owner
		for _, c := range decision.Checks {
			o.Decision.Checks = append(o.Decision.Checks, c.ID)
		}
		encoded, err := json.Marshal(o)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "%s\n", encoded)
		return err
	}
	for _, item := range report.Items {
		for _, c := range item.Checks {
			line := fmt.Sprintf("%-8s %s", c.Status, c.ID)
			if c.Cause != "" {
				line += " [" + string(c.Cause) + "]"
			}
			if c.Detail != "" {
				line += ": " + c.Detail
			}
			if len(c.Paths) > 0 {
				line += " (" + strings.Join(c.Paths, ", ") + ")"
			}
			fmt.Fprintln(out, line)
		}
	}
	summary := "decision: " + string(decision.Kind)
	if decision.Owner != "" {
		summary += " owner=" + string(decision.Owner)
	}
	_, err := fmt.Fprintln(out, summary)
	return err
}
