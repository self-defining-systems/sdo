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
	"sort"

	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/rendered"
)

// DeploymentUsage documents "sdo-operator deployment".
const DeploymentUsage = `usage: sdo-operator deployment render [--root .] [--plan FILE] [--namespace NS] [--json]

  render
        Render offline, with no cluster, what Go would deploy from your working
        state: the checkout's .sdo/deployment directory (values, manifests,
        build, overlays) with the working patch series applied to the base
        tree, rendered by the plan's source. Prints one line per object
        (--json: the objects). A renderer or patch error is printed as the
        error Go would hold the deployment on. App files you changed but did
        not capture with "patch create" are not part of the render; they are
        listed as uncaptured.
        --plan defaults to .sdo/deployment/plan.yaml in the checkout.
        Exit 0 on success, 1 when the render or the patches fail, 2 for usage
        or infrastructure errors.`

// RunDeployment runs "sdo-operator deployment <args>" and returns the exit code.
func RunDeployment(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if getenv == nil {
		getenv = processio.Environment
	}
	if len(args) == 0 {
		fmt.Fprintln(stderr, DeploymentUsage)
		return exitInfra
	}
	var code int
	var err error
	switch args[0] {
	case "render":
		code, err = deploymentRender(ctx, args[1:], stdout, stderr)
	case "help", "-h", "--help":
		fmt.Fprintln(stdout, DeploymentUsage)
		return exitClean
	default:
		err = fmt.Errorf("unknown deployment command %q\n%s", args[0], DeploymentUsage)
	}
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(stdout, DeploymentUsage)
		return exitClean
	}
	if err != nil {
		fmt.Fprintln(stderr, "sdo-operator deployment:", err)
		return exitInfra
	}
	return code
}

func deploymentRender(ctx context.Context, args []string, stdout, stderr io.Writer) (int, error) {
	flags := flag.NewFlagSet("deployment render", flag.ContinueOnError)
	root := flags.String("root", ".", "repository checkout")
	planPath := flags.String("plan", "", "deployment plan file (default .sdo/deployment/plan.yaml)")
	namespace := flags.String("namespace", "default", "namespace objects are rendered into")
	asJSON := flags.Bool("json", false, "print the objects as JSON")
	rest, err := parseFlags(flags, args)
	if err != nil {
		return exitInfra, err
	}
	if len(rest) != 0 {
		return exitInfra, fmt.Errorf("render takes no arguments, got %q", rest)
	}
	absolute, err := absRoot(*root)
	if err != nil {
		return exitInfra, err
	}
	if *planPath == "" {
		*planPath = filepath.Join(absolute, ".sdo", "deployment", "plan.yaml")
	}
	data, err := os.ReadFile(*planPath)
	if err != nil {
		return exitInfra, fmt.Errorf("read the deployment plan: %w (write .sdo/deployment/plan.yaml or pass --plan)", err)
	}
	plan, err := planfile.Parse(data, planCatalogs())
	if err != nil {
		fmt.Fprintln(stdout, "deployment.invalid: "+err.Error())
		return exitDiagnostics, nil
	}
	report, err := patchset.Check(ctx, absolute)
	var perr *patchset.PatchError
	if errors.As(err, &perr) {
		fmt.Fprintln(stdout, patchDiagnostic(perr))
		return exitDiagnostics, nil
	}
	if err != nil {
		return exitInfra, err
	}
	tree, err := patchset.PublishedTree(ctx, absolute)
	if err != nil {
		return exitInfra, err
	}
	source := rendered.Source{Repository: absolute, Deployment: plan.GetSource(), Catalog: renderers.Catalog()}
	inventory, err := source.AcceptedInventory(ctx, tree, *namespace)
	if err != nil {
		var sourceErr *rendered.SourceError
		if errors.As(err, &sourceErr) && sourceErr.Kind == rendered.SourceInvalid {
			fmt.Fprintln(stdout, "render.invalid: "+err.Error())
			return exitDiagnostics, nil
		}
		return exitInfra, err
	}
	if *asJSON {
		objects := inventory.Objects
		if len(objects) == 0 {
			_, err = fmt.Fprintln(stdout, "[]")
		} else {
			err = json.NewEncoder(stdout).Encode(objects)
		}
		if err != nil {
			return exitInfra, err
		}
	} else {
		var lines []string
		for _, o := range inventory.Objects {
			lines = append(lines, fmt.Sprintf("%s %s/%s", o.GetKind(), o.GetNamespace(), o.GetName()))
		}
		sort.Strings(lines)
		for _, l := range lines {
			fmt.Fprintln(stdout, l)
		}
		fmt.Fprintf(stdout, "%d object(s) rendered from effective tree of %d patch(es)\n", len(lines), report.Patches)
	}
	if len(report.Uncaptured) > 0 {
		fmt.Fprintf(stderr, "note: %d app file(s) are scratch edits no patch reproduces, so this render does not include them:\n", len(report.Uncaptured))
		for _, line := range uncapturedLines(report.Uncaptured) {
			fmt.Fprintln(stderr, line)
		}
	}
	return exitClean, nil
}
