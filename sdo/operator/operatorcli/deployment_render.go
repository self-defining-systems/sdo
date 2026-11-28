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
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sort"

	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/rendered"
)

// DeploymentUsage documents "sdo-operator deployment".
const DeploymentUsage = `usage: sdo-operator deployment render [--root .] [--plan FILE] [--namespace NS] [--json]
       sdo-operator deployment build [IMAGE...]

  render
        Render offline, with no cluster, what Go would deploy from your
        checkout: its last commit (uncommitted app files are never deployed),
        rendered by the source derived from the files or named in the
        optional override .sdo/deployment/plan.yaml. Prints one line per
        object (--json: the objects). A renderer error is printed as the error
        Go would hold the deployment on.
        --plan defaults to .sdo/deployment/plan.yaml in the checkout.
        Exit 0 on success, 1 when the render fails, 2 for usage or
        infrastructure errors.

  build [IMAGE...]
        In a deployer task: build images of your last commit now, the way Go's
        deploy builds them (same render, image rule, BuildKit and registry),
        and print one line per image, then each failure with its failing step
        and build-log tail. IMAGE is a rendered image reference, its
        repository or the repository's last path segment; none builds every
        image Go would build. Nothing is applied.
        A passing build is cached, so Go's deploy of the same commit reuses it.
        A build can take minutes. Exit 0 when every image built, 1 when one
        failed, 2 when nothing could be built (render, unknown image, or not
        a deployer task).`

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
	case "build":
		code, err = deploymentBuild(ctx, args[1:], getenv, stdout, stderr)
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
	explicit := *planPath != ""
	if !explicit {
		*planPath = filepath.Join(absolute, ".sdo", "deployment", "plan.yaml")
	}
	plan := &contracts.DeploymentPlan{}
	data, err := os.ReadFile(*planPath)
	switch {
	case errors.Is(err, os.ErrNotExist) && !explicit:
		// No override: the source is derived from the committed files.
	case err != nil:
		return exitInfra, fmt.Errorf("read the deployment plan: %w", err)
	default:
		if plan, err = planfile.Parse(data, planCatalogs()); err != nil {
			fmt.Fprintln(stdout, "deployment.invalid: "+err.Error())
			return exitDiagnostics, nil
		}
	}
	tree, err := broker.ProposalTreeDigest(ctx, absolute)
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
		fmt.Fprintf(stdout, "%d object(s) rendered from the committed tree\n", len(lines))
	}
	return exitClean, nil
}
