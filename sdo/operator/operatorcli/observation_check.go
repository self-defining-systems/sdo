package operatorcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	runtime "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk/program"
)

// RunObservationCheck performs production typed reads only: no mutations,
// model calls, database command output or Secret reads.
func RunObservationCheck(ctx context.Context, args []string, out io.Writer) error {
	flags := flag.NewFlagSet("sdo-operator observation-check", flag.ContinueOnError)
	flags.SetOutput(out)
	namespace := flags.String("namespace", "", "application namespace")
	kubeconfig := flags.String("kubeconfig", "", "Kubernetes configuration (in-cluster by default)")
	selector := flags.String("selector", "", "optional workload label selector")
	targetJSON := flags.String("target", "", "optional reference-only DatabaseTarget JSON; includes auth separately from create-user password")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *namespace == "" || flags.NArg() != 0 {
		return errors.New("observation-check requires --namespace and no positional arguments")
	}
	config, err := hostKubernetesConfig(*kubeconfig)
	if err != nil {
		return errors.New("observation-check: Kubernetes configuration unavailable")
	}
	config.Timeout = 5 * time.Second
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return errors.New("observation-check: Kubernetes client unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pods, err := client.CoreV1().Pods(*namespace).List(ctx, metav1.ListOptions{LabelSelector: *selector})
	if err != nil {
		return errors.New("observation-check: Pod inventory unavailable; check namespace and RBAC")
	}
	var overrides []program.DatabaseTarget
	if *targetJSON != "" {
		var target program.DatabaseTarget
		if json.Unmarshal([]byte(*targetJSON), &target) != nil || target.Validate() != nil || target.Pod.Namespace != *namespace {
			return errors.New("observation-check: invalid reference-only target in application namespace")
		}
		overrides = append(overrides, target)
	}
	report := runtime.CheckObservationConformance(ctx, runtime.TypedPlanClient{Client: client, ExecConfig: config}, pods.Items, overrides)
	if err := json.NewEncoder(out).Encode(report); err != nil {
		return err
	}
	if !report.Passed {
		return errors.New("observation-check failed; see named primitive, target and failure_stage")
	}
	return nil
}
