package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sdo.dev/operator/sdk/program"
)

// RunObservationCheck performs production typed reads only: no mutations,
// model calls, database command output or Secret reads.
func RunObservationCheck(ctx context.Context, args []string, out io.Writer) error {
	flags := flag.NewFlagSet("sdo-observation-check", flag.ContinueOnError)
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
	config, err := observationKubernetesConfig(*kubeconfig)
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
	pods, err := client.CoreV1().Pods(*namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return errors.New("observation-check: Pod inventory unavailable; check namespace and RBAC")
	}
	selected, err := SelectObservationPods(pods.Items, *selector)
	if err != nil {
		report := ObservationConformance{Passed: false, Observations: []ObservationCheck{{Primitive: "DatabaseUsers", FailureStage: "workload-discovery", Error: err.Error()}}}
		_ = json.NewEncoder(out).Encode(report)
		return err
	}
	var overrides []program.DatabaseTarget
	if *targetJSON != "" {
		var target program.DatabaseTarget
		if json.Unmarshal([]byte(*targetJSON), &target) != nil || target.Validate() != nil || target.Pod.Namespace != *namespace {
			return errors.New("observation-check: invalid reference-only target in application namespace")
		}
		overrides = append(overrides, target)
	}
	report := CheckObservationConformance(ctx, TypedPlanClient{Client: client, ExecConfig: config}, selected, overrides)
	if err := json.NewEncoder(out).Encode(report); err != nil {
		return err
	}
	if !report.Passed {
		return errors.New("observation-check failed; see named primitive, target and failure_stage")
	}
	return nil
}

func observationKubernetesConfig(path string) (*rest.Config, error) {
	if path == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", path)
}
