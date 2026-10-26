// Emit benchmark expectations from the same public installer configuration.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli"
)

func main() {
	launch, err := operatorcli.ParseInstallExistingLaunch(os.Args[1:])
	if err != nil {
		panic(err)
	}
	cfg := launch.Install
	cfg.AcceptedCommit = strings.Repeat("0", 40)
	var resources []*metav1.APIResourceList
	namespaces := []string{}
	if cfg.PermissiveResponder {
		rest, err := clientcmd.BuildConfigFromFlags("", launch.Kubeconfig)
		if err != nil {
			panic(err)
		}
		rest.Timeout = 10 * time.Second
		client, err := kubernetes.NewForConfig(rest)
		if err != nil {
			panic(err)
		}
		_, resources, err = client.Discovery().ServerGroupsAndResources()
		if err != nil {
			panic(err)
		}
		rows, err := client.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
		if err != nil {
			panic(err)
		}
		for _, row := range rows.Items {
			namespaces = append(namespaces, row.Name)
		}
	}
	plan, err := install.Resources(cfg, resources...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// The installer adds its generated read binding in every existing namespace.
	// Derive each binding through Resources too; never broaden the configured
	// writable namespace set while constructing expected read-only grants.
	seen := map[string]bool{}
	for _, b := range plan.AppBindings {
		seen[b.Namespace+"/"+b.Name] = true
	}
	for _, ns := range namespaces {
		if ns == cfg.ControlNamespace {
			continue
		}
		scoped := cfg
		scoped.Namespace = ns
		generated, err := install.Resources(scoped, resources...)
		if err != nil {
			panic(err)
		}
		for _, b := range generated.AppBindings {
			key := b.Namespace + "/" + b.Name
			if b.Namespace == ns && b.RoleRef.Kind == "ClusterRole" && !seen[key] {
				plan.AppBindings = append(plan.AppBindings, b)
				seen[key] = true
			}
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(plan); err != nil {
		panic(err)
	}
}
