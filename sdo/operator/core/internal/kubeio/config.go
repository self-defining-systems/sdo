// Package kubeio owns Kubernetes configuration discovery and the default observation clock.
package kubeio

import (
	"fmt"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"os"
	"time"
)

func Now() time.Time                      { return time.Now() }
func LookupEnv(key string) (string, bool) { return os.LookupEnv(key) }
func Config() (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if err == nil {
		return config, nil
	}

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
		loadingRules.ExplicitPath = kubeconfig
	} else if home := homedir.HomeDir(); home != "" {
		loadingRules.ExplicitPath = home + "/.kube/config"
	}
	config, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubernetes config: %w", err)
	}
	return config, nil
}
