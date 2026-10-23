package runtime

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// The menu uses the responder's ordinary executor and existing credentials.
func productionResponderMenu() *ResponderMenuConfig { return &ResponderMenuConfig{} }

// Reuse the same in-cluster or kubeconfig credential source as the controller.
func responderPlanExecConfig() (*rest.Config, error) {
	if config, err := rest.InClusterConfig(); err == nil {
		return config, nil
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
}
