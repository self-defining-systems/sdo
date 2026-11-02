package runtime

import (
	"fmt"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/core"
)

// clientInjected records the caller's choice before native launch constructs its
// own typed client. A production-created client still needs dynamic/exec transports.
func initializePlanTransports(options RuntimeOptions, clientInjected bool, namespace string) (*rest.Config, *core.GenericCapture, error) {
	config := options.PlanExecConfig
	if config == nil && !clientInjected {
		var err error
		config, err = responderPlanExecConfig()
		if err != nil {
			return nil, nil, fmt.Errorf("load plan exec client configuration: %w", err)
		}
	}
	capture := options.GenericCapture
	if capture == nil && !clientInjected {
		var err error
		capture, err = core.NewGenericCapture(namespace)
		if err != nil {
			return nil, nil, fmt.Errorf("create generic Kubernetes capture: %w", err)
		}
	}
	return config, capture, nil
}
