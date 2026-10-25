package program

import (
	"encoding/json"
	"fmt"
	"sdo.dev/operator/sdk"
	"strings"
)

// RemoveServicePort removes exactly one Service port. Select by Name OR Port;
// Protocol optionally disambiguates a numeric port (omitted protocol means TCP).
// The last port is protected unless AllowEmpty is explicitly set; Kubernetes
// schema validation still applies. Conditional writes preserve concurrent edits.
type RemoveServicePort struct {
	Ref        sdk.ObjectRef
	Name       string
	Port       int32
	Protocol   string
	AllowEmpty bool
	Reason     string
}

func (RemoveServicePort) action() {}

// ValidateRemoveServicePort checks the target and exclusive port identity.
func ValidateRemoveServicePort(a RemoveServicePort) error {
	if a.Ref.APIVersion != "v1" || a.Ref.Kind != "Service" || a.Ref.Namespace == "" || a.Ref.Name == "" {
		return fmt.Errorf("RemoveServicePort requires a named v1 Service")
	}
	if (a.Name == "") == (a.Port == 0) || a.Port < 0 || a.Port > 65535 || strings.TrimSpace(a.Name) != a.Name {
		return fmt.Errorf("RemoveServicePort requires exactly one name or valid port number")
	}
	if a.Protocol != "" && (a.Name != "" || (a.Protocol != "TCP" && a.Protocol != "UDP" && a.Protocol != "SCTP")) {
		return fmt.Errorf("RemoveServicePort protocol requires a numeric port and TCP, UDP or SCTP")
	}
	return nil
}

// ApplyRemoveServicePort projects a removal without changing any unrelated field.
// Missing or ambiguous matches and a protected final port leave object untouched.
func ApplyRemoveServicePort(object map[string]any, a RemoveServicePort) error {
	if err := ValidateRemoveServicePort(a); err != nil {
		return err
	}
	spec, ok := object["spec"].(map[string]any)
	if !ok {
		return fmt.Errorf("Service spec is absent")
	}
	ports, ok := spec["ports"].([]any)
	if !ok {
		return fmt.Errorf("Service ports are absent")
	}
	count, index := 0, -1
	for i, item := range ports {
		p, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("Service port is not an object")
		}
		match := a.Name != "" && p["name"] == a.Name
		if a.Port != 0 {
			raw, err := json.Marshal(p["port"])
			var port int32
			if err != nil || json.Unmarshal(raw, &port) != nil {
				return fmt.Errorf("Service port number is invalid")
			}
			protocol, _ := p["protocol"].(string)
			if protocol == "" {
				protocol = "TCP"
			}
			match = port == a.Port && (a.Protocol == "" || protocol == a.Protocol)
		}
		if match {
			count++
			index = i
		}
	}
	if count != 1 {
		return fmt.Errorf("Service port identity must match exactly once (matched %d)", count)
	}
	if len(ports) == 1 && !a.AllowEmpty {
		return fmt.Errorf("removing the final Service port requires AllowEmpty")
	}
	next := make([]any, 0, len(ports)-1)
	next = append(next, ports[:index]...)
	next = append(next, ports[index+1:]...)
	spec["ports"] = next
	return nil
}

func validateServicePortAction(action Action) error {
	switch a := action.(type) {
	case RemoveServicePort:
		return ValidateRemoveServicePort(a)
	case *RemoveServicePort:
		if a == nil {
			return fmt.Errorf("nil RemoveServicePort")
		}
		return ValidateRemoveServicePort(*a)
	}
	return nil
}
