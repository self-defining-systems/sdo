package install

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const PermissiveResponderAnnotation = "sdo.dev/permissive-responder-namespaces"

// validatePermissive checks the opt-in that lets the responder repair outside
// the application namespace.
func (c Config) validatePermissive() error {
	if !c.PermissiveResponder {
		if len(c.ResponderNamespaces) != 0 {
			return fmt.Errorf("responder namespaces require permissive responder opt-in")
		}
		return nil
	}
	if c.Namespace == c.controlNamespace() {
		return fmt.Errorf("permissive responder requires a separate control namespace")
	}
	for _, ns := range c.ResponderNamespaces {
		if !namespaceName.MatchString(ns) || ns == c.controlNamespace() || strings.HasPrefix(ns, "kube-") {
			return fmt.Errorf("invalid permissive responder namespace %q", ns)
		}
	}
	return nil
}

// A fault may create its backend only during injection. Prepare its namespace
// now so the responder's Role already exists when that happens.
func (i Installer) ensurePermissiveNamespaces(ctx context.Context, c Config) error {
	if !c.PermissiveResponder {
		return nil
	}
	for _, ns := range c.ResponderNamespaces {
		if _, err := i.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("ensure responder namespace %s: %w", ns, err)
		}
	}
	return nil
}
