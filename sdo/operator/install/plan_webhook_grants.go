package install

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const PlanWebhookGrantAnnotation = "sdo.dev/named-plan-webhooks"

func planWebhookGrantName(namespace string) string { return "sdo-plan-webhooks-" + namespace }

func (i Installer) removeUnconfiguredPlanWebhookGrant(ctx context.Context, c Config) error {
	name := planWebhookGrantName(c.controlNamespace())
	if err := i.Client.RbacV1().ClusterRoleBindings().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("revoke named plan webhook binding: %w", err)
	}
	if err := i.Client.RbacV1().ClusterRoles().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("revoke named plan webhook role: %w", err)
	}
	return nil
}
