package install

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
)

// ReconcileAcceptedSourceGrants is called only by trusted install/onboarding
// with the selected manifests at the accepted commit. Workers receive none of
// these grants. Names come from source and API discovery decides resource scope.
func ReconcileAcceptedSourceGrants(ctx context.Context, client kubernetes.Interface, app, control string, objects []*unstructured.Unstructured) error {
	if !namespaceName.MatchString(app) || !namespaceName.MatchString(control) {
		return fmt.Errorf("source grants require application and control namespaces")
	}
	name := fmt.Sprintf("sdo-source-%x", sha256.Sum256([]byte(control+"/"+app)))
	rules := []rbacv1.PolicyRule{}
	seen := map[string]bool{}
	resources := map[string]*metav1.APIResourceList{}
	for _, object := range objects {
		if object == nil || object.GetNamespace() != "" || strings.EqualFold(object.GetKind(), "Secret") {
			continue
		}
		version := object.GetAPIVersion()
		list := resources[version]
		if list == nil {
			var err error
			list, err = client.Discovery().ServerResourcesForGroupVersion(version)
			if err != nil {
				return fmt.Errorf("discover accepted source %s: %w", version, err)
			}
			resources[version] = list
		}
		found := false
		for _, resource := range list.APIResources {
			if resource.Kind != object.GetKind() || strings.Contains(resource.Name, "/") {
				continue
			}
			found = true
			if resource.Namespaced {
				return fmt.Errorf("accepted source %s/%s is missing its namespace", object.GetKind(), object.GetName())
			}
			if object.GetName() == "" {
				return fmt.Errorf("accepted cluster source needs a name")
			}
			key := version + "/" + resource.Name + "/" + object.GetName()
			if seen[key] {
				break
			}
			seen[key] = true
			gv, err := schema.ParseGroupVersion(version)
			if err != nil {
				return err
			}
			verbs := []string{"get", "patch", "update"}
			// Kubernetes checks privilege escalation in addition to patch authority.
			// Both exceptional verbs remain bound to this source-declared role name.
			if gv.Group == "rbac.authorization.k8s.io" && resource.Name == "clusterroles" {
				verbs = append(verbs, "escalate", "bind")
			}
			rules = append(rules, rbacv1.PolicyRule{APIGroups: []string{gv.Group}, Resources: []string{resource.Name}, ResourceNames: []string{object.GetName()}, Verbs: verbs})
			break
		}
		if !found {
			return fmt.Errorf("accepted cluster kind %s/%s is not discoverable", version, object.GetKind())
		}
	}
	if len(rules) == 0 {
		if err := client.RbacV1().ClusterRoleBindings().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err := client.RbacV1().ClusterRoles().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].APIGroups[0]+"/"+rules[i].Resources[0]+"/"+rules[i].ResourceNames[0] < rules[j].APIGroups[0]+"/"+rules[j].Resources[0]+"/"+rules[j].ResourceNames[0]
	})
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"sdo.dev/source-owner": app}}, Rules: rules}
	old, err := client.RbacV1().ClusterRoles().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.RbacV1().ClusterRoles().Create(ctx, role, metav1.CreateOptions{})
	} else if err == nil {
		role.ResourceVersion = old.ResourceVersion
		_, err = client.RbacV1().ClusterRoles().Update(ctx, role, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("source role grant: %w", err)
	}
	binding := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"sdo.dev/source-owner": app}}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sdo-controller", Namespace: control}, {Kind: "ServiceAccount", Name: "sdo-operate", Namespace: control}}}
	prior, err := client.RbacV1().ClusterRoleBindings().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{})
	} else if err == nil {
		binding.ResourceVersion = prior.ResourceVersion
		_, err = client.RbacV1().ClusterRoleBindings().Update(ctx, binding, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("source role binding: %w", err)
	}
	return nil
}
