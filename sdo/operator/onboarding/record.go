// Package onboarding owns the immutable identity acknowledgement made by the
// native lifecycle after independent health verification, before controller install.
package onboarding

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const ConfigMapName = "sdo-namespace-onboarding"
const DataKey = "onboarding.json"

func Decode(data []byte, namespace string) (*contracts.NamespaceOnboarding, error) {
	record := &contracts.NamespaceOnboarding{}
	if err := protojson.Unmarshal(data, record); err != nil {
		return nil, fmt.Errorf("decode onboarding: %w", err)
	}
	if err := Validate(record, namespace); err != nil {
		return nil, err
	}
	return record, nil
}
func Validate(record *contracts.NamespaceOnboarding, namespace string) error {
	if record == nil || record.Namespace != namespace || namespace == "" {
		return fmt.Errorf("onboarding namespace does not match %q", namespace)
	}
	commit, err := hex.DecodeString(record.AcceptedCommit)
	if err != nil || (len(commit) != 20 && len(commit) != 32) || record.EstablishedAt == nil || record.EstablishedAt.CheckValid() != nil || record.EstablishedAt.AsTime().IsZero() {
		return fmt.Errorf("onboarding requires an accepted commit and establishment time")
	}
	for _, ref := range record.Objects {
		if ref == nil || ref.Name == "" || ref.ApiVersion == "" || ref.Kind == "" || ref.Kind == "Secret" || (ref.Namespace != namespace && !(ref.Namespace == "" && (ref.Kind == "ClusterRole" || ref.Kind == "ClusterRoleBinding"))) {
			return fmt.Errorf("invalid onboarding identity %v", ref)
		}
	}
	return nil
}
func Load(ctx context.Context, client kubernetes.Interface, control, namespace string) (*contracts.NamespaceOnboarding, error) {
	cm, err := client.CoreV1().ConfigMaps(control).Get(ctx, ConfigMapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read onboarding acknowledgement: %w", err)
	}
	return Decode([]byte(cm.Data[DataKey]), namespace)
}

// Establish is create-once even across reinstall or memory/source publication.
// Only the independently verified native install path calls it. A restart,
// incident, or a later healthy interval can never expand this acknowledgement.
func Establish(ctx context.Context, client kubernetes.Interface, control, namespace, commit string, source []*unstructured.Unstructured, now time.Time) (*contracts.NamespaceOnboarding, error) {
	prior, err := Load(ctx, client, control, namespace)
	if err != nil || prior != nil {
		return prior, err
	}
	objects, err := inventory(ctx, client, namespace)
	if err != nil {
		return nil, err
	}
	declared := map[string]bool{}
	for _, o := range source {
		if o == nil {
			continue
		}
		ns := o.GetNamespace()
		if o.GetKind() == "ClusterRole" || o.GetKind() == "ClusterRoleBinding" {
			ns = ""
		} else if ns == "" {
			ns = namespace
		}
		declared[identity(o.GetAPIVersion(), o.GetKind(), ns, o.GetName())] = true
	}
	relevant := map[string]bool{}
	for _, o := range objects {
		if o.GetKind() != "ClusterRoleBinding" && o.GetKind() != "RoleBinding" {
			continue
		}
		subjects, _, _ := unstructured.NestedSlice(o.Object, "subjects")
		for _, raw := range subjects {
			subject, _ := raw.(map[string]any)
			subjectNamespace, _ := subject["namespace"].(string)
			if subjectNamespace == "" && o.GetKind() == "RoleBinding" {
				subjectNamespace = o.GetNamespace()
			}
			if subject["kind"] == "ServiceAccount" && subjectNamespace == namespace {
				relevant[identity(o.GetAPIVersion(), o.GetKind(), "", o.GetName())] = true
				role, _, _ := unstructured.NestedMap(o.Object, "roleRef")
				name, _ := role["name"].(string)
				if role["kind"] == "ClusterRole" {
					relevant[identity("rbac.authorization.k8s.io/v1", "ClusterRole", "", name)] = true
				}
			}
		}
	}
	record := &contracts.NamespaceOnboarding{Namespace: namespace, AcceptedCommit: commit, EstablishedAt: timestamppb.New(now.UTC())}
	for _, o := range objects {
		key := identity(o.GetAPIVersion(), o.GetKind(), o.GetNamespace(), o.GetName())
		if declared[key] || (o.GetNamespace() != namespace && !relevant[key]) || o.GetLabels()["app.kubernetes.io/managed-by"] == "sdo" {
			continue
		}
		record.Objects = append(record.Objects, &contracts.ObjectRef{ApiVersion: o.GetAPIVersion(), Kind: o.GetKind(), Namespace: o.GetNamespace(), Name: o.GetName()})
	}
	sort.Slice(record.Objects, func(i, j int) bool {
		a, b := record.Objects[i], record.Objects[j]
		return identity(a.ApiVersion, a.Kind, a.Namespace, a.Name) < identity(b.ApiVersion, b.Kind, b.Namespace, b.Name)
	})
	if err := Validate(record, namespace); err != nil {
		return nil, err
	}
	data, err := protojson.Marshal(record)
	if err != nil {
		return nil, err
	}
	immutable := true
	_, err = client.CoreV1().ConfigMaps(control).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: control, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}}, Immutable: &immutable, Data: map[string]string{DataKey: string(data)}}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return Load(ctx, client, control, namespace)
	}
	if err != nil {
		return nil, fmt.Errorf("persist onboarding acknowledgement: %w", err)
	}
	return record, nil
}
func identity(api, kind, namespace, name string) string {
	return api + "/" + kind + "/" + namespace + "/" + name
}

func inventory(ctx context.Context, client kubernetes.Interface, namespace string) ([]*unstructured.Unstructured, error) {
	options := metav1.ListOptions{}
	type listing struct {
		api, kind string
		read      func() (runtime.Object, error)
	}
	lists := []listing{
		{"v1", "ConfigMap", func() (runtime.Object, error) { return client.CoreV1().ConfigMaps(namespace).List(ctx, options) }},
		{"v1", "Service", func() (runtime.Object, error) { return client.CoreV1().Services(namespace).List(ctx, options) }},
		{"v1", "ServiceAccount", func() (runtime.Object, error) { return client.CoreV1().ServiceAccounts(namespace).List(ctx, options) }},
		{"v1", "Pod", func() (runtime.Object, error) { return client.CoreV1().Pods(namespace).List(ctx, options) }},
		{"v1", "PersistentVolumeClaim", func() (runtime.Object, error) {
			return client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, options)
		}},
		{"apps/v1", "Deployment", func() (runtime.Object, error) { return client.AppsV1().Deployments(namespace).List(ctx, options) }},
		{"apps/v1", "StatefulSet", func() (runtime.Object, error) { return client.AppsV1().StatefulSets(namespace).List(ctx, options) }},
		{"apps/v1", "DaemonSet", func() (runtime.Object, error) { return client.AppsV1().DaemonSets(namespace).List(ctx, options) }},
		{"apps/v1", "ReplicaSet", func() (runtime.Object, error) { return client.AppsV1().ReplicaSets(namespace).List(ctx, options) }},
		{"batch/v1", "Job", func() (runtime.Object, error) { return client.BatchV1().Jobs(namespace).List(ctx, options) }},
		{"batch/v1", "CronJob", func() (runtime.Object, error) { return client.BatchV1().CronJobs(namespace).List(ctx, options) }},
		{"networking.k8s.io/v1", "NetworkPolicy", func() (runtime.Object, error) {
			return client.NetworkingV1().NetworkPolicies(namespace).List(ctx, options)
		}},
		{"rbac.authorization.k8s.io/v1", "Role", func() (runtime.Object, error) { return client.RbacV1().Roles(namespace).List(ctx, options) }},
		{"rbac.authorization.k8s.io/v1", "RoleBinding", func() (runtime.Object, error) { return client.RbacV1().RoleBindings(namespace).List(ctx, options) }},
		{"rbac.authorization.k8s.io/v1", "ClusterRole", func() (runtime.Object, error) { return client.RbacV1().ClusterRoles().List(ctx, options) }},
		{"rbac.authorization.k8s.io/v1", "ClusterRoleBinding", func() (runtime.Object, error) { return client.RbacV1().ClusterRoleBindings().List(ctx, options) }},
	}
	result := []*unstructured.Unstructured{}
	for _, list := range lists {
		raw, err := list.read()
		if err != nil {
			return nil, fmt.Errorf("onboarding list %s: %w", list.kind, err)
		}
		items, err := meta.ExtractList(raw)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(item)
			if err != nil {
				return nil, err
			}
			o := &unstructured.Unstructured{Object: data}
			o.SetAPIVersion(list.api)
			o.SetKind(list.kind)
			result = append(result, o)
		}
	}
	return result, nil
}
