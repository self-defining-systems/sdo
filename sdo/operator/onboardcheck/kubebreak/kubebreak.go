// Package kubebreak breaks and restores one application workload through the
// Kubernetes API for the onboarding check: it scales the workload to zero, or
// denies ingress to its pods with a NetworkPolicy when the cluster's CNI
// enforces NetworkPolicy. It acts on a target the onboarding agent owns, never
// on one with users.
package kubebreak

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/memory/appmap"
)

// Modes.
const (
	ModeScale   = "scale"
	ModeNetwork = "network"
)

const (
	// restoreAnnotation records a scaled-down workload's replica count on the
	// workload itself, so a restore survives the check process dying.
	restoreAnnotation = "sdo.dev/onboarding-check-replicas"
	policyPrefix      = "sdo-onboarding-check-"
)

// Config is one Breaker.
type Config struct {
	Client    kubernetes.Interface
	Namespace string
	// Mode is ModeScale, or ModeNetwork which falls back to ModeScale when the
	// cluster does not enforce NetworkPolicy.
	Mode string
	// Bound limits how long a restore waits for the workload to be ready.
	Bound    time.Duration
	Interval time.Duration
	Wait     func(ctx context.Context, d time.Duration) error
}

// Breaker implements onboardcheck.Breaker over Kubernetes.
type Breaker struct {
	cfg  Config
	mode string
	note string
}

// New resolves the effective mode. In ModeNetwork it checks that the CNI
// enforces NetworkPolicy and otherwise falls back to scaling to zero.
func New(ctx context.Context, cfg Config) (*Breaker, error) {
	if cfg.Client == nil || cfg.Namespace == "" || cfg.Wait == nil || cfg.Bound <= 0 || cfg.Interval <= 0 {
		return nil, errors.New("kubebreak: Client, Namespace, Wait, Bound and Interval are required")
	}
	b := &Breaker{cfg: cfg}
	switch cfg.Mode {
	case "", ModeScale:
		b.mode = ModeScale
	case ModeNetwork:
		enforced, which := enforcesNetworkPolicy(ctx, cfg.Client)
		if enforced {
			b.mode, b.note = ModeNetwork, "deny-ingress NetworkPolicy ("+which+" enforces it)"
		} else {
			b.mode, b.note = ModeScale, "scale to zero (this cluster's CNI does not enforce NetworkPolicy, so the deny-ingress policy was not used)"
		}
	default:
		return nil, fmt.Errorf("kubebreak: unknown mode %q (scale or network)", cfg.Mode)
	}
	return b, nil
}

// Mode describes how components are broken.
func (b *Breaker) Mode() string {
	if b.note != "" {
		return b.note
	}
	return "scale to zero"
}

var enforcingCNIs = []string{"calico", "cilium", "weave", "antrea", "kube-router", "canal"}

// enforcesNetworkPolicy reports whether kube-system runs a CNI agent known to
// enforce NetworkPolicy. A default kind cluster (kindnet) does not count.
func enforcesNetworkPolicy(ctx context.Context, client kubernetes.Interface) (bool, string) {
	sets, err := client.AppsV1().DaemonSets(metav1.NamespaceSystem).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, ""
	}
	for _, s := range sets.Items {
		for _, name := range enforcingCNIs {
			if strings.Contains(s.Name, name) {
				return true, name
			}
		}
	}
	return false, ""
}

// workload is a Deployment or StatefulSet reduced to what the breaker needs.
type workload struct {
	replicas *int32
	ready    int32
	selector *metav1.LabelSelector
	annot    map[string]string
	apply    func(ctx context.Context, replicas int32, annot map[string]string) error
	refresh  func(ctx context.Context) error
}

// split reads a component's workload reference: a bare name, or Kind/name.
func split(ref string) (kind, name string) {
	if k, n, ok := strings.Cut(ref, "/"); ok {
		return k, n
	}
	return "", ref
}

func bare(ref string) string { _, name := split(ref); return name }

func (b *Breaker) find(ctx context.Context, ref string) (*workload, error) {
	apps := b.cfg.Client.AppsV1()
	ns := b.cfg.Namespace
	kind, name := split(ref)
	if kind != "" && kind != "Deployment" && kind != "StatefulSet" {
		return nil, fmt.Errorf("workload %s: only a Deployment or StatefulSet can be scaled", ref)
	}
	if kind == "StatefulSet" {
		return b.findStatefulSet(ctx, name)
	}
	if d, err := apps.Deployments(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
		w := &workload{}
		load := func(d *appsv1.Deployment) {
			w.replicas, w.ready, w.selector, w.annot = d.Spec.Replicas, d.Status.ReadyReplicas, d.Spec.Selector, d.Annotations
		}
		load(d)
		w.apply = func(ctx context.Context, n int32, annot map[string]string) error {
			cur, err := apps.Deployments(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			cur.Spec.Replicas, cur.Annotations = &n, annot
			_, err = apps.Deployments(ns).Update(ctx, cur, metav1.UpdateOptions{})
			return err
		}
		w.refresh = func(ctx context.Context) error {
			cur, err := apps.Deployments(ns).Get(ctx, name, metav1.GetOptions{})
			if err == nil {
				load(cur)
			}
			return err
		}
		return w, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	} else if kind == "Deployment" {
		return nil, fmt.Errorf("no Deployment %q in namespace %s", name, ns)
	}
	return b.findStatefulSet(ctx, name)
}

func (b *Breaker) findStatefulSet(ctx context.Context, name string) (*workload, error) {
	apps := b.cfg.Client.AppsV1()
	ns := b.cfg.Namespace
	s, err := apps.StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("no Deployment or StatefulSet %q in namespace %s", name, ns)
		}
		return nil, err
	}
	w := &workload{}
	load := func(s *appsv1.StatefulSet) {
		w.replicas, w.ready, w.selector, w.annot = s.Spec.Replicas, s.Status.ReadyReplicas, s.Spec.Selector, s.Annotations
	}
	load(s)
	w.apply = func(ctx context.Context, n int32, annot map[string]string) error {
		cur, err := apps.StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		cur.Spec.Replicas, cur.Annotations = &n, annot
		_, err = apps.StatefulSets(ns).Update(ctx, cur, metav1.UpdateOptions{})
		return err
	}
	w.refresh = func(ctx context.Context) error {
		cur, err := apps.StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			load(cur)
		}
		return err
	}
	return w, nil
}

func desired(w *workload) int32 {
	if w.replicas == nil {
		return 1
	}
	return *w.replicas
}

func policyName(workload string) string { return policyPrefix + workload }

// Break makes the component's workload unavailable.
func (b *Breaker) Break(ctx context.Context, c appmap.Component) error {
	w, err := b.find(ctx, c.Workload)
	if err != nil {
		return err
	}
	if _, broken := w.annot[restoreAnnotation]; broken {
		return fmt.Errorf("workload %s is already broken by an earlier check (annotation %s); restore it first", c.Workload, restoreAnnotation)
	}
	if b.mode == ModeNetwork {
		if w.selector == nil || len(w.selector.MatchLabels) == 0 {
			return fmt.Errorf("workload %s has no matchLabels selector to deny traffic to", c.Workload)
		}
		policy := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: policyName(bare(c.Workload)), Namespace: b.cfg.Namespace},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{MatchLabels: w.selector.MatchLabels},
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			},
		}
		_, err := b.cfg.Client.NetworkingV1().NetworkPolicies(b.cfg.Namespace).Create(ctx, policy, metav1.CreateOptions{})
		return err
	}
	annot := map[string]string{}
	for k, v := range w.annot {
		annot[k] = v
	}
	annot[restoreAnnotation] = strconv.Itoa(int(desired(w)))
	return w.apply(ctx, 0, annot)
}

// Restore undoes Break and verifies the workload is ready again. An error
// means the restore is uncertain.
func (b *Breaker) Restore(ctx context.Context, c appmap.Component) error {
	w, err := b.find(ctx, c.Workload)
	if err != nil {
		return fmt.Errorf("restore %s: %w", c.Workload, err)
	}
	if b.mode == ModeNetwork {
		err := b.cfg.Client.NetworkingV1().NetworkPolicies(b.cfg.Namespace).Delete(ctx, policyName(bare(c.Workload)), metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("restore %s: delete NetworkPolicy: %w", c.Workload, err)
		}
	} else if saved, ok := w.annot[restoreAnnotation]; ok {
		n, err := strconv.Atoi(saved)
		if err != nil {
			return fmt.Errorf("restore %s: bad %s annotation %q", c.Workload, restoreAnnotation, saved)
		}
		annot := map[string]string{}
		for k, v := range w.annot {
			if k != restoreAnnotation {
				annot[k] = v
			}
		}
		if err := w.apply(ctx, int32(n), annot); err != nil {
			return fmt.Errorf("restore %s: scale back to %d: %w", c.Workload, n, err)
		}
	}
	return b.awaitReady(ctx, bare(c.Workload), w)
}

func (b *Breaker) awaitReady(ctx context.Context, name string, w *workload) error {
	for waited := time.Duration(0); ; waited += b.cfg.Interval {
		if err := w.refresh(ctx); err != nil {
			return fmt.Errorf("restore %s: %w", name, err)
		}
		_, annotated := w.annot[restoreAnnotation]
		policyGone := true
		if _, err := b.cfg.Client.NetworkingV1().NetworkPolicies(b.cfg.Namespace).Get(ctx, policyName(name), metav1.GetOptions{}); err == nil {
			policyGone = false
		}
		if !annotated && policyGone && w.ready >= desired(w) {
			return nil
		}
		if waited >= b.cfg.Bound {
			return fmt.Errorf("restore %s: %d of %d replicas ready after %s (annotation left: %v, policy removed: %v)", name, w.ready, desired(w), b.cfg.Bound, annotated, policyGone)
		}
		if err := b.cfg.Wait(ctx, b.cfg.Interval); err != nil {
			return fmt.Errorf("restore %s: %w", name, err)
		}
	}
}
