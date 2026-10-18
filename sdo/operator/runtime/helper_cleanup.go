package runtime

import (
	"context"
	"fmt"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ResponderHelperLabel marks pods and Jobs a responder created for its own
// investigation or repair checks (for example a curl or DNS debug pod). The
// controller deletes them when the responder completes, before verifying
// recovery, so they neither linger nor skew the health verdict.
const ResponderHelperLabel = "sdo.dev/responder-helper"

// helperCleanupTimeout bounds cleanup so it never delays closure noticeably.
const helperCleanupTimeout = 10 * time.Second

// HelperCleaner deletes responder helpers and names what it deleted.
type HelperCleaner interface {
	CleanupHelpers(ctx context.Context) ([]string, error)
}

// KubernetesHelperCleaner deletes labelled helper pods and Jobs in the given
// namespaces, normally the application and control namespaces.
type KubernetesHelperCleaner struct {
	Client     kubernetes.Interface
	Namespaces []string
}

// CleanupHelpers implements HelperCleaner. It deletes only objects labelled
// ResponderHelperLabel=true and returns them as sorted Kind/namespace/name.
func (c KubernetesHelperCleaner) CleanupHelpers(ctx context.Context) ([]string, error) {
	selector := metav1.ListOptions{LabelSelector: ResponderHelperLabel + "=true"}
	background := metav1.DeletePropagationBackground
	options := metav1.DeleteOptions{PropagationPolicy: &background}
	deleted := []string{}
	var failures []error
	seen := map[string]bool{}
	for _, namespace := range c.Namespaces {
		if namespace == "" || seen[namespace] {
			continue
		}
		seen[namespace] = true
		jobs, err := c.Client.BatchV1().Jobs(namespace).List(ctx, selector)
		if err != nil {
			failures = append(failures, fmt.Errorf("list helper Jobs in %s: %w", namespace, err))
		} else {
			for _, job := range jobs.Items {
				err := c.Client.BatchV1().Jobs(namespace).Delete(ctx, job.Name, options)
				if err != nil && !apierrors.IsNotFound(err) {
					failures = append(failures, fmt.Errorf("delete helper Job %s/%s: %w", namespace, job.Name, err))
					continue
				}
				deleted = append(deleted, "Job/"+namespace+"/"+job.Name)
			}
		}
		pods, err := c.Client.CoreV1().Pods(namespace).List(ctx, selector)
		if err != nil {
			failures = append(failures, fmt.Errorf("list helper pods in %s: %w", namespace, err))
			continue
		}
		for _, pod := range pods.Items {
			err := c.Client.CoreV1().Pods(namespace).Delete(ctx, pod.Name, options)
			if err != nil && !apierrors.IsNotFound(err) {
				failures = append(failures, fmt.Errorf("delete helper pod %s/%s: %w", namespace, pod.Name, err))
				continue
			}
			deleted = append(deleted, "Pod/"+namespace+"/"+pod.Name)
		}
	}
	sort.Strings(deleted)
	if len(failures) > 0 {
		return deleted, fmt.Errorf("clean up responder helpers: %v", failures)
	}
	return deleted, nil
}
