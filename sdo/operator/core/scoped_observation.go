package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/sdk"
	"sort"
	"time"
)

// ObserveScoped performs fresh bounded reads for each declared target. Named
// webhooks use GET: unrelated webhook objects and foreign Secrets are never read.
// Failures publish only a categorical reason, never server error bodies.
func ObserveScoped(ctx context.Context, client kubernetes.Interface, scope, ceiling sdk.ObservationScope, generation string) (sdk.ScopedSnapshot, error) {
	if err := scope.Within(ceiling); err != nil {
		return sdk.ScopedSnapshot{}, err
	}
	out := sdk.ScopedSnapshot{Version: 1, Scope: scope, Generation: generation + ":" + ceiling.Identity() + ":" + scope.Identity()}
	targets := append([]sdk.ObservationTarget(nil), scope.Targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].Key() < targets[j].Key() })
	out.Scope.Targets = targets
	for _, t := range targets {
		status := sdk.ObservationStatus{Target: t, State: sdk.ObservationObserved}
		var err error
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		switch t.Kind {
		case "Deployment":
			objects, e := client.AppsV1().Deployments(t.Namespace).List(readCtx, metav1.ListOptions{Limit: sdk.MaxObservationObjects + 1})
			err = e
			if err == nil {
				if objects.Continue != "" || len(objects.Items) > sdk.MaxObservationObjects {
					err = fmt.Errorf("object cap")
				} else {
					for _, o := range objects.Items {
						out.Deployments = append(out.Deployments, sdk.ProjectForeignDeployment(o))
					}
				}
			}
		case "Service":
			objects, e := client.CoreV1().Services(t.Namespace).List(readCtx, metav1.ListOptions{Limit: sdk.MaxObservationObjects + 1})
			err = e
			if err == nil {
				if objects.Continue != "" || len(objects.Items) > sdk.MaxObservationObjects {
					err = fmt.Errorf("object cap")
				} else {
					for _, o := range objects.Items {
						out.Services = append(out.Services, sdk.ProjectForeignService(o))
					}
				}
			}
		case "Endpoints":
			objects, e := client.CoreV1().Endpoints(t.Namespace).List(readCtx, metav1.ListOptions{Limit: sdk.MaxObservationObjects + 1})
			err = e
			if err == nil {
				if objects.Continue != "" || len(objects.Items) > sdk.MaxObservationObjects {
					err = fmt.Errorf("object cap")
				} else {
					for _, o := range objects.Items {
						out.Endpoints = append(out.Endpoints, sdk.ProjectForeignEndpoints(o))
					}
				}
			}
		case "MutatingWebhookConfiguration":
			o, e := client.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(readCtx, t.Name, metav1.GetOptions{})
			err = e
			if err == nil {
				out.Mutating = append(out.Mutating, sdk.ProjectMutatingWebhook(*o))
			}
		case "ValidatingWebhookConfiguration":
			o, e := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(readCtx, t.Name, metav1.GetOptions{})
			err = e
			if err == nil {
				out.Validating = append(out.Validating, sdk.ProjectValidatingWebhook(*o))
			}
		}
		cancel()
		if err == nil {
			encoded, _ := json.Marshal(out)
			if len(encoded) > 2<<20 {
				err = fmt.Errorf("output cap")
				out.Deployments = nil
				out.Services = nil
				out.Endpoints = nil
				out.Mutating = nil
				out.Validating = nil
				for i := range out.Statuses {
					out.Statuses[i].State = "unavailable"
					out.Statuses[i].Reason = "output cap exceeded"
				}
			}
		}
		if err != nil {
			switch {
			case namedObservationAbsent(err, t): // successful named absence
			case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
				status.State = "forbidden"
				status.Reason = "authorization denied"
			case apierrors.IsNotFound(err), apierrors.IsMethodNotSupported(err):
				status.State = "unsupported"
				status.Reason = "API unavailable"
			default:
				status.State = "unavailable"
				status.Reason = "read failed or object cap exceeded"
			}
		}
		out.Statuses = append(out.Statuses, status)
	}
	return out, nil
}

func namedObservationAbsent(err error, target sdk.ObservationTarget) bool {
	if !target.Cluster() || !apierrors.IsNotFound(err) {
		return false
	}
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return false
	}
	details := status.Status().Details
	return details != nil && details.Name == target.Name
}
