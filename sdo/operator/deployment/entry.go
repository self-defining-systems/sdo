package deployment

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/deployment/internal/deploymentio"
	"sdo.dev/operator/rendered"
)

// DefaultEntryWait bounds how long the entry check waits for every entry
// point to answer after the workloads are ready.
const DefaultEntryWait = 90 * time.Second

// EntryPoint is one Service port users enter the application through.
type EntryPoint struct {
	Service string
	Port    int64
	// HTTP marks a port that speaks HTTP (named http*, appProtocol http, or
	// 80, 8080, 3000): it must answer a GET / with a status below 500.
	HTTP bool
}

func (e EntryPoint) String() string { return fmt.Sprintf("%s:%d", e.Service, e.Port) }

// EntryDialer reports whether one entry point of the namespace answers: a TCP
// connection, plus a GET / below 500 for an HTTP port. Production dials the
// Service's cluster DNS name; tests inject one.
type EntryDialer interface {
	Answer(ctx context.Context, namespace string, entry EntryPoint) error
}

// EntryResult is the outcome of one entry point.
type EntryResult struct {
	Entry EntryPoint
	Err   error
}

// EntryPoints are the TCP Service ports the desired set exposes to users:
// Services of type NodePort or LoadBalancer and the Services Ingress rules
// route to. With none exposed, every Service with a cluster IP is an entry.
func EntryPoints(set rendered.DesiredSet) []EntryPoint {
	services := map[string]*unstructured.Unstructured{}
	exposed := map[string]bool{}
	for _, o := range set.Objects() {
		switch {
		case o.Ref.Kind == "Service" && o.Ref.APIVersion == "v1" && !o.Foreign:
			services[o.Ref.Name] = o.Object
			kind, _, _ := unstructured.NestedString(o.Object.Object, "spec", "type")
			if kind == "NodePort" || kind == "LoadBalancer" {
				exposed[o.Ref.Name] = true
			}
		case o.Ref.Kind == "Ingress" && !o.Foreign:
			for _, name := range ingressServices(o.Object) {
				exposed[name] = true
			}
		}
	}
	var names []string
	for name, svc := range services {
		if len(exposed) > 0 && !exposed[name] {
			continue
		}
		if ip, _, _ := unstructured.NestedString(svc.Object, "spec", "clusterIP"); ip == "None" {
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)
	var out []EntryPoint
	for _, name := range names {
		ports, _, _ := unstructured.NestedSlice(services[name].Object, "spec", "ports")
		for _, p := range ports {
			port, _ := p.(map[string]any)
			number, ok := port["port"].(int64)
			if !ok {
				if f, isFloat := port["port"].(float64); isFloat {
					number, ok = int64(f), true
				}
			}
			if !ok {
				continue
			}
			// Users enter over TCP (Kubernetes' default protocol); a UDP or SCTP
			// port cannot answer a connection, so it is not an entry.
			if transport, _ := port["protocol"].(string); transport != "" && transport != "TCP" {
				continue
			}
			portName, _ := port["name"].(string)
			protocol, _ := port["appProtocol"].(string)
			out = append(out, EntryPoint{Service: name, Port: number,
				HTTP: strings.HasPrefix(portName, "http") || strings.HasPrefix(protocol, "http") || number == 80 || number == 8080 || number == 3000})
		}
	}
	return out
}

func ingressServices(ingress *unstructured.Unstructured) []string {
	var names []string
	if name, ok, _ := unstructured.NestedString(ingress.Object, "spec", "defaultBackend", "service", "name"); ok {
		names = append(names, name)
	}
	rules, _, _ := unstructured.NestedSlice(ingress.Object, "spec", "rules")
	for _, r := range rules {
		rule, _ := r.(map[string]any)
		paths, _, _ := unstructured.NestedSlice(rule, "http", "paths")
		for _, p := range paths {
			path, _ := p.(map[string]any)
			if name, ok, _ := unstructured.NestedString(path, "backend", "service", "name"); ok {
				names = append(names, name)
			}
		}
	}
	return names
}

// probeEntries asks the dialer for every entry point until all answer or the
// wait bound passes, and returns the last result of each.
func (d Deployer) probeEntries(ctx context.Context, entries []EntryPoint) []EntryResult {
	wait := d.EntryWait
	if wait <= 0 {
		wait = DefaultEntryWait
	}
	const interval = 5 * time.Second
	results := make([]EntryResult, len(entries))
	for i, e := range entries {
		results[i].Entry = e
	}
	for waited := time.Duration(0); ; waited += interval {
		failing := false
		for i := range results {
			if waited > 0 && results[i].Err == nil {
				continue
			}
			results[i].Err = d.EntryDialer.Answer(ctx, d.Namespace, results[i].Entry)
			failing = failing || results[i].Err != nil
		}
		if !failing || waited+interval > wait || d.sleep(ctx, interval) != nil {
			return results
		}
	}
}

// ClusterEntryDialer dials a Service by its cluster DNS name. It works where
// the process runs inside the application's cluster.
type ClusterEntryDialer struct{}

// Answer implements EntryDialer.
func (ClusterEntryDialer) Answer(ctx context.Context, namespace string, entry EntryPoint) error {
	return deploymentio.AnswerEntry(ctx, entry.Service+"."+namespace+".svc", entry.Port, entry.HTTP)
}
