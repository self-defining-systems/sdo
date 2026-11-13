package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"k8s.io/client-go/rest"
	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/kubeproxy"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/sdk"
)

const APIProxyAddress = "127.0.0.1:18080"

// RunAPIProxy uses only the Job Pod's in-cluster service-account identity.
// Provider credentials and incoming Authorization are never inspected or stored.
func RunAPIProxy(ctx context.Context, args []string) error {
	if len(args) == 1 && args[0] == "ready" {
		return processio.DialReady(APIProxyAddress)
	}
	flags := flag.NewFlagSet("api-proxy", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	scopeFlag := flags.String("scope", "", "incident:<id> for a responder, task:<key> for any other agent (read-only)")
	directory := flags.String("directory", "", "runtime history directory")
	recording := flags.Bool("recording", false, "record every Kubernetes call in the history directory (agent.recording)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *scopeFlag == "" || *directory == "" || flags.NArg() != 0 {
		return errors.New("api-proxy requires --scope and --directory")
	}
	scope, err := parseProxyScope(*scopeFlag)
	if err != nil {
		return err
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	reads, writes, err := apiProxyTransports(config)
	if err != nil {
		return err
	}
	upstream, err := url.Parse(config.Host)
	if err != nil {
		return err
	}
	// With recording off the proxy keeps no history: no lease, no store, no admission. It still forwards, forces
	// the field manager and applies the fence; an exec or a write moves no clock because nothing is stored.
	var recorder kubeproxy.Recorder
	if *recording {
		// The directory lease exposes this forwarding generation even if storage
		// writes fail and an old closed manifest remains readable.
		lease, leaseErr := apihistory.BeginWriter(*directory)
		if leaseErr == nil {
			defer lease.Close()
		}
		// A store that cannot open leaves the endpoint up (the startup probe and reads still work) but refuses every
		// write, exec, attach and port-forward: no write is forwarded without a record.
		recorder = unavailableRecorder{}
		store, storeErr := apihistory.Open(*directory, scope.historyIdentity(), apihistory.Limits{})
		if storeErr == nil {
			recorder = store
			defer store.Close()
		}
	}
	// Disabled markers and writer liveness make failed initialization Unknown.
	var gate *apihistory.MutationGate
	if scope.Kind == "incident" {
		gate, err = apihistory.PrepareMutationGate(*directory)
		if err != nil {
			return err
		}
	}
	return processio.ServeHTTP(ctx, APIProxyAddress, kubeproxy.NewRecording(upstream, reads, writes, scope.config(recorder, gate)))
}

// proxyScope is whom the proxy fronts. An incident scope is a responder: it may write, every write is forced to
// the responder field manager, and the incident's mutation fence applies. Any other agent is read-only.
type proxyScope struct{ Kind, ID string }

func parseProxyScope(value string) (proxyScope, error) {
	kind, id, ok := strings.Cut(value, ":")
	if !ok || id == "" || (kind != "incident" && kind != "task") {
		return proxyScope{}, fmt.Errorf("api-proxy scope %q must be incident:<id> or task:<key>", value)
	}
	return proxyScope{Kind: kind, ID: id}, nil
}

// historyIdentity is the identity stored in the history manifest: the incident ID, which readers already match.
func (s proxyScope) historyIdentity() string {
	if s.Kind == "incident" {
		return s.ID
	}
	return s.Kind + ":" + s.ID
}

func (s proxyScope) config(recorder kubeproxy.Recorder, gate *apihistory.MutationGate) kubeproxy.Config {
	if s.Kind == "incident" {
		return kubeproxy.Config{Recorder: recorder, Gate: gate, FieldManager: sdk.ResponderFieldManager}
	}
	return kubeproxy.Config{Recorder: recorder, ReadOnly: true}
}

// unavailableRecorder stands in for a store that failed to open. It records nothing and admits nothing.
type unavailableRecorder struct{}

func (unavailableRecorder) Record(apihistory.Event, []byte) {}
func (unavailableRecorder) Gap()                            {}
func (unavailableRecorder) Admit() error                    { return apihistory.ErrRecordingUnavailable }

// apiProxyTransports shares one authenticated connection pool for an incident.
// The proxy strips retry-authorizing headers and replay bodies on writes.
func apiProxyTransports(config *rest.Config) (http.RoundTripper, http.RoundTripper, error) {
	tlsConfig, err := rest.TLSConfigFor(config)
	if err != nil {
		return nil, nil, err
	}
	base := processio.NewTransport(tlsConfig)
	reads, err := rest.HTTPWrappersForConfig(config, base)
	if err != nil {
		return nil, nil, err
	}
	return reads, reads, nil
}
