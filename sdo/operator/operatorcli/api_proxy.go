package operatorcli

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/rest"
	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/kubeproxy"
)

const APIProxyAddress = "127.0.0.1:18080"

// RunAPIProxy uses only the Job Pod's in-cluster service-account identity.
// Provider credentials and incoming Authorization are never inspected or stored.
func RunAPIProxy(ctx context.Context, args []string) error {
	if len(args) == 1 && args[0] == "ready" {
		c, err := net.DialTimeout("tcp", APIProxyAddress, time.Second)
		if err == nil {
			c.Close()
		}
		return err
	}
	flags := flag.NewFlagSet("api-proxy", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	incident := flags.String("incident", "", "controller-assigned incident")
	captureNamespace := flags.String("capture-namespace", "", "application namespace for private write images")
	controlNamespace := flags.String("control-namespace", "", "SDO control namespace")
	directory := flags.String("directory", "", "runtime history directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *incident == "" || *directory == "" || flags.NArg() != 0 {
		return errors.New("api-proxy requires --incident and --directory")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	config.QPS, config.Burst = 50, 100
	reads, writes, err := apiProxyTransports(config)
	if err != nil {
		return err
	}
	upstream, err := url.Parse(config.Host)
	if err != nil {
		return err
	}
	// The directory lease exposes this forwarding generation even if storage
	// writes fail and an old closed manifest remains readable.
	lease, leaseErr := apihistory.BeginWriter(*directory)
	if leaseErr == nil {
		defer lease.Close()
	}
	var recorder kubeproxy.Recorder
	store, storeErr := apihistory.Open(*directory, *incident, apihistory.Limits{})
	if storeErr == nil {
		recorder = store
		defer store.Close()
	}
	if *captureNamespace != "" {
		core, err := typedcore.NewForConfigAndClient(config, &http.Client{Transport: reads})
		if err != nil {
			return err
		}
		capture := newScriptRecorder(ctx, upstream, reads)
		owner := os.Getenv("SDO_PROXY_POD_UID")
		if owner == "" {
			return errors.New("proxy Pod identity required for write capture")
		}
		prefix := fmt.Sprintf("sdo-proxy-%x", sha256.Sum256([]byte(*incident)))[:30]
		images := scriptCaptureStore{maps: core.ConfigMaps(*captureNamespace), namespace: *captureNamespace, application: prefix, jobUID: owner}
		capture.persistImage = images.save
		capture.persistReceipt = images.receipt
		recorder = proxyRecorders{recorder, capture}
	}
	// Disabled markers and writer liveness make failed initialization Unknown.
	// Recording failure must not prevent the forwarding endpoint from starting.
	gate, err := apihistory.PrepareMutationGate(*directory)
	if err != nil {
		return err
	}
	server := &http.Server{Addr: APIProxyAddress, Handler: kubeproxy.NewWithPolicy(upstream, reads, writes, recorder, gate, *controlNamespace), ReadHeaderTimeout: 10 * time.Second}
	listener, err := net.Listen("tcp", APIProxyAddress)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			server.Close()
		case <-done:
		}
	}()
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// apiProxyTransports shares one authenticated connection pool for an incident.
// The proxy strips retry-authorizing headers and replay bodies on writes.
func apiProxyTransports(config *rest.Config) (http.RoundTripper, http.RoundTripper, error) {
	tlsConfig, err := rest.TLSConfigFor(config)
	if err != nil {
		return nil, nil, err
	}
	base := &http.Transport{TLSClientConfig: tlsConfig, DisableCompression: true, MaxIdleConnsPerHost: 32, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 60 * time.Second}
	reads, err := rest.HTTPWrappersForConfig(config, base)
	if err != nil {
		return nil, nil, err
	}
	return reads, reads, nil
}

// One proxy event fan-out adds write images to the existing incident recorder.
type proxyRecorders []kubeproxy.Recorder

func (rs proxyRecorders) Record(e apihistory.Event, body []byte) {
	for _, r := range rs {
		if r != nil {
			r.Record(e, body)
		}
	}
}
func (rs proxyRecorders) Gap() {
	for _, r := range rs {
		if r != nil {
			r.Gap()
		}
	}
}
