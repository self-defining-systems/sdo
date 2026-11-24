package hostio

import (
	"context"
	"errors"
	"fmt"
	"io"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net"
	"net/http"
	"net/url"
	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"strconv"
	"strings"
	"time"
)

// ReadBackends are the host's Kubernetes read transports. Credentials belong to
// the host; none of these can write through the reads the view serves.
type ReadBackends struct {
	Client kubernetes.Interface
	// Config reaches the API for raw reads and pod exec. Nil makes those reads
	// fail with a named error instead of returning an empty answer.
	Config *rest.Config
	// Exec runs the fixed read commands Observe and DatabaseUsers build. Nil
	// uses the pod exec subresource through Client and Config.
	Exec kubereads.PodExec
	// APITransport resolves one raw read. Nil uses Client and Config.
	APITransport func(program.KubernetesRequest) (kubereads.APIDoer, *url.URL, error)
	// ProtectedNamespace is the SDO control namespace every read refuses.
	ProtectedNamespace string
	// ApplicationMap is the accepted application map the host was built with.
	// A store read grounds its target in it, so a view without one refuses
	// every store read (its replay holds no recorded AppMap call). Nil keeps
	// the replay answer.
	ApplicationMap *program.AppMap
}

// ApplicationMapFromYAML parses the application map region embedded in the
// host's build; empty text is no map (nil).
func ApplicationMapFromYAML(text string) (*program.AppMap, error) {
	if strings.TrimSpace(text) == "" {
		return nil, nil
	}
	parsed, err := appmap.Parse(text)
	if err != nil {
		return nil, fmt.Errorf("embedded application map: %w", err)
	}
	m := parsed.Program()
	return &m, nil
}

// AppMap is the accepted application map the host carries, else the replay's.
func (v *LiveReadView) AppMap() (program.AppMap, error) {
	if v.backends.ApplicationMap != nil {
		return *v.backends.ApplicationMap, nil
	}
	return v.ReplayReadView.AppMap()
}

func (b ReadBackends) exec() kubereads.PodExec {
	if b.Exec != nil {
		return b.Exec
	}
	return kubereads.SPDYExec{Client: b.Client, Config: b.Config}
}

// LiveReadView exposes reads only. Kubernetes credentials belong to the host;
// it never invokes a controller callback or submits a mutation (even dry-run).
type LiveReadView struct {
	*sdktest.ReplayReadView
	ctx      context.Context
	client   kubernetes.Interface
	backends ReadBackends
}

func NewLiveReadView(ctx context.Context, snapshot sdktest.Snapshot, backends ReadBackends) *LiveReadView {
	return &LiveReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot}, ctx: ctx, client: backends.Client, backends: backends}
}

// QueryMetrics answers from the frozen observation. The host has no metrics
// source: a query the observation does not hold is reported as
// metrics.NotFrozenError. Accepted playbooks query the controller view directly.
func (v *LiveReadView) QueryMetrics(q metrics.Query) metrics.Result {
	if err := q.Validate(); err != nil {
		return metrics.Result{Query: q, Error: err.Error()}
	}
	if r, ok := v.Snapshot.Metrics[q.ID]; ok && r.Query == q {
		return r
	}
	return metrics.Result{Query: q, Error: metrics.NotFrozenError}
}

// MetricResult, AlertsResult, TrafficWindow and ServiceReachability answer
// from the supplied observation. The embedded replay holds no recording.
func (v *LiveReadView) MetricResult(id string) (metrics.Result, bool) {
	return v.Snapshot.MetricResult(id)
}
func (v *LiveReadView) AlertsResult() (metrics.AlertsResult, bool) { return v.Snapshot.AlertsResult() }
func (v *LiveReadView) TrafficWindow(name string) (traffic.Window, bool) {
	return v.Snapshot.TrafficWindow(name)
}
func (v *LiveReadView) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	return v.Snapshot.ServiceReachability()
}
func (v *LiveReadView) Logs(ref sdk.ObjectRef, container string, since time.Duration) (string, error) {
	if ref.Kind != "Pod" || ref.Namespace != v.Namespace() || ref.Name == "" || since < 0 || v.client == nil {
		return "", errors.New("logs require a namespace-scoped Pod, nonnegative duration and Kubernetes reader")
	}
	tail := int64(program.DefaultLogTailLines)
	seconds := int64(since.Seconds())
	options := &corev1.PodLogOptions{Container: container, TailLines: &tail}
	if seconds > 0 {
		options.SinceSeconds = &seconds
	}
	ctx, cancel := context.WithTimeout(v.ctx, 5*time.Second)
	defer cancel()
	data, err := v.client.CoreV1().Pods(ref.Namespace).GetLogs(ref.Name, options).DoRaw(ctx)
	return program.NewestLogBytes(string(data), program.MaxLogBytes), err
}

// LogsWithOptions reads bounded container logs live; the embedded replay view
// holds no recording and must not answer it.
func (v *LiveReadView) LogsWithOptions(ref sdk.ObjectRef, options corev1.PodLogOptions) (string, error) {
	if ref.Kind != "Pod" || ref.Namespace != v.Namespace() || ref.Name == "" || v.client == nil {
		return "", errors.New("logs require a namespace-scoped Pod and Kubernetes reader")
	}
	bounded, limit, err := program.BoundedLogOptions(options)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(v.ctx, 5*time.Second)
	defer cancel()
	data, err := v.client.CoreV1().Pods(ref.Namespace).GetLogs(ref.Name, &bounded).DoRaw(ctx)
	return program.NewestLogBytes(string(data), limit), err
}
func (v *LiveReadView) target(ref sdk.ObjectRef, port int32) (string, error) {
	if ref.Namespace != v.Namespace() || ref.Name == "" || port < 1 || port > 65535 {
		return "", errors.New("probe requires a namespace-scoped target and valid port")
	}
	switch ref.Kind {
	case "Pod":
		for _, p := range v.Pods() {
			if p.Name == ref.Name && p.Namespace == ref.Namespace && !p.Spec.HostNetwork && net.ParseIP(p.Status.PodIP) != nil {
				return net.JoinHostPort(p.Status.PodIP, strconv.Itoa(int(port))), nil
			}
		}
	case "Service":
		for _, s := range v.Services() {
			if s.Name != ref.Name || s.Namespace != ref.Namespace {
				continue
			}
			if s.Spec.Type == corev1.ServiceTypeExternalName {
				return "", errors.New("ExternalName probes are unavailable")
			}
			for _, p := range s.Spec.Ports {
				if p.Port == port && (p.Protocol == "" || p.Protocol == corev1.ProtocolTCP) {
					if net.ParseIP(s.Spec.ClusterIP) == nil {
						return "", errors.New("probe requires a Service cluster IP")
					}
					return net.JoinHostPort(s.Spec.ClusterIP, strconv.Itoa(int(port))), nil
				}
			}
		}
	}
	return "", errors.New("probe target is absent from mirror or has no matching TCP port")
}
func (v *LiveReadView) ProbeTCP(ref sdk.ObjectRef, port int32) (program.ProbeResult, error) {
	target, err := v.target(ref, port)
	if err != nil {
		return program.ProbeResult{}, err
	}
	ctx, cancel := context.WithTimeout(v.ctx, 5*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", target)
	if err != nil {
		if ctx.Err() != nil {
			return program.ProbeResult{}, ctx.Err()
		}
		return program.ProbeResult{Detail: err.Error()}, nil
	}
	_ = conn.Close()
	return program.ProbeResult{Success: true}, nil
}
func (v *LiveReadView) ProbeHTTP(ref sdk.ObjectRef, p program.HTTPProbe) (program.ProbeResult, error) {
	if p.Method != "" && p.Method != "GET" && p.Method != "HEAD" {
		return program.ProbeResult{}, errors.New("HTTP primitive permits GET and HEAD only")
	}
	if ref.Kind != "Service" {
		return program.ProbeResult{}, errors.New("HTTP probe requires a Service")
	}
	var port int32
	found := false
	for _, s := range v.Services() {
		if s.Name == ref.Name && s.Namespace == ref.Namespace {
			found = true
			port, _ = program.HTTPProbePort(s)
		}
	}
	if !found {
		return program.ProbeResult{}, fmt.Errorf("probe target Service %s/%s is absent from the memory host's mirror", ref.Namespace, ref.Name)
	}
	if port == 0 {
		return program.ProbeResult{}, fmt.Errorf("probe target Service %s/%s declares no TCP port for HTTP", ref.Namespace, ref.Name)
	}
	target, err := v.target(ref, port)
	if err != nil {
		return program.ProbeResult{}, err
	}
	path, err := url.Parse(p.Path)
	if err != nil || !strings.HasPrefix(p.Path, "/") || path.IsAbs() || path.Host != "" {
		return program.ProbeResult{}, errors.New("HTTP probe requires a relative absolute-path reference")
	}
	ctx, cancel := context.WithTimeout(v.ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, p.Method, "http://"+target+path.String(), nil)
	if err != nil {
		return program.ProbeResult{}, err
	}
	for k, value := range p.Headers {
		req.Header.Set(k, value)
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return program.ProbeResult{}, ctx.Err()
		}
		return program.ProbeResult{Detail: err.Error()}, nil
	}
	defer response.Body.Close()
	result := program.ProbeResult{Success: response.StatusCode >= 200 && response.StatusCode < 400, Detail: fmt.Sprintf("HTTP %d", response.StatusCode), Status: response.StatusCode}
	body, _ := io.ReadAll(io.LimitReader(response.Body, program.MaxProbeBodyBytes+1))
	if len(p.Fields) > 0 {
		if len(body) > program.MaxProbeBodyBytes {
			return result, fmt.Errorf("probe response body longer than %d bytes", program.MaxProbeBodyBytes)
		}
		fields, extractErr := program.ExtractJSONFields(body, p.Fields)
		if extractErr != nil {
			return result, extractErr
		}
		result.Fields = fields
	}
	return result, nil
}

// LogMatches scans the container's whole log window live and returns only the matching lines.
func (v *LiveReadView) LogMatches(ref sdk.ObjectRef, container string, since time.Duration, pattern string, max int) (program.LogMatchResult, error) {
	re, err := program.ValidateLogMatchRequest(since, pattern, max)
	if err != nil {
		return program.LogMatchResult{}, err
	}
	if ref.Kind != "Pod" || ref.Namespace != v.Namespace() || ref.Name == "" || v.client == nil || container == "" {
		return program.LogMatchResult{}, errors.New("log matches require a namespace-scoped Pod, a container and a Kubernetes reader")
	}
	ctx, cancel := context.WithTimeout(v.ctx, 15*time.Second)
	defer cancel()
	options := program.LogMatchOptions(container, since)
	stream, err := v.client.CoreV1().Pods(ref.Namespace).GetLogs(ref.Name, &options).Stream(ctx)
	if err != nil {
		return program.LogMatchResult{}, err
	}
	defer stream.Close()
	return program.ScanLogMatches(stream, re, max)
}
