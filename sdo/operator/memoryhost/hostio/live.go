package hostio

import (
	"context"
	"errors"
	"fmt"
	"io"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"net"
	"net/http"
	"net/url"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strconv"
	"strings"
	"time"
)

// LiveReadView exposes reads only. Kubernetes credentials belong to the host;
// it never invokes a controller callback or submits a mutation (even dry-run).
type LiveReadView struct {
	*sdktest.ReplayReadView
	ctx    context.Context
	client kubernetes.Interface
}

func NewLiveReadView(ctx context.Context, snapshot sdktest.Snapshot, client kubernetes.Interface) *LiveReadView {
	return &LiveReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot}, ctx: ctx, client: client}
}
func (v *LiveReadView) Logs(ref sdk.ObjectRef, container string, since time.Duration) (string, error) {
	if ref.Kind != "Pod" || ref.Namespace != v.Namespace() || ref.Name == "" || since < 0 || v.client == nil {
		return "", errors.New("logs require a namespace-scoped Pod, nonnegative duration and Kubernetes reader")
	}
	limit := int64(64 * 1024)
	seconds := int64(since.Seconds())
	options := &corev1.PodLogOptions{Container: container, LimitBytes: &limit}
	if seconds > 0 {
		options.SinceSeconds = &seconds
	}
	ctx, cancel := context.WithTimeout(v.ctx, 5*time.Second)
	defer cancel()
	data, err := v.client.CoreV1().Pods(ref.Namespace).GetLogs(ref.Name, options).DoRaw(ctx)
	return string(data), err
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
	for _, s := range v.Services() {
		if s.Name == ref.Name && s.Namespace == ref.Namespace && len(s.Spec.Ports) > 0 {
			port = s.Spec.Ports[0].Port
		}
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
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
	return program.ProbeResult{Success: response.StatusCode >= 200 && response.StatusCode < 400, Detail: fmt.Sprintf("HTTP %d", response.StatusCode)}, nil
}
