package prober

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/traffic"
)

// PrimitiveRequest names one read-only probe from the isolated prober pod.
type PrimitiveRequest struct {
	PodIP string             `json:"pod_ip,omitempty"`
	To    sdk.ObjectRef      `json:"to"`
	Port  int32              `json:"port"`
	HTTP  *program.HTTPProbe `json:"http,omitempty"`
}

func (p *Prober) Primitive(ctx context.Context, request PrimitiveRequest) (program.ProbeResult, error) {
	if request.To.Kind != "Service" && request.To.Kind != "Pod" || request.To.Namespace != p.config.Namespace || request.To.Name == "" || request.Port < 1 || request.Port > 65535 {
		return program.ProbeResult{}, fmt.Errorf("primitive probe requires a Service or Pod in namespace %q and a valid port", p.config.Namespace)
	}
	if request.To.Kind == "Pod" {
		ip := net.ParseIP(request.PodIP)
		if request.HTTP != nil || ip == nil || !ip.IsGlobalUnicast() {
			return program.ProbeResult{}, fmt.Errorf("Pod primitive requires a captured unicast Pod IP and TCP")
		}
	} else if request.PodIP != "" {
		return program.ProbeResult{}, fmt.Errorf("Service primitive cannot override its address")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if request.HTTP == nil {
		address := p.config.LinkAddress(traffic.Link{To: request.To.Name, Port: int(request.Port)})
		if request.To.Kind == "Pod" {
			address = net.JoinHostPort(request.PodIP, strconv.Itoa(int(request.Port)))
		}
		connection, err := p.config.DialContext(ctx, "tcp", address)
		if err != nil {
			return program.ProbeResult{Detail: err.Error()}, nil
		}
		_ = connection.Close()
		return program.ProbeResult{Success: true}, nil
	}
	method := strings.ToUpper(request.HTTP.Method)
	if method == "" {
		method = http.MethodGet
	}
	if method != http.MethodGet && method != http.MethodHead && method != http.MethodOptions {
		return program.ProbeResult{}, fmt.Errorf("HTTP probe method %q is not read-only", method)
	}
	if !strings.HasPrefix(request.HTTP.Path, "/") || strings.HasPrefix(request.HTTP.Path, "//") {
		return program.ProbeResult{}, fmt.Errorf("HTTP probe path must be absolute within the Service")
	}
	base := p.config.BaseURL(traffic.Target{Service: request.To.Name, Port: int(request.Port)})
	httpRequest, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(base, "/")+request.HTTP.Path, nil)
	if err != nil {
		return program.ProbeResult{}, err
	}
	for key, value := range request.HTTP.Headers {
		httpRequest.Header.Set(key, value)
	}
	response, err := p.config.Client.Do(httpRequest)
	if err != nil {
		return program.ProbeResult{Detail: err.Error()}, nil
	}
	defer response.Body.Close()
	return program.ProbeResult{Success: response.StatusCode < 400, Detail: fmt.Sprintf("HTTP %d", response.StatusCode)}, nil
}
