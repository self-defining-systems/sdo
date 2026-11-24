package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// AppContent identifies the deployment inputs independently of the tool used to deploy them.
// Attachments identify the optional run inputs kept by a prepared subject across a swap.
type AppContent struct {
	Render      string `json:"render"`
	Images      string `json:"images"`
	Data        string `json:"data"`
	Attachments string `json:"attachments,omitempty"`
}

func readAppContent(out string) (*AppContent, error) {
	last := lastNonEmpty([]byte(out))
	if isUnsupported(0, last) {
		return nil, nil
	}
	var content AppContent
	if err := json.Unmarshal([]byte(last), &content); err != nil {
		return nil, fmt.Errorf("invalid app-content response: %w", err)
	}
	if content.Render == "" || content.Images == "" || content.Data == "" {
		return nil, fmt.Errorf("app-content response requires render, images and data")
	}
	return &content, nil
}

func appDifference(have, want *Prepared) string {
	switch {
	case have.Namespace != want.Namespace:
		return "namespace"
	case have.AppContent == nil && want.AppContent == nil:
		// Legacy workloads only identify the copy by the pinned pack location.
		if have.AppPack != want.AppPack {
			return "app_pack (no app-content)"
		}
	case have.AppContent == nil || want.AppContent == nil:
		return "app_content (missing)"
	default:
		var fields []string
		if have.AppContent.Render != want.AppContent.Render {
			fields = append(fields, "render")
		}
		if have.AppContent.Images != want.AppContent.Images {
			fields = append(fields, "images")
		}
		if have.AppContent.Data != want.AppContent.Data {
			fields = append(fields, "data")
		}
		return strings.Join(fields, ", ")
	}
	return ""
}

const appContentTimeout = 30 * time.Second

type appContentRequest struct {
	host, dir, workload, pack, namespace string
}

func (q appContentRequest) env() []string {
	return []string{"BENCH_PACK=" + q.pack, "BENCH_NAMESPACE=" + q.namespace}
}

type appContentResult struct {
	content *AppContent
	err     error
}

// desiredAppContent runs once per distinct host and deployment input per assignment pass.
// It needs no cluster and can answer before a lane has been selected.
func (m *Manager) desiredAppContent(ctx context.Context, q appContentRequest) (*AppContent, error) {
	if _, builtin := builtinWorkloads[q.workload]; builtin {
		return nil, nil
	}
	if result, ok := m.appContents[q]; ok {
		return result.content, result.err
	}
	ctx, cancel := context.WithTimeout(ctx, appContentTimeout)
	defer cancel()
	run := m.appContentRunner
	if run == nil {
		run = HostRunner(m.inv)
	}
	env := append(m.inv.Hosts[q.host].EnvFor(q.workload), q.env()...)
	cmd := ExportEnv(WorkloadEnv(WorkloadCall{Verb: VerbAppContent, Env: env})) +
		"cd " + shellQuote(q.dir) + " || exit; sh -c " + shellQuote(q.workload+" "+VerbAppContent) +
		`; status=$?; if [ "$status" = 64 ]; then echo '{"unsupported":true}'; else exit "$status"; fi`
	out, err := run(ctx, q.host, cmd)
	var content *AppContent
	if err == nil {
		content, err = readAppContent(out)
	}
	if err != nil {
		err = fmt.Errorf("app-content for %s on %s: %w", q.pack, q.host, err)
	}
	if m.appContents == nil {
		m.appContents = map[appContentRequest]appContentResult{}
	}
	m.appContents[q] = appContentResult{content, err}
	return content, err
}

// checkAppContent confirms the inputs used by the build still match the manager's observation before any mutation.
func (r *stepRunner) checkAppContent(ctx context.Context, p *Prepared) bool {
	if p.AppContent == nil {
		return true
	}
	q := appContentRequest{pack: p.AppPack, namespace: p.Namespace}
	var out bytes.Buffer
	call := WorkloadCall{Verb: VerbAppContent, Env: append(append([]string{}, r.s.HostEnv...), q.env()...), Stdout: &out, Log: r.log}
	code, tail, err := r.workload().Call(ctx, call)
	if err != nil || code != 0 {
		r.config("app-content recheck: %s", failText(code, tail, err))
		return false
	}
	content, err := readAppContent(out.String())
	if err != nil {
		r.config("app-content recheck: %v", err)
		return false
	}
	got := *p
	got.AppContent = content
	if changed := appDifference(p, &got); changed != "" {
		r.config("app-content changed since placement: %s", changed)
		return false
	}
	if content.Attachments != p.AppContent.Attachments {
		r.config("app-content changed since placement: attachments")
		return false
	}
	return true
}
