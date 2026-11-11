package generichealth

import (
	"fmt"
	"sort"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

// Secret reader rules. A Secret's data is read into env at container start,
// and an application often reads mounted key files only at start too, so a
// reader that started before the Secret's data last changed may hold the old
// value. Both rules judge metadata only (SecretMetadata cannot hold values)
// and fire only while a user-facing probe fails, because a reader that
// reloads files, or a rotation nobody depends on yet, is not a fault.
const (
	// RuleSecretReaderSkew: some readers started before the change and
	// others after it, so they may hold different versions of one Secret.
	RuleSecretReaderSkew = "secret-reader-skew"
	// RuleSecretReadersPredateChange: every reader started before the change.
	RuleSecretReadersPredateChange = "secret-readers-predate-change"
)

type secretReader struct {
	pod     string
	owner   sdk.ObjectRef
	started time.Time
	via     string
}

func (r secretReader) String() string {
	return fmt.Sprintf("%s/%s pod=%s started=%s via=%s", r.owner.Kind, r.owner.Name, r.pod, r.started.UTC().Format(time.RFC3339), r.via)
}

// secretReaders reports how a pod consumes each Secret ("env" for values fixed
// at container start, "volume" for mounted files). Image pull Secrets are read
// by the kubelet, not by the application, and are not readers.
func secretReaders(p core.Pod) map[string]string {
	via := map[string]map[string]bool{}
	add := func(name, mode string) {
		if name == "" {
			return
		}
		if via[name] == nil {
			via[name] = map[string]bool{}
		}
		via[name][mode] = true
	}
	for _, v := range p.Spec.Volumes {
		if v.Secret != nil {
			add(v.Secret.SecretName, "volume")
		}
		if v.Projected != nil {
			for _, s := range v.Projected.Sources {
				if s.Secret != nil {
					add(s.Secret.Name, "volume")
				}
			}
		}
	}
	env := func(c core.Container) {
		for _, from := range c.EnvFrom {
			if from.SecretRef != nil {
				add(from.SecretRef.Name, "env")
			}
		}
		for _, e := range c.Env {
			if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
				add(e.ValueFrom.SecretKeyRef.Name, "env")
			}
		}
	}
	for _, c := range p.Spec.InitContainers {
		env(c)
	}
	for _, c := range p.Spec.Containers {
		env(c)
	}
	result := map[string]string{}
	for name, modes := range via {
		list := make([]string, 0, len(modes))
		for mode := range modes {
			list = append(list, mode)
		}
		sort.Strings(list)
		result[name] = strings.Join(list, "+")
	}
	return result
}

// podStarted is when the pod's oldest running container last started, the
// oldest view of its Secrets it may hold; the pod start time otherwise.
func podStarted(p core.Pod) time.Time {
	var oldest time.Time
	for _, status := range p.Status.ContainerStatuses {
		if status.State.Running == nil || status.State.Running.StartedAt.IsZero() {
			continue
		}
		if at := status.State.Running.StartedAt.Time; oldest.IsZero() || at.Before(oldest) {
			oldest = at
		}
	}
	if oldest.IsZero() && p.Status.StartTime != nil {
		oldest = p.Status.StartTime.Time
	}
	return oldest
}

func (e *evaluation) secretVersions() {
	ns := e.ctx.Namespace()
	readers := map[string][]secretReader{}
	for _, p := range e.ctx.Pods() {
		if p.Namespace != ns || p.DeletionTimestamp != nil || p.Status.Phase != core.PodRunning {
			continue
		}
		started := podStarted(p)
		if started.IsZero() {
			continue
		}
		for name, via := range secretReaders(p) {
			readers[name] = append(readers[name], secretReader{pod: p.Name, owner: e.podOwner(p), started: started, via: via})
		}
	}
	if len(readers) == 0 {
		return
	}
	failing := traffic.FailingUserScenarios(e.ctx)
	if len(failing) == 0 {
		return
	}
	for _, secret := range e.ctx.Secrets() {
		changed := secret.DataChangedAt
		if secret.Namespace != ns || changed.IsZero() || secret.Type == core.SecretTypeServiceAccountToken {
			continue
		}
		var stale, fresh []secretReader
		for _, r := range readers[secret.Name] {
			if r.started.Before(changed) {
				stale = append(stale, r)
			} else {
				fresh = append(fresh, r)
			}
		}
		if len(stale) == 0 {
			continue
		}
		rule := RuleSecretReadersPredateChange
		summary := "every reader started before its data last changed"
		if len(fresh) > 0 {
			rule = RuleSecretReaderSkew
			summary = "readers hold different versions: some started before its data last changed, others after"
		}
		sortReaders(stale)
		sortReaders(fresh)
		r := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: ns, Name: secret.Name}
		detail := fmt.Sprintf("Secret %s %s. data changed at %s (from metadata; values never read). stale readers (started before): %s. fresh readers (started after): %s. user-facing failures: %s",
			secret.Name, summary, changed.UTC().Format(time.RFC3339), joinReaders(stale), joinReaders(fresh), strings.Join(failing, ", "))
		e.emit(rule, r, detail, false)
		finding := &e.findings[len(e.findings)-1]
		finding.Summary = rule + ": Secret/" + secret.Name + " (" + summary + ")"
		seen := map[sdk.ObjectRef]bool{}
		for _, reader := range stale {
			if !seen[reader.owner] {
				seen[reader.owner] = true
				finding.RelatedResources = append(finding.RelatedResources, reader.owner)
			}
		}
	}
}

func sortReaders(rs []secretReader) {
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].owner.Name != rs[j].owner.Name {
			return rs[i].owner.Name < rs[j].owner.Name
		}
		return rs[i].pod < rs[j].pod
	})
}

func joinReaders(rs []secretReader) string {
	if len(rs) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(rs))
	for _, r := range rs {
		parts = append(parts, r.String())
	}
	return strings.Join(parts, "; ")
}
