package plan

import (
	"context"
	"fmt"
	"time"
)

// Alarm kinds.
const (
	AlarmStage        = "stage"         // a stage took more than Factor times its minimum
	AlarmIdleLane     = "idle-lane"     // a free lane while work waits
	AlarmArmStall     = "arm-stall"     // an arm with work made no progress
	AlarmHostPressure = "host-pressure" // a host over its load or disk budget
)

// AlarmConfig tunes the runner alarms. Zero values take the defaults.
type AlarmConfig struct {
	// Factor: a stage alarms above Factor x its minimum (default 2).
	Factor float64 `json:"factor,omitempty"`
	// IdleLane: a free lane alarms when it has been free, and work has waited, longer than this (default 10m).
	IdleLane string `json:"idle_lane,omitempty"`
	// ArmStall: an arm with released or ready work and no node ended for this long alarms (default 90m).
	ArmStall string `json:"arm_stall,omitempty"`
	// Hook is a shell command run once per alarm with ALARM_JSON, ALARM_KIND, ALARM_PLAN, ALARM_KEY and ALARM_DETAIL
	// in its environment, so a notifier can push it.
	Hook string `json:"hook,omitempty"`
}

// Alarm is one raised alarm: a line of <root>/alarms.jsonl.
type Alarm struct {
	At      time.Time `json:"at"`
	Plan    string    `json:"plan"`
	Alarm   string    `json:"alarm"`
	Key     string    `json:"key"`
	Subject string    `json:"subject,omitempty"` // a node, an arm, a lane or a host
	Host    string    `json:"host,omitempty"`
	Detail  string    `json:"detail"`
}

// AlarmSink receives alarms (the file, the hook). Raise must not fail the engine.
type AlarmSink interface{ Raise(Alarm) }

// AlarmState is what the engine remembers between passes so an alarm is raised once while its condition holds.
type AlarmState struct {
	// Active maps the key of a standing alarm to when it was raised; a condition that clears drops its key.
	Active map[string]time.Time `json:"active,omitempty"`
	// WaitSince is when work first waited for a lane without a break (zero: nothing waits).
	WaitSince time.Time `json:"wait_since,omitempty"`
	// Progress is, per arm, when a node of it last ended or the arm first had work.
	Progress map[string]time.Time `json:"progress,omitempty"`
}

const (
	defaultFactor   = 2.0
	defaultIdleLane = 10 * time.Minute
	defaultArmStall = 90 * time.Minute
)

func (p *Plan) validateAlarms() []string {
	var errs []string
	for stage, d := range p.Stages {
		if v, err := time.ParseDuration(d); err != nil || v <= 0 {
			errs = append(errs, fmt.Sprintf("stages.%s: %q is not a positive duration", stage, d))
		}
	}
	for _, s := range p.Steps {
		if s.Timeout != "" {
			if v, err := time.ParseDuration(s.Timeout); err != nil || v <= 0 {
				errs = append(errs, fmt.Sprintf("step %q: timeout %q is not a positive duration", s.Name, s.Timeout))
			}
		}
	}
	if a := p.Alarms; a != nil {
		if a.Factor != 0 && a.Factor < 1 {
			errs = append(errs, "alarms.factor must be at least 1")
		}
		for name, d := range map[string]string{"idle_lane": a.IdleLane, "arm_stall": a.ArmStall} {
			if d == "" {
				continue
			}
			if v, err := time.ParseDuration(d); err != nil || v <= 0 {
				errs = append(errs, fmt.Sprintf("alarms.%s: %q is not a positive duration", name, d))
			}
		}
	}
	return errs
}

type alarmCfg struct {
	factor    float64
	idleLane  time.Duration
	armStall  time.Duration
	minimum   map[string]time.Duration
	hasAlarms bool
}

func (e *Engine) alarmCfg() alarmCfg {
	c := alarmCfg{factor: defaultFactor, idleLane: defaultIdleLane, armStall: defaultArmStall, minimum: map[string]time.Duration{}}
	for k, d := range e.Plan.Stages {
		c.minimum[k], _ = time.ParseDuration(d)
	}
	if a := e.Plan.Alarms; a != nil {
		if a.Factor != 0 {
			c.factor = a.Factor
		}
		if a.IdleLane != "" {
			c.idleLane, _ = time.ParseDuration(a.IdleLane)
		}
		if a.ArmStall != "" {
			c.armStall, _ = time.ParseDuration(a.ArmStall)
		}
	}
	return c
}

// raise records an alarm once per key while the condition stands. The key is dropped by clear when it stops holding;
// stage alarms are about a past fact and are never cleared.
func (e *Engine) raise(st *State, a Alarm) {
	if st.Alarms.Active == nil {
		st.Alarms.Active = map[string]time.Time{}
	}
	if _, ok := st.Alarms.Active[a.Key]; ok {
		return
	}
	a.At, a.Plan = e.Clock.Now().UTC(), e.Plan.Name
	st.Alarms.Active[a.Key] = a.At
	e.logf("ALARM %s %s: %s", a.Alarm, a.Subject, a.Detail)
	e.event(a.Subject, "alarm", "%s: %s", a.Alarm, a.Detail)
	if e.Alarms != nil {
		e.Alarms.Raise(a)
	}
}

// clearUnless drops the standing alarms of kind whose key is not in holding.
func (e *Engine) clearUnless(st *State, kind string, holding map[string]bool) {
	for k := range st.Alarms.Active {
		if len(k) > len(kind) && k[:len(kind)+1] == kind+":" && !holding[k] {
			delete(st.Alarms.Active, k)
		}
	}
}

// checkStages alarms for each stage of an ended attempt that took more than Factor x its minimum.
func (e *Engine) checkStages(st *State, n *NodeState, obs Observation) {
	c := e.alarmCfg()
	for _, name := range sortedKeys(obs.Stages) {
		min, ok := c.minimum[name]
		if !ok || obs.Stages[name] <= time.Duration(float64(min)*c.factor) {
			continue
		}
		e.raise(st, Alarm{Alarm: AlarmStage, Key: fmt.Sprintf("%s:%s:a%d:%s", AlarmStage, n.ID, n.Attempt, name), Subject: n.ID,
			Detail: fmt.Sprintf("stage %s took %s, over %gx its minimum %s (%s)", name, obs.Stages[name].Round(time.Second), c.factor, min, n.Exp)})
	}
}

// watchStage alarms for a running experiment whose current stage is already over its limit: no need to wait for the
// stage to end to know it is slow.
func (e *Engine) watchStage(st *State, n *NodeState, obs Observation) {
	c := e.alarmCfg()
	min, ok := c.minimum[obs.Stage]
	if !ok || obs.StageSince.IsZero() {
		return
	}
	if el := e.Clock.Now().Sub(obs.StageSince); el > time.Duration(float64(min)*c.factor) {
		e.raise(st, Alarm{Alarm: AlarmStage, Key: fmt.Sprintf("%s:%s:a%d:%s", AlarmStage, n.ID, n.Attempt, obs.Stage), Subject: n.ID,
			Detail: fmt.Sprintf("stage %s has run %s, over %gx its minimum %s (%s)", obs.Stage, el.Round(time.Second), c.factor, min, n.Exp)})
	}
}

// checkAlarms runs the alarms that read the whole plan and the platform: an idle lane while work waits, an arm that
// stopped, a host over its budget. Liveness comes from the plan's own records, never from a process table.
func (e *Engine) checkAlarms(ctx context.Context, st *State) {
	c := e.alarmCfg()
	now := e.Clock.Now().UTC()
	e.armStalls(st, c, now)
	capy, err := e.Backend.Capacity(ctx)
	if err != nil {
		e.logf("capacity: %v (next pass)", err)
		return
	}
	// Work waits when the platform has queued runs or the plan has ready nodes it holds back.
	waiting := capy.Waiting
	for _, n := range st.Nodes {
		if n.Status == NodeWaiting {
			if why, _ := e.blocked(st, n); why == "" {
				waiting++
			}
		}
	}
	switch {
	case waiting == 0:
		st.Alarms.WaitSince = time.Time{}
	case st.Alarms.WaitSince.IsZero():
		st.Alarms.WaitSince = now
	}
	idle := map[string]bool{}
	if !st.Alarms.WaitSince.IsZero() && now.Sub(st.Alarms.WaitSince) > c.idleLane {
		for _, l := range capy.Lanes {
			if !l.Free || now.Sub(l.Since) <= c.idleLane {
				continue
			}
			key := fmt.Sprintf("%s:%s/%s", AlarmIdleLane, l.Host, l.Lane)
			idle[key] = true
			e.raise(st, Alarm{Alarm: AlarmIdleLane, Key: key, Subject: l.Host + "/" + l.Lane, Host: l.Host,
				Detail: fmt.Sprintf("free for %s while %d run(s) wait (waiting for %s)", now.Sub(l.Since).Round(time.Minute), waiting, now.Sub(st.Alarms.WaitSince).Round(time.Minute))})
		}
	}
	e.clearUnless(st, AlarmIdleLane, idle)
	hot := map[string]bool{}
	for _, h := range capy.Hosts {
		var why string
		switch {
		case h.Error != "":
			continue // an unreachable host is the host probe's alarm, not a pressure reading
		case h.MaxLoad5 > 0 && h.Load5 > h.MaxLoad5:
			why = fmt.Sprintf("load5 %.0f over its budget %.0f", h.Load5, h.MaxLoad5)
		case h.MinFree > 0 && h.Free < h.MinFree:
			why = fmt.Sprintf("%d GiB free, under its floor of %d GiB", h.Free>>30, h.MinFree>>30)
		default:
			continue
		}
		key := fmt.Sprintf("%s:%s", AlarmHostPressure, h.Name)
		hot[key] = true
		e.raise(st, Alarm{Alarm: AlarmHostPressure, Key: key, Subject: h.Name, Host: h.Name, Detail: why})
	}
	e.clearUnless(st, AlarmHostPressure, hot)
}

// armStalls alarms for an arm that has work (a node released or ready) and no node ended for the stall limit. Progress
// restarts when a node of the arm ends and when an arm that had no work gets some.
func (e *Engine) armStalls(st *State, c alarmCfg, now time.Time) {
	if st.Alarms.Progress == nil {
		st.Alarms.Progress = map[string]time.Time{}
	}
	hasWork := map[string]bool{}
	last := map[string]time.Time{}
	for _, n := range st.Nodes {
		switch n.Status {
		case NodeSubmitted:
			hasWork[n.Arm] = true
		case NodeWaiting:
			if why, _ := e.blocked(st, n); why == "" {
				hasWork[n.Arm] = true
			}
		}
		if !n.Ended.IsZero() && n.Ended.After(last[n.Arm]) {
			last[n.Arm] = n.Ended
		}
	}
	stalled := map[string]bool{}
	for _, arm := range sortedKeys(hasWork) {
		since := st.Alarms.Progress[arm]
		if last[arm].After(since) {
			since = last[arm]
		}
		if since.IsZero() {
			since = now // first sight of work in this arm
		}
		st.Alarms.Progress[arm] = since
		if now.Sub(since) > c.armStall {
			key := fmt.Sprintf("%s:%s", AlarmArmStall, arm)
			stalled[key] = true
			e.raise(st, Alarm{Alarm: AlarmArmStall, Key: key, Subject: arm,
				Detail: fmt.Sprintf("arm %s has had work and no run ended for %s", arm, now.Sub(since).Round(time.Minute))})
		}
	}
	for arm := range st.Alarms.Progress {
		if !hasWork[arm] {
			delete(st.Alarms.Progress, arm) // no work: the clock restarts when it gets some
		}
	}
	e.clearUnless(st, AlarmArmStall, stalled)
}
