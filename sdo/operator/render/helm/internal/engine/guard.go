package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"text/template"

	"sdo.dev/operator/render/helm/internal/policy"
)

// guard holds the bounds of one render. Everything here exists because a chart
// is attacker-influenced Git content rendered inside the controller process:
// unbounded tpl recursion is a Go stack overflow (a fatal no recover() can
// catch), and amplifying functions or a template that fans out can burn CPU and
// memory without end. Violations are typed policy errors.
type guard struct {
	ctx                         context.Context
	maxTplDepth, maxIncludeDeep int
	maxOutput                   int64
	tplDepth, includeDepth      int
}

func newGuard(e Engine) *guard {
	g := &guard{ctx: e.Context, maxTplDepth: e.MaxTplDepth, maxIncludeDeep: e.MaxIncludeDepth, maxOutput: e.MaxOutputBytes}
	if g.maxTplDepth <= 0 {
		g.maxTplDepth = DefaultMaxTplDepth
	}
	if g.maxIncludeDeep <= 0 {
		g.maxIncludeDeep = DefaultMaxIncludeDepth
	}
	if g.maxOutput <= 0 {
		g.maxOutput = DefaultMaxOutputBytes
	}
	return g
}

// maxCount caps the numeric argument of the functions that allocate in
// proportion to it (until, untilStep, seq, repeat, indent, nindent).
const maxCount = 1 << 20

func budget(format string, args ...any) error {
	return &policy.Error{Rule: policy.RuleRenderBudget, Detail: fmt.Sprintf(format, args...)}
}

// tick fails the render once its context is done. The cause is the deadline's
// typed error when the caller set one with context.WithTimeoutCause.
func (g *guard) tick() error {
	if g.ctx == nil || g.ctx.Err() == nil {
		return nil
	}
	cause := context.Cause(g.ctx)
	var perr *policy.Error
	if errors.As(cause, &perr) {
		return cause
	}
	return fmt.Errorf("render stopped: %w", cause)
}

func (g *guard) enterTpl() error {
	if err := g.tick(); err != nil {
		return err
	}
	if g.tplDepth >= g.maxTplDepth {
		return &policy.Error{Rule: policy.RuleRenderDepth, Detail: fmt.Sprintf("tpl calls nest deeper than %d levels (a template that expands itself?)", g.maxTplDepth)}
	}
	g.tplDepth++
	return nil
}

func (g *guard) leaveTpl() { g.tplDepth-- }

func (g *guard) enterInclude() error {
	if err := g.tick(); err != nil {
		return err
	}
	if g.includeDepth >= g.maxIncludeDeep {
		return &policy.Error{Rule: policy.RuleRenderDepth, Detail: fmt.Sprintf("include calls nest deeper than %d levels (a template that includes itself?)", g.maxIncludeDeep)}
	}
	g.includeDepth++
	return nil
}

func (g *guard) leaveInclude() { g.includeDepth-- }

// limitedBuffer is a strings.Builder that fails the write that would pass the cap.
type limitedBuffer struct {
	strings.Builder
	max int64
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if int64(b.Len())+int64(len(p)) > b.max {
		return 0, budget("a rendered buffer passed %d bytes", b.max)
	}
	return b.Builder.Write(p)
}

func (b *limitedBuffer) WriteString(s string) (int, error) {
	if int64(b.Len())+int64(len(s)) > b.max {
		return 0, budget("a rendered buffer passed %d bytes", b.max)
	}
	return b.Builder.WriteString(s)
}

func (g *guard) buffer() *limitedBuffer { return &limitedBuffer{max: g.maxOutput} }

// wrapAll bounds every template function: each call first checks the deadline,
// and the functions that allocate in proportion to an argument check it. A
// panic is how a function without an error result fails; text/template turns
// it into the error of the call.
func (g *guard) wrapAll(funcs template.FuncMap) {
	for name, fn := range funcs {
		v := reflect.ValueOf(fn)
		if v.Kind() != reflect.Func {
			continue
		}
		name := name
		funcs[name] = reflect.MakeFunc(v.Type(), func(args []reflect.Value) []reflect.Value {
			if err := g.tick(); err != nil {
				panic(err)
			}
			if err := g.checkAmplification(name, args, g.maxOutput); err != nil {
				panic(err)
			}
			if v.Type().IsVariadic() {
				return v.CallSlice(args)
			}
			return v.Call(args)
		}).Interface()
	}
}

func ints(args []reflect.Value) []int64 {
	var out []int64
	for _, a := range args {
		for a.Kind() == reflect.Interface && !a.IsNil() {
			a = a.Elem()
		}
		switch a.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			out = append(out, a.Int())
		case reflect.Slice:
			out = append(out, ints(sliceValues(a))...)
		}
	}
	return out
}

func sliceValues(v reflect.Value) []reflect.Value {
	out := make([]reflect.Value, v.Len())
	for i := range out {
		out[i] = v.Index(i)
	}
	return out
}

func (g *guard) checkAmplification(name string, args []reflect.Value, maxOutput int64) error {
	switch name {
	case "until", "indent", "nindent":
		for _, n := range ints(args) {
			if n > maxCount {
				return budget("%s(%d) exceeds the limit of %d", name, n, maxCount)
			}
		}
	case "repeat":
		if len(args) == 2 {
			n, s := ints(args[:1]), args[1]
			for s.Kind() == reflect.Interface && !s.IsNil() {
				s = s.Elem()
			}
			if len(n) == 1 && s.Kind() == reflect.String && n[0] > 0 && n[0]*int64(len(s.String())) > maxOutput {
				return budget("repeat of %d x %d bytes exceeds the %d byte buffer limit", n[0], len(s.String()), maxOutput)
			}
			if len(n) == 1 && n[0] > maxCount*16 {
				return budget("repeat(%d) exceeds the limit", n[0])
			}
		}
	case "untilStep", "seq":
		var start, stop, step int64
		xs := ints(args)
		switch {
		case name == "untilStep" && len(xs) == 3:
			start, stop, step = xs[0], xs[1], xs[2]
		case name == "seq" && len(xs) == 1:
			start, stop, step = 1, xs[0], 1
		case name == "seq" && len(xs) == 2:
			start, stop, step = xs[0], xs[1], 1
		case name == "seq" && len(xs) == 3:
			start, stop, step = xs[0], xs[2], xs[1]
		default:
			return nil
		}
		if step == 0 {
			return nil
		}
		if span := (stop - start) / step; span > maxCount {
			return budget("%s over %d steps exceeds the limit of %d", name, span, maxCount)
		}
	}
	return nil
}
