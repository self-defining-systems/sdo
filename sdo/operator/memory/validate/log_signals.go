package validate

import (
	"fmt"
	"sort"
	"strings"

	"sdo.dev/operator/sdk/logsignal"
)

// logSignalRules checks every log signal against its schema and bounds
// (logsignal.Validate), the application map (each component it names is a
// map component with a workload) and the pattern budget all log signals
// share (logsignal.MaxTotalPatterns).
func (s *run) logSignalRules() {
	if len(s.logSignals) == 0 {
		return
	}
	components, mapErr := s.mapDataComponents()
	ids := make([]string, 0, len(s.logSignals))
	for id := range s.logSignals {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	total := 0
	for _, id := range ids {
		file := s.logSignals[id]
		signal, err := logsignal.Parse(file.content)
		if err == nil {
			err = signal.Validate(id)
		}
		if err != nil {
			for _, problem := range strings.Split(err.Error(), "\n") {
				s.add(Semantic, Diagnostic{Rule: RuleLogSignalInvalid, Path: file.path, Line: errorLine(err, 0), Message: problem,
					Hint: "a log signal names map components, an optional window and RE2 patterns: signals (failure lines) and expect (activity lines)"})
			}
			continue
		}
		total += len(signal.Patterns())
		if total > logsignal.MaxTotalPatterns {
			s.add(Semantic, Diagnostic{Rule: RuleLogSignalInvalid, Path: file.path, Message: fmt.Sprintf("log signals declare more than %d patterns together", logsignal.MaxTotalPatterns),
				Hint: "keep the lines that name a component's failure; merge or drop the rest"})
		}
		if mapErr != "" {
			s.add(Semantic, Diagnostic{Rule: RuleLogSignalComponent, Path: file.path, Message: "log signals name application-map components, but " + mapErr,
				Hint: "write the application map with a component for every workload"})
			continue
		}
		for _, name := range signal.Components {
			if c, ok := components[name]; !ok || c.Workload == "" {
				s.add(Semantic, Diagnostic{Rule: RuleLogSignalComponent, Path: file.path, Message: fmt.Sprintf("component %s is not an application-map component with a workload", name),
					Hint: "name components exactly as the application map does"})
			}
		}
	}
}
