package runtime

import "slices"

// namedPlanLists recognizes only unambiguous named lists. Other arrays retain
// positional matching; duplicate or missing names never establish ownership.
func namedPlanLists(before, after, live []any) (map[string]any, map[string]any, map[string]any, bool) {
	index := func(list []any) (map[string]any, bool) {
		items := map[string]any{}
		for _, value := range list {
			item, ok := value.(map[string]any)
			if !ok {
				return nil, false
			}
			name, ok := item["name"].(string)
			if !ok || name == "" {
				return nil, false
			}
			if _, duplicate := items[name]; duplicate {
				return nil, false
			}
			items[name] = value
		}
		return items, true
	}
	b, bok := index(before)
	a, aok := index(after)
	l, lok := index(live)
	if !bok || !aok || !lok {
		return b, a, l, false
	}
	// Preserve the relative identity of existing items. Permit additions/removals,
	// but never relax the existing reordered-list refusal.
	commonOrder := func(left, right []any, li, ri map[string]any) bool {
		var ln, rn []string
		for _, item := range left {
			name := item.(map[string]any)["name"].(string)
			if _, ok := ri[name]; ok {
				ln = append(ln, name)
			}
		}
		for _, item := range right {
			name := item.(map[string]any)["name"].(string)
			if _, ok := li[name]; ok {
				rn = append(rn, name)
			}
		}
		return slices.Equal(ln, rn)
	}
	return b, a, l, commonOrder(before, after, b, a) && commonOrder(after, live, a, l) && commonOrder(before, live, b, l)
}
