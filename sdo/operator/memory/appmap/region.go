// Package appmap parses and checks the application map: the region of
// .sdo/arch.md in which the detection designer records an application's user
// operations, data-holding components, call edges and probes. It is pure data
// and logic, shared by the memory check and the generated scenario test.
package appmap

import (
	"fmt"
	"strings"
)

const (
	// BeginMarker and EndMarker each sit alone on a line and delimit the region.
	BeginMarker = "<!-- sdo:application-map:begin -->"
	EndMarker   = "<!-- sdo:application-map:end -->"

	fenceOpen  = "```yaml"
	fenceClose = "```"
)

// Region is the located application-map region of an architecture document.
type Region struct {
	// YAML is the text of the fenced block.
	YAML string
	// Block is the whole region, begin marker line through end marker line.
	Block string
	// Line is the 1-based line of the begin marker; YAMLLine is that of the first YAML line.
	Line, YAMLLine int
}

// Find locates the single region of doc. found is false when doc has no
// markers at all; a malformed region is an error that names what is wrong.
func Find(doc string) (Region, bool, error) {
	lines := strings.SplitAfter(doc, "\n")
	begin, end := -1, -1
	for i, line := range lines {
		switch strings.TrimRight(line, " \t\r\n") {
		case BeginMarker:
			if begin >= 0 {
				return Region{}, true, fmt.Errorf("the application map has more than one begin marker (line %d and line %d); keep exactly one region", begin+1, i+1)
			}
			begin = i
		case EndMarker:
			if end >= 0 {
				return Region{}, true, fmt.Errorf("the application map has more than one end marker (line %d and line %d); keep exactly one region", end+1, i+1)
			}
			end = i
		}
	}
	switch {
	case begin < 0 && end < 0:
		return Region{}, false, nil
	case begin < 0:
		return Region{}, true, fmt.Errorf("the application map end marker on line %d has no begin marker %s", end+1, BeginMarker)
	case end < 0:
		return Region{}, true, fmt.Errorf("the application map begin marker on line %d has no end marker %s", begin+1, EndMarker)
	case end < begin:
		return Region{}, true, fmt.Errorf("the application map end marker (line %d) comes before its begin marker (line %d)", end+1, begin+1)
	}
	inner := lines[begin+1 : end]
	first, last := -1, -1
	for i, line := range inner {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if first < 0 {
			first = i
		}
		last = i
	}
	if first < 0 || strings.TrimRight(inner[first], " \t\r\n") != fenceOpen || last == first || strings.TrimRight(inner[last], " \t\r\n") != fenceClose {
		return Region{}, true, fmt.Errorf("the application map region must hold exactly one fenced block opened by %s and closed by %s", fenceOpen, fenceClose)
	}
	body := inner[first+1 : last]
	for _, line := range body {
		if strings.TrimRight(line, " \t\r\n") == fenceClose {
			return Region{}, true, fmt.Errorf("the application map region must hold exactly one fenced block; found a second %s", fenceClose)
		}
	}
	return Region{
		YAML:     strings.Join(body, ""),
		Block:    strings.Join(lines[begin:end+1], ""),
		Line:     begin + 1,
		YAMLLine: begin + 1 + first + 2,
	}, true, nil
}

// Outside is doc with its region removed and trailing whitespace trimmed: the
// text Go owns. Two documents with equal Outside differ only in the region. A
// document without a well-formed region is returned whole.
func Outside(doc string) string {
	region, found, err := Find(doc)
	if !found || err != nil {
		return strings.TrimRight(doc, " \t\r\n")
	}
	i := strings.Index(doc, region.Block)
	return strings.TrimRight(doc[:i]+doc[i+len(region.Block):], " \t\r\n")
}

// Carry returns regenerated with the region of previous appended verbatim, so
// a Go rewrite of the inventory never loses the designer's map. Without a
// well-formed region in previous it returns regenerated unchanged.
func Carry(previous, regenerated string) string {
	region, found, err := Find(previous)
	if !found || err != nil {
		return regenerated
	}
	block := region.Block
	if !strings.HasSuffix(block, "\n") {
		block += "\n"
	}
	return strings.TrimRight(regenerated, "\n") + "\n\n" + block
}
