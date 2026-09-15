// Package internalmsg guards message bodies that are internal agent context
// and must never be shown to the user.
package internalmsg

import "strings"

// Label marks the first line of an internal reasoning summary message.
const Label = "[REASONING SUMMARY]"

// IsInternal reports whether content is an internal-only message body that must never reach the user.
func IsInternal(content string) bool {
	return strings.HasPrefix(strings.TrimSpace(content), Label)
}

// Strip removes internal summary blocks (label line + its bullet body) from text, keeping surrounding text.
func Strip(text string) string {
	if !strings.Contains(text, Label) {
		return text
	}

	lines := strings.Split(text, "\n")
	kept := make([]string, 0, len(lines))
	for i := 0; i < len(lines); i++ {
		labelIdx := strings.Index(lines[i], Label)
		if labelIdx < 0 {
			kept = append(kept, lines[i])
			continue
		}
		if labelIdx > 0 {
			kept = append(kept, strings.TrimRight(lines[i][:labelIdx], " \t"))
		}
		i = endOfBlock(lines, i)
	}
	return strings.TrimSpace(strings.Join(kept, "\n"))
}

func endOfBlock(lines []string, labelLine int) int {
	last := labelLine
	for i := labelLine + 1; i < len(lines); i++ {
		switch {
		case isBodyLine(lines[i]):
			last = i
		case strings.TrimSpace(lines[i]) == "" && resumesBody(lines[i+1:]):
			continue
		default:
			return last
		}
	}
	return last
}

func resumesBody(lines []string) bool {
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		return isBodyLine(line)
	}
	return false
}

func isBodyLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	return strings.HasPrefix(trimmed, "- ") || trimmed == "-" ||
		strings.HasPrefix(trimmed, "* ") || trimmed == "*" ||
		strings.HasPrefix(trimmed, "•")
}
