package ingest

import (
	"strings"
)

// normalizeSpace collapses multiple spaces into one and trims the string.
func normalizeSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// appendUnique appends a string to a slice if it doesn't already exist (case-insensitive).
func appendUnique(list []string, v string) []string {
	vClean := strings.TrimSpace(v)
	if vClean == "" {
		return list
	}

	vLower := strings.ToLower(vClean)
	for _, existing := range list {
		if strings.ToLower(existing) == vLower {
			return list
		}
	}
	return append(list, vClean)
}

// cleanText normalizes whitespace (alias for normalizeSpace)
func cleanText(s string) string {
	return normalizeSpace(s)
}

func normalizeRegion(s string) string {
	return cleanText(s)
}

func normalizeCountry(s string) string {
	return cleanText(s)
}

func normalizeFunderType(s string) string {
	return cleanText(s)
}

func splitAndCleanList(block string) []string {
	block = strings.ReplaceAll(block, "\r\n", "\n")
	block = strings.ReplaceAll(block, "\r", "\n")

	var out []string
	for _, raw := range strings.Split(block, "\n") {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}

		s = strings.TrimLeft(s, " \t-*•–—")
		s = strings.TrimSpace(s)
		s = stripLeadingNumbering(s)
		s = cleanText(s)
		if s == "" {
			continue
		}

		out = append(out, s)
	}

	return mergeUniqueFold(nil, out)
}

func stripLeadingNumbering(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}

	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 || i >= len(s) {
		return s
	}

	for i < len(s) {
		switch s[i] {
		case '.', ')', '-', ':':
			i++
		case ' ', '\t':
			i++
		default:
			return strings.TrimSpace(s[i:])
		}
	}

	return strings.TrimSpace(s)
}

func mergeUniqueFold(dst []string, items []string) []string {
	seen := make(map[string]struct{}, len(dst))
	for _, v := range dst {
		k := strings.ToLower(strings.TrimSpace(v))
		if k != "" {
			seen[k] = struct{}{}
		}
	}

	for _, v := range items {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		k := strings.ToLower(v)
		if _, ok := seen[k]; ok {
			continue
		}
		dst = append(dst, v)
		seen[k] = struct{}{}
	}

	return dst
}
