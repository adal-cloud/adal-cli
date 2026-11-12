// Package console formats one operational event as a block.
package console

import (
	"fmt"
	"strings"
)

// Event is one stdout block.
// Empty strings are omitted.
// Status outside 100 through 599 is omitted.
// Duration, queue, and delay are omitted unless the matching flag is set.
type Event struct {
	Title       string
	Host        string
	Request     string
	Endpoint    string
	Attempt     string
	Outcome     string
	Error       string
	Code        string
	Diagnostic  string
	Reason      string
	Status      int
	Bytes       int
	Sequence    uint64
	Revision    int64
	DurationMS  int64
	QueueMS     int64
	DelayMS     int64
	HasDuration bool
	HasQueue    bool
	HasDelay    bool
}

// Format returns the block.
// The result ends with a blank line.
// Print does not append another newline after that blank line.
func Format(e Event) string {
	var b strings.Builder
	b.WriteString(e.Title)
	b.WriteByte('\n')
	for _, field := range fields(e) {
		fmt.Fprintf(&b, "    %-*s  %s\n", 10, field.label, flatten(field.value))
	}
	b.WriteByte('\n')
	return b.String()
}

type field struct {
	label string
	value string
}

func fields(e Event) []field {
	var out []field
	add := func(label, value string) {
		if value == "" {
			return
		}
		out = append(out, field{label: label, value: value})
	}
	add("host", e.Host)
	add("request", e.Request)
	add("endpoint", e.Endpoint)
	add("attempt", e.Attempt)
	add("outcome", e.Outcome)
	if e.Status >= 100 && e.Status <= 599 {
		add("status", fmt.Sprintf("HTTP %d", e.Status))
	}
	if e.Sequence > 0 {
		add("sequence", fmt.Sprint(e.Sequence))
		add("bytes", fmt.Sprint(e.Bytes))
	}
	if e.Revision > 0 {
		add("revision", fmt.Sprint(e.Revision))
	}
	add("error", e.Error)
	add("code", e.Code)
	switch e.Diagnostic {
	case "dns", "connect", "tls", "read", "timeout", "request":
		add("diagnostic", e.Diagnostic)
	}
	if e.HasDuration {
		add("duration", formatMS(e.DurationMS))
	}
	if e.HasQueue {
		add("queue", formatMS(e.QueueMS))
	}
	if e.HasDelay {
		add("delay", formatMS(e.DelayMS))
	}
	add("reason", e.Reason)
	return out
}

// formatMS keeps the millisecond count exact.
// A coarser unit is used only when it names that same count.
func formatMS(ms int64) string {
	if ms < 0 {
		ms = 0
	}
	if ms == 0 {
		return "0 ms"
	}
	if ms%60000 == 0 {
		return fmt.Sprintf("%d min", ms/60000)
	}
	if ms < 60000 && ms%1000 == 0 {
		return fmt.Sprintf("%d s", ms/1000)
	}
	return fmt.Sprintf("%d ms", ms)
}

func flatten(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	return strings.ReplaceAll(value, "\n", " ")
}
