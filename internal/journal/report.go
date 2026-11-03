package journal

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

const (
	reportLimit = 8192
	errorLimit  = 1024
)

// ErrInvalidReport means the completed result cannot be encoded as a report.
// Retrying a disk write cannot repair an invalid result.
var ErrInvalidReport = errors.New("report was not stored")

var errReport = ErrInvalidReport

type storedReport struct {
	requestID  string
	endpointID string
	attemptID  string
	frame      []byte
}

func dropReports(reports []storedReport, match func(storedReport) bool) []storedReport {
	kept := make([]storedReport, 0, len(reports))
	for _, report := range reports {
		if !match(report) {
			kept = append(kept, report)
		}
	}
	return kept
}

func buildReport(requestID, endpointID, attemptID string, number int, result Result) ([]byte, error) {
	if requestID == "" || endpointID == "" || attemptID == "" || number < 1 || number > attemptBudget {
		return nil, errReport
	}
	if result.Outcome != OutcomeSuccess && result.Outcome != OutcomeFailure {
		return nil, errReport
	}
	duration := result.Duration.Milliseconds()
	if duration < 0 {
		duration = 0
	}
	var queueWait *int64
	if wait, ok := result.QueueWaitMS(); ok {
		queueWait = &wait
	}
	var status *int
	switch result.Outcome {
	case OutcomeSuccess:
		if result.Status < 200 || result.Status > 299 {
			return nil, errReport
		}
		status = &result.Status
	default:
		if result.Status >= 100 && result.Status <= 599 && (result.Status < 200 || result.Status > 299) {
			status = &result.Status
		}
	}
	var text *string
	if result.Outcome == OutcomeFailure {
		clipped := clipError(result.Error)
		if clipped == "" {
			clipped = "request failed"
		}
		text = &clipped
	}
	for {
		body, err := json.Marshal(struct {
			Type          string  `json:"type"`
			Version       int     `json:"version"`
			RequestID     string  `json:"request_id"`
			EndpointID    string  `json:"endpoint_id"`
			AttemptID     string  `json:"attempt_id"`
			AttemptNumber int     `json:"attempt_number"`
			Outcome       string  `json:"outcome"`
			HTTPStatus    *int    `json:"http_status,omitempty"`
			Error         *string `json:"error,omitempty"`
			DurationMS    int64   `json:"duration_ms"`
			QueueWait     *int64  `json:"queue_wait_ms,omitempty"`
		}{
			Type: "report", Version: 3, RequestID: requestID, EndpointID: endpointID,
			AttemptID: attemptID, AttemptNumber: number, Outcome: result.Outcome,
			HTTPStatus: status, Error: text, DurationMS: duration, QueueWait: queueWait,
		})
		if err != nil {
			return nil, errReport
		}
		if len(body) <= reportLimit {
			return body, nil
		}
		if text == nil || *text == "" {
			return nil, errReport
		}
		shorter := clipRunes(*text, utf8.RuneCountInString(*text)/2)
		if shorter == *text {
			return nil, errReport
		}
		text = &shorter
	}
}

func clipError(text string) string {
	return clipRunes(text, errorLimit)
}

func clipRunes(text string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if utf8.RuneCountInString(text) <= limit {
		return text
	}
	runes := []rune(text)
	return string(runes[:limit])
}

func newAttemptID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:]), nil
}
