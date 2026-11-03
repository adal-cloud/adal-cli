// Package journal stores assignments, delivery progress, and unacknowledged reports.
// Open persists them in a directory. New keeps them in memory for tests.
package journal

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"sync"
	"time"
)

const receivedAtLayout = "2006-01-02T15:04:05.000000Z"

var (
	// ErrInvalidJSON means the frame is not a JSON object.
	ErrInvalidJSON = errors.New("invalid json")
	// ErrNotAssignment means the frame is a JSON value of another type.
	ErrNotAssignment = errors.New("not an assignment")
	// ErrIncomplete means an assignment frame is missing a required field.
	ErrIncomplete = errors.New("incomplete assignment")
	// ErrCorrupt means the journal file cannot be read back.
	ErrCorrupt = errors.New("journal is corrupt")
	// ErrBusy means another process already holds this journal.
	ErrBusy = errors.New("journal is busy")
	// ErrUnavailable means the journal directory cannot be created.
	ErrUnavailable = errors.New("journal is unavailable")

	uuidCanonical = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	uuid4         = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

// Endpoint is one destination from the assignment snapshot.
type Endpoint struct {
	AttemptCount       int
	ID                 string
	URL                string
	DeliveryIntervalMS int
	IdempotencyKey     bool
	Enabled            bool
}

// Assignment is one accepted request and the endpoint snapshot that came with it.
// Each endpoint carries its own server progress.
type Assignment struct {
	Version   int
	RequestID string
	Method    string
	Path      string
	Query     string
	Headers   map[string][]string
	Body      string
	Endpoints []Endpoint
}

// Result is one endpoint call. Status is zero when the call produced no HTTP status.
// QueueWait is the time from entering that endpoint queue until the call starts.
// QueueWaitKnown is false when that wait was not measured.
type Result struct {
	// Diagnostic is a safe local transport category; it is not sent in reports.
	Diagnostic     string
	EndpointID     string
	Outcome        string
	Status         int
	Error          string
	Duration       time.Duration
	QueueWait      time.Duration
	QueueWaitKnown bool
}

// QueueWaitMS returns the queue wait in milliseconds, rounded down.
// The second result is false when the wait is unknown or below zero.
func (r Result) QueueWaitMS() (int64, bool) {
	if !r.QueueWaitKnown || r.QueueWait < 0 {
		return 0, false
	}
	return r.QueueWait.Milliseconds(), true
}

const (
	// OutcomeSuccess is an HTTP status from 200 through 299.
	OutcomeSuccess = "success"
	// OutcomeFailure is any other result.
	OutcomeFailure = "failure"
)

// Journal is the record of assignments for one server and channel.
type Journal struct {
	mu     sync.Mutex
	dir    string
	lock   *os.File
	saver  func([]byte) error
	handed map[string]bool
	state  snapshot
}

// New returns an empty in-memory journal.
func New() *Journal {
	return &Journal{handed: map[string]bool{}, state: newSnapshot()}
}

func (j *Journal) commit(fn func(*snapshot) error) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	next := j.state.clone()
	if err := fn(&next); err != nil {
		return err
	}
	if j.saver != nil {
		raw, err := next.marshal()
		if err != nil {
			return err
		}
		if err := j.saver(raw); err != nil {
			return err
		}
	}
	j.state = next
	return nil
}

// Parse reads an assignment frame of protocol version 3.
// Invalid JSON returns ErrInvalidJSON.
// A non-assignment JSON value returns ErrNotAssignment.
// A frame that is missing a required field returns ErrIncomplete.
func Parse(data []byte) (Assignment, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil || raw == nil {
		return Assignment{}, ErrInvalidJSON
	}
	var typ string
	if json.Unmarshal(raw["type"], &typ) != nil || typ == "" {
		return Assignment{}, ErrNotAssignment
	}
	if typ != "assignment" {
		return Assignment{}, ErrNotAssignment
	}
	for _, key := range []string{"version", "request_id", "received_at", "method", "path", "query", "headers", "trailers", "body", "endpoints"} {
		if _, ok := raw[key]; !ok || string(raw[key]) == "null" {
			return Assignment{}, ErrIncomplete
		}
	}
	var version int
	if json.Unmarshal(raw["version"], &version) != nil || version != 3 {
		return Assignment{}, ErrIncomplete
	}
	var receivedAt string
	if json.Unmarshal(raw["received_at"], &receivedAt) != nil || receivedAt == "" {
		return Assignment{}, ErrIncomplete
	}
	if _, err := time.Parse(receivedAtLayout, receivedAt); err != nil {
		return Assignment{}, ErrIncomplete
	}
	var requestID, method, path, query, body string
	if json.Unmarshal(raw["request_id"], &requestID) != nil || !uuidCanonical.MatchString(requestID) {
		return Assignment{}, ErrIncomplete
	}
	if json.Unmarshal(raw["method"], &method) != nil || method == "" {
		return Assignment{}, ErrIncomplete
	}
	if json.Unmarshal(raw["path"], &path) != nil || path == "" {
		return Assignment{}, ErrIncomplete
	}
	if json.Unmarshal(raw["query"], &query) != nil {
		return Assignment{}, ErrIncomplete
	}
	if json.Unmarshal(raw["body"], &body) != nil {
		return Assignment{}, ErrIncomplete
	}
	if body != "" {
		if _, err := base64.StdEncoding.DecodeString(body); err != nil {
			return Assignment{}, ErrIncomplete
		}
	}
	headers, err := stringMap(raw["headers"])
	if err != nil {
		return Assignment{}, ErrIncomplete
	}
	if _, err := stringMap(raw["trailers"]); err != nil {
		return Assignment{}, ErrIncomplete
	}
	var encoded []json.RawMessage
	if json.Unmarshal(raw["endpoints"], &encoded) != nil {
		return Assignment{}, ErrIncomplete
	}
	endpoints := make([]Endpoint, 0, len(encoded))
	for _, item := range encoded {
		endpoint, err := parseEndpoint(item)
		if err != nil {
			return Assignment{}, ErrIncomplete
		}
		if version == 3 {
			var e struct {
				AttemptCount *int   `json:"attempt_count"`
				State        string `json:"state"`
			}
			if json.Unmarshal(item, &e) != nil || e.AttemptCount == nil || *e.AttemptCount < 0 || *e.AttemptCount > attemptBudget || e.State != "pending" {
				return Assignment{}, ErrIncomplete
			}
			endpoint.AttemptCount = *e.AttemptCount
		}
		endpoints = append(endpoints, endpoint)
	}
	return Assignment{
		Version: version, RequestID: requestID,
		Method:    method,
		Path:      path,
		Query:     query,
		Headers:   headers,
		Body:      body,
		Endpoints: endpoints,
	}, nil
}

func parseEndpoint(data []byte) (Endpoint, error) {
	var raw map[string]json.RawMessage
	if json.Unmarshal(data, &raw) != nil || raw == nil {
		return Endpoint{}, ErrIncomplete
	}
	for _, key := range []string{"id", "url", "delivery_interval_ms", "idempotency_key", "enabled"} {
		if _, ok := raw[key]; !ok || string(raw[key]) == "null" {
			return Endpoint{}, ErrIncomplete
		}
	}
	var id, url string
	var interval int
	var idempotency, enabled bool
	if json.Unmarshal(raw["id"], &id) != nil || !uuid4.MatchString(id) {
		return Endpoint{}, ErrIncomplete
	}
	if json.Unmarshal(raw["url"], &url) != nil || url == "" {
		return Endpoint{}, ErrIncomplete
	}
	if json.Unmarshal(raw["delivery_interval_ms"], &interval) != nil || interval < 0 {
		return Endpoint{}, ErrIncomplete
	}
	if json.Unmarshal(raw["idempotency_key"], &idempotency) != nil || json.Unmarshal(raw["enabled"], &enabled) != nil {
		return Endpoint{}, ErrIncomplete
	}
	return Endpoint{
		ID: id, URL: url, DeliveryIntervalMS: interval,
		IdempotencyKey: idempotency, Enabled: enabled,
	}, nil
}

func stringMap(data []byte) (map[string][]string, error) {
	var decoded map[string][]string
	if err := json.Unmarshal(data, &decoded); err != nil || decoded == nil {
		return nil, ErrIncomplete
	}
	return cloneHeaders(decoded), nil
}

// Put stores an assignment and applies its attempt count.
// A repeated request keeps the first endpoint snapshot.
// The boolean is true only when this request was not already stored.
func (j *Journal) Put(assignment Assignment) (bool, error) {
	var created bool
	err := j.commit(func(state *snapshot) error {
		if current, ok := state.items[assignment.RequestID]; ok {
			applyAssignmentCounts(current, assignment)
			return nil
		}
		current := newItem(assignment)
		applyAssignmentCounts(current, assignment)
		state.items[assignment.RequestID] = current
		state.order = append(state.order, assignment.RequestID)
		created = true
		return nil
	})
	return created, err
}

// Drop removes an assignment and its pending reports.
func (j *Journal) Drop(requestID string) error {
	j.mu.Lock()
	delete(j.handed, requestID)
	j.mu.Unlock()
	return j.commit(func(state *snapshot) error {
		delete(state.items, requestID)
		order := make([]string, 0, len(state.order))
		for _, id := range state.order {
			if id != requestID {
				order = append(order, id)
			}
		}
		state.order = order
		state.reports = dropReports(state.reports, func(report storedReport) bool {
			return report.requestID == requestID
		})
		return nil
	})
}

// Contains reports whether the assignment is still stored.
func (j *Journal) Contains(requestID string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	_, ok := j.state.items[requestID]
	return ok
}

// HasEnabled reports whether the stored assignment has an enabled endpoint.
func (j *Journal) HasEnabled(requestID string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	current := j.state.item(requestID)
	if current == nil {
		return false
	}
	return hasEnabled(current.assignment)
}

// Stored returns assignments in arrival order.
func (j *Journal) Stored() []Assignment {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []Assignment
	for _, id := range j.state.order {
		current := j.state.items[id]
		if current != nil {
			out = append(out, cloneAssignment(current.assignment))
		}
	}
	return out
}

// Results returns the recorded calls for an assignment.
func (j *Journal) Results(requestID string) []Result {
	j.mu.Lock()
	defer j.mu.Unlock()
	current := j.state.item(requestID)
	if current == nil {
		return nil
	}
	var out []Result
	for _, endpoint := range current.assignment.Endpoints {
		d := current.deliveries[endpoint.ID]
		if d == nil {
			continue
		}
		for _, attempt := range d.attempts {
			out = append(out, attempt.result(endpoint.ID))
		}
	}
	return out
}

// Arm marks a stored assignment as handed to this process.
// It returns false when the assignment is missing.
// It returns false when the assignment is paused.
// It returns false when the assignment was already handed to this process.
// It returns false when it has no enabled unfinished endpoint.
func (j *Journal) Arm(requestID string) (Assignment, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	current := j.state.item(requestID)
	if current == nil || current.pauseReason != "" || j.handed[requestID] || !hasEnabledUnfinished(current) {
		return Assignment{}, false
	}
	j.handed[requestID] = true
	return cloneAssignment(current.assignment), true
}

func hasEnabled(assignment Assignment) bool {
	for _, endpoint := range assignment.Endpoints {
		if endpoint.Enabled {
			return true
		}
	}
	return false
}

func hasEnabledUnfinished(current *item) bool {
	for _, endpoint := range current.assignment.Endpoints {
		if !endpoint.Enabled {
			continue
		}
		d := current.deliveries[endpoint.ID]
		if d != nil && !d.exhausted() {
			return true
		}
	}
	return false
}

// Body returns the stored request body.
// The boolean is false when the assignment is absent.
// A true boolean with an empty string means the body is empty.
func (j *Journal) Body(requestID string) (string, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	current := j.state.item(requestID)
	if current == nil {
		return "", false
	}
	return current.assignment.Body, true
}

// Pause preserves the assignment and pending reports.
// Delivery stays disabled.
// A repeated assignment does not clear this persisted data error.
func (j *Journal) Pause(requestID, reason string) error {
	return j.commit(func(state *snapshot) error {
		if current := state.item(requestID); current != nil {
			current.pauseReason = reason
		}
		return nil
	})
}
