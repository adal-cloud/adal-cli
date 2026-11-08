package journal

import (
	"encoding/json"
	"time"

	"github.com/adal-cloud/adal-cli/internal/headerwire"
)

type snapshot struct {
	order   []string
	items   map[string]*item
	reports []storedReport
}

type item struct {
	pauseReason string
	assignment  Assignment
	deliveries  map[string]*delivery
}

type delivery struct {
	budget   int
	baseline int
	final    bool
	next     *time.Time
	inflight string
	attempts []attemptRec
}

type attemptRec struct {
	attemptID      string
	number         int
	outcome        string
	status         int
	hasStatus      bool
	error          string
	duration       time.Duration
	queueWait      time.Duration
	queueWaitKnown bool
}

func newSnapshot() snapshot {
	return snapshot{items: map[string]*item{}}
}

func (s snapshot) clone() snapshot {
	out := snapshot{
		order:   append([]string(nil), s.order...),
		items:   make(map[string]*item, len(s.items)),
		reports: make([]storedReport, len(s.reports)),
	}
	for i, report := range s.reports {
		out.reports[i] = report
		out.reports[i].frame = append([]byte(nil), report.frame...)
	}
	for id, current := range s.items {
		out.items[id] = current.clone()
	}
	return out
}

func (current *item) clone() *item {
	if current == nil {
		return nil
	}
	out := &item{
		assignment:  cloneAssignment(current.assignment),
		pauseReason: current.pauseReason,
		deliveries:  make(map[string]*delivery, len(current.deliveries)),
	}
	for id, item := range current.deliveries {
		out.deliveries[id] = item.clone()
	}
	return out
}

func (d *delivery) clone() *delivery {
	if d == nil {
		return nil
	}
	out := &delivery{
		budget:   d.budget,
		baseline: d.baseline,
		final:    d.final,
		inflight: d.inflight,
		attempts: append([]attemptRec(nil), d.attempts...),
	}
	if d.next != nil {
		at := *d.next
		out.next = &at
	}
	return out
}

func cloneAssignment(assignment Assignment) Assignment {
	out := assignment
	out.Headers = cloneHeaders(assignment.Headers)
	out.Trailers = cloneHeaders(assignment.Trailers)
	out.Endpoints = append([]Endpoint(nil), assignment.Endpoints...)
	return out
}

func cloneHeaders(in map[string][]string) map[string][]string {
	if in == nil {
		return nil
	}
	out := make(map[string][]string, len(in))
	for key, values := range in {
		out[key] = append([]string(nil), values...)
	}
	return out
}

func (s *snapshot) item(requestID string) *item {
	if s == nil {
		return nil
	}
	return s.items[requestID]
}

func (s *snapshot) delivery(requestID, endpointID string) *delivery {
	current := s.item(requestID)
	if current == nil {
		return nil
	}
	return current.deliveries[endpointID]
}

func newItem(assignment Assignment) *item {
	current := &item{
		assignment: cloneAssignment(assignment),
		deliveries: map[string]*delivery{},
	}
	for _, endpoint := range assignment.Endpoints {
		current.deliveries[endpoint.ID] = &delivery{}
	}
	return current
}

func (d *delivery) reported() int {
	if d == nil {
		return 0
	}
	return d.baseline + len(d.attempts)
}

func (d *delivery) exhausted() bool {
	return d == nil || d.final || d.budget >= attemptBudget || d.reported() >= attemptBudget
}

func (rec attemptRec) result(endpointID string) Result {
	result := Result{
		EndpointID:     endpointID,
		Outcome:        rec.outcome,
		Error:          rec.error,
		Duration:       rec.duration,
		QueueWait:      rec.queueWait,
		QueueWaitKnown: rec.queueWaitKnown,
	}
	if rec.hasStatus {
		result.Status = rec.status
	}
	return result
}

type fileState struct {
	Assignments []fileAssignment `json:"assignments"`
	Reports     []fileReport     `json:"reports"`
}

type fileAssignment struct {
	PauseReason string            `json:"pause_reason,omitempty"`
	RequestID   string            `json:"request_id"`
	Method      string            `json:"method"`
	Path        string            `json:"path"`
	Query       string            `json:"query"`
	Headers     headerwire.Fields `json:"headers"`
	Trailers    headerwire.Fields `json:"trailers,omitempty"`
	Body        string            `json:"body"`
	Endpoints   []fileEndpoint    `json:"endpoints"`
	Deliveries  []fileDelivery    `json:"deliveries"`
}

type fileEndpoint struct {
	ID                 string `json:"id"`
	URL                string `json:"url"`
	DeliveryIntervalMS int    `json:"delivery_interval_ms"`
	IdempotencyKey     bool   `json:"idempotency_key"`
	Enabled            bool   `json:"enabled"`
}

type fileDelivery struct {
	EndpointID string        `json:"endpoint_id"`
	BudgetUsed int           `json:"budget_used"`
	Baseline   int           `json:"baseline"`
	Final      bool          `json:"final"`
	Next       *time.Time    `json:"next_attempt_at,omitempty"`
	Inflight   string        `json:"inflight,omitempty"`
	Attempts   []fileAttempt `json:"attempts"`
}

type fileAttempt struct {
	AttemptID      string `json:"attempt_id"`
	Number         int    `json:"attempt_number"`
	Outcome        string `json:"outcome"`
	HTTPStatus     *int   `json:"http_status,omitempty"`
	Error          string `json:"error,omitempty"`
	DurationMS     int64  `json:"duration_ms"`
	QueueWaitMS    *int64 `json:"queue_wait_ms,omitempty"`
	QueueWaitKnown bool   `json:"queue_wait_known,omitempty"`
}

type fileReport struct {
	RequestID  string `json:"request_id"`
	EndpointID string `json:"endpoint_id"`
	AttemptID  string `json:"attempt_id"`
	Frame      []byte `json:"frame"`
}

func (s snapshot) file() fileState {
	out := fileState{Reports: []fileReport{}}
	for _, report := range s.reports {
		out.Reports = append(out.Reports, fileReport{
			RequestID: report.requestID, EndpointID: report.endpointID, AttemptID: report.attemptID,
			Frame: append([]byte(nil), report.frame...),
		})
	}
	for _, id := range s.order {
		current := s.items[id]
		if current == nil {
			continue
		}
		item := fileAssignment{
			PauseReason: current.pauseReason,
			RequestID:   current.assignment.RequestID,
			Method:      current.assignment.Method,
			Path:        current.assignment.Path,
			Query:       current.assignment.Query,
			Headers:     cloneHeaders(current.assignment.Headers),
			Trailers:    cloneHeaders(current.assignment.Trailers),
			Body:        current.assignment.Body,
		}
		for _, endpoint := range current.assignment.Endpoints {
			item.Endpoints = append(item.Endpoints, fileEndpoint{
				ID: endpoint.ID, URL: endpoint.URL, DeliveryIntervalMS: endpoint.DeliveryIntervalMS,
				IdempotencyKey: endpoint.IdempotencyKey, Enabled: endpoint.Enabled,
			})
			d := current.deliveries[endpoint.ID]
			if d == nil {
				continue
			}
			stored := fileDelivery{
				EndpointID: endpoint.ID, BudgetUsed: d.budget, Baseline: d.baseline,
				Final: d.final, Next: d.next, Inflight: d.inflight,
			}
			for _, attempt := range d.attempts {
				row := fileAttempt{
					AttemptID: attempt.attemptID, Number: attempt.number, Outcome: attempt.outcome,
					Error: attempt.error, DurationMS: attempt.duration.Milliseconds(),
					QueueWaitKnown: attempt.queueWaitKnown,
				}
				if attempt.hasStatus {
					status := attempt.status
					row.HTTPStatus = &status
				}
				if attempt.queueWaitKnown {
					wait := attempt.queueWait.Milliseconds()
					row.QueueWaitMS = &wait
				}
				stored.Attempts = append(stored.Attempts, row)
			}
			item.Deliveries = append(item.Deliveries, stored)
		}
		out.Assignments = append(out.Assignments, item)
	}
	if out.Assignments == nil {
		out.Assignments = []fileAssignment{}
	}
	return out
}

func (file fileState) snapshot() snapshot {
	out := newSnapshot()
	for _, report := range file.Reports {
		out.reports = append(out.reports, storedReport{
			requestID: report.RequestID, endpointID: report.EndpointID, attemptID: report.AttemptID,
			frame: append([]byte(nil), report.Frame...),
		})
	}
	for _, stored := range file.Assignments {
		assignment := Assignment{
			RequestID: stored.RequestID, Method: stored.Method, Path: stored.Path, Query: stored.Query,
			Headers: cloneHeaders(stored.Headers), Trailers: cloneHeaders(stored.Trailers), Body: stored.Body,
		}
		for _, endpoint := range stored.Endpoints {
			assignment.Endpoints = append(assignment.Endpoints, Endpoint{
				ID: endpoint.ID, URL: endpoint.URL, DeliveryIntervalMS: endpoint.DeliveryIntervalMS,
				IdempotencyKey: endpoint.IdempotencyKey, Enabled: endpoint.Enabled,
			})
		}
		current := newItem(assignment)
		current.pauseReason = stored.PauseReason
		for _, storedDelivery := range stored.Deliveries {
			d := current.deliveries[storedDelivery.EndpointID]
			if d == nil {
				d = &delivery{}
				current.deliveries[storedDelivery.EndpointID] = d
			}
			d.budget = storedDelivery.BudgetUsed
			d.baseline = storedDelivery.Baseline
			d.final = storedDelivery.Final
			d.inflight = storedDelivery.Inflight
			if storedDelivery.Next != nil {
				at := storedDelivery.Next.UTC()
				d.next = &at
			}
			for _, attempt := range storedDelivery.Attempts {
				rec := attemptRec{
					attemptID: attempt.AttemptID, number: attempt.Number, outcome: attempt.Outcome,
					error: attempt.Error, duration: time.Duration(attempt.DurationMS) * time.Millisecond,
					queueWaitKnown: attempt.QueueWaitKnown,
				}
				if attempt.HTTPStatus != nil {
					rec.hasStatus = true
					rec.status = *attempt.HTTPStatus
				}
				if attempt.QueueWaitMS != nil {
					rec.queueWaitKnown = true
					rec.queueWait = time.Duration(*attempt.QueueWaitMS) * time.Millisecond
				}
				d.attempts = append(d.attempts, rec)
			}
		}
		out.items[assignment.RequestID] = current
		out.order = append(out.order, assignment.RequestID)
	}
	return out
}

func (s snapshot) marshal() ([]byte, error) {
	return json.Marshal(s.file())
}
