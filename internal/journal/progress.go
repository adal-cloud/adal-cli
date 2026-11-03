package journal

import (
	"time"
)

const attemptBudget = 10

// RetryDelays are the pauses after failed attempts 1 through 9.
var RetryDelays = []time.Duration{
	time.Minute, time.Minute, 2 * time.Minute, 5 * time.Minute,
	10 * time.Minute, 20 * time.Minute, 20 * time.Minute,
	60 * time.Minute, 60 * time.Minute,
}

// Decision is the persisted outcome of one HTTP attempt.
type Decision struct {
	Frame []byte
	Retry bool
	Due   time.Time
}

const (
	// PlanSkip means the endpoint must not be called.
	PlanSkip = "skip"
	// PlanNow means the endpoint is eligible to start.
	PlanNow = "now"
	// PlanLater means the endpoint becomes eligible at Due.
	PlanLater = "later"
)

// BeginAttempt reserves one HTTP attempt and stores that reservation before the call.
// The attempt id is stable for this try.
// Recovery records an interrupted attempt with this same id.
func (j *Journal) BeginAttempt(requestID, endpointID string) (string, bool, error) {
	var id string
	var ok bool
	err := j.commit(func(state *snapshot) error {
		current := state.item(requestID)
		if current == nil || current.pauseReason != "" {
			return nil
		}
		d := state.delivery(requestID, endpointID)
		if d.exhausted() || d.inflight != "" {
			return nil
		}
		nextID, err := newAttemptID()
		if err != nil {
			return err
		}
		d.budget++
		d.inflight = nextID
		id = nextID
		ok = true
		return nil
	})
	return id, ok, err
}

// FinishAttempt stores the outcome, the next eligibility, and the report bytes.
func (j *Journal) FinishAttempt(requestID, endpointID, attemptID string, result Result, at time.Time) (Decision, error) {
	var decision Decision
	err := j.commit(func(state *snapshot) error {
		d := state.delivery(requestID, endpointID)
		if d == nil || d.inflight != attemptID {
			return errReport
		}
		number := d.reported() + 1
		if number < 1 || number > attemptBudget {
			return errReport
		}
		frame, err := buildReport(requestID, endpointID, attemptID, number, result)
		if err != nil {
			return err
		}
		rec := attemptRec{
			attemptID: attemptID, number: number, outcome: result.Outcome, error: result.Error,
			duration: result.Duration, queueWait: result.QueueWait, queueWaitKnown: result.QueueWaitKnown,
		}
		if result.Outcome == OutcomeSuccess {
			if result.Status < 200 || result.Status > 299 {
				return errReport
			}
			rec.hasStatus = true
			rec.status = result.Status
		} else if result.Status >= 100 && result.Status <= 599 && (result.Status < 200 || result.Status > 299) {
			rec.hasStatus = true
			rec.status = result.Status
		}
		d.inflight = ""
		d.attempts = append(d.attempts, rec)
		state.reports = append(state.reports, storedReport{
			requestID: requestID, endpointID: endpointID, attemptID: attemptID, frame: append([]byte(nil), frame...),
		})
		decision.Frame = append([]byte(nil), frame...)
		if result.Outcome == OutcomeSuccess || number >= attemptBudget || d.budget >= attemptBudget {
			d.final = true
			d.next = nil
			return nil
		}
		delay := RetryDelays[number-1]
		due := at.UTC().Add(delay)
		d.next = &due
		decision.Retry = true
		decision.Due = due
		return nil
	})
	return decision, err
}

// Release returns a reservation that did not run to a report.
// The budget is restored.
// No failure is stored.
// A crash that leaves the reservation writes attempt interrupted on the next start.
func (j *Journal) Release(requestID, endpointID, attemptID string) error {
	return j.commit(func(state *snapshot) error {
		d := state.delivery(requestID, endpointID)
		if d == nil || d.inflight != attemptID {
			return nil
		}
		d.inflight = ""
		if d.budget > d.reported() {
			d.budget--
		}
		return nil
	})
}

// Abandon clears an in-flight reservation after the result was discarded.
// The budget already spent stays spent.
// No report is created.
func (j *Journal) Abandon(requestID, endpointID, attemptID string) error {
	return j.commit(func(state *snapshot) error {
		d := state.delivery(requestID, endpointID)
		if d == nil || d.inflight != attemptID {
			return nil
		}
		d.inflight = ""
		return nil
	})
}

// Recover closes attempts that were started and never finished.
// Each one consumes budget and records a failure.
// The next eligible attempt is due immediately.
func (j *Journal) Recover(now time.Time) error {
	return j.commit(func(state *snapshot) error {
		for requestID, current := range state.items {
			for endpointID, d := range current.deliveries {
				if d.inflight == "" {
					continue
				}
				id := d.inflight
				number := d.reported() + 1
				frame, err := buildReport(requestID, endpointID, id, number, Result{Outcome: OutcomeFailure, Error: "attempt interrupted"})
				if err != nil {
					return err
				}
				d.attempts = append(d.attempts, attemptRec{attemptID: id, number: number, outcome: OutcomeFailure, error: "attempt interrupted"})
				state.reports = append(state.reports, storedReport{requestID: requestID, endpointID: endpointID, attemptID: id, frame: frame})
				d.inflight = ""
				if d.final {
					continue
				}
				if d.budget >= attemptBudget || d.reported() >= attemptBudget {
					d.final = true
					d.next = nil
					continue
				}
				at := now.UTC()
				d.next = &at
			}
		}
		return nil
	})
}

// Plan reports whether an endpoint may start at now.
func (j *Journal) Plan(requestID, endpointID string, now time.Time) (string, time.Time) {
	j.mu.Lock()
	defer j.mu.Unlock()
	current := j.state.item(requestID)
	if current == nil || current.pauseReason != "" {
		return PlanSkip, time.Time{}
	}
	var endpoint Endpoint
	var found bool
	for _, item := range current.assignment.Endpoints {
		if item.ID == endpointID {
			endpoint = item
			found = true
			break
		}
	}
	d := current.deliveries[endpointID]
	if !found || !endpoint.Enabled || d.exhausted() || d.inflight != "" {
		return PlanSkip, time.Time{}
	}
	if d.next != nil && d.next.After(now) {
		return PlanLater, *d.next
	}
	return PlanNow, time.Time{}
}

// Pending returns unacknowledged report frames in arrival order.
func (j *Journal) Pending() [][]byte {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([][]byte, len(j.state.reports))
	for i, report := range j.state.reports {
		out[i] = append([]byte(nil), report.frame...)
	}
	return out
}

// PendingSendable returns reports whose assignments are not paused.
// Paused reports remain in Pending for local diagnosis.
func (j *Journal) PendingSendable() [][]byte {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out [][]byte
	for _, report := range j.state.reports {
		if current := j.state.item(report.requestID); current != nil && current.pauseReason != "" {
			continue
		}
		out = append(out, append([]byte(nil), report.frame...))
	}
	return out
}

// ForgetReport drops one pending report. Delivery progress stays.
func (j *Journal) ForgetReport(requestID, attemptID string) error {
	return j.commit(func(state *snapshot) error {
		state.reports = dropReports(state.reports, func(report storedReport) bool {
			return report.requestID == requestID && report.attemptID == attemptID
		})
		return nil
	})
}

// applyAssignmentCounts keeps each endpoint's budget independent in protocol 3.
func applyAssignmentCounts(current *item, assignment Assignment) {

	for _, e := range assignment.Endpoints {
		d := current.deliveries[e.ID]
		if d == nil || e.AttemptCount <= d.reported() {
			continue
		}
		if d.inflight != "" && len(d.attempts) < e.AttemptCount {
			if e.AttemptCount > d.budget {
				d.budget = e.AttemptCount
			}
			continue
		}
		d.baseline = e.AttemptCount - len(d.attempts)
		if e.AttemptCount > d.budget {
			d.budget = e.AttemptCount
		}
		if d.exhausted() {
			d.final = true
			d.next = nil
			d.inflight = ""
		}
	}
}
