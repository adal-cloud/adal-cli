package deliver

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/adal-cloud/adal-cli/internal/console"
	"github.com/adal-cloud/adal-cli/internal/journal"
)

// Clock is the time source used to space endpoint calls.
type Clock interface {
	Now() time.Time
	Until(time.Time) <-chan struct{}
}

// WallClock is the process clock.
type WallClock struct{}

// Now returns the current time.
func (WallClock) Now() time.Time { return time.Now() }

// Until reports when the deadline is reached.
// A deadline that is already due is reported immediately.
func (WallClock) Until(deadline time.Time) <-chan struct{} {
	ch := make(chan struct{})
	delay := time.Until(deadline)
	if delay <= 0 {
		close(ch)
		return ch
	}
	timer := time.AfterFunc(delay, func() { close(ch) })
	_ = timer
	return ch
}

// Caller performs one endpoint call.
type Caller func(context.Context, journal.Assignment, journal.Endpoint) journal.Result

type job struct {
	assignment journal.Assignment
	endpoint   journal.Endpoint
	queuedAt   time.Time
}

type lane struct {
	jobs      []job
	running   bool
	hasStart  bool
	lastStart time.Time
}

// Reporter is called after a finished call whose assignment is still stored.
// frame is the exact report to send.
// frame is empty when the report could not be stored.
type Reporter func(requestID string, result journal.Result, frame []byte)

// Scheduler runs one queue per endpoint id.
type Scheduler struct {
	ctx     context.Context
	clock   Clock
	call    Caller
	book    *journal.Journal
	report  Reporter
	log     *log.Logger
	mu      sync.Mutex
	lanes   map[string]*lane
	seen    map[string]struct{}
	waiting map[string]struct{}
	dropped map[string]struct{}
	paused  map[string]struct{}
}

// NewScheduler starts no goroutines until Submit.
// A nil logger discards delivery lines.
func NewScheduler(ctx context.Context, clock Clock, call Caller, book *journal.Journal, report Reporter, logger *log.Logger) *Scheduler {
	s := &Scheduler{
		ctx:     ctx,
		clock:   clock,
		call:    call,
		book:    book,
		report:  report,
		log:     logger,
		lanes:   map[string]*lane{},
		seen:    map[string]struct{}{},
		waiting: map[string]struct{}{},
		dropped: map[string]struct{}{},
		paused:  map[string]struct{}{},
	}
	return s
}

// Submit queues one call for each enabled endpoint that is eligible now.
// An endpoint waiting for its next attempt is armed instead of jumping the queue.
func (s *Scheduler) Submit(assignment journal.Assignment) {
	s.offer(assignment)
}

// Resume queues stored work that this process has not already started.
func (s *Scheduler) Resume() {
	if s == nil || s.book == nil {
		return
	}
	for _, assignment := range s.book.Stored() {
		s.offer(assignment)
	}
}

// Cancel removes queued calls for an assignment.
// A call that already started is left running.
func (s *Scheduler) Cancel(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropped[requestID] = struct{}{}
	s.removeQueuedLocked(requestID)
}

// Pause stops new calls.
// A call that already started keeps its outcome.
// A completed result that is waiting for the journal is kept.
func (s *Scheduler) Pause(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused[requestID] = struct{}{}
	s.removeQueuedLocked(requestID)
}

func (s *Scheduler) removeQueuedLocked(requestID string) {
	for _, lane := range s.lanes {
		kept := make([]job, 0, len(lane.jobs))
		for _, item := range lane.jobs {
			if item.assignment.RequestID != requestID {
				kept = append(kept, item)
				continue
			}
			s.logCall("Call cancelled", item.assignment, item.endpoint)
		}
		lane.jobs = kept
	}
}

func (s *Scheduler) offer(assignment journal.Assignment) {
	if s.ctx.Err() != nil {
		return
	}
	for _, endpoint := range assignment.Endpoints {
		if !endpoint.Enabled {
			continue
		}
		key := pair(assignment.RequestID, endpoint.ID)
		s.mu.Lock()
		_, paused := s.paused[assignment.RequestID]
		if _, ok := s.seen[key]; ok || paused || s.ctx.Err() != nil {
			s.mu.Unlock()
			continue
		}
		s.seen[key] = struct{}{}
		s.mu.Unlock()

		mode, due := s.book.Plan(assignment.RequestID, endpoint.ID, s.clock.Now())
		switch mode {
		case journal.PlanSkip:
			continue
		case journal.PlanLater:
			s.armRetry(assignment, endpoint, due)
		default:
			s.mu.Lock()
			s.enqueueLocked(assignment, endpoint, s.clock.Now())
			s.mu.Unlock()
		}
	}
}

func (s *Scheduler) lane(id string) *lane {
	current, ok := s.lanes[id]
	if !ok {
		current = &lane{}
		s.lanes[id] = current
	}
	return current
}

func (s *Scheduler) enqueueLocked(assignment journal.Assignment, endpoint journal.Endpoint, queuedAt time.Time) {
	_, paused := s.paused[assignment.RequestID]
	_, dropped := s.dropped[assignment.RequestID]
	if s.ctx.Err() != nil || paused || dropped {
		return
	}
	lane := s.lane(endpoint.ID)
	lane.jobs = append(lane.jobs, job{assignment: assignment, endpoint: endpoint, queuedAt: queuedAt})
	s.logCall("Call queued", assignment, endpoint)
	if !lane.running {
		lane.running = true
		go s.loop(endpoint.ID)
	}
}

func (s *Scheduler) armRetry(assignment journal.Assignment, endpoint journal.Endpoint, due time.Time) {
	key := pair(assignment.RequestID, endpoint.ID)
	s.mu.Lock()
	_, paused := s.paused[assignment.RequestID]
	if _, ok := s.waiting[key]; ok || paused {
		s.mu.Unlock()
		return
	}
	s.waiting[key] = struct{}{}
	s.mu.Unlock()

	wait := s.clock.Until(due)
	delay := due.Sub(s.clock.Now()).Milliseconds()
	if delay < 0 {
		delay = 0
	}
	address, _ := Address(endpoint.URL, assignment.Path, assignment.Query)
	s.logEvent(console.Event{
		Title:    "Retry scheduled",
		Request:  assignment.RequestID,
		Endpoint: address,
		DelayMS:  delay,
		HasDelay: true,
	})
	go func() {
		select {
		case <-s.ctx.Done():
			return
		case <-wait:
		}
		s.mu.Lock()
		delete(s.waiting, key)
		if s.ctx.Err() != nil {
			s.mu.Unlock()
			return
		}
		s.enqueueLocked(assignment, endpoint, s.clock.Now())
		s.mu.Unlock()
	}()
}

func (s *Scheduler) loop(id string) {
	for {
		s.mu.Lock()
		if s.ctx.Err() != nil || len(s.lanes[id].jobs) == 0 {
			s.lanes[id].running = false
			s.mu.Unlock()
			return
		}
		lane := s.lanes[id]
		next := lane.jobs[0]
		lane.jobs = lane.jobs[1:]
		hasStart := lane.hasStart
		last := lane.lastStart
		s.mu.Unlock()

		if s.skipStart(next) {
			s.logCall("Call cancelled", next.assignment, next.endpoint)
			continue
		}
		if hasStart {
			ready := last.Add(time.Duration(next.endpoint.DeliveryIntervalMS) * time.Millisecond)
			select {
			case <-s.ctx.Done():
				return
			case <-s.clock.Until(ready):
			}
			if s.ctx.Err() != nil {
				return
			}
		}
		if s.skipStart(next) {
			s.logCall("Call cancelled", next.assignment, next.endpoint)
			continue
		}
		mode, due := s.book.Plan(next.assignment.RequestID, next.endpoint.ID, s.clock.Now())
		if mode == journal.PlanLater {
			s.armRetry(next.assignment, next.endpoint, due)
			continue
		}
		if mode == journal.PlanSkip {
			s.logCall("Call cancelled", next.assignment, next.endpoint)
			continue
		}
		var attemptID string
		for {
			if s.ctx.Err() != nil || s.skipStart(next) {
				break
			}
			id, ok, err := s.book.BeginAttempt(next.assignment.RequestID, next.endpoint.ID)
			if err != nil {
				if !s.storageRetry("Call was not stored") {
					return
				}
				continue
			}
			if ok {
				attemptID = id
			}
			break
		}
		if attemptID == "" {
			s.logCall("Call cancelled", next.assignment, next.endpoint)
			continue
		}
		// Commit the reservation in queue order.
		// Wait for the caller to start before calculating the next interval.
		// Disk writes stay outside that clock.
		started := make(chan struct{})
		go s.perform(next, attemptID, started)
		select {
		case <-s.ctx.Done():
			return
		case <-started:
		}
	}
}

func (s *Scheduler) perform(next job, attemptID string, ready chan<- struct{}) {
	if s.ctx.Err() != nil {
		_ = s.book.Release(next.assignment.RequestID, next.endpoint.ID, attemptID)
		close(ready)
		s.logCall("Call cancelled", next.assignment, next.endpoint)
		return
	}
	if s.skipStart(next) {
		_ = s.book.Abandon(next.assignment.RequestID, next.endpoint.ID, attemptID)
		close(ready)
		s.logCall("Call cancelled", next.assignment, next.endpoint)
		return
	}
	s.logCall("Call starting", next.assignment, next.endpoint)
	started := s.clock.Now()
	s.mu.Lock()
	_, paused := s.paused[next.assignment.RequestID]
	_, dropped := s.dropped[next.assignment.RequestID]
	stopped := s.ctx.Err() != nil
	if paused || dropped || stopped {
		s.mu.Unlock()
		if stopped && !paused && !dropped {
			_ = s.book.Release(next.assignment.RequestID, next.endpoint.ID, attemptID)
		} else {
			_ = s.book.Abandon(next.assignment.RequestID, next.endpoint.ID, attemptID)
		}
		close(ready)
		s.logCall("Call cancelled", next.assignment, next.endpoint)
		return
	}
	s.lanes[next.endpoint.ID].lastStart = started
	s.lanes[next.endpoint.ID].hasStart = true
	s.mu.Unlock()
	close(ready)
	result := s.call(s.ctx, next.assignment, next.endpoint)
	if s.ctx.Err() != nil && result.Outcome != journal.OutcomeSuccess {
		_ = s.book.Release(next.assignment.RequestID, next.endpoint.ID, attemptID)
		s.logCall("Call cancelled", next.assignment, next.endpoint)
		return
	}
	if wait := started.Sub(next.queuedAt); wait >= 0 {
		result.QueueWait = wait
		result.QueueWaitKnown = true
	}
	result.EndpointID = next.endpoint.ID
	if s.skip(next) {
		_ = s.book.Abandon(next.assignment.RequestID, next.endpoint.ID, attemptID)
		s.logCall("Call discarded", next.assignment, next.endpoint)
		return
	}
	// Keep the completed result until the journal write succeeds.
	// Keep the original completion time until that write succeeds.
	// Retrying this write does not repeat the HTTP call.
	completed := s.clock.Now()
	var decision journal.Decision
	for {
		if s.skip(next) {
			_ = s.book.Abandon(next.assignment.RequestID, next.endpoint.ID, attemptID)
			s.logCall("Call discarded", next.assignment, next.endpoint)
			return
		}
		var err error
		decision, err = s.book.FinishAttempt(next.assignment.RequestID, next.endpoint.ID, attemptID, result, completed)
		if err == nil {
			break
		}
		if errors.Is(err, journal.ErrInvalidReport) {
			s.logEvent(console.Event{Title: "Report was not stored"})
			return
		}
		if !s.storageRetry("Report was not stored") {
			return
		}
	}
	s.logFinished(next.assignment, next.endpoint, result)
	if s.isPaused(next.assignment.RequestID) {
		return
	}
	if decision.Retry {
		s.armRetry(next.assignment, next.endpoint, decision.Due)
	}
	if s.report != nil {
		s.report(next.assignment.RequestID, result, decision.Frame)
	}
}

// storageRetry returns false when the process is cancelled during the wait.
func (s *Scheduler) storageRetry(message string) bool {
	wait := s.clock.Until(s.clock.Now().Add(time.Second))
	s.logEvent(console.Event{Title: message})
	select {
	case <-s.ctx.Done():
		return false
	case <-wait:
		return s.ctx.Err() == nil
	}
}

func (s *Scheduler) logEvent(e console.Event) {
	if s.log == nil {
		return
	}
	s.log.Print(console.Format(e))
}

func (s *Scheduler) logCall(title string, assignment journal.Assignment, endpoint journal.Endpoint) {
	address, _ := Address(endpoint.URL, assignment.Path, assignment.Query)
	s.logEvent(console.Event{Title: title, Request: assignment.RequestID, Endpoint: address})
}

func (s *Scheduler) logFinished(assignment journal.Assignment, endpoint journal.Endpoint, result journal.Result) {
	address, _ := Address(endpoint.URL, assignment.Path, assignment.Query)
	duration := result.Duration.Milliseconds()
	if duration < 0 {
		duration = 0
	}
	event := console.Event{
		Title:       "Call finished",
		Request:     assignment.RequestID,
		Endpoint:    address,
		Outcome:     result.Outcome,
		Error:       result.Error,
		Diagnostic:  result.Diagnostic,
		Status:      result.Status,
		DurationMS:  duration,
		HasDuration: true,
	}
	if wait, ok := result.QueueWaitMS(); ok {
		event.QueueMS = wait
		event.HasQueue = true
	}
	s.logEvent(event)
}

func (s *Scheduler) skip(next job) bool {
	s.mu.Lock()
	_, dropped := s.dropped[next.assignment.RequestID]
	s.mu.Unlock()
	if dropped {
		return true
	}
	return !s.book.Contains(next.assignment.RequestID)
}

func pair(requestID, endpointID string) string {
	return requestID + "\n" + endpointID
}

func (s *Scheduler) isPaused(requestID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, paused := s.paused[requestID]
	return paused
}

func (s *Scheduler) skipStart(next job) bool {
	return s.isPaused(next.assignment.RequestID) || s.skip(next)
}
