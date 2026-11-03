package deliver

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adal-cloud/adal-cli/internal/journal"
)

type recorder struct {
	mu    sync.Mutex
	times []time.Time
	ids   []string
}

func (r *recorder) add(at time.Time, id string) {
	r.mu.Lock()
	r.times = append(r.times, at)
	r.ids = append(r.ids, id)
	r.mu.Unlock()
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.times)
}

func (r *recorder) snapshot() ([]time.Time, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	times := append([]time.Time(nil), r.times...)
	ids := append([]string(nil), r.ids...)
	return times, ids
}

func waitCount(t *testing.T, rec *recorder, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rec.count() >= n {
			if rec.count() != n {
				t.Fatalf("calls = %d, want %d", rec.count(), n)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("calls = %d, want %d", rec.count(), n)
}

func newSched(book *journal.Journal, clock *ManualClock, rec *recorder) *Scheduler {
	return NewScheduler(context.Background(), clock, func(_ context.Context, _ journal.Assignment, endpoint journal.Endpoint) journal.Result {
		rec.add(clock.Now(), endpoint.ID)
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeSuccess, Status: 204}
	}, book, nil, nil)
}

func store(t *testing.T, book *journal.Journal, assignment journal.Assignment) {
	t.Helper()
	if _, err := book.Put(assignment); err != nil {
		t.Fatal("assignment was not stored")
	}
}

func assignmentWith(requestID, endpointID string, interval int) journal.Assignment {
	return journal.Assignment{
		RequestID: requestID,
		Method:    "POST",
		Path:      "/orders",
		Endpoints: []journal.Endpoint{{
			ID:                 endpointID,
			URL:                "http://example.test/hook",
			DeliveryIntervalMS: interval,
			Enabled:            true,
		}},
	}
}

func requestID(n int) string {
	return fmt.Sprintf("018f3c2e-7b1a-7000-8000-%012d", n)
}

func logField(label, value string) string {
	return fmt.Sprintf("    %-*s  %s", 10, label, value)
}

func TestIntervalZeroDoesNotPause(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	sched := newSched(book, clock, rec)
	first := assignmentWith(requestID(1), endpointA, 0)
	second := assignmentWith(requestID(2), endpointA, 0)
	store(t, book, first)
	store(t, book, second)
	sched.Submit(first)
	sched.Submit(second)
	waitCount(t, rec, 2)
	times, _ := rec.snapshot()
	for i, got := range times {
		if !got.Equal(clock.Now()) {
			t.Fatalf("start %d = %s, want %s", i, got, clock.Now())
		}
	}
}

func TestEndpointsDoNotShareAQueue(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	sched := newSched(book, clock, rec)
	assignment := assignmentWith(requestID(1), endpointA, 5000)
	assignment.Endpoints = append(assignment.Endpoints, journal.Endpoint{
		ID:                 endpointB,
		URL:                "http://example.test/other",
		DeliveryIntervalMS: 5000,
		Enabled:            true,
	})
	store(t, book, assignment)
	sched.Submit(assignment)
	waitCount(t, rec, 2)
	times, ids := rec.snapshot()
	seen := map[string]bool{}
	for i, got := range times {
		if !got.Equal(time.Unix(0, 0).UTC()) {
			t.Fatalf("start %d = %s", i, got)
		}
		seen[ids[i]] = true
	}
	if !seen[endpointA] || !seen[endpointB] {
		t.Fatalf("ids = %v", ids)
	}
}

func TestArrivalsDuringTheFirstSecondDoNotResetTheClock(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	sched := newSched(book, clock, rec)
	origin := clock.Now()
	for n := 0; n < 10; n++ {
		if n > 0 {
			clock.Advance(100 * time.Millisecond)
		}
		assignment := assignmentWith(requestID(n), endpointA, 1000)
		store(t, book, assignment)
		sched.Submit(assignment)
		if n == 0 {
			waitCount(t, rec, 1)
			continue
		}
		if rec.count() != 1 {
			t.Fatalf("call started early at %s", clock.Now())
		}
	}
	clock.Advance(100 * time.Millisecond)
	waitCount(t, rec, 2)
	for n := 3; n <= 10; n++ {
		clock.Advance(time.Second)
		waitCount(t, rec, n)
	}
	times, _ := rec.snapshot()
	if len(times) != 10 {
		t.Fatalf("starts = %d", len(times))
	}
	for i, got := range times {
		want := origin.Add(time.Duration(i) * time.Second)
		if !got.Equal(want) {
			t.Fatalf("start %d = %s, want %s", i, got, want)
		}
	}
	if len(book.Results(requestID(0))) != 1 || len(book.Results(requestID(9))) != 1 {
		t.Fatal("outcomes were not recorded")
	}
}

func TestIntervalUsesTheCallSnapshot(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	sched := newSched(book, clock, rec)
	origin := clock.Now()
	first := assignmentWith(requestID(1), endpointA, 5000)
	second := assignmentWith(requestID(2), endpointA, 500)
	third := assignmentWith(requestID(3), endpointA, 1000)
	store(t, book, first)
	store(t, book, second)
	store(t, book, third)
	sched.Submit(first)
	sched.Submit(second)
	sched.Submit(third)
	waitCount(t, rec, 1)
	clock.Advance(500 * time.Millisecond)
	waitCount(t, rec, 2)
	clock.Advance(time.Second)
	waitCount(t, rec, 3)
	times, _ := rec.snapshot()
	want := []time.Time{origin, origin.Add(500 * time.Millisecond), origin.Add(1500 * time.Millisecond)}
	for i, got := range times {
		if !got.Equal(want[i]) {
			t.Fatalf("start %d = %s, want %s", i, got, want[i])
		}
	}
}

func TestIntervalSurvivesAnEmptyQueue(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	sched := newSched(book, clock, rec)
	origin := clock.Now()
	first := assignmentWith(requestID(1), endpointA, 1000)
	second := assignmentWith(requestID(2), endpointA, 1000)
	store(t, book, first)
	store(t, book, second)
	sched.Submit(first)
	waitCount(t, rec, 1)
	time.Sleep(20 * time.Millisecond)
	clock.Advance(100 * time.Millisecond)
	sched.Submit(second)
	time.Sleep(50 * time.Millisecond)
	if rec.count() != 1 {
		t.Fatalf("second call started at %s", clock.Now())
	}
	clock.Advance(900 * time.Millisecond)
	waitCount(t, rec, 2)
	times, _ := rec.snapshot()
	if !times[1].Equal(origin.Add(time.Second)) {
		t.Fatalf("second start = %s", times[1])
	}
}

func TestDisabledEndpointIsNotCalled(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	sched := newSched(book, clock, rec)
	assignment := assignmentWith(requestID(1), endpointA, 0)
	assignment.Endpoints = append(assignment.Endpoints, journal.Endpoint{
		ID:      endpointB,
		URL:     "http://example.test/other",
		Enabled: false,
	})
	store(t, book, assignment)
	sched.Submit(assignment)
	waitCount(t, rec, 1)
	time.Sleep(20 * time.Millisecond)
	_, ids := rec.snapshot()
	if len(ids) != 1 || ids[0] != endpointA {
		t.Fatalf("ids = %v", ids)
	}
}

func TestSubmitDoesNotCallTheSameEndpointTwice(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	sched := newSched(book, clock, rec)
	assignment := assignmentWith(requestID(1), endpointA, 0)
	assignment.Endpoints = append(assignment.Endpoints, assignment.Endpoints[0])
	store(t, book, assignment)
	sched.Submit(assignment)
	sched.Submit(assignment)
	waitCount(t, rec, 1)
	time.Sleep(20 * time.Millisecond)
	if rec.count() != 1 {
		t.Fatalf("calls = %d", rec.count())
	}
}

func TestCancelSkipsAQueuedCall(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	release := make(chan struct{})
	sched := NewScheduler(context.Background(), clock, func(_ context.Context, assignment journal.Assignment, endpoint journal.Endpoint) journal.Result {
		rec.add(clock.Now(), endpoint.ID)
		if assignment.RequestID == requestID(1) {
			<-release
		}
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeSuccess}
	}, book, nil, nil)
	first := assignmentWith(requestID(1), endpointA, 0)
	second := assignmentWith(requestID(2), endpointA, 1000)
	store(t, book, first)
	store(t, book, second)
	sched.Submit(first)
	waitCount(t, rec, 1)
	sched.Submit(second)
	book.Drop(second.RequestID)
	sched.Cancel(second.RequestID)
	close(release)
	clock.Advance(2 * time.Second)
	time.Sleep(50 * time.Millisecond)
	if rec.count() != 1 || len(book.Results(requestID(2))) != 0 {
		t.Fatalf("calls = %d results = %d", rec.count(), len(book.Results(requestID(2))))
	}
}

func TestQueueWaitIncludesTheInterval(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	sched := newSched(book, clock, rec)
	first := assignmentWith(requestID(1), endpointA, 1000)
	second := assignmentWith(requestID(2), endpointA, 1000)
	store(t, book, first)
	store(t, book, second)
	sched.Submit(first)
	sched.Submit(second)
	waitCount(t, rec, 1)
	clock.Advance(time.Second)
	waitCount(t, rec, 2)
	assertQueueWait(t, book, requestID(1), 0)
	assertQueueWait(t, book, requestID(2), time.Second)
}

func TestBackwardClockLeavesQueueWaitUnknown(t *testing.T) {
	later := time.Unix(10, 0).UTC()
	clock := &scriptClock{times: []time.Time{later, later, later.Add(-time.Second)}}
	book := journal.New()
	assignment := assignmentWith(requestID(1), endpointA, 0)
	store(t, book, assignment)
	buf := &safeBuf{}
	sched := NewScheduler(context.Background(), clock, func(_ context.Context, _ journal.Assignment, endpoint journal.Endpoint) journal.Result {
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeSuccess, Status: 204, Duration: 15 * time.Millisecond}
	}, book, nil, log.New(buf, "", log.LstdFlags))
	sched.Submit(assignment)
	text := waitLog(t, buf, "Call finished\n"+logField("request", requestID(1)))
	got := book.Results(requestID(1))
	if len(got) != 1 || got[0].QueueWaitKnown || strings.Contains(text, "\n    queue ") ||
		!strings.Contains(text, logField("duration", "15 ms")) ||
		!strings.Contains(text, logField("outcome", "success")) ||
		!strings.Contains(text, logField("status", "HTTP 204")) {
		t.Fatalf("known %v log %q", len(got) == 1 && got[0].QueueWaitKnown, text)
	}
}

func assertQueueWait(t *testing.T, book *journal.Journal, requestID string, want time.Duration) {
	t.Helper()
	got := book.Results(requestID)
	if len(got) != 1 || !got[0].QueueWaitKnown || got[0].QueueWait != want {
		t.Fatalf("wait = %+v, want %s", got, want)
	}
}

type scriptClock struct {
	mu    sync.Mutex
	times []time.Time
	n     int
}

func (c *scriptClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n >= len(c.times) {
		return c.times[len(c.times)-1]
	}
	current := c.times[c.n]
	c.n++
	return current
}

func (c *scriptClock) Until(time.Time) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func TestCallSuccessIsLogged(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	assignment := assignmentWith(requestID(1), endpointA, 0)
	assignment.Body = "c2VjcmV0LWJvZHk="
	assignment.Headers = map[string][]string{"X-Secret": {"header-secret"}}
	assignment.Endpoints[0].URL = "http://enabled.example/hook"
	assignment.Endpoints = append(assignment.Endpoints, journal.Endpoint{
		ID:      endpointB,
		URL:     "http://disabled.example/secret",
		Enabled: false,
	})
	store(t, book, assignment)
	sched, buf := loggedSched(book, clock, func(_ context.Context, _ journal.Assignment, endpoint journal.Endpoint) journal.Result {
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeSuccess, Status: 204, Duration: 15 * time.Millisecond}
	})
	sched.Submit(assignment)
	text := waitLog(t, buf, "Call finished\n"+logField("request", requestID(1)))
	if !strings.Contains(text, "Call queued\n"+logField("request", requestID(1))) ||
		!strings.Contains(text, "Call starting\n"+logField("request", requestID(1))) ||
		!strings.Contains(text, logField("endpoint", "http://enabled.example/hook/orders")) ||
		!strings.Contains(text, logField("outcome", "success")) ||
		!strings.Contains(text, logField("status", "HTTP 204")) ||
		!strings.Contains(text, logField("duration", "15 ms")) ||
		!strings.Contains(text, logField("queue", "0 ms")) {
		t.Fatalf("log = %q", text)
	}
	if strings.Contains(text, endpointB) || strings.Contains(text, "disabled.example") || strings.Contains(text, "c2VjcmV0LWJvZHk=") || strings.Contains(text, "header-secret") {
		t.Fatalf("request leaked into %q", text)
	}
}

func TestCallFailureIsLogged(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	assignment := assignmentWith(requestID(1), endpointA, 0)
	assignment.Endpoints[0].URL = "http://enabled.example/hook"
	store(t, book, assignment)
	sched, buf := loggedSched(book, clock, func(_ context.Context, _ journal.Assignment, endpoint journal.Endpoint) journal.Result {
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeFailure, Error: "timeout", Duration: -time.Second}
	})
	sched.Submit(assignment)
	text := waitLog(t, buf, "Call finished\n"+logField("request", requestID(1)))
	if strings.Contains(text, "\n    status ") ||
		!strings.Contains(text, logField("endpoint", "http://enabled.example/hook/orders")) ||
		!strings.Contains(text, logField("outcome", "failure")) ||
		!strings.Contains(text, logField("error", "timeout")) ||
		!strings.Contains(text, logField("duration", "0 ms")) ||
		!strings.Contains(text, logField("queue", "0 ms")) {
		t.Fatalf("log = %q", text)
	}

	clock = NewManualClock()
	book = journal.New()
	second := assignmentWith(requestID(2), endpointA, 0)
	second.Endpoints[0].URL = "http://enabled.example/hook"
	store(t, book, second)
	sched, buf = loggedSched(book, clock, func(_ context.Context, _ journal.Assignment, endpoint journal.Endpoint) journal.Result {
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeFailure, Status: 502, Error: "unexpected status", Duration: 4 * time.Millisecond}
	})
	sched.Submit(second)
	waitLog(t, buf, "Call finished\n"+logField("request", requestID(2)))
	text = buf.String()
	if !strings.Contains(text, logField("error", "unexpected status")) ||
		!strings.Contains(text, logField("status", "HTTP 502")) ||
		!strings.Contains(text, logField("duration", "4 ms")) ||
		!strings.Contains(text, logField("endpoint", "http://enabled.example/hook/orders")) {
		t.Fatalf("log = %q", text)
	}
}

func TestCallCancelIsLogged(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	release := make(chan struct{})
	sched, buf := loggedSched(book, clock, func(_ context.Context, assignment journal.Assignment, endpoint journal.Endpoint) journal.Result {
		if assignment.RequestID == requestID(1) {
			<-release
		}
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeSuccess, Status: 204, Duration: time.Millisecond}
	})
	first := assignmentWith(requestID(1), endpointA, 0)
	second := assignmentWith(requestID(2), endpointA, 1000)
	store(t, book, first)
	store(t, book, second)
	sched.Submit(first)
	waitLog(t, buf, "Call starting\n"+logField("request", requestID(1)))
	sched.Submit(second)
	waitLog(t, buf, "Call queued\n"+logField("request", requestID(2)))
	book.Drop(second.RequestID)
	sched.Cancel(second.RequestID)
	close(release)
	clock.Advance(2 * time.Second)
	text := waitLog(t, buf, "Call cancelled\n"+logField("request", requestID(2)))
	waitLog(t, buf, "Call finished\n"+logField("request", requestID(1)))
	time.Sleep(20 * time.Millisecond)
	if !strings.Contains(buf.String(), logField("duration", "1 ms")) || !strings.Contains(buf.String(), logField("queue", "0 ms")) {
		t.Fatalf("log = %q", buf.String())
	}
	if strings.Count(text, "Call cancelled\n"+logField("request", requestID(2))) != 1 {
		t.Fatalf("log = %q", text)
	}
}

func TestCallDiscardIsLogged(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	assignment := assignmentWith(requestID(1), endpointA, 0)
	assignment.Endpoints[0].URL = "http://enabled.example/hook"
	store(t, book, assignment)
	sched, buf := loggedSched(book, clock, func(_ context.Context, assignment journal.Assignment, endpoint journal.Endpoint) journal.Result {
		book.Drop(assignment.RequestID)
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeFailure, Error: "timeout", Duration: 30 * time.Millisecond}
	})
	sched.Submit(assignment)
	text := waitLog(t, buf, "Call discarded\n"+logField("request", requestID(1)))
	time.Sleep(20 * time.Millisecond)
	if strings.Contains(buf.String(), "Call finished") || !strings.Contains(text, logField("endpoint", "http://enabled.example/hook/orders")) {
		t.Fatalf("log = %q", buf.String())
	}
}

func loggedSched(book *journal.Journal, clock *ManualClock, call Caller) (*Scheduler, *safeBuf) {
	buf := &safeBuf{}
	return NewScheduler(context.Background(), clock, call, book, nil, log.New(buf, "", log.LstdFlags)), buf
}

type safeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitLog(t *testing.T, buf *safeBuf, want string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		text := buf.String()
		if strings.Contains(text, want) {
			return text
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("log = %q, missing %q", buf.String(), want)
	return ""
}
