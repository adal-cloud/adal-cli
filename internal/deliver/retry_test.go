package deliver

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/adal-cloud/adal-cli/internal/journal"
)

func TestTenFailuresFollowTheDelaySequence(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	assignment := assignmentWith(requestID(1), endpointA, 0)
	store(t, book, assignment)
	sched, buf := loggedSched(book, clock, func(_ context.Context, item journal.Assignment, endpoint journal.Endpoint) journal.Result {
		rec.add(clock.Now(), item.RequestID)
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeFailure, Error: "unexpected status", Status: 500}
	})
	sched.Submit(assignment)

	for n := 1; n <= 10; n++ {
		waitCount(t, rec, n)
		if n == 10 {
			break
		}
		waitDelays(t, buf, n)
		got := scheduledDelays(buf.String())
		want := journal.RetryDelays[n-1].Milliseconds()
		if len(got) != n || got[n-1] != want {
			t.Fatalf("delays after attempt %d = %v, want %d", n, got, want)
		}
		clock.Advance(journal.RetryDelays[n-1])
	}
	clock.Advance(24 * time.Hour)
	time.Sleep(30 * time.Millisecond)
	if rec.count() != 10 || strings.Count(buf.String(), "Retry scheduled") != 9 {
		t.Fatalf("calls = %d log = %s", rec.count(), buf.String())
	}
}

func TestSuccessStopsFurtherAttempts(t *testing.T) {
	for _, succeedAt := range []int{1, 3, 10} {
		t.Run(fmt.Sprintf("attempt-%d", succeedAt), func(t *testing.T) {
			clock := NewManualClock()
			book := journal.New()
			rec := &recorder{}
			assignment := assignmentWith(requestID(1), endpointA, 0)
			store(t, book, assignment)
			var calls int
			sched, _ := loggedSched(book, clock, func(_ context.Context, item journal.Assignment, endpoint journal.Endpoint) journal.Result {
				rec.add(clock.Now(), item.RequestID)
				calls++
				if calls == succeedAt {
					return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeSuccess, Status: 204}
				}
				return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeFailure, Error: "timeout"}
			})
			sched.Submit(assignment)
			for n := 1; n < succeedAt; n++ {
				waitCount(t, rec, n)
				clock.Advance(journal.RetryDelays[n-1])
			}
			waitCount(t, rec, succeedAt)
			clock.Advance(24 * time.Hour)
			time.Sleep(30 * time.Millisecond)
			if rec.count() != succeedAt {
				t.Fatalf("calls = %d", rec.count())
			}
		})
	}
}

func TestLongOfflineRunsOneAttempt(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	assignment := assignmentWith(requestID(1), endpointA, 0)
	store(t, book, assignment)
	sched, buf := loggedSched(book, clock, failureCall(rec, clock))
	sched.Submit(assignment)
	waitCount(t, rec, 1)
	waitDelays(t, buf, 1)
	clock.Advance(5 * time.Hour)
	waitCount(t, rec, 2)
	time.Sleep(30 * time.Millisecond)
	if rec.count() != 2 || strings.Count(buf.String(), "Retry scheduled") != 2 {
		t.Fatalf("calls = %d log = %s", rec.count(), buf.String())
	}
	clock.Advance(journal.RetryDelays[1] - time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if rec.count() != 2 {
		t.Fatalf("early call count = %d", rec.count())
	}
	clock.Advance(time.Millisecond)
	waitCount(t, rec, 3)
}

func TestFailedAttemptWaitsBehindALaterArrival(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	first := assignmentWith(requestID(1), endpointA, 0)
	second := assignmentWith(requestID(2), endpointA, 0)
	store(t, book, first)
	store(t, book, second)
	sched, _ := loggedSched(book, clock, func(_ context.Context, item journal.Assignment, endpoint journal.Endpoint) journal.Result {
		rec.add(clock.Now(), item.RequestID)
		if item.RequestID == first.RequestID {
			return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeFailure, Error: "request failed"}
		}
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeSuccess, Status: 204}
	})
	sched.Submit(first)
	waitCount(t, rec, 1)
	sched.Submit(second)
	waitCount(t, rec, 2)
	if ids := rec.idsSnapshot(); ids[0] != first.RequestID || ids[1] != second.RequestID {
		t.Fatalf("order = %v", ids)
	}
	clock.Advance(time.Minute)
	waitCount(t, rec, 3)
	if ids := rec.idsSnapshot(); ids[2] != first.RequestID {
		t.Fatalf("order = %v", ids)
	}
}

func TestRetryAlsoWaitsForTheInterval(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	first := assignmentWith(requestID(1), endpointA, 1000)
	second := assignmentWith(requestID(2), endpointA, 1000)
	store(t, book, first)
	store(t, book, second)
	sched, _ := loggedSched(book, clock, func(_ context.Context, item journal.Assignment, endpoint journal.Endpoint) journal.Result {
		rec.add(clock.Now(), item.RequestID)
		if item.RequestID == first.RequestID {
			return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeFailure, Error: "unexpected status", Status: 404}
		}
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeSuccess, Status: 200}
	})
	sched.Submit(first)
	waitCount(t, rec, 1)
	sched.Submit(second)
	time.Sleep(20 * time.Millisecond)
	if rec.count() != 1 {
		t.Fatalf("second call started early: %d", rec.count())
	}
	clock.Advance(999 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if rec.count() != 1 {
		t.Fatalf("interval was shortened: %d", rec.count())
	}
	clock.Advance(time.Millisecond)
	waitCount(t, rec, 2)
	clock.Advance(time.Minute)
	waitCount(t, rec, 3)
	if ids := rec.idsSnapshot(); ids[0] != first.RequestID || ids[1] != second.RequestID || ids[2] != first.RequestID {
		t.Fatalf("order = %v", ids)
	}
}

func TestReplayDoesNotScheduleTwice(t *testing.T) {
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	assignment := assignmentWith(requestID(1), endpointA, 0)
	store(t, book, assignment)
	sched, _ := loggedSched(book, clock, failureCall(rec, clock))
	sched.Submit(assignment)
	waitCount(t, rec, 1)
	sched.Resume()
	sched.Resume()
	time.Sleep(20 * time.Millisecond)
	clock.Advance(time.Minute)
	waitCount(t, rec, 2)
	time.Sleep(20 * time.Millisecond)
	if rec.count() != 2 {
		t.Fatalf("calls = %d", rec.count())
	}
}

func TestRestartResumesOnlyUnfinishedEndpoints(t *testing.T) {
	clock := NewManualClock()
	dir := t.TempDir()
	book, err := journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	failed := assignmentWith(requestID(1), endpointA, 0)
	done := assignmentWith(requestID(2), endpointB, 0)
	store(t, book, failed)
	store(t, book, done)
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	buf := &safeBuf{}
	sched := NewScheduler(ctx, clock, func(_ context.Context, item journal.Assignment, endpoint journal.Endpoint) journal.Result {
		rec.add(clock.Now(), item.RequestID)
		if item.RequestID == done.RequestID {
			return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeSuccess, Status: 204}
		}
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeFailure, Error: "timeout"}
	}, book, nil, log.New(buf, "", 0))
	sched.Submit(failed)
	sched.Submit(done)
	waitCount(t, rec, 2)
	waitDelays(t, buf, 1)
	cancel()
	time.Sleep(20 * time.Millisecond)
	if err := book.Close(); err != nil {
		t.Fatal(err)
	}

	reloaded, err := journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()
	again := &recorder{}
	next, _ := loggedSched(reloaded, clock, func(_ context.Context, item journal.Assignment, endpoint journal.Endpoint) journal.Result {
		again.add(clock.Now(), item.RequestID)
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeFailure, Error: "timeout"}
	})
	next.Resume()
	time.Sleep(30 * time.Millisecond)
	if again.count() != 0 {
		t.Fatalf("restart called early: %v", again.idsSnapshot())
	}
	clock.Advance(time.Minute)
	waitCount(t, again, 1)
	if ids := again.idsSnapshot(); ids[0] != failed.RequestID {
		t.Fatalf("resumed = %v", ids)
	}
	clock.Advance(24 * time.Hour)
	waitCount(t, again, 2)
	if ids := again.idsSnapshot(); ids[0] != failed.RequestID || ids[1] != failed.RequestID {
		t.Fatalf("resumed = %v", ids)
	}
}

func TestNonSuccessResultsRetry(t *testing.T) {
	cases := []journal.Result{
		{Outcome: journal.OutcomeFailure, Error: "unexpected status", Status: 302},
		{Outcome: journal.OutcomeFailure, Error: "unexpected status", Status: 404},
		{Outcome: journal.OutcomeFailure, Error: "unexpected status", Status: 500},
		{Outcome: journal.OutcomeFailure, Error: "timeout"},
		{Outcome: journal.OutcomeFailure, Error: "request failed"},
	}
	for _, result := range cases {
		name := result.Error
		if result.Status != 0 {
			name = fmt.Sprintf("%s-%d", name, result.Status)
		}
		t.Run(name, func(t *testing.T) {
			clock := NewManualClock()
			book := journal.New()
			rec := &recorder{}
			assignment := assignmentWith(requestID(1), endpointA, 0)
			store(t, book, assignment)
			sched, _ := loggedSched(book, clock, func(_ context.Context, item journal.Assignment, endpoint journal.Endpoint) journal.Result {
				rec.add(clock.Now(), item.RequestID)
				result.EndpointID = endpoint.ID
				return result
			})
			sched.Submit(assignment)
			waitCount(t, rec, 1)
			clock.Advance(time.Minute)
			waitCount(t, rec, 2)
		})
	}
}

func failureCall(rec *recorder, clock *ManualClock) Caller {
	return func(_ context.Context, item journal.Assignment, endpoint journal.Endpoint) journal.Result {
		rec.add(clock.Now(), item.RequestID)
		return journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeFailure, Error: "request failed"}
	}
}

func (r *recorder) idsSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...)
}

func waitDelays(t *testing.T, buf *safeBuf, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Count(buf.String(), "Retry scheduled") >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("scheduled %d, log = %s", strings.Count(buf.String(), "Retry scheduled"), buf.String())
}

func scheduledDelays(text string) []int64 {
	var out []int64
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "    delay ") {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(line, "    delay"))
		var n int64
		var unit string
		if _, err := fmt.Sscanf(value, "%d %s", &n, &unit); err != nil {
			continue
		}
		switch unit {
		case "ms":
			out = append(out, n)
		case "s":
			out = append(out, n*1000)
		case "min":
			out = append(out, n*60000)
		}
	}
	return out
}
