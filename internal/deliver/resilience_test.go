package deliver

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adal-cloud/adal-cli/internal/journal"
)

func TestStopDoesNotSpendTheAttempt(t *testing.T) {
	dir := t.TempDir()
	book, err := journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer book.Close()
	a := assignmentWith(requestID(77), endpointA, 0)
	store(t, book, a)
	started := make(chan struct{})
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	sched := NewScheduler(ctx, WallClock{}, func(context.Context, journal.Assignment, journal.Endpoint) journal.Result {
		close(started)
		<-release
		return journal.Result{Outcome: journal.OutcomeFailure, Error: "request failed"}
	}, book, nil, nil)
	sched.Submit(a)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("call did not start")
	}
	cancel()
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(book.Results(a.RequestID)) != 0 || len(book.Pending()) != 0 {
			t.Fatal("stop stored an ordinary failure")
		}
		id, ok, err := book.BeginAttempt(a.RequestID, endpointA)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if err := book.Release(a.RequestID, endpointA, id); err != nil {
				t.Fatal(err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stop left the attempt reserved")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := book.Recover(time.Now()); err != nil {
		t.Fatal(err)
	}
	if reports := book.Pending(); len(reports) != 0 {
		t.Fatalf("clean stop became an interrupted attempt: %s", reports[0])
	}
}

func TestResponsePast32KiBMustFinishBeforeSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "65536")
		_, _ = io.WriteString(w, strings.Repeat("x", 32768))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	client := NewClient()
	client.Timeout = 60 * time.Millisecond
	got := Call(context.Background(), client, sampleAssignment(), journal.Endpoint{ID: endpointA, URL: srv.URL})
	if got.Outcome != journal.OutcomeFailure || got.Error != errTimeout || got.Status != 0 {
		t.Fatalf("incomplete large body = %+v", got)
	}
}

func TestBeginWriteFailureRecoversWithoutRestart(t *testing.T) {
	dir := t.TempDir()
	book, err := journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer book.Close()
	a := assignmentWith(requestID(71), endpointA, 1000)
	store(t, book, a)
	blocker := filepath.Join(dir, "state.json.tmp")
	if err := os.Mkdir(blocker, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := NewManualClock()
	rec := &recorder{}
	buf := &safeBuf{}
	sched := NewScheduler(ctx, clock, func(context.Context, journal.Assignment, journal.Endpoint) journal.Result {
		rec.add(clock.Now(), a.RequestID)
		return journal.Result{Outcome: journal.OutcomeSuccess, Status: 204}
	}, book, nil, log.New(buf, "", 0))
	sched.Submit(a)
	waitLog(t, buf, "Call was not stored")
	if rec.count() != 0 {
		t.Fatal("HTTP started before journal commit")
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	waitCount(t, rec, 1)
}

func TestFinishWriteFailureRetriesPersistenceOnly(t *testing.T) {
	dir := t.TempDir()
	book, err := journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer book.Close()
	a := assignmentWith(requestID(72), endpointA, 0)
	store(t, book, a)
	blocker := filepath.Join(dir, "state.json.tmp")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := NewManualClock()
	rec := &recorder{}
	buf := &safeBuf{}
	sched := NewScheduler(ctx, clock, func(context.Context, journal.Assignment, journal.Endpoint) journal.Result {
		rec.add(clock.Now(), a.RequestID)
		if err := os.Mkdir(blocker, 0700); err != nil {
			t.Error(err)
		}
		return journal.Result{Outcome: journal.OutcomeSuccess, Status: 204}
	}, book, nil, log.New(buf, "", 0))
	sched.Submit(a)
	waitLog(t, buf, "Report was not stored")
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for len(book.Pending()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(book.Pending()) != 1 || len(book.Results(a.RequestID)) != 1 {
		t.Fatal("completed HTTP result was not persisted after disk recovery")
	}
	sched.Resume()
	clock.Advance(time.Hour)
	time.Sleep(20 * time.Millisecond)
	if rec.count() != 1 {
		t.Fatalf("HTTP repeated %d times", rec.count())
	}
}

type blockingStartLog struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (b *blockingStartLog) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "Call starting") {
		b.once.Do(func() { close(b.entered); <-b.release })
	}
	return len(p), nil
}
func TestIntervalMeasuredAfterStartPreparation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	block := &blockingStartLog{entered: make(chan struct{}), release: make(chan struct{})}
	defer func() {
		select {
		case <-block.release:
		default:
			close(block.release)
		}
	}()
	sched := NewScheduler(ctx, clock, func(_ context.Context, a journal.Assignment, _ journal.Endpoint) journal.Result {
		rec.add(clock.Now(), a.RequestID)
		return journal.Result{Outcome: journal.OutcomeSuccess, Status: 204}
	}, book, nil, log.New(block, "", 0))
	a := assignmentWith(requestID(73), endpointA, 1000)
	b := assignmentWith(requestID(74), endpointA, 1000)
	store(t, book, a)
	store(t, book, b)
	sched.Submit(a)
	sched.Submit(b)
	select {
	case <-block.entered:
	case <-time.After(time.Second):
		t.Fatal("first did not prepare")
	}
	clock.Advance(5 * time.Second)
	close(block.release)
	waitCount(t, rec, 1)
	time.Sleep(30 * time.Millisecond)
	if rec.count() != 1 {
		t.Fatal("second call bypassed interval after delayed start")
	}
	clock.Advance(time.Second)
	waitCount(t, rec, 2)
	times, ids := rec.snapshot()
	if ids[0] != a.RequestID || ids[1] != b.RequestID || times[1].Sub(times[0]) < time.Second {
		t.Fatalf("starts=%v order=%v", times, ids)
	}
}

func TestInvalidResultDoesNotRetryJournalForever(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := NewManualClock()
	book := journal.New()
	buf := &safeBuf{}
	a := assignmentWith(requestID(75), endpointA, 0)
	store(t, book, a)
	sched := NewScheduler(ctx, clock, func(context.Context, journal.Assignment, journal.Endpoint) journal.Result {
		return journal.Result{Outcome: journal.OutcomeSuccess}
	}, book, nil, log.New(buf, "", 0))
	sched.Submit(a)
	waitLog(t, buf, "Report was not stored")
	clock.Advance(time.Second)
	time.Sleep(30 * time.Millisecond)
	if strings.Count(buf.String(), "Report was not stored") != 1 {
		t.Fatal("permanent invalid result retried as transient disk error")
	}
}

func TestSnapshotPauseKeepsCompletedResultWhileDiskRecovers(t *testing.T) {
	dir := t.TempDir()
	book, err := journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer book.Close()
	a := assignmentWith(requestID(76), endpointA, 0)
	store(t, book, a)
	blocker := filepath.Join(dir, "state.json.tmp")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := NewManualClock()
	rec := &recorder{}
	buf := &safeBuf{}
	reported := make(chan []byte, 2)
	sched := NewScheduler(ctx, clock, func(context.Context, journal.Assignment, journal.Endpoint) journal.Result {
		rec.add(clock.Now(), a.RequestID)
		if err := os.Mkdir(blocker, 0700); err != nil {
			t.Error(err)
		}
		return journal.Result{Outcome: journal.OutcomeSuccess, Status: 202, Duration: 17 * time.Millisecond}
	}, book, func(_ string, _ journal.Result, frame []byte) { reported <- frame }, log.New(buf, "", 0))
	sched.Submit(a)
	waitLog(t, buf, "Report was not stored")
	sched.Pause(a.RequestID)
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if err := book.Pause(a.RequestID, "snapshot_unavailable"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for len(book.Pending()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	got := book.Results(a.RequestID)
	if len(got) != 1 || got[0].Outcome != journal.OutcomeSuccess || got[0].Status != 202 || got[0].Duration != 17*time.Millisecond || len(book.Pending()) != 1 {
		t.Fatalf("paused completed result was lost: results=%+v reports=%d", got, len(book.Pending()))
	}
	select {
	case <-reported:
		t.Fatal("paused report sent")
	default:
	}
	sched.Resume()
	clock.Advance(time.Hour)
	time.Sleep(20 * time.Millisecond)
	if rec.count() != 1 {
		t.Fatalf("HTTP repeated %d times", rec.count())
	}
}

func TestPauseDuringPreparationDoesNotStartHTTP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := NewManualClock()
	book := journal.New()
	rec := &recorder{}
	block := &blockingStartLog{entered: make(chan struct{}), release: make(chan struct{})}
	sched := NewScheduler(ctx, clock, func(context.Context, journal.Assignment, journal.Endpoint) journal.Result {
		rec.add(clock.Now(), "started")
		return journal.Result{Outcome: journal.OutcomeSuccess, Status: 204}
	}, book, nil, log.New(block, "", 0))
	a := assignmentWith(requestID(77), endpointA, 1000)
	store(t, book, a)
	sched.Submit(a)
	select {
	case <-block.entered:
	case <-time.After(time.Second):
		t.Fatal("call did not prepare")
	}
	sched.Pause(a.RequestID)
	close(block.release)
	time.Sleep(30 * time.Millisecond)
	if rec.count() != 0 {
		t.Fatal("HTTP started after pause during preparation")
	}
	sched.mu.Lock()
	moved := sched.lanes[endpointA].hasStart
	sched.mu.Unlock()
	if moved {
		t.Fatal("paused call moved the start clock")
	}
}
