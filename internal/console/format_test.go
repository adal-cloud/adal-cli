package console

import "testing"

func TestFormat(t *testing.T) {
	success := Event{
		Title:       "Call finished",
		Request:     "1b2c3d4e-5f60-4711-8a9b-0c1d2e3f4a5b",
		Endpoint:    "https://example.test/hook/orders?a=1",
		Outcome:     "success",
		Status:      204,
		DurationMS:  15,
		HasDuration: true,
		QueueMS:     0,
		HasQueue:    true,
	}
	if got, want := Format(success), ""+
		"Call finished\n"+
		"    request     1b2c3d4e-5f60-4711-8a9b-0c1d2e3f4a5b\n"+
		"    endpoint    https://example.test/hook/orders?a=1\n"+
		"    outcome     success\n"+
		"    status      HTTP 204\n"+
		"    duration    15 ms\n"+
		"    queue       0 ms\n"+
		"\n"; got != want {
		t.Fatalf("success = %q, want %q", got, want)
	}

	failure := Event{
		Title:       "Call finished",
		Request:     "1b2c3d4e-5f60-4711-8a9b-0c1d2e3f4a5b",
		Endpoint:    "https://example.test/hook/orders?a=1",
		Outcome:     "failure",
		Error:       "timeout",
		Diagnostic:  "timeout",
		DurationMS:  30000,
		HasDuration: true,
		QueueMS:     1000,
		HasQueue:    true,
	}
	if got, want := Format(failure), ""+
		"Call finished\n"+
		"    request     1b2c3d4e-5f60-4711-8a9b-0c1d2e3f4a5b\n"+
		"    endpoint    https://example.test/hook/orders?a=1\n"+
		"    outcome     failure\n"+
		"    error       timeout\n"+
		"    diagnostic  timeout\n"+
		"    duration    30 s\n"+
		"    queue       1 s\n"+
		"\n"; got != want {
		t.Fatalf("failure = %q, want %q", got, want)
	}

	if got, want := Format(Event{Title: "Connected"}), "Connected\n\n"; got != want {
		t.Fatalf("title = %q, want %q", got, want)
	}
	if got, want := Format(Event{Title: "Connecting", Host: "127.0.0.1:8009"}), ""+
		"Connecting\n"+
		"    host        127.0.0.1:8009\n"+
		"\n"; got != want {
		t.Fatalf("host = %q, want %q", got, want)
	}

	omitted := Format(Event{
		Title:      "Call finished",
		Outcome:    "failure",
		Error:      "timeout",
		Status:     99,
		Diagnostic: "secret resolver",
	})
	if omitted != "Call finished\n    outcome     failure\n    error       timeout\n\n" {
		t.Fatalf("omitted = %q", omitted)
	}

	units := []struct {
		ms   int64
		want string
	}{
		{0, "0 ms"},
		{-5, "0 ms"},
		{15, "15 ms"},
		{1000, "1 s"},
		{1500, "1500 ms"},
		{60000, "1 min"},
		{120000, "2 min"},
		{90000, "90000 ms"},
	}
	for _, tc := range units {
		got := Format(Event{Title: "Retry scheduled", DelayMS: tc.ms, HasDelay: true})
		want := "Retry scheduled\n    delay       " + tc.want + "\n\n"
		if got != want {
			t.Fatalf("delay %d = %q, want %q", tc.ms, got, want)
		}
	}

	flat := Format(Event{Title: "Ignored frame", Reason: "a\r\nb"})
	if flat != "Ignored frame\n    reason      a  b\n\n" {
		t.Fatalf("flat = %q", flat)
	}
}
