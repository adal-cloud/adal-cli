package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestParseDefaultsAndTrim(t *testing.T) {
	t.Setenv("ADAL_API_HOST", "")
	cfg, err := Parse([]string{"--token", "  secret  "}, ioDiscard())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != defaultHost {
		t.Fatalf("host = %q, want %q", cfg.Host, defaultHost)
	}
	if cfg.Token != "secret" {
		t.Fatalf("token = %q, want trimmed secret", cfg.Token)
	}

	cfg, err = Parse([]string{"--host", "  10.0.0.8:8009  ", "--token", "secret"}, ioDiscard())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "wss://10.0.0.8:8009" || cfg.Token != "secret" {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestParseRejectsConfiguration(t *testing.T) {
	secret := "super-secret-token"
	cases := []struct {
		name string
		args []string
		err  error
	}{
		{name: "missing token", args: nil, err: errTokenRequired},
		{name: "blank token", args: []string{"--token", "   "}, err: errTokenRequired},
		{name: "blank host", args: []string{"--host", "   ", "--token", secret}, err: errHostRequired},
		{name: "http scheme", args: []string{"--host", "http://127.0.0.1:8009", "--token", secret}, err: errHostInvalid},
		{name: "ftp scheme", args: []string{"--host", "ftp://127.0.0.1:8009", "--token", secret}, err: errHostInvalid},
		{name: "positional", args: []string{"--token", secret, "extra"}, err: errUnexpectedArg},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stderr bytes.Buffer
			_, err := Parse(tc.args, &stderr)
			if !errors.Is(err, tc.err) {
				t.Fatalf("error = %v, want %v", err, tc.err)
			}
			if strings.Contains(stderr.String(), secret) || strings.Contains(err.Error(), secret) {
				t.Fatalf("token leaked into error %q stderr %q", err, stderr.String())
			}
		})
	}
}

func TestRunRejectsConfiguration(t *testing.T) {
	secret := "super-secret-token"
	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), []string{"--host", "http://127.0.0.1:8009", "--token", secret}, &stdout, &stderr)
	if code == exitOK {
		t.Fatal("exit code 0, want failure")
	}
	if !strings.Contains(stderr.String(), errHostInvalid.Error()) {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if strings.Contains(stderr.String(), secret) {
		t.Fatalf("token leaked into stderr %q", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunHelp(t *testing.T) {
	t.Setenv("ADAL_API_HOST", "")
	var stderr bytes.Buffer
	code := Run(context.Background(), []string{"--help"}, io.Discard, &stderr)
	if code != exitOK {
		t.Fatalf("exit code %d, want %d", code, exitOK)
	}
	text := stderr.String()
	if !strings.Contains(text, "-token") || !strings.Contains(text, defaultHost) {
		t.Fatalf("stderr = %q", text)
	}
}

func TestRunStopsWhenContextIsCancelled(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := "ws://" + ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	stderr := ioDiscard()
	go func() {
		done <- Run(ctx, []string{"--host", host, "--token", "s3cr3t-value"}, io.Discard, stderr)
	}()

	select {
	case code := <-done:
		t.Fatalf("Run returned %d before cancellation", code)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	select {
	case code := <-done:
		if code != exitOK {
			t.Fatalf("exit code %d, want %d", code, exitOK)
		}
		if strings.Contains(stderr.String(), "s3cr3t-value") {
			t.Fatalf("token leaked into stderr %q", stderr.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestStopSignalsExcludeKill(t *testing.T) {
	got := StopSignals()
	if len(got) != 2 || got[0] != os.Interrupt || got[1] != syscall.SIGTERM {
		t.Fatalf("signals = %v", got)
	}
	for _, sig := range got {
		if sig == os.Kill || sig == syscall.SIGKILL {
			t.Fatalf("SIGKILL is included: %v", got)
		}
	}
}

func TestRunUnknownFlag(t *testing.T) {
	const secret = "s3cr3t-value"
	var stderr bytes.Buffer
	code := Run(context.Background(), []string{"--token", secret, "--nope"}, io.Discard, &stderr)
	if code == exitOK {
		t.Fatal("exit code 0, want failure")
	}
	text := stderr.String()
	if strings.Count(text, "not defined") != 1 {
		t.Fatalf("stderr = %q", text)
	}
	if strings.Contains(text, secret) {
		t.Fatalf("token leaked into stderr %q", text)
	}
}

func TestRunWritesConnectionLog(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := "ws://" + ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- Run(ctx, []string{"--host", host, "--token", "s3cr3t-value"}, &stdout, &stderr)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case code := <-done:
		if code != exitOK {
			t.Fatalf("exit code %d, stderr %q", code, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	text := stdout.String()
	if !strings.Contains(text, "Connecting") || !strings.Contains(text, logField("host", host)) || !strings.Contains(text, "Stopped") {
		t.Fatalf("stdout = %q", text)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if strings.Contains(text, "s3cr3t-value") {
		t.Fatalf("token leaked into stdout %q", text)
	}
}

func ioDiscard() *bytes.Buffer {
	return &bytes.Buffer{}
}

func logField(label, value string) string {
	return fmt.Sprintf("    %-*s  %s", 10, label, value)
}
