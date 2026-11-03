package cli

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
)

func TestUsageDoesNotExposeAPIHost(t *testing.T) {
	const secret = "private-host-credential"
	t.Setenv("ADAL_API_HOST", "wss://user:"+secret+"@example.test?token="+secret)
	for _, args := range [][]string{{"--help"}, {"--token", "secret", "--nope"}, {"--host", "ws://localhost:8009", "--nope"}} {
		var stderr bytes.Buffer
		Run(context.Background(), args, io.Discard, &stderr)
		if strings.Contains(stderr.String(), secret) {
			t.Fatal("usage exposed the unvalidated environment value")
		}
	}
}

func TestAPIHostSelection(t *testing.T) {
	for _, tc := range []struct {
		name, env, flag, want string
	}{
		{name: "default", want: "wss://api.adal.cloud"},
		{name: "blank environment", env: "  ", want: "wss://api.adal.cloud"},
		{name: "environment", env: "  wss://api.example.test/  ", want: "wss://api.example.test/"},
		{name: "environment without scheme", env: " api.se.adal.cloud ", want: "wss://api.se.adal.cloud"},
		{name: "environment with explicit ws", env: "ws://api.se.adal.cloud", want: "ws://api.se.adal.cloud"},
		{name: "environment without scheme with port", env: "localhost:8009", want: "wss://localhost:8009"},
		{name: "flag wins", env: "wss://api.example.test", flag: " ws://localhost:8009 ", want: "ws://localhost:8009"},
		{name: "flag replaces invalid environment", env: "invalid://host", flag: "127.0.0.1:8009", want: "wss://127.0.0.1:8009"},
		{name: "flag without scheme", flag: "api.se.adal.cloud", want: "wss://api.se.adal.cloud"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ADAL_API_HOST", tc.env)
			args := []string{"--token", "secret"}
			if tc.flag != "" {
				args = append(args, "--host", tc.flag)
			}
			cfg, err := Parse(args, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Host != tc.want {
				t.Fatalf("host = %q, want %q", cfg.Host, tc.want)
			}
		})
	}
}

func TestRejectInvalidAPIHost(t *testing.T) {
	for _, host := range []string{
		"wss://", "wss:///missing", "wss://user:secret@example.test",
		"wss://example.test/path", "wss://example.test?token=secret", "wss://example.test#fragment",
		"wss://example.test?", "wss://example.test#", "wss://example.test:bad", "bad host:8009",
	} {
		t.Run(host, func(t *testing.T) {
			t.Setenv("ADAL_API_HOST", host)
			if _, err := Parse([]string{"--token", "secret"}, io.Discard); err == nil {
				t.Fatal("invalid host was accepted")
			}
		})
	}
}
