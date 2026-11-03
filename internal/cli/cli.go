// Package cli parses process arguments and owns the process lifetime.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/adal-cloud/adal-cli/internal/session"
)

const (
	exitOK    = 0
	exitError = 1

	defaultHost = "wss://api.adal.cloud"
)

var (
	errTokenRequired = errors.New("token is required")
	errHostRequired  = errors.New("host is required")
	errHostInvalid   = session.ErrInvalidHost
	errUnexpectedArg = errors.New("unexpected argument")
)

// Config is the process configuration after flags are parsed and trimmed.
type Config struct {
	Host  string
	Token string
}

// StopSignals are the signals that stop the process.
// SIGKILL is absent because the operating system cannot deliver it to the program.
func StopSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}

// Run parses args and blocks until ctx is cancelled.
// Operational events are written to stdout.
// A configuration error prints an English message to stderr.
// That error returns a non-zero code.
// The message does not include the token.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	cfg, err := Parse(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		if configError(err) {
			fmt.Fprintln(stderr, err.Error())
		}
		return exitError
	}
	if err := session.Run(ctx, cfg.Host, cfg.Token, stdout); err != nil {
		fmt.Fprintln(stderr, err.Error())
		return exitError
	}
	return exitOK
}

// Parse reads --host and --token from args.
// --host overrides ADAL_API_HOST, which overrides the public default.
// Surrounding whitespace is removed.
func Parse(args []string, stderr io.Writer) (Config, error) {
	fs := flag.NewFlagSet("cli", flag.ContinueOnError)
	fs.SetOutput(stderr)
	host := fs.String("host", defaultHost, "CLI Server address; defaults to wss:// without a scheme (ADAL_API_HOST)")
	token := fs.String("token", "", "channel secret")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if len(fs.Args()) > 0 {
		return Config{}, errUnexpectedArg
	}
	hostProvided := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "host" {
			hostProvided = true
		}
	})
	if !hostProvided {
		if envHost := strings.TrimSpace(os.Getenv("ADAL_API_HOST")); envHost != "" {
			*host = envHost
		}
	}
	cfg := Config{
		Host:  strings.TrimSpace(*host),
		Token: strings.TrimSpace(*token),
	}
	if cfg.Token == "" {
		return Config{}, errTokenRequired
	}
	if cfg.Host == "" {
		return Config{}, errHostRequired
	}
	if !strings.Contains(cfg.Host, "://") {
		cfg.Host = "wss://" + cfg.Host
	}
	if _, err := session.URL(cfg.Host); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func configError(err error) bool {
	return errors.Is(err, errTokenRequired) ||
		errors.Is(err, errHostRequired) ||
		errors.Is(err, errHostInvalid) ||
		errors.Is(err, errUnexpectedArg)
}
