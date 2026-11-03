package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/adal-cloud/adal-cli/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), cli.StopSignals()...)
	defer stop()
	os.Exit(cli.Run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
