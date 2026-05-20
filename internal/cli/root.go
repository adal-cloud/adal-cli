package cli

import (
	"adal-cli/internal/config"
	"adal-cli/internal/session"
	"adal-cli/internal/ws"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

var runOptions = config.RunOptions{
	ShowVersion: false,
}

var rootOptions = config.RootOptions{
	VerboseLevel:    config.VerboseLevelNone,
	Version:         "0.1.0",
	ControlPlaneURL: "https://cp.adal.cloud/auth/ws",
}

var rootCmd = &cobra.Command{
	Use:           "adal-cli -t <token>",
	Short:         "Adal client CLI",
	Long:          "Command line client for Adal service.",
	RunE:          run,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		log.Println("Error:", err)
		os.Exit(config.RunErrorExecute)
	}
}

func init() {
	buildOptions()

	rootCmd.PersistentFlags().IntVar(&rootOptions.VerboseLevel, "verbose", 0, "Verbose level:\n  1 for warnings\n  2 for info\n  3 for debug")
	rootCmd.PersistentFlags().BoolVarP(&runOptions.ShowVersion, "version", "v", false, "Print version information")
	rootCmd.PersistentFlags().StringVarP(&rootOptions.EndpointToken, "token", "t", rootOptions.EndpointToken, "Authentication token")
}

func run(cmd *cobra.Command, _ []string) error {
	signalCtx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if runOptions.ShowVersion {
		fmt.Printf("adal-cli %s\n", rootOptions.Version)
		return nil
	}

	if rootOptions.EndpointToken == "" {
		if os.Getenv("ADAL_ENDPOINT_TOKEN") != "" {
			rootOptions.EndpointToken = os.Getenv("ADAL_ENDPOINT_TOKEN")
		}
	}

	if rootOptions.EndpointToken == "" {
		log.Println("token is required. Use -t or --token flag to provide it")
		os.Exit(config.RunErrorToken)
	}

	log.Println("Welcome to Adal CLI v" + rootOptions.Version)

	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()

	for {
		sess, err := session.NewSession(ctx, rootOptions)
		if err == nil {
			err = sess.Start(ctx)
			if errors.Is(err, ws.ErrTooManyConnections) {
				log.Println("Too many connections")
				cancel()
			} else if err != nil {
				log.Println(err)
			}
		} else {
			if errors.Is(err, session.ErrorUnauthorized) {
				cancel()
			}
		}

		select {
		case <-ctx.Done():
			log.Println("Shutting down...")
			return nil

		case <-time.After(time.Second):
			log.Println("Reconnecting...")
		}
	}
}

func buildOptions() {
	if rootOptions.VerboseLevel > config.VerboseLevelMaximum {
		rootOptions.VerboseLevel = config.VerboseLevelMaximum
	}

	if os.Getenv("CONTROL_PLANE_URL") != "" {
		rootOptions.ControlPlaneURL = os.Getenv("CONTROL_PLANE_URL")
	}
}
