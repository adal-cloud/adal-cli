package session

import (
	"adal-cli/internal/config"
	"adal-cli/internal/ws"
	"context"
	"log"
)

func (s *Session) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if s.RootOptions.VerboseLevel >= config.VerboseLevelMaximum {
		log.Println("Trying to establish websocket connection to", s.ConnectionData.ConnectionURL)
	}
	conn, err := ws.NewWSConnection(ctx, s.ConnectionData, s.RootOptions)
	if err != nil {
		if s.RootOptions.VerboseLevel >= config.VerboseLevelMaximum {
			log.Println("Error connecting to websocket on", s.ConnectionData.ConnectionURL)
		}
		return err
	}
	defer conn.Close()

	if s.RootOptions.VerboseLevel >= config.VerboseLevelMaximum {
		log.Println("Session started. Listening for incoming requests...")
	}

	conn.Run()

	return nil
}
