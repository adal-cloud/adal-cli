package session

import (
	"adal-cli/internal/config"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

var (
	ErrorUnauthorized = errors.New("unauthorized")
)

type Session struct {
	RootOptions    config.RootOptions
	ConnectionData config.ConnectionData
}

func NewSession(ctx context.Context, rootOptions config.RootOptions) (*Session, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2: true,
			//MaxIdleConns:      100,
			//MaxConnsPerHost:   20,
			//IdleConnTimeout:   90 * time.Second,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
			},
		},
	}

	request, err := http.NewRequestWithContext(reqCtx, http.MethodGet, rootOptions.ControlPlaneURL, nil)
	if err != nil {
		if rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
			log.Println("Error creating request:", err)
		}
		return &Session{}, err
	}

	request.Header.Add("Authorization", "Bearer "+rootOptions.EndpointToken)
	request.Header.Add("Accept", "application/json")

	if rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
		log.Printf("Request URL: %s\n", request.URL)
	}

	response, err := httpClient.Do(request)
	if err != nil {
		if rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
			log.Println("Error making request:", err)
		}
		return &Session{}, err
	}
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			if rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
				log.Println("Error closing body:", err)
			}
		}
	}(response.Body)

	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusUnauthorized {
			log.Println("Unauthorized:", response.StatusCode)
			return &Session{}, ErrorUnauthorized
		}
		if rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
			log.Println("Unexpected response status:", response.Status)
		}

		return &Session{}, errors.New(response.Status)
	}

	if rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
		log.Printf("Response status: %s\n", response.Status)
	}

	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		if rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
			log.Println("Error reading response body:", err)
		}
		return &Session{}, err
	}

	var connectionData config.ConnectionData
	err = json.Unmarshal(bodyBytes, &connectionData)
	if err != nil {
		if rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
			log.Println("Error parsing connection data:", err)
		}
		return &Session{}, err
	}

	if rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
		log.Println("Connection data received")
	}

	return &Session{
		RootOptions:    rootOptions,
		ConnectionData: connectionData,
	}, nil
}
