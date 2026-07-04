package ws

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

func (connection *Connection) requestPropagate(request Request) {
	ctx, cancel := context.WithTimeout(connection.ctx, 60*time.Second)
	defer cancel()

	startAt := time.Now()
	var wg sync.WaitGroup
	wg.Add(len(request.Deliveries))

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2: true,
			MaxIdleConns:      100,
			MaxConnsPerHost:   20,
			IdleConnTimeout:   90 * time.Second,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
			},
		},
	}

	for _, destination := range request.Deliveries {
		go func() {
			defer wg.Done()

			req, err := http.NewRequestWithContext(ctx, request.Method, destination.URL+request.URI, bytes.NewReader(request.Body))
			if err != nil {
				connection.sendDeliveryResult(DeliveryResult{
					Status:           RequestDeliveryStatusFailed,
					DeliveryId:       destination.Id,
					DestinationId:    destination.Destination.Id,
					URL:              destination.URL,
					AttemptNumber:    destination.AttemptNumber,
					Message:          new("invalid_url"),
					DeliveryDuration: time.Since(startAt).Microseconds(),
				})
				log.Println("Invalid URL:", destination.URL)
				return
			}
			for k, v := range request.Headers {
				for _, vv := range v {
					req.Header.Add(k, vv)
				}
			}

			if destination.Destination.AddIdempotencyKey {
				idempotencyKey := make([]byte, 36)
				hex.Encode(idempotencyKey[0:8], request.IdempotencyKey[0:4])
				idempotencyKey[8] = '-'
				hex.Encode(idempotencyKey[9:13], request.IdempotencyKey[4:6])
				idempotencyKey[13] = '-'
				hex.Encode(idempotencyKey[14:18], request.IdempotencyKey[6:8])
				idempotencyKey[18] = '-'
				hex.Encode(idempotencyKey[19:23], request.IdempotencyKey[8:10])
				idempotencyKey[23] = '-'
				hex.Encode(idempotencyKey[24:36], request.IdempotencyKey[10:16])
				req.Header.Add("X-Adal-Idempotency", string(idempotencyKey))
			}

			resp, err := client.Do(req)
			if err != nil {
				connection.sendDeliveryResult(DeliveryResult{
					Status:           RequestDeliveryStatusFailed,
					DeliveryId:       destination.Id,
					DestinationId:    destination.Destination.Id,
					URL:              destination.URL,
					AttemptNumber:    destination.AttemptNumber,
					Message:          new("network_error"),
					DeliveryDuration: time.Since(startAt).Microseconds(),
				})
				log.Println("Network error:", destination.URL)
				log.Println(err)
				return
			}

			deliveryResult := DeliveryResult{
				DeliveryId:       destination.Id,
				DestinationId:    destination.Destination.Id,
				URL:              destination.URL,
				AttemptNumber:    destination.AttemptNumber,
				ResponseCode:     &resp.StatusCode,
				DeliveryDuration: time.Since(startAt).Microseconds(),
			}

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				deliveryResult.Status = RequestDeliveryStatusSuccess
			} else {
				deliveryResult.Status = RequestDeliveryStatusFailed
			}

			log.Println(destination.URL, resp.Status)
			connection.sendDeliveryResult(deliveryResult)

			defer func(Body io.ReadCloser) {
				err = Body.Close()
				if err != nil {
					log.Println(err)
				}
			}(resp.Body)
		}()
	}

	wg.Wait()
}
