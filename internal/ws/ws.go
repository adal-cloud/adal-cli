package ws

import (
	"adal-cli/internal/config"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var (
	ErrTooManyConnections = errors.New("too many connections")
)

type Connection struct {
	ctx         context.Context
	conn        *websocket.Conn
	send        chan []byte
	done        chan struct{}
	closeOnce   sync.Once
	rootOptions config.RootOptions
}

func NewWSConnection(ctx context.Context, connectionData config.ConnectionData, rootOptions config.RootOptions) (*Connection, error) {
	var headers map[string][]string
	headers = http.Header{
		"Authorization": []string{"Bearer " + connectionData.AccessToken},
	}
	var conn *websocket.Conn
	var r *http.Response
	var err error
	conn, r, err = websocket.DefaultDialer.DialContext(ctx, connectionData.ConnectionURL, headers)
	if err != nil {
		if r != nil {
			response, err := io.ReadAll(r.Body)
			if err != nil && rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
				log.Println("Error reading response body")
				log.Println("Status code: " + strconv.Itoa(r.StatusCode))
				log.Println("Response: " + string(response))
			}

			if r.StatusCode == http.StatusTooManyRequests {
				return nil, ErrTooManyConnections
			}
		}

		log.Println("Error connecting to websocket")
		return nil, err
	}

	log.Printf("Successfully connected to \"%s\" (%s, %s)", connectionData.EndpointName, connectionData.City, connectionData.Country)

	return &Connection{
		ctx:         ctx,
		conn:        conn,
		send:        make(chan []byte, 256),
		done:        make(chan struct{}),
		rootOptions: rootOptions,
	}, nil
}

func (connection *Connection) Run() {
	go func() {
		<-connection.ctx.Done()
		connection.Close()
	}()

	go connection.writePump()
	connection.readPump()
}

func (connection *Connection) Close() {
	connection.closeOnce.Do(func() {
		if connection.conn != nil {
			var err error
			err = connection.conn.Close()
			if err != nil {
				if connection.rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
					log.Println(err)
				}
			}
		}

		if connection.send != nil {
			close(connection.send)
		}

		close(connection.done)
	})
}

func (connection *Connection) readPump() {
	defer connection.Close()

	const (
		maxMessageSize = 1 << 20
		pongWait       = 60 * time.Second
	)

	// set read limit and deadline
	connection.conn.SetReadLimit(maxMessageSize)
	var err error
	err = connection.conn.SetReadDeadline(time.Now().Add(pongWait))
	if err != nil {
		if connection.rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
			log.Println("SetReadDeadline error")
		}
		connection.Close()
		return
	}

	connection.conn.SetPongHandler(func(string) error {
		return connection.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		select {
		case <-connection.ctx.Done():
			return
		case <-connection.done:
			// shutdown requested
			return
		default:
			// ReadMessage blocks; Close() will cause ReadMessage to return with an error.
		}

		var mt int
		var msg []byte
		mt, msg, err = connection.conn.ReadMessage()
		if err != nil {
			// Log and terminate the read loop so we shut down cleanly instead of spinning.
			if connection.rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
				log.Println("WebSocker read error")
			}
			return
		}

		if mt != websocket.BinaryMessage {
			if connection.rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
				log.Println("Unsupported message type:", mt)
			}
			continue
		}

		var request Request

		err = json.Unmarshal(msg, &request)
		if err != nil {
			if connection.rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
				log.Println("Bad json format")
			}
			continue
		}

		connection.requestPropagate(request)
	}
}

func (connection *Connection) writePump() {
	defer connection.Close()

	const (
		pingPeriod = 30 * time.Second
		writeWait  = 10 * time.Second
	)

	var ticker *time.Ticker
	ticker = time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-connection.done:
			return

		case msg, ok := <-connection.send:
			var err error
			err = connection.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err != nil {
				if connection.rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
					log.Println("SetWriteDeadline error")
				}
			}

			if !ok {
				// channel is closed — close the socket cleanly
				err = connection.conn.WriteMessage(websocket.CloseMessage, []byte{})
				if err != nil {
					if connection.rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
						log.Println("Write close message error")
					}
				}
				return
			}

			err = connection.conn.WriteMessage(websocket.BinaryMessage, msg)
			if err != nil {
				if connection.rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
					log.Println("WebSocket write error")
				}
				connection.Close()
				return
			}

		case <-ticker.C:
			// control ping; peer should respond with pong, PongHandler will extend deadline
			var err error
			err = connection.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err != nil {
				if connection.rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
					log.Println("SetWriteDeadline error")
				}
			}
			err = connection.conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(writeWait))
			if err != nil {
				if connection.rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
					log.Println("Ping error")
				}
				connection.Close()
				return
			}
		}
	}
}

func (connection *Connection) sendDeliveryResult(deliveryResult DeliveryResult) {
	resultBytes, err := json.Marshal(deliveryResult)
	if err != nil {
		if connection.rootOptions.VerboseLevel >= config.VerboseLevelMaximum {
			log.Println("Failed to marshal delivery result")
		}
		return
	}

	connection.send <- resultBytes
}
