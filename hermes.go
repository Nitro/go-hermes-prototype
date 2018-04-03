package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/kelseyhightower/envconfig"
	"github.com/nats-io/go-nats"
	"github.com/relistan/rubberneck"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	HTTPPort         uint          `envconfig:"HTTP_PORT" default:"8000"`
	NatsURL          string        `envconfig:"NATS_URL" default:"nats://localhost:4222"`
	WebsocketTimeout time.Duration `envconfig:"WEBSOCKET_TIMEOUT" default:"15m"`
	LoggingLevel     string        `envconfig:"LOGGING_LEVEL" default:"info"`
}

type Subscription interface {
	Unsubscribe() error
}

type natsSubscription struct {
	*nats.Subscription
}

type NatsBridge interface {
	Publish(subj string, data []byte) error
	Subscribe(subj string, cb nats.MsgHandler) (Subscription, error)
}

type natsConn struct {
	*nats.Conn
}

func (nc *natsConn) Publish(subj string, data []byte) error {
	return nc.Conn.Publish(subj, data)
}

func (nc *natsConn) Subscribe(subj string, cb nats.MsgHandler) (Subscription, error) {
	s, err := nc.Conn.Subscribe(subj, cb)
	if err != nil {
		return nil, err
	}

	return &natsSubscription{Subscription: s}, nil
}

func NewNatsConn(nc *nats.Conn) NatsBridge {
	return &natsConn{Conn: nc}
}

type WSBridge interface {
	UpgradeHTTP(r *http.Request, w http.ResponseWriter) error
	ReadClientData() ([]byte, ws.OpCode, error)
	WriteServerMessage(op ws.OpCode, p []byte) error
	WriteFrame(code ws.StatusCode, reason string) error
	WriteError(msg string)
	Close() error
}

type wsConn struct {
	net.Conn
}

func (wsc *wsConn) UpgradeHTTP(r *http.Request, w http.ResponseWriter) error {
	c, _, _, err := ws.UpgradeHTTP(r, w, nil)
	if err != nil {
		return err
	}

	wsc.Conn = c

	return nil
}

func (wsc *wsConn) ReadClientData() ([]byte, ws.OpCode, error) {
	return wsutil.ReadClientData(wsc.Conn)
}

func (wsc *wsConn) WriteServerMessage(op ws.OpCode, p []byte) error {
	return wsutil.WriteServerMessage(wsc.Conn, op, p)
}

func (wsc *wsConn) WriteFrame(code ws.StatusCode, reason string) error {
	return ws.WriteFrame(wsc.Conn, ws.NewCloseFrame(code, reason))
}

func (wsc *wsConn) WriteError(msg string) {
	log.Errorf(msg)
	err := wsc.WriteFrame(ws.StatusProtocolError, msg)
	if err != nil {
		log.Debugf("Failed to write to Websocket: %s", err)
	}
	err = wsc.Close()
	if err != nil {
		log.Debugf("Failed to close Websocket: %s", err)
	}
}

func (wsc *wsConn) Close() error {
	return wsc.Conn.Close()
}

func NewWSBridge() WSBridge {
	return &wsConn{}
}

type Hermes struct {
	nats             NatsBridge
	wsGenerator      func() WSBridge
	websocketTimeout time.Duration
}

func (h *Hermes) ConnectToNatsServer(natsURL string) error {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return err
	}

	h.nats = NewNatsConn(nc)

	return nil
}

func (h *Hermes) SubscribeHandler(w http.ResponseWriter, r *http.Request) {
	wsb := h.wsGenerator()

	// Upgrade HTTP connection to Websocket
	err := wsb.UpgradeHTTP(r, w)
	if err != nil {
		msg := fmt.Sprintf("Failed to upgrade HTTP connection to Websocket: %s", err)
		log.Error(msg)
		http.Error(w, msg, 400)
		return
	}

	log.Debug("Upgraded HTTP connection to Websocket")

	go func() {
		// Read client channel name from the Websocket
		// TODO: implement authentication
		msg, op, err := wsb.ReadClientData()
		if err != nil {
			wsb.WriteError(fmt.Sprintf("Failed to read client data: %s", err))
			return
		}
		if op != ws.OpText {
			wsb.WriteError(fmt.Sprintf("Unexpected OP code received: %d", op))
			return
		}

		subj := string(msg)
		log.Debugf("Received channel subscription request: %s", subj)

		closeChan := make(chan struct{})
		timer := time.NewTimer(h.websocketTimeout)

		// Async unsubscribe from NATS server and close Websocket
		var subscr Subscription
		go func() {
			select {
			case <-timer.C:
				log.Debugf("Websocket timeout, removing subscription on subject: %s", subj)
			case <-closeChan:
				log.Debugf("Websocket closed, removing subscription on subject: %s", subj)
			}

			if subscr != nil {
				err := subscr.Unsubscribe()
				if err != nil {
					log.Errorf("Failed to unsubscribe from subject '%s': %s", subj, err)
				}
			}
			err = wsb.Close()
			if err != nil {
				log.Errorf("Failed to close Websocket: %s", err)
			}
		}()

		// The NATS client interleaves subscriptions to the server over one connection,
		// so we create a subscription with a unique subject for each opened websocket.
		// If Hermes dies, the NATS server will automatically drop the initiated subscriptions
		subscr, err = h.nats.Subscribe(subj, func(m *nats.Msg) {
			log.Debugf("Received message '%s' from NATS on subject '%s'", string(m.Data), subj)

			// Send the received message to the corresponding websocket client
			errNew := wsb.WriteServerMessage(ws.OpText, m.Data)
			if errNew != nil {
				log.Errorf("Failed to write to Websocket: %s", errNew)
				closeChan <- struct{}{}
				return
			}

			timer.Reset(h.websocketTimeout)
		})
		if err != nil {
			wsb.WriteError(fmt.Sprintf("Failed NATS subscription on subject '%s': %s", subj, err))
			return
		}
	}()
}

func (h *Hermes) PublishHandler(w http.ResponseWriter, r *http.Request) {
	subj := r.FormValue("subj")
	if len(subj) == 0 {
		msg := "Publish requires a subject query parameter"
		log.Error(msg)
		http.Error(w, msg, 400)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil || len(body) == 0 {
		msg := "Publish requires a message payload"
		log.Error(msg)
		http.Error(w, msg, 400)
		return
	}

	log.Debugf("Received message body: %s", string(body))

	err = h.nats.Publish(subj, body)
	if err != nil {
		msg := fmt.Sprintf("Failed to publish message to NATS server: %s", err)
		log.Error(msg)
		http.Error(w, msg, 500)
		return
	}
}

func NewHermes(wsTimeout time.Duration) *Hermes {
	return &Hermes{
		wsGenerator:      NewWSBridge,
		websocketTimeout: wsTimeout,
	}
}

func configureLoggingLevel(level string) {
	switch {
	case level == "info":
		log.SetLevel(log.InfoLevel)
	case level == "warn":
		log.SetLevel(log.WarnLevel)
	case level == "error":
		log.SetLevel(log.ErrorLevel)
	case level == "debug":
		log.SetLevel(log.DebugLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}
}

func main() {
	var config Config
	err := envconfig.Process("hermes", &config)
	if err != nil {
		log.Fatalf("Failed to parse the configuration parameters: %s", err)
	}

	rubberneck.Print(config)

	configureLoggingLevel(config.LoggingLevel)

	h := NewHermes(config.WebsocketTimeout)

	// Open NATS connection
	err = h.ConnectToNatsServer(config.NatsURL)
	if err != nil {
		log.Fatalf("Error connecting to NATS server: %s", err)
	}
	log.Infof("Connected to NATS server '%s'", config.NatsURL)

	http.HandleFunc("/favicon.ico", http.NotFound)

	r := mux.NewRouter()
	r.HandleFunc("/subscribe", h.SubscribeHandler)
	r.HandleFunc("/publish", h.PublishHandler)

	err = http.ListenAndServe(
		fmt.Sprintf(":%d", config.HTTPPort), handlers.LoggingHandler(os.Stdout, r),
	)
	if err != nil {
		log.Fatalf("Error starting HTTP server: %s", err)
	}
}
