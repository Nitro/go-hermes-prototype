package main

import (
	"net/http"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	nats "github.com/nats-io/go-nats"
)

const (
	inactivityTimeout = time.Second * 15
)

func main() {
	// Open NATS connection
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatalf("Unable to connect to NATS: %s", err)
	}
	log.Infof("Connected to NATS server")

	// Open websocket server
	//connections := sync.Map{}
	err = http.ListenAndServe(":8080", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w, nil)
		if err != nil {
			log.Errorf("Failed to upgrade HTTP connection: %s", err)
			return
		}

		go func() {
			// expect to read user unqiue channel name
			msg, op, err := wsutil.ReadClientData(conn)
			if err != nil {
				log.Errorf("failed read channel: %s", err)
				return
			}
			if op != ws.OpText {
				log.Errorf("unexpected OP code received: %d", op)
				conn.Close()
				return
			}

			channel := string(msg)
			log.Infof("Channel subscription request: %s", channel)

			closeChan := make(chan struct{})
			timer := time.NewTimer(inactivityTimeout)
			subscription, err := nc.Subscribe(channel, func(m *nats.Msg) {
				log.Infof("Received msg from NATS: %s = %s", channel, string(m.Data))
				// The manager will fan this out to all the live connections if there are any
				// expect to read user unqiue channel name
				errNew := wsutil.WriteServerMessage(conn, ws.OpText, m.Data)
				if errNew != nil {
					log.Errorf("failed Write to ws", errNew)
					conn.Close()
					closeChan <- struct{}{}
					return
				}

				timer = time.NewTimer(inactivityTimeout)
			})
			if err != nil {
				log.Errorf("failed NATS subscribe", err)
				return
			}

			go func() {
				select {
				case <-timer.C:
					log.Infof("Websocket timeout, unsubscribing: %s", channel)
					subscription.Unsubscribe()
				case <-closeChan:
					log.Infof("Websocket closed, unsubscribing: %s", channel)
					subscription.Unsubscribe()
				}
			}()
		}()
	}))

	log.Errorf("Error starting web server: %s", err)
}
