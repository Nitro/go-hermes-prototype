package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	nats "github.com/nats-io/go-nats"
)

func main() {
	// Open NATS connection
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatalf("Unable to connect to NATS: %s", err)
	}
	fmt.Println("Connected to NATS server")

	// Open websocket server
	//connections := sync.Map{}
	http.ListenAndServe(":8080", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w, nil)
		if err != nil {
			fmt.Println(err)
			return
		}

		go func() {
			//defer conn.Close()

			// expect to read user unqiue channel name
			msg, op, err := wsutil.ReadClientData(conn)
			if err != nil {
				fmt.Printf("failed read channel.'%s'", err)
				return
			}

			if op == ws.OpClose {
				conn.Close()
				return
			}

			channel := string(msg)
			fmt.Printf("Channel subscription request: %s", channel)

			closeChan := make(chan struct{})
			subscription, err := nc.Subscribe(channel, func(m *nats.Msg) {
				fmt.Printf("Received message from NATS: %s\n", string(m.Data))
				// The manager will fan this out to all the live connections if there are any
				errNew := wsutil.WriteServerMessage(conn, op, m.Data)
				if errNew != nil {
					fmt.Println("failed Write to ws", errNew)
					conn.Close()
					closeChan <- struct{}{}
				}
			})
			if err != nil {
				fmt.Println("failed NATS subscribe", err)
			}

			go func() {
				<-closeChan
				fmt.Printf("Websocket closed, unsubscribing: %s", channel)
				subscription.Unsubscribe()
			}()

			fmt.Println("End of WS handler")
		}()
	}))

}
