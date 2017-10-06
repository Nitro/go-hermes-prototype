# hermes

## Purpose
Hermes is an asynchronous pub-sub messaging system that uses websocket protocol to talk to its clients. It's meant to be a replacement for Pusher currently in use in Cloud infastructure

## Dependencies
Websockets: https://github.com/gobwas/ws
NATS: https://github.com/nats-io/go-nats

# Design

NATS is used for flexible pub-sub functionality. A NATS server has to run first for Hermes to work

Hermes exposes an HTTP server that serves two routes:

- `/subscribe` Client can use to initiate a websocket connection and request subscription to mesages with a "subject"
- `/publish` Backend services can use an HTTP POST with "subject" to publish a message to all subscribed clients on this "subject"


