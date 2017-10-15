package main

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/nats-io/go-nats"
	. "github.com/smartystreets/goconvey/convey"
)

type dummyNatsSubscription struct {
}

func (s *dummyNatsSubscription) Unsubscribe() error {
	return nil
}

type dummyNatsConn struct {
	errSubscribe error
}

func (nc *dummyNatsConn) Publish(subj string, data []byte) error {
	return nil
}

func (nc *dummyNatsConn) Subscribe(subj string, cb nats.MsgHandler) (Subscription, error) {
	if nc.errSubscribe != nil {
		return nil, nc.errSubscribe
	}

	cb(&nats.Msg{})

	return &dummyNatsSubscription{}, nil
}

type dummyWSConn struct {
	subj           string
	op             ws.OpCode
	closeChan      chan struct{}
	upgradeHTTPErr error
	errRead        error
	errWrite       error
	errServer      error
	failureChan    chan struct{}
}

func (wsc *dummyWSConn) UpgradeHTTP(r *http.Request, w http.ResponseWriter) error {
	return wsc.upgradeHTTPErr
}

func (wsc *dummyWSConn) ReadClientData() ([]byte, ws.OpCode, error) {
	if wsc.errRead != nil {
		return []byte{}, wsc.op, wsc.errRead
	}
	return []byte(wsc.subj), wsc.op, nil
}

func (wsc *dummyWSConn) WriteServerMessage(op ws.OpCode, p []byte) error {
	if wsc.errServer != nil {
		return wsc.errServer
	}
	return nil
}

func (wsc *dummyWSConn) WriteFrame(code ws.StatusCode, reason string) error {
	return nil
}

func (wsc *dummyWSConn) WriteError(msg string) {
	wsc.errWrite = errors.New(msg)
	wsc.failureChan <- struct{}{}
}

func (wsc *dummyWSConn) Close() error {
	close(wsc.closeChan)
	return nil
}

func Test_Subscribe(t *testing.T) {
	Convey("Subscribe", t, func() {
		wsc := &dummyWSConn{
			subj:      "test_subj",
			op:        ws.OpText,
			closeChan: make(chan struct{}),
		}

		nc := &dummyNatsConn{}

		h := &Hermes{
			nats:             nc,
			wsGenerator:      func() WSBridge { return wsc },
			websocketTimeout: 0,
		}

		req := httptest.NewRequest("GET", "http://localhost/subscribe", nil)
		respRec := httptest.NewRecorder()

		Convey("runs successfully and closes the websocket on timeout", func() {
			h.SubscribeHandler(respRec, req)

			<-wsc.closeChan

			resp := respRec.Result()
			So(resp.StatusCode, ShouldEqual, 200)
		})

		Convey("returns an error if it can't upgrade the HTTP connection to Websocket", func() {
			wsc.upgradeHTTPErr = errors.New("Some error")

			h.SubscribeHandler(respRec, req)

			resp := respRec.Result()
			So(resp.StatusCode, ShouldEqual, 400)

			bodyBytes, err := ioutil.ReadAll(resp.Body)
			So(err, ShouldBeNil)

			So(string(bodyBytes), ShouldContainSubstring, wsc.upgradeHTTPErr.Error())
		})

		Convey("returns an error if it can't read client data", func() {
			wsc.errRead = errors.New("some error")
			wsc.failureChan = make(chan struct{})

			h.SubscribeHandler(respRec, req)

			<-wsc.failureChan

			So(wsc.errWrite.Error(), ShouldResemble, "Failed to read client data: some error")

			resp := respRec.Result()
			So(resp.StatusCode, ShouldEqual, 200)
		})

		Convey("returns an error if it receives an unexpected OP code", func() {
			wsc.op = ws.OpPing
			wsc.failureChan = make(chan struct{})

			h.SubscribeHandler(respRec, req)

			<-wsc.failureChan

			So(wsc.errWrite.Error(), ShouldEqual, "Unexpected OP code received: 9")

			resp := respRec.Result()
			So(resp.StatusCode, ShouldEqual, 200)
		})

		Convey("closes the Websocket immediately if it fails to write a server message", func() {
			wsc.errServer = errors.New("some error")
			h.websocketTimeout = 1 * time.Hour

			h.SubscribeHandler(respRec, req)

			<-wsc.closeChan

			resp := respRec.Result()
			So(resp.StatusCode, ShouldEqual, 200)
		})

		Convey("returns an error if it fails to subscribe to the NATS server", func() {
			nc.errSubscribe = errors.New("some error")
			wsc.failureChan = make(chan struct{})

			h.SubscribeHandler(respRec, req)

			<-wsc.failureChan

			So(wsc.errWrite.Error(), ShouldEqual, "Failed NATS subscription on subject 'test_subj': some error")

			resp := respRec.Result()
			So(resp.StatusCode, ShouldEqual, 200)
		})
	})
}

func Test_Publish(t *testing.T) {
}
