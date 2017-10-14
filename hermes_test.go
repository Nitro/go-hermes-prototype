package main

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gobwas/ws"
	nats "github.com/nats-io/go-nats"
	. "github.com/smartystreets/goconvey/convey"
)

type dummyNatsSubscription struct {
}

func (s *dummyNatsSubscription) Unsubscribe() error {
	return nil
}

type dummyNatsConn struct {
}

func (nc *dummyNatsConn) Publish(subj string, data []byte) error {
	return nil
}

func (nc *dummyNatsConn) Subscribe(subj string, cb nats.MsgHandler) (Subscription, error) {
	return &dummyNatsSubscription{}, nil
}

type dummyWSConn struct {
	subj           string
	op             ws.OpCode
	closeChan      chan struct{}
	upgradeHTTPErr error
}

func (wsc *dummyWSConn) UpgradeHTTP(r *http.Request, w http.ResponseWriter) error {
	return wsc.upgradeHTTPErr
}

func (wsc *dummyWSConn) ReadClientData() ([]byte, ws.OpCode, error) {
	return []byte(wsc.subj), wsc.op, nil
}

func (wsc *dummyWSConn) WriteServerMessage(op ws.OpCode, p []byte) error {
	return nil
}

func (wsc *dummyWSConn) WriteFrame(code ws.StatusCode, reason string) error {
	return nil
}

func (wsc *dummyWSConn) WriteError(msg string) {
}

func (wsc *dummyWSConn) Close() error {
	close(wsc.closeChan)
	return nil
}

func Test_Subscribe(t *testing.T) {
	Convey("Test subscribe", t, func() {
		wsConn := &dummyWSConn{
			subj:      "test_subj",
			op:        ws.OpText,
			closeChan: make(chan struct{}),
		}

		h := &Hermes{
			nats:             &dummyNatsConn{},
			wsGenerator:      func() WSBridge { return wsConn },
			websocketTimeout: 0,
		}

		req := httptest.NewRequest("GET", "http://localhost/subscribe", nil)
		respRec := httptest.NewRecorder()

		Convey("runs successfully and closes the websocket on timeout", func() {
			h.SubscribeHandler(respRec, req)

			<-wsConn.closeChan

			resp := respRec.Result()

			So(resp.StatusCode, ShouldEqual, 200)
		})

		Convey("returns an error if it can't upgrade the HTTP connection to Websocket", func() {
			wsConn.upgradeHTTPErr = errors.New("Some error")

			h.SubscribeHandler(respRec, req)

			resp := respRec.Result()
			So(resp.StatusCode, ShouldEqual, 400)

			bodyBytes, err := ioutil.ReadAll(resp.Body)
			So(err, ShouldBeNil)

			So(string(bodyBytes), ShouldContainSubstring, wsConn.upgradeHTTPErr.Error())
		})
	})
}

func Test_Publish(t *testing.T) {
}
