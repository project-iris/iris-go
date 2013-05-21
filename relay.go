// Iris Go Binding
// Copyright 2013 Peter Szilagyi. All rights reserved.
//
// The current language binding is an official support library of the Iris
// decentralized messaging framework, and as such, the same licensing terms
// hold. For details please see http://github.com/karalabe/iris/LICENSE.md
//
// Author: peterke@gmail.com (Peter Szilagyi)

package iris

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"
)

// Message relay between the local app and the local iris node.
type relay struct {
	// Application layer fields
	handler ConnectionHandler // Handler for connection events

	reqIdx  uint64                 // Index to assign the next request
	reqPend map[uint64]chan []byte // Active requests waiting for a reply
	reqLock sync.RWMutex           // Mutex to protect the request map

	subLive map[string]SubscriptionHandler // Active subscriptions
	subLock sync.RWMutex                   // Mutex to protect the subscription map

	tunIdx  uint64             // Index to assign the next tunnel
	tunLive map[uint64]*tunnel // Active tunnels
	tunLock sync.RWMutex       // Mutex to protect the tunnel map

	// Network layer fields
	sock     net.Conn   // Network connection to the iris node
	sockLock sync.Mutex // Mutex to atomise message sending

	outVarBuf []byte // Buffer for variable int encoding
	inByteBuf []byte // Buffer for byte decoding
	inVarBuf  []byte // Buffer for variable int decoding

	init chan struct{}   // Init channel to receive a success signal
	quit chan chan error // Quit channel to synchronize receiver termination
}

// Connects to a local relay endpoint on port and logs in with id app.
func newRelay(port int, app string, handler ConnectionHandler) (Connection, error) {
	// Connect to the iris relay node
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, err
	}
	sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	// Create the relay object
	rel := &relay{
		// Application layer
		handler: handler,
		reqPend: make(map[uint64]chan []byte),
		subLive: make(map[string]SubscriptionHandler),
		tunLive: make(map[uint64]*tunnel),

		// Network layer
		sock:      sock,
		outVarBuf: make([]byte, binary.MaxVarintLen64),
		inByteBuf: make([]byte, 1),
		inVarBuf:  make([]byte, binary.MaxVarintLen64),
		quit:      make(chan chan error),
	}
	// Initialize the connection and wait for a confirmation
	if err := rel.sendInit(app); err != nil {
		return nil, err
	}
	if err := rel.procInit(); err != nil {
		return nil, err
	}
	// All ok, start processing messages and return
	go rel.process()
	return rel, nil
}

// Implements iris.Connection.Broadcast.
func (r *relay) Broadcast(app string, msg []byte) error {
	// Sanity check on the arguments
	if len(app) == 0 {
		panic("iris: empty application identifier")
	}
	if msg == nil {
		panic("iris: nil message")
	}
	// Broadcast and return
	return r.sendBroadcast(app, msg)
}

// Implements iris.Connection.Request.
func (r *relay) Request(app string, req []byte, timeout int) ([]byte, error) {
	// Sanity check on the arguments
	if len(app) == 0 {
		panic("iris: empty application identifier")
	}
	if req == nil {
		panic("iris: nil request")
	}
	if timeout <= 0 {
		panic(fmt.Sprintf("iris: invalid timeout %d <= 0", timeout))
	}
	// Create a reply channel for the results
	r.reqLock.Lock()
	reqCh := make(chan []byte, 1)
	reqId := r.reqIdx
	r.reqIdx++
	r.reqPend[reqId] = reqCh
	r.reqLock.Unlock()

	// Make sure reply channel is cleaned up
	defer func() {
		r.reqLock.Lock()
		defer r.reqLock.Unlock()

		delete(r.reqPend, reqId)
		close(reqCh)
	}()
	// Send the request
	if err := r.sendRequest(reqId, app, req, timeout); err != nil {
		return nil, err
	}
	// Retrieve the results or time out
	select {
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		err := &relayError{
			message:   fmt.Sprintf("iris: no reply within %d ms", timeout),
			temporary: true,
			timeout:   true,
		}
		return nil, err
	case rep := <-reqCh:
		return rep, nil
	}
}

// Implements iris.Connection.Subscribe.
func (r *relay) Subscribe(topic string, handler SubscriptionHandler) error {
	// Sanity check on the arguments
	if len(topic) == 0 {
		panic("iris: empty topic identifier")
	}
	if handler == nil {
		panic("iris: nil subscription handler")
	}
	// Subscribe locally or panic
	r.subLock.Lock()
	if _, ok := r.subLive[topic]; ok {
		r.subLock.Unlock()
		panic("iris: already subscribed")
	}
	r.subLive[topic] = handler
	r.subLock.Unlock()

	// Subscribe through the relay
	err := r.sendSubscribe(topic)
	if err != nil {
		r.subLock.Lock()
		if _, ok := r.subLive[topic]; ok {
			delete(r.subLive, topic)
		}
		r.subLock.Unlock()
	}
	return err
}

// Implements iris.Connection.Publish.
func (r *relay) Publish(topic string, msg []byte) error {
	// Sanity check on the arguments
	if len(topic) == 0 {
		panic("iris: empty topic identifier")
	}
	if msg == nil {
		panic("iris: nil message")
	}
	// Publish and return
	return r.sendPublish(topic, msg)
}

// Implements iris.Connection.Unsubscribe.
func (r *relay) Unsubscribe(topic string) error {
	// Sanity check on the arguments
	if len(topic) == 0 {
		panic("iris: empty topic identifier")
	}
	// Unsubscribe through the relay and remove if successful
	err := r.sendUnsubscribe(topic)
	if err == nil {
		r.subLock.Lock()
		defer r.subLock.Unlock()

		if _, ok := r.subLive[topic]; !ok {
			panic("iris: not subscribed")
		}
		delete(r.subLive, topic)
	}
	return err
}

// Implements iris.Connection.Tunnel.
func (r *relay) Tunnel(app string, timeout int) (Tunnel, error) {
	// Simple call indirection to move into the tunnel source file
	return r.initiateTunnel(app, timeout)
}

// Implements iris.Connection.Close.
func (r *relay) Close() error {
	// Send a graceful close to the relay node
	if err := r.sendClose(); err != nil {
		return err
	}
	// Wait till the close syncs and return
	errc := make(chan error, 1)
	r.quit <- errc
	return <-errc
}
