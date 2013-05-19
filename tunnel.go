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
	"fmt"
	"sync"
	"time"
)

// Error to return on a closed connection or tunnel.
var tunnelClosedError = &relayError{
	message:   fmt.Sprintf("iris: tunnel terminating"),
	temporary: false,
	timeout:   false,
}

// Error to return on a timed-out tunnel operation.
var tunnelTimeError = &relayError{
	message:   fmt.Sprintf("iris: tunnel operation timed out"),
	temporary: true,
	timeout:   true,
}

// Ordered message stream between two endpoints.
type tunnel struct {
	id  uint64 // Tunnel identifier for traffic relay
	rel *relay // Message relay to the iris node

	// Throttling fields
	pending chan struct{} // Limits the number of sends to the window size
	acked   uint64        // Sequence number of the last acked message
	lock    sync.Mutex    // Sync primitive to serialize parallel acks

	polled bool        // Specifies whether a receive was already sent and data is imminent
	queued chan []byte // Message queued for delivery

	// Bookkeeping fields
	init chan struct{} // Initialization channel for outbount tunnels
	quit chan struct{} // Quit channel to signal tunnel termination
}

// Implements iris.Tunnel.Send.
func (t *tunnel) Send(msg []byte, timeout int) error {
	// Sanity check on the arguments
	if msg == nil {
		panic("iris: nil message")
	}
	if timeout < 0 {
		panic(fmt.Sprintf("iris: invalid timeout %d < 0", timeout))
	}
	// Create (the possibly nil) timeout signaller
	var after <-chan time.Time
	if timeout > 0 {
		after = time.After(time.Duration(timeout) * time.Millisecond)
	}
	// Query for a send allowance
	select {
	case <-t.quit:
		return tunnelClosedError
	case <-after:
		return tunnelTimeError
	case t.pending <- struct{}{}:
		return t.rel.sendTunnelData(t.id, msg)
	}
}

// Implements iris.Tunnel.Recv.
func (t *tunnel) Recv(timeout int) ([]byte, error) {
	// Sanity check on the arguments
	if timeout < 0 {
		panic(fmt.Sprintf("iris: invalid timeout %d < 0", timeout))
	}
	// Check for leftovers from a previous timed-out recieve
	if t.polled {
		var after <-chan time.Time
		if timeout > 0 {
			after = time.After(time.Duration(timeout) * time.Millisecond)
		}
		select {
		case <-t.quit:
			return nil, tunnelClosedError
		case <-after:
			return nil, tunnelTimeError
		case msg := <-t.queued:
			// Message arrived, clear poll flag and return
			t.polled = false
			return msg, nil
		}
	}
	// No leftovers to be queried, send a poll
	if err := t.rel.sendTunnelPoll(t.id); err != nil {
		return nil, err
	}
	// Create (the possibly nil) timeout signaller and wait for the reply
	var after <-chan time.Time
	if timeout > 0 {
		after = time.After(time.Duration(timeout) * time.Millisecond)
	}
	select {
	case <-t.quit:
		return nil, tunnelClosedError
	case <-after:
		// Poll timed out, set polled flag to prevent poll resend and return
		t.polled = true
		return nil, tunnelTimeError
	case msg := <-t.queued:
		return msg, nil
	}
}

// Implements iris.Tunnel.Close.
func (t *tunnel) Close() error {
	// Send a graceful close to the relay node
	return t.rel.sendTunnelClose(t.id)
}

// Initiates a new tunnel to a remote app. If timeout is reached before the init
// reply arrives the tunnel is discarded.
func (r *relay) initiateTunnel(app string, timeout int) (Tunnel, error) {
	// Sanity check on the arguments
	if len(app) == 0 {
		panic("iris: empty application identifier")
	}
	if timeout <= 0 {
		panic(fmt.Sprintf("iris: invalid timeout %d <= 0", timeout))
	}
	// Create a potential tunnel
	r.tunLock.Lock()
	tunId := r.tunIdx
	tun := &tunnel{
		id:     tunId,
		rel:    r,
		queued: make(chan []byte, 1),
		init:   make(chan struct{}, 1),
		quit:   make(chan struct{}),
	}
	r.tunIdx++
	r.tunLive[tunId] = tun
	r.tunLock.Unlock()

	// Send the tunneling request and clean up in case of a failure
	if err := r.sendTunnelRequest(tunId, app, timeout); err != nil {
		r.tunLock.Lock()
		delete(r.tunLive, tunId)
		r.tunLock.Unlock()
		return nil, err
	}
	// Wait for tunneling completion or a timeout
	select {
	case <-tun.init:
		// All ok, return the new tunnel
		return tun, nil
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		// Timeout, remove the tunnel leftover
		r.tunLock.Lock()
		delete(r.tunLive, tunId)
		r.tunLock.Unlock()

		// Error out with a temporary failure
		return nil, tunnelTimeError
	}
}

// Accepts an incoming tunneling request from a remote app, assembling a local
// tunnel with the given send window and replies to the relay with the final
// permanent tunnel id.
func (r *relay) acceptTunnel(tmpId uint64, win int) (Tunnel, error) {
	// Create the local tunnel endpoint
	r.tunLock.Lock()
	tunId := r.tunIdx
	tun := &tunnel{
		id:      tunId,
		rel:     r,
		pending: make(chan struct{}, win),
		queued:  make(chan []byte, 1),
		quit:    make(chan struct{}),
	}
	r.tunIdx++
	r.tunLive[tunId] = tun
	r.tunLock.Unlock()

	// Acknowledge the tunnel creation to the relay
	if err := r.sendTunnelReply(tmpId, tunId); err != nil {
		r.tunLock.Lock()
		delete(r.tunLive, tunId)
		r.tunLock.Unlock()
		return nil, err
	}
	// Return the accepted tunnel
	return tun, nil
}

// Finalizes the successfull tunneling initiation by setting the relay window.
func (t *tunnel) handleInit(win int) {
	t.pending = make(chan struct{}, win)
	t.init <- struct{}{}
}

// Acknowledges the sent messages till ack, allowing more to proceed.
func (t *tunnel) handleAck(ack uint64) {
	t.lock.Lock()
	defer t.lock.Unlock()

	for i := 0; i < int(ack-t.acked); i++ {
		<-t.pending
	}
	t.acked = ack
}

// Places the received data into the local queue for delivery.
func (t *tunnel) handleData(msg []byte) {
	t.queued <- msg
}

// Handles the gracefull remote closure of the tunnel.
func (t *tunnel) handleClose() {
	close(t.quit)
}
