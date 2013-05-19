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

// Ordered message stream between two endpoints.
type tunnel struct {
	rel *relay // Message relay to the iris node

	id   uint64   // Tunnel identifier (either given or received)
	init chan int // Initialization channel receiving the send allowance

	// Throttling
	send chan struct{} // Allowance of sends
	last uint64        // Last acknowledges send
	lock sync.Mutex    // Mutex to protect the last field
	recv chan []byte   // Recv data channel
}

// Initiates a new tunnel to a remote app.
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
		rel:  r,
		id:   tunId,
		recv: make(chan []byte, 1),
		init: make(chan int, 1),
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
	// Wait for tunneling completion or a timeouot
	tick := time.Tick(time.Duration(timeout) * time.Millisecond)
	select {
	case <-tick:
		// Timeout, remove the tunnel leftover
		r.tunLock.Lock()
		delete(r.tunLive, tunId)
		r.tunLock.Unlock()

		// Error out with a temporary failure
		err := &relayError{
			message:   fmt.Sprintf("iris: couldn't tunnel within %d ms", timeout),
			temporary: true,
			timeout:   true,
		}
		return nil, err
	case win := <-tun.init:
		tun.send = make(chan struct{}, win)
		return tun, nil
	}
}

// Accepts an incoming tunneling request from a remote app.
func (r *relay) acceptTunnel(peerId uint64, win int) Tunnel {
	// Create the local tunnel endpoint
	r.tunLock.Lock()
	tunId := r.tunIdx
	tun := &tunnel{
		rel:  r,
		id:   tunId,
		send: make(chan struct{}, win),
		recv: make(chan []byte, 1),
	}
	r.tunIdx++
	r.tunLive[tunId] = tun
	r.tunLock.Unlock()

	// Ack the tunnel creation to the relay
	if err := r.sendTunnelReply(peerId, tunId); err != nil {
		r.tunLock.Lock()
		delete(r.tunLive, tunId)
		r.tunLock.Unlock()
	}
	// Return the accepted tunnel
	return tun
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
	var tick <-chan time.Time
	if timeout > 0 {
		tick = time.Tick(time.Duration(timeout) * time.Millisecond)
	}
	// Query for a send allowance
	select {
	case <-tick:
		return &relayError{
			message:   fmt.Sprintf("iris: failed to forward within %d ms", timeout),
			temporary: true,
			timeout:   true,
		}
	case t.send <- struct{}{}:
		return t.rel.sendTunnelSend(t.id, msg)
	}
}

// Implements iris.Tunnel.Recv.
func (t *tunnel) Recv(timeout int) ([]byte, error) {
	// Sanity check on the arguments
	if timeout < 0 {
		panic(fmt.Sprintf("iris: invalid timeout %d < 0", timeout))
	}
	// Check for leftovers from a previous timeout
	select {
	case msg := <-t.recv:
		return msg, nil
	default:
	}
	// Send the retrieval request
	if err := t.rel.sendTunnelRecv(t.id); err != nil {
		return nil, err
	}
	// Create (the possibly nil) timeout signaller and wait for the reply
	var tick <-chan time.Time
	if timeout > 0 {
		tick = time.Tick(time.Duration(timeout) * time.Millisecond)
	}
	select {
	case <-tick:
		return nil, &relayError{
			message:   fmt.Sprintf("iris: failed to receive within %d ms", timeout),
			temporary: true,
			timeout:   true,
		}
	case msg := <-t.recv:
		return msg, nil
	}
}

// Implements iris.Tunnel.Close.
func (t *tunnel) Close() error {
	return t.rel.sendTunnelClose(t.id)
}

// Acknowledges messages till ack, increasing the send allowance.
func (t *tunnel) handleAck(ack uint64) {
	t.lock.Lock()
	defer t.lock.Unlock()

	for i := 0; i < int(ack-t.last); i++ {
		<-t.send
	}
	t.last = ack
}

// Forwards msg as to the receiving process.
func (t *tunnel) handleRecv(msg []byte) {
	t.recv <- msg
}
