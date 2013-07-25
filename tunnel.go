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
	"log"
	"time"
)

// Iris to app buffer size for flow control.
var tunnelBuffer = 128

// Ordered message stream between two endpoints.
type tunnel struct {
	id  uint64 // Tunnel identifier for traffic relay
	rel *relay // Message relay to the iris node

	// Throttling fields
	itoa chan []byte   // Iris to application message buffer
	atoi chan struct{} // Application to Iris pending ack buffer

	// Bookkeeping fields
	init chan bool     // Initialization channel for outbound tunnels
	term chan struct{} // Channel to signal termination to blocked go-routines
}

// Implements iris.Tunnel.Send.
func (t *tunnel) Send(msg []byte, timeout time.Duration) error {
	// Sanity check on the arguments
	if msg == nil {
		panic("iris: nil message")
	}
	if timeout != 0 {
		timeoutms := int(timeout.Nanoseconds() / 1000000)
		if timeoutms < 1 {
			panic(fmt.Sprintf("iris: invalid timeout %d < 1ms", timeoutms))
		}
	}
	// Create (the possibly nil) timeout signaller
	var after <-chan time.Time
	if timeout != 0 {
		after = time.After(timeout)
	}
	// Query for a send allowance
	select {
	case <-t.term:
		return permError(fmt.Errorf("tunnel closed"))
	case <-after:
		return timeError(fmt.Errorf("send timeout"))
	case t.atoi <- struct{}{}:
		return t.rel.sendTunnelData(t.id, msg)
	}
}

// Implements iris.Tunnel.Recv.
func (t *tunnel) Recv(timeout time.Duration) ([]byte, error) {
	// Sanity check on the arguments
	if timeout != 0 {
		timeoutms := int(timeout.Nanoseconds() / 1000000)
		if timeoutms < 1 {
			panic(fmt.Sprintf("iris: invalid timeout %d < 1ms", timeoutms))
		}
	}
	// Create the timeout signaller
	var after <-chan time.Time
	if timeout != 0 {
		after = time.After(time.Duration(timeout) * time.Millisecond)
	}
	// Retrieve the next message
	select {
	case <-t.term:
		return nil, permError(fmt.Errorf("tunnel closed"))
	case <-after:
		return nil, timeError(fmt.Errorf("recv timeout"))
	case msg := <-t.itoa:
		// Message arrived, ack and return
		go func() {
			if err := t.rel.sendTunnelAck(t.id); err != nil {
				log.Printf("iris: tunnel ack failed: %v.", err)
			}
		}()
		return msg, nil
	}
}

// Implements iris.Tunnel.Close.
func (t *tunnel) Close() error {
	// Signal the relay and wait for closure (either remote confirm or local fail)
	err := t.rel.sendTunnelClose(t.id)
	<-t.term
	return err
}

// Initiates a new tunnel to a remote app. Timeouts are handled framework side!
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
		id:  tunId,
		rel: r,

		itoa: make(chan []byte, tunnelBuffer),
		init: make(chan bool),
		term: make(chan struct{}),
	}
	r.tunIdx++
	r.tunLive[tunId] = tun
	r.tunLock.Unlock()

	// Send the tunneling request and clean up in case of a failure
	if err := r.sendTunnelRequest(tunId, app, tunnelBuffer, timeout); err != nil {
		r.tunLock.Lock()
		delete(r.tunLive, tunId)
		r.tunLock.Unlock()
		return nil, err
	}
	// Wait for tunneling completion or a timeout
	select {
	case init := <-tun.init:
		if init {
			return tun, nil
		} else {
			// Tunneling timed out, clean up
			r.tunLock.Lock()
			delete(r.tunLive, tunId)
			r.tunLock.Unlock()

			// Report timeout failure
			return nil, timeError(fmt.Errorf("tunneling timed out"))
		}
	case <-r.term:
		// Relay terminating, clean up
		r.tunLock.Lock()
		delete(r.tunLive, tunId)
		r.tunLock.Unlock()

		// Error out with a temporary failure
		return nil, permError(fmt.Errorf("relay terminating"))
	}
}

// Accepts an incoming tunneling request from a remote app, assembling a local
// tunnel with the given send window and replies to the relay with the final
// permanent tunnel id.
func (r *relay) acceptTunnel(tmpId uint64, buf int) (Tunnel, error) {
	// Create the local tunnel endpoint
	r.tunLock.Lock()
	tunId := r.tunIdx
	tun := &tunnel{
		id:   tunId,
		rel:  r,
		itoa: make(chan []byte, tunnelBuffer),
		atoi: make(chan struct{}, buf),
		term: make(chan struct{}),
	}
	r.tunIdx++
	r.tunLive[tunId] = tun
	r.tunLock.Unlock()

	// Acknowledge the tunnel creation to the relay
	if err := r.sendTunnelReply(tmpId, tunId, tunnelBuffer); err != nil {
		r.tunLock.Lock()
		delete(r.tunLive, tunId)
		r.tunLock.Unlock()
		return nil, err
	}
	// Return the accepted tunnel
	return tun, nil
}

// Finalizes the tunneling initiation by setting the output buffer or signalling
// a timeout.
func (t *tunnel) handleInit(buf int, timeout bool) {
	if !timeout {
		t.atoi = make(chan struct{}, buf)
	}
	t.init <- !timeout
}

// Acknowledges one message as sent and allows further transfers.
func (t *tunnel) handleAck() {
	select {
	case <-t.atoi:
		// All ok
	default:
		panic("protocol violation")
	}
}

// Places the received data into the local queue for delivery.
func (t *tunnel) handleData(msg []byte) {
	select {
	case t.itoa <- msg:
		// All ok
	default:
		panic("protocol violation")
	}
}

// Handles the gracefull remote closure of the tunnel.
func (t *tunnel) handleClose() {
	// Nil out the buffers to block send and receive ops
	t.itoa = nil
	t.atoi = nil

	// Signal termination
	close(t.term)
}
