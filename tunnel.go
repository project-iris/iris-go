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
	"time"
)

// Ordered message stream between two endpoints.
type tunnel struct {
	rel *relay // Message relay to the iris node

	id    uint64 // Tunnel identifier (either given or received)
	local bool   // Flag specifying the tunnel originator

	init chan struct{} // Successful connection signaller
	quit chan struct{} // Termination singaller
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
	r.tunOutLock.Lock()
	tunId := r.tunOutIdx
	tun := &tunnel{
		rel:   r,
		id:    tunId,
		local: true,
		init:  make(chan struct{}, 1),
		quit:  make(chan struct{}),
	}
	r.tunOutIdx++
	r.tunOutLive[tunId] = tun
	r.tunOutLock.Unlock()

	// Send the tunneling request and clean up in case of a failure
	if err := r.sendTunnelRequest(tunId, app, timeout); err != nil {
		r.tunOutLock.Lock()
		delete(r.tunOutLive, tunId)
		r.tunOutLock.Unlock()
		return nil, err
	}
	// Wait for tunneling completion or a timeouot
	tick := time.Tick(time.Duration(timeout) * time.Millisecond)
	select {
	case <-tick:
		// Timeout, remove the tunnel leftover
		r.tunOutLock.Lock()
		delete(r.tunOutLive, tunId)
		r.tunOutLock.Unlock()

		// Error out with a temporary failure
		err := &relayError{
			message:   fmt.Sprintf("iris: couldn't tunnel within %d ms", timeout),
			temporary: true,
			timeout:   true,
		}
		return nil, err
	case <-tun.init:
		return tun, nil
	}
}

// Accepts an incoming tunneling request from a remote app.
func (r *relay) acceptTunnel(tunId uint64) Tunnel {
	// Create the local tunnel endpoint
	r.tunInLock.Lock()
	tun := &tunnel{
		rel:   r,
		id:    tunId,
		local: false,
		quit:  make(chan struct{}),
	}
	r.tunInLive[tunId] = tun
	r.tunInLock.Unlock()

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
	// Send the message over to the relay
	return nil
}

func (t *tunnel) Recv(timeout int) ([]byte, error) {
	return nil, nil
}

// Implements iris.Tunnel.Close.
func (t *tunnel) Close() error {
	return t.rel.sendTunnelClose(t.id, t.local)
}

// Stops transfer threads and releases attached resources.
func (t *tunnel) cleanup() {
	close(t.quit)
}
