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

// Relay tunnel implementing the iris.Tunnel interface.
type tunnel struct {
	rel  *relay // Message relay to the iris node
	peer uint64 // Peer tunnel id

	init chan uint64 // Channel for the init signal (remote tunnel id)
}

// Implements iris.Connection.Tunnel.
func (r *relay) Tunnel(app string, timeout int) (Tunnel, error) {
	// Sanity check on the arguments
	if len(app) == 0 {
		panic("iris: empty application identifier")
	}
	if timeout <= 0 {
		panic(fmt.Sprintf("iris: invalid timeout %d <= 0", timeout))
	}
	// Create a potential tunnel
	r.tunLock.Lock()
	tun := &tunnel{
		init: make(chan uint64, 1),
	}
	tunId := r.tunIdx
	r.tunIdx++
	r.tunLive[tunId] = tun
	r.tunLock.Unlock()

	// Send the tunneling request
	if err := r.sendTunnelRequest(tunId, app, timeout); err != nil {
		return nil, err
	}
	// Retrieve the results or time out
	tick := time.Tick(time.Duration(timeout) * time.Millisecond)
	select {
	case peer := <-tun.init:
		// Remote id arrived, save and return
		tun.peer = peer
		return tun, nil
	case <-tick:
		// Timeout, remove the tunnel leftover and error out
		r.tunLock.Lock()
		delete(r.tunLive, tunId)
		r.tunLock.Unlock()

		err := &relayError{
			message:   fmt.Sprintf("iris: couldn't tunnel within %d ms", timeout),
			temporary: false,
			timeout:   true,
		}
		return nil, err
	}
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

func (t *tunnel) Close() error {
	return nil
}
