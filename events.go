// Iris Go Binding
// Copyright 2013 Peter Szilagyi. All rights reserved.
//
// The current language binding is an official support library of the Iris
// decentralized messaging framework, and as such, the same licensing terms
// hold. For details please see http://github.com/karalabe/iris/LICENSE.md
//
// Author: peterke@gmail.com (Peter Szilagyi)

// Event handlers for relay side messages. Almost all methods in this file are
// assumed to be running in a separate go routine! The only exception is the
// tunnel data handler which requires strict order.

package iris

import (
	"log"
)

// Forwards an application targeted broadcast event to the connection handler.
func (r *relay) handleBroadcast(msg []byte) {
	r.handler.HandleBroadcast(msg)
}

// Services a request by calling the app layer handler on a new thread and
// replying with the result.
func (r *relay) handleRequest(reqId uint64, req []byte) {
	rep := r.handler.HandleRequest(req)
	if err := r.sendReply(reqId, rep); err != nil {
		log.Printf("iris: failed to send reply: %v.", err)
	}
}

// Looks up the pending application request and forwards the reply.
func (r *relay) handleReply(reqId uint64, rep []byte) {
	r.reqLock.RLock()
	defer r.reqLock.RUnlock()
	r.reqPend[reqId] <- rep
}

// Forwards a remote topic publish event to the subscription handler.
func (r *relay) handlePublish(topic string, msg []byte) {
	// Fetch the handler and release the lock fast
	r.subLock.RLock()
	sub, ok := r.subLive[topic]
	r.subLock.RUnlock()

	// Make sure the subscription is still live
	if ok {
		sub.HandleEvent(msg)
	} else {
		log.Printf("iris: stale publish arrived on: %v.", topic)
	}
}

// Notifies the application of the relay link going down.
func (r *relay) handleDrop(reason error) {
	// Notify the app of the drop if premature
	if reason != nil {
		r.handler.HandleDrop(reason)
	}

	// Close all open tunnels
	r.tunLock.Lock()
	for _, tun := range r.tunLive {
		tun.handleClose()
	}
	r.tunLive = nil
	r.tunLock.Unlock()
}

// Opens a new local tunnel endpoint and binds it to the remote side.
func (r *relay) handleTunnelRequest(tmpId uint64, buf int) {
	if tun, err := r.acceptTunnel(tmpId, buf); err == nil {
		r.handler.HandleTunnel(tun)
	} else {
		log.Printf("iris: failed to accept inbound tunnel: %v.", err)
	}
}

// Forwards the tunneling reply to the requested tunnel.
func (r *relay) handleTunnelReply(tunId uint64, buf int, timeout bool) {
	// Retrieve the tunnel
	r.tunLock.RLock()
	tun := r.tunLive[tunId]
	r.tunLock.RUnlock()

	// Finalize initialization
	tun.handleInit(buf, timeout)
}

// Forwards a tunnel send acknowledgment to the specific tunnel.
func (r *relay) handleTunnelAck(tunId uint64) {
	// Retrieve the tunnel
	r.tunLock.RLock()
	tun, ok := r.tunLive[tunId]
	r.tunLock.RUnlock()

	// Make sure the tunnel is still alive
	if ok {
		tun.handleAck()
	} else {
		// Rare race, valid, left in for debugging purposes
		//log.Printf("iris: stale tunnel ack.")
	}
}

// Forwards the received data to the tunnel for delivery.
func (r *relay) handleTunnelData(tunId uint64, msg []byte) {
	r.tunLock.RLock()
	defer r.tunLock.RUnlock()

	// Make sure the tunnel is still alive
	if tun, ok := r.tunLive[tunId]; ok {
		tun.handleData(msg)
	} else {
		// Rare race, valid, left in for debugging purposes
		//log.Printf("iris: stale data for tunnel #%v.", tunId)
	}
}

// Terminates a tunnel, stopping all data transfers.
func (r *relay) handleTunnelClose(tunId uint64) {
	r.tunLock.Lock()
	defer r.tunLock.Unlock()

	// Make sure the tunnel is still alive
	if tun, ok := r.tunLive[tunId]; ok {
		tun.handleClose()
		delete(r.tunLive, tunId)
	} else {
		// Rare race, valid, left in for debugging purposes
		//log.Printf("iris: stale close of tunnel #%v.", tunId)
	}
}
