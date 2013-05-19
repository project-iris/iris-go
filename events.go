// Iris Go Binding
// Copyright 2013 Peter Szilagyi. All rights reserved.
//
// The current language binding is an official support library of the Iris
// decentralized messaging framework, and as such, the same licensing terms
// hold. For details please see http://github.com/karalabe/iris/LICENSE.md
//
// Author: peterke@gmail.com (Peter Szilagyi)

// Event handlers for relay side messages. All methods in this file are assumed
// to be running in a separate go routine!

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

	// Make sure the request is still alive and don't block if dying
	if ch, ok := r.reqPend[reqId]; ok {
		ch <- rep
	} else {
		log.Printf("iris: stale reply arrived for request #%v.", reqId)
	}
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

// Opens a new tunnel endpoint and binds it to the remote one.
func (r *relay) handleTunnelRequest(tunId uint64, win int) {
	tun := r.acceptTunnel(tunId, win)
	r.handler.HandleTunnel(tun)
}

// Finalizes a locally initiated tunneling operation with the remote endpoint.
func (r *relay) handleTunnelReply(tunId uint64, win int) {
	r.tunLock.RLock()
	defer r.tunLock.RUnlock()

	// Make sure the request is still alive and don't block if dying
	if tun, ok := r.tunLive[tunId]; ok {
		tun.init <- win
	} else {
		log.Printf("iris: stale reply arrived for tunnelling request #%v.", tunId)
	}
}

// Acknowledges the messages till ack as sent, allowing more to proceed.
func (r *relay) handleTunnelAck(tunId uint64, ack uint64) {
	r.tunLock.Lock()
	defer r.tunLock.Unlock()

	if tun, ok := r.tunLive[tunId]; ok {
		tun.handleAck(ack)
	} else {
		log.Printf("iris: stale ack for tunnel #%v.", tunId)
	}
}

// Forwards the data to a receive request to the recipient.
func (r *relay) handleTunnelRecv(tunId uint64, msg []byte) {
	r.tunLock.Lock()
	defer r.tunLock.Unlock()

	if tun, ok := r.tunLive[tunId]; ok {
		tun.handleRecv(msg)
	} else {
		log.Printf("iris: stale receive for tunnel #%v.", tunId)
	}
}

// Terminates a tunnel, stopping all data transfers.
func (r *relay) handleTunnelClose(tunId uint64) {
	r.tunLock.Lock()
	defer r.tunLock.Unlock()

	if _, ok := r.tunLive[tunId]; ok {
		delete(r.tunLive, tunId)
	} else {
		log.Printf("iris: stale close of tunnel #%v.", tunId)
	}
}

// Notifies the application of the relay link going down.
func (r *relay) handleDrop(reason error) {
	r.handler.HandleDrop(reason)
}
