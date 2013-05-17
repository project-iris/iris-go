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

// Forwards an application targeted broadcast event to the connection handler.
func (r *relay) handleBroadcast(msg []byte) {
	r.handler.HandleBroadcast(msg)
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
func (r *relay) handleDrop() {
	r.handler.HandleDrop()
}

/*
// Handles a remote tunneling event.
func (r *relay) handleTunnelRequest(src *carrier.Address, peerId uint64) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Assemble the local tunnel.
	tun := &tunnel{
		relay:     r.relay,
		peerAddr:  src,
		peerTunId: peerId,
		buff:      make(chan []byte, 4096),
	}
	tunId := r.tunIdx
	r.tuns[tunId] = tun
	r.tunIdx++

	// Reply with a successful tunnel setup message
	r.relay.Direct(src, assembleTunnelReply(peerId, tunId))

	// Call the handler
	go r.hand.HandleTunnel(tun)
}

// Handles teh response to a local tunneling request.
func (r *relay) handleTunnelReply(src *carrier.Address, localId uint64, peerId uint64) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Make sure the tunnel is still pending and initialize it if so
	if tun, ok := r.tuns[localId]; ok {
		tun.peerAddr = src
		tun.peerTunId = peerId
		tun.init <- struct{}{}
	}
}
*/
