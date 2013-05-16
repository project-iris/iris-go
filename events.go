// Iris Go Binding
// Copyright 2013 Peter Szilagyi. All rights reserved.
//
// The current language binding is an official support library of the Iris
// decentralized messaging framework, and as such, the same licensing terms
// hold. For details please see http://github.com/karalabe/iris/LICENSE.md
//
// Author: peterke@gmail.com (Peter Szilagyi)

// Event handlers for the incoming messages on the relay.

package iris

// Handles a remote request by calling the app layer handler on a new thread and
// replying to the source node with the result.
func (c *connection) handleRequest(reqId uint64, req []byte) {
	go func() {
		rep := c.hand.HandleRequest(req)
		c.relay.sendReply(reqId, rep)
	}()
}

// Handles a remote reply by looking up the pending request and forwarding the
// reply.
func (c *connection) handleReply(reqId uint64, rep []byte) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Make sure the request is still alive and don't block if dying
	if ch, ok := c.reqs[reqId]; ok {
		ch <- rep
	}
}

// Handles an application broadcast event.
func (c *connection) handleBroadcast(msg []byte) {
	go c.hand.HandleBroadcast(msg)
}

// Handles a remote topic publish event.
func (c *connection) handlePublish(topic string, msg []byte) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if hand, ok := c.subs[topic]; ok {
		go hand.HandleEvent(msg)
	}
}

/*
// Handles a remote tunneling event.
func (c *connection) handleTunnelRequest(src *carrier.Address, peerId uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Assemble the local tunnel.
	tun := &tunnel{
		relay:     c.relay,
		peerAddr:  src,
		peerTunId: peerId,
		buff:      make(chan []byte, 4096),
	}
	tunId := c.tunIdx
	c.tuns[tunId] = tun
	c.tunIdx++

	// Reply with a successful tunnel setup message
	c.relay.Direct(src, assembleTunnelReply(peerId, tunId))

	// Call the handler
	go c.hand.HandleTunnel(tun)
}

// Handles teh response to a local tunneling request.
func (c *connection) handleTunnelReply(src *carrier.Address, localId uint64, peerId uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Make sure the tunnel is still pending and initialize it if so
	if tun, ok := c.tuns[localId]; ok {
		tun.peerAddr = src
		tun.peerTunId = peerId
		tun.init <- struct{}{}
	}
}
*/
