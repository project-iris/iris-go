// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

// Event handlers for relay side messages.

package iris

import (
	"errors"
	"log"
	"sync/atomic"
)

// Schedules an application broadcast message for the service handler to process.
func (c *Connection) handleBroadcast(message []byte) {
	// Make sure there is enough memory for the message
	if int(atomic.LoadInt32(&c.bcastUsed))+len(message) <= c.limits.BroadcastMemory {
		// Increment the memory usage of the queue and schedule the broadcast
		atomic.AddInt32(&c.bcastUsed, int32(len(message)))
		c.bcastPool.Schedule(func() {
			// Start the processing by decrementing the memory usage
			atomic.AddInt32(&c.bcastUsed, -int32(len(message)))
			c.handler.HandleBroadcast(message)
		})
		return
	}
	// Not enough memory in the broadcast queue
	log.Printf("memory allowance exceeded, broadcast dropped.")
}

// Schedules an application request for the service handler to process.
func (c *Connection) handleRequest(id uint64, request []byte, timeout int) {
	// Make sure there is enough memory for the request
	if int(atomic.LoadInt32(&c.reqUsed))+len(request) <= c.limits.RequestMemory {
		// Increment the memory usage of the queue and schedule the request
		atomic.AddInt32(&c.reqUsed, int32(len(request)))
		c.reqPool.Schedule(func() {
			// Start the processing by decrementing the memory usage
			atomic.AddInt32(&c.reqUsed, -int32(len(request)))

			reply, fault := c.handler.HandleRequest(request)
			if fault == nil {
				fault = errors.New("")
			}
			if err := c.sendReply(id, reply, fault.Error()); err != nil {
				log.Printf("iris: failed to send reply: %v.", err)
			}
		})
		return
	}
	// Not enough memory in the request queue
	log.Printf("memory allowance exceeded, request dropped.")
}

// Looks up a pending request and delivers the result.
func (c *Connection) handleReply(id uint64, reply []byte, fault string) {
	c.reqLock.RLock()
	defer c.reqLock.RUnlock()

	if reply == nil && len(fault) == 0 {
		c.reqErrs[id] <- ErrTimeout
	} else if reply == nil {
		c.reqErrs[id] <- errors.New(fault)
	} else {
		c.reqReps[id] <- reply
	}
}

// Forwards a topic publish event to the topic subscription.
func (c *Connection) handlePublish(topic string, event []byte) {
	// Fetch the handler and release the lock fast
	c.subLock.RLock()
	top, ok := c.subLive[topic]
	c.subLock.RUnlock()

	// Make sure the subscription is still live
	if ok {
		top.handlePublish(event)
	} else {
		log.Printf("iris: stale publish arrived on: %v.", topic)
	}
}

// Notifies the application of the relay link going down.
func (c *Connection) handleClose(reason error) {
	// Notify the client of the drop if premature
	if reason != nil {
		c.handler.HandleDrop(reason)
	}
	// Close all open tunnels
	c.tunLock.Lock()
	for _, tun := range c.tunLive {
		tun.handleClose("connection dropped")
	}
	c.tunLive = nil
	c.tunLock.Unlock()
}

// Opens a new local tunnel endpoint and binds it to the remote side.
func (c *Connection) handleTunnelInit(id uint64, chunkLimit int) {
	if tun, err := c.acceptTunnel(id, chunkLimit); err == nil {
		c.handler.HandleTunnel(tun)
	} else {
		log.Printf("iris: failed to accept inbound tunnel: %v.", err)
	}
}

// Forwards the tunnel construction result to the requested tunnel.
func (c *Connection) handleTunnelResult(id uint64, chunkLimit int) {
	// Retrieve the tunnel
	c.tunLock.RLock()
	tun := c.tunLive[id]
	c.tunLock.RUnlock()

	// Finalize initialization
	tun.handleInitResult(chunkLimit)
}

// Forwards a tunnel data allowance to the requested tunnel.
func (c *Connection) handleTunnelAllowance(id uint64, space int) {
	// Retrieve the tunnel
	c.tunLock.RLock()
	tun, ok := c.tunLive[id]
	c.tunLock.RUnlock()

	// Notify it of the granted data allowance
	if ok {
		tun.handleAllowance(space)
	}
}

// Forwards a message chunk transfer to the requested tunnel.
func (c *Connection) handleTunnelTransfer(id uint64, size int, chunk []byte) {
	// Retrieve the tunnel
	c.tunLock.RLock()
	tun, ok := c.tunLive[id]
	c.tunLock.RUnlock()

	// Notify it of the arrived message chunk
	if ok {
		tun.handleTransfer(size, chunk)
	}
}

// Terminates a tunnel, stopping all data transfers.
func (c *Connection) handleTunnelClose(id uint64, reason string) {
	c.tunLock.Lock()
	defer c.tunLock.Unlock()

	// Make sure the tunnel is still alive
	if tun, ok := c.tunLive[id]; ok {
		tun.handleClose(reason)
		delete(c.tunLive, id)
	}
}
