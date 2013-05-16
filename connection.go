// Iris Go Binding
// Copyright 2013 Peter Szilagyi. All rights reserved.
//
// The current language binding is an official support library of the Iris
// decentralized messaging framework, and as such, the same licensing terms
// hold. For details please see http://github.com/karalabe/iris/LICENSE.md
//
// Author: peterke@gmail.com (Peter Szilagyi)

// Contains the handlers for the events originating from the Iris node.

package iris

import (
	"fmt"
	"sync"
	"time"
)

type connection struct {
	relay *relay // Message relay into the network

	reqIdx uint64                         // Index to assign the next request
	reqs   map[uint64]chan []byte         // Active requests waiting for a reply
	subs   map[string]SubscriptionHandler // Active subscriptions
	tunIdx uint64                         // Index to assign the next tunnel
	//tuns   map[uint64]*tunnel     // Active tunnels

	hand ConnectionHandler
	lock sync.Mutex
}

// Connects to the Iris message relay running on locally on port, registering
// with the name app, using hand as the inbound event handler.
func Connect(port int, app string, handler ConnectionHandler) (Connection, error) {
	// Create the new connection
	conn := &connection{
		reqs: make(map[uint64]chan []byte),
		subs: make(map[string]SubscriptionHandler),
		//tuns: make(map[uint64]*tunnel),
		hand: handler,
	}
	// Create the message relay and return
	if rel, err := connect(port, app, conn); err != nil {
		return nil, err
	} else {
		conn.relay = rel
		return conn, nil
	}
}

// Implements iris.Connection.Request.
func (c *connection) Request(app string, req []byte, timeout int) ([]byte, error) {
	// Create a reply channel for the results
	c.lock.Lock()
	reqChan := make(chan []byte, 1)
	reqId := c.reqIdx
	c.reqs[reqId] = reqChan
	c.reqIdx++
	c.lock.Unlock()

	// Make sure reply channel is cleaned up
	defer func() {
		c.lock.Lock()
		defer c.lock.Unlock()
		delete(c.reqs, reqId)
		close(reqChan)
	}()
	// Send the request
	if err := c.relay.sendRequest(reqId, app, req, timeout); err != nil {
		return nil, err
	}
	// Retrieve the results or time out
	tick := time.Tick(time.Duration(timeout) * time.Millisecond)
	select {
	case <-tick:
		return nil, fmt.Errorf("request timed out")
	case rep := <-reqChan:
		return rep, nil
	}
}

// Implements iris.Connection.Broadcast.
func (c *connection) Broadcast(app string, msg []byte) {
	c.relay.sendBroadcast(app, msg)
}

// Implements iris.Connection.Subscribe.
func (c *connection) Subscribe(topic string, handler SubscriptionHandler) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.subs[topic]; ok {
		return fmt.Errorf("already subscribed")
	}
	c.subs[topic] = handler
	return c.relay.sendSubscribe(topic)
}

// Implements iris.Connection.Publish.
func (c *connection) Publish(topic string, msg []byte) {
	c.relay.sendPublish(topic, msg)
}

// Implements iris.Connection.Unsubscribe.
func (c *connection) Unsubscribe(topic string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.subs[topic]; !ok {
		return fmt.Errorf("not subscribed")
	}
	delete(c.subs, topic)
	return c.relay.sendUnsubscribe(topic)
}

func (c *connection) Tunnel(app string, handler TunnelHandler, timeout int) (Tunnel, error) {
	return nil, fmt.Errorf("not implemented.")
}

// Implements iris.Connection.Close.
func (c *connection) Close() {
	c.relay.sendClose()
	c.relay.sock.Close()
}
