// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package iris

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// Message relay between the local app and the local iris node.
type connection struct {
	// Application layer fields
	handler ConnectionHandler // Handler for connection events

	reqIdx  uint64                 // Index to assign the next request
	reqReps map[uint64]chan []byte // Reply channels for active requests
	reqErrs map[uint64]chan error  // Error channels for active requests
	reqLock sync.RWMutex           // Mutex to protect the result channel maps

	subLive map[string]SubscriptionHandler // Active subscriptions
	subLock sync.RWMutex                   // Mutex to protect the subscription map

	tunIdx  uint64             // Index to assign the next tunnel
	tunLive map[uint64]*tunnel // Active tunnels
	tunLock sync.RWMutex       // Mutex to protect the tunnel map

	// Network layer fields
	sock     net.Conn          // Network connection to the iris node
	sockBuf  *bufio.ReadWriter // Buffered access to the network socket
	sockLock sync.Mutex        // Mutex to atomize message sending

	// Bookkeeping fields
	init chan struct{}   // Init channel to receive a success signal
	quit chan chan error // Quit channel to synchronize receiver termination
	term chan struct{}   // Channel to signal termination to blocked go-routines
}

// Connects to a local relay endpoint on port and registers as cluster.
func newConnection(port int, cluster string, handler ConnectionHandler) (Connection, error) {
	// Connect to the iris relay node
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, err
	}
	sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	// Create the relay object
	conn := &connection{
		// Application layer
		handler: handler,

		reqReps: make(map[uint64]chan []byte),
		reqErrs: make(map[uint64]chan error),
		subLive: make(map[string]SubscriptionHandler),
		tunLive: make(map[uint64]*tunnel),

		// Network layer
		sock:    sock,
		sockBuf: bufio.NewReadWriter(bufio.NewReader(sock), bufio.NewWriter(sock)),

		// Bookkeeping
		quit: make(chan chan error),
		term: make(chan struct{}),
	}
	// Initialize the connection and wait for a confirmation
	if err := conn.sendInit(cluster); err != nil {
		return nil, err
	}
	if _, err := conn.procInit(); err != nil {
		return nil, err
	}
	// All ok, start processing messages and return
	go conn.process()
	return conn, nil
}

// Implements iris.Connection.Broadcast.
func (c *connection) Broadcast(cluster string, message []byte) error {
	// Sanity check on the arguments
	if len(cluster) == 0 {
		return errors.New("empty cluster identifier")
	}
	if message == nil {
		return errors.New("nil message")
	}
	// Broadcast and return
	return c.sendBroadcast(cluster, message)
}

// Implements iris.Connection.Request.
func (c *connection) Request(cluster string, request []byte, timeout time.Duration) ([]byte, error) {
	// Sanity check on the arguments
	if len(cluster) == 0 {
		return nil, errors.New("empty cluster identifier")
	}
	if request == nil {
		return nil, errors.New("nil request")
	}
	timeoutms := int(timeout.Nanoseconds() / 1000000)
	if timeoutms < 1 {
		return nil, fmt.Errorf("invalid timeout %v < 1ms", timeout)
	}
	// Create a reply and error channel for the results
	repc := make(chan []byte, 1)
	errc := make(chan error, 1)

	c.reqLock.Lock()
	reqId := c.reqIdx
	c.reqIdx++
	c.reqReps[reqId] = repc
	c.reqErrs[reqId] = errc
	c.reqLock.Unlock()

	// Make sure the result channels are cleaned up
	defer func() {
		c.reqLock.Lock()
		delete(c.reqReps, reqId)
		delete(c.reqErrs, reqId)
		close(repc)
		close(errc)
		c.reqLock.Unlock()
	}()
	// Send the request
	if err := c.sendRequest(reqId, cluster, request, timeoutms); err != nil {
		return nil, err
	}
	// Retrieve the results or fail if terminating
	select {
	case <-c.term:
		return nil, ErrClosing
	case reply := <-repc:
		return reply, nil
	case err := <-errc:
		return nil, err
	}
}

// Implements iris.Connection.Subscribe.
func (c *connection) Subscribe(topic string, handler SubscriptionHandler) error {
	// Sanity check on the arguments
	if len(topic) == 0 {
		return errors.New("empty topic identifier")
	}
	if handler == nil {
		return errors.New("nil subscription handler")
	}
	// Subscribe locally
	c.subLock.Lock()
	if _, ok := c.subLive[topic]; ok {
		c.subLock.Unlock()
		return errors.New("already subscribed")
	}
	c.subLive[topic] = handler
	c.subLock.Unlock()

	// Send the subscription request
	err := c.sendSubscribe(topic)
	if err != nil {
		c.subLock.Lock()
		if _, ok := c.subLive[topic]; ok {
			delete(c.subLive, topic)
		}
		c.subLock.Unlock()
	}
	return err
}

// Implements iris.Connection.Publish.
func (c *connection) Publish(topic string, event []byte) error {
	// Sanity check on the arguments
	if len(topic) == 0 {
		return errors.New("empty topic identifier")
	}
	if event == nil {
		return errors.New("nil event")
	}
	// Publish and return
	return c.sendPublish(topic, event)
}

// Implements iris.Connection.Unsubscribe.
func (c *connection) Unsubscribe(topic string) error {
	// Sanity check on the arguments
	if len(topic) == 0 {
		return errors.New("empty topic identifier")
	}
	// Unsubscribe through the relay and remove if successful
	err := c.sendUnsubscribe(topic)
	if err == nil {
		c.subLock.Lock()
		defer c.subLock.Unlock()

		if _, ok := c.subLive[topic]; !ok {
			return errors.New("not subscribed")
		}
		delete(c.subLive, topic)
	}
	return err
}

// Implements iris.Connection.Tunnel.
func (c *connection) Tunnel(cluster string, timeout time.Duration) (Tunnel, error) {
	// Sanity check on the arguments
	if len(cluster) == 0 {
		return nil, errors.New("empty cluster identifier")
	}
	timeoutms := int(timeout.Nanoseconds() / 1000000)
	if timeoutms < 1 {
		return nil, fmt.Errorf("invalid timeout %v < 1ms", timeout)
	}
	// Simple call indirection to move into the tunnel source file
	return c.initTunnel(cluster, timeoutms)
}

// Implements iris.Connection.Close.
func (c *connection) Close() error {
	// Send a graceful close to the relay node
	if err := c.sendClose(); err != nil {
		return err
	}
	// Wait till the close syncs and return
	errc := make(chan error, 1)
	c.quit <- errc
	return <-errc
}
