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

	"github.com/project-iris/iris/pool"
)

// Client connection to the Iris network.
type Connection struct {
	// Application layer fields
	handler ServiceHandler // Handler for connection events

	reqIdx  uint64                 // Index to assign the next request
	reqReps map[uint64]chan []byte // Reply channels for active requests
	reqErrs map[uint64]chan error  // Error channels for active requests
	reqLock sync.RWMutex           // Mutex to protect the result channel maps

	subLive map[string]*topic // Active subscriptions
	subLock sync.RWMutex      // Mutex to protect the subscription map

	tunIdx  uint64             // Index to assign the next tunnel
	tunLive map[uint64]*Tunnel // Active tunnels
	tunLock sync.RWMutex       // Mutex to protect the tunnel map

	// Quality of service fields
	limits *ServiceLimits // Limits on the inbound message processing

	bcastPool *pool.ThreadPool // Queue and concurrency limiter for the broadcast handlers
	bcastUsed int32            // Actual memory usage of the broadcast queue

	reqPool *pool.ThreadPool // Queue and concurrency limiter for the request handlers
	reqUsed int32            // Actual memory usage of the request queue

	// Network layer fields
	sock     net.Conn          // Network connection to the iris node
	sockBuf  *bufio.ReadWriter // Buffered access to the network socket
	sockLock sync.Mutex        // Mutex to atomize message sending

	// Bookkeeping fields
	init chan struct{}   // Init channel to receive a success signal
	quit chan chan error // Quit channel to synchronize receiver termination
	term chan struct{}   // Channel to signal termination to blocked go-routines
}

// Connects to the Iris network as a simple client.
func Connect(port int) (*Connection, error) {
	return newConnection(port, "", nil, nil)
}

// Connects to a local relay endpoint on port and registers as cluster.
func newConnection(port int, cluster string, handler ServiceHandler, limits *ServiceLimits) (*Connection, error) {
	limits = finalizeServiceLimits(limits)

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
	conn := &Connection{
		// Application layer
		handler: handler,

		reqReps: make(map[uint64]chan []byte),
		reqErrs: make(map[uint64]chan error),
		subLive: make(map[string]*topic),
		tunLive: make(map[uint64]*Tunnel),

		// Quality of service
		limits: limits,

		bcastPool: pool.NewThreadPool(limits.BroadcastThreads),
		reqPool:   pool.NewThreadPool(limits.RequestThreads),

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
	// Start the network receiver and return
	go conn.process()
	return conn, nil
}

// Merges the user requested limits with the defaults.
func finalizeServiceLimits(user *ServiceLimits) *ServiceLimits {
	// If the user didn't specify anything, load the full default set
	if user == nil {
		return &defaultServiceLimits
	}
	// Check each field and merge only non-specified ones
	limits := new(ServiceLimits)
	*limits = *user

	if user.BroadcastThreads == 0 {
		limits.BroadcastThreads = defaultServiceLimits.BroadcastThreads
	}
	if user.BroadcastMemory == 0 {
		limits.BroadcastMemory = defaultServiceLimits.BroadcastMemory
	}
	if user.RequestThreads == 0 {
		limits.RequestThreads = defaultServiceLimits.RequestThreads
	}
	if user.RequestMemory == 0 {
		limits.RequestMemory = defaultServiceLimits.RequestMemory
	}
	return limits
}

// Broadcasts a message to all members of a cluster. No guarantees are made that
// all recipients receive the message (best effort).
//
// The call blocks until the message is forwarded to the local Iris node.
func (c *Connection) Broadcast(cluster string, message []byte) error {
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

// Executes a synchronous request to be serviced by a member of the specified
// cluster, load-balanced between all participant, returning the received reply.
//
// The timeout unit is in milliseconds. Anything lower will fail with an error.
func (c *Connection) Request(cluster string, request []byte, timeout time.Duration) ([]byte, error) {
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
		return nil, ErrClosed
	case reply := <-repc:
		return reply, nil
	case err := <-errc:
		return nil, err
	}
}

// Subscribes to a topic, using handler as the callback for arriving events.
//
// The method blocks until the subscription is forwarded to the relay. There
// might be a small delay between subscription completion and start of event
// delivery. This is caused by subscription propagation through the network.
func (c *Connection) Subscribe(topic string, handler TopicHandler, limits *TopicLimits) error {
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
	c.subLive[topic] = newTopic(handler, limits)
	c.subLock.Unlock()

	// Send the subscription request
	err := c.sendSubscribe(topic)
	if err != nil {
		c.subLock.Lock()
		if top, ok := c.subLive[topic]; ok {
			top.terminate()
			delete(c.subLive, topic)
		}
		c.subLock.Unlock()
	}
	return err
}

// Publishes an event asynchronously to topic. No guarantees are made that all
// subscribers receive the message (best effort).
//
// The method blocks until the message is forwarded to the local Iris node.
func (c *Connection) Publish(topic string, event []byte) error {
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

// Unsubscribes from topic, receiving no more event notifications for it.
//
// The method blocks until the unsubscription is forwarded to the local Iris node.
func (c *Connection) Unsubscribe(topic string) error {
	// Sanity check on the arguments
	if len(topic) == 0 {
		return errors.New("empty topic identifier")
	}
	// Unsubscribe through the relay and remove if successful
	err := c.sendUnsubscribe(topic)
	if err == nil {
		c.subLock.Lock()
		defer c.subLock.Unlock()

		if top, ok := c.subLive[topic]; !ok {
			return errors.New("not subscribed")
		} else {
			top.terminate()
			delete(c.subLive, topic)
		}
	}
	return err
}

// Opens a direct tunnel to a member of a remote cluster, allowing pairwise-
// exclusive, order-guaranteed and throttled message passing between them.
//
// The method blocks until the newly created tunnel is set up, or the time
// limit is reached.
//
// The timeout unit is in milliseconds. Anything lower will fail with an error.
func (c *Connection) Tunnel(cluster string, timeout time.Duration) (*Tunnel, error) {
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

// Gracefully terminates the connection removing all subscriptions and closing
// all active tunnels.
//
// The call blocks until the connection tear-down is confirmed by the Iris node.
func (c *Connection) Close() error {
	// Send a graceful close to the relay node
	if err := c.sendClose(); err != nil {
		return err
	}
	// Wait till the close syncs and return
	errc := make(chan error, 1)
	c.quit <- errc

	// Terminate all running subscription handlers
	c.subLock.Lock()
	for _, topic := range c.subLive {
		topic.terminate()
	}
	c.subLock.Unlock()

	return <-errc
}
