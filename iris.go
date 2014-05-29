// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

// Package iris contains the Go binding to the Iris cloud messaging framework.
package iris

import (
	"errors"
	"time"
)

var ErrTimeout = errors.New("operation timed out")
var ErrClosing = errors.New("connection closing")
var ErrClosed = errors.New("connection closed")

// Returns the relay protocol version implemented. Connecting to an Iris node
// will fail unless the versions match exactly.
func Version() string {
	return protoVersion
}

// Connects to the local Iris relay as a member of cluster, using handler as the
// callback for inbound application events.
func Connect(port int, cluster string, handler ConnectionHandler) (Connection, error) {
	return newConnection(port, cluster, handler)
}

// Link to the local Iris node. All communication passes through one of these.
type Connection interface {
	// Broadcasts a message to all members of a cluster. No guarantees are made
	// that all recipients receive the message (best effort).
	//
	// The call blocks until the message is forwarded to the relay node.
	Broadcast(cluster string, message []byte) error

	// Executes a synchronous request to be serviced by a member of cluster, load
	// balanced between all active members, returning the received reply.
	//
	// The timeout unit is in milliseconds. Anything lower will fail with an error.
	Request(cluster string, request []byte, timeout time.Duration) ([]byte, error)

	// Subscribes to topic, using handler as the callback for arriving events.
	//
	// The method blocks until the subscription is forwarded to the relay. There
	// might be a small delay between subscription completion and start of event
	// delivery. This is caused by subscription propagation through the network.
	Subscribe(topic string, handler SubscriptionHandler) error

	// Publishes an event asynchronously to topic. No guarantees are made that all
	// subscribers receive the message (best effort).
	//
	// The method blocks until the message is forwarded to the relay.
	Publish(topic string, event []byte) error

	// Unsubscribes from topic, receiving no more event notifications for it.
	//
	// The method blocks until the unsubscription is forwarded to the relay.
	Unsubscribe(topic string) error

	// Opens a direct tunnel to a member of cluster, allowing pairwise-exclusive
	// order-guaranteed and throttled message passing between them.
	//
	// The method blocks until the newly created tunnel is set up, or the time
	// limit is reached.
	//
	// The timeout unit is in milliseconds. Anything lower will fail with an error.
	Tunnel(cluster string, timeout time.Duration) (Tunnel, error)

	// Gracefully terminates the connection removing all subscriptions and closing
	// all tunnels.
	//
	// The call blocks until the connection is torn down.
	Close() error
}

// Communication stream between the local application and a remote endpoint. The
// ordered delivery of messages is guaranteed and the message flow between the
// peers is throttled.
type Tunnel interface {
	// Sends a message over the tunnel to the remote pair, blocking until the
	// local relay node receives the message or the operation times out.
	//
	// Infinite blocking is supported with by setting the timeout to zero (0).
	Send(message []byte, timeout time.Duration) error

	// Retrieves a message from the tunnel, blocking until one is available or the
	// operation times out.
	//
	// Infinite blocking is supported with by setting the timeout to zero (0).
	Recv(timeout time.Duration) ([]byte, error)

	// Closes the tunnel between the pair. Any blocked read and write operation
	// will terminate with a failure.
	//
	// The method blocks until the local relay node acknowledges the tear-down.
	Close() error
}

// Handler for the connection scope events.
type ConnectionHandler interface {
	// Handles a message broadcasted to all members of the local cluster.
	HandleBroadcast(message []byte)

	// Handles a request, returning the reply or error that should be forwarded
	// back to the caller. If the method crashes, nothing is returned and the
	// caller will eventually time out.
	//
	// Note, that only the error string is forwarded as there is no cross language
	// representation of errors.
	HandleRequest(request []byte) ([]byte, error)

	// Handles an inbound tunnel from a remote node in the network.
	HandleTunnel(tun Tunnel)

	// Handles the unexpected termination of the relay connection.
	HandleDrop(reason error)
}

// Subscription handler receiving events from a single subscribed topic.
type SubscriptionHandler interface {
	// Handles an event published to the subscribed topic.
	HandleEvent(event []byte)
}
