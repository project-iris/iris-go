// Iris Go Binding
// Copyright 2013 Peter Szilagyi. All rights reserved.
//
// The current language binding is an official support library of the Iris
// decentralized messaging framework, and as such, the same licensing terms
// hold. For details please see http://github.com/karalabe/iris/LICENSE.md
//
// Author: peterke@gmail.com (Peter Szilagyi)

// Package iris contains the go binding to the iris messaging framework.
package iris

// Returns the relay protocol version implemented. Connecting to an Iris node
// will fail unless the versions match exactly.
func Version() string {
	return relayVersion
}

// Connects to the iris message relay running locally, registering with the id
// app and using handler as the inbound application event handler.
func Connect(port int, app string, handler ConnectionHandler) (Connection, error) {
	return newRelay(port, app, handler)
}

// Communication interface to the iris network.
type Connection interface {
	// Broadcasts a message to all applications of type app. No guarantees are
	// made that all recipients receive the message (best effort).
	//
	// The call blocks until the message is sent to the relay, returning an
	// iris.Error in case of a failure.
	Broadcast(app string, msg []byte) error

	// Executes a synchronous request to app, load balanced between all the active
	// ones, returning the received reply.
	//
	// In case of a failure, the function returns a nil reply with an iris.Error
	// stating the reason.
	//
	// The timeout is in milliseconds. Setting infinite timeouts is not supported,
	// hence the usual value of 0 (or less) will result in a panic!
	Request(app string, req []byte, timeout int) ([]byte, error)

	// Subscribes to topic, using handler as the callback for arriving events.
	//
	// The method blocks until the subscription is forwarded to the relay, or an
	// error occures, in which case an iris.Error is returned.
	//
	// Double subscription is considered a programming error and will result in a
	// panic!
	Subscribe(topic string, handler SubscriptionHandler) error

	// Publishes an event asynchronously to topic. No guarantees are made that all
	// subscribers receive the message (best effort).
	//
	// The method does blocks until the message is forwarded to the relay, or an
	// error occures, in which case an iris.Error is returned.
	Publish(topic string, msg []byte) error

	// Unsubscribes from topic, receiving no more event notifications for it.
	//
	// The method does blocks until the unsubscription is forwarded to the relay,
	// or an error occures, in which case an iris.Error is returned.
	//
	// Unsubscribing from a topic not subscribed to is considered a programming
	// error and will result in a panic!
	Unsubscribe(topic string) error

	// Opens a direct tunnel to an instance of app, allowing pairwise-exclusive
	// and order-guaranteed message passing between them.
	//
	// The method blocks until either the newly created tunnel is set up, or an
	// error occurs, in which case a nil tunnel and an iris.Error is returned.
	//
	// The timeout is in milliseconds. Setting infinite timeouts is not supported,
	// hence the usual value of 0 (or less) will result in a panic!
	Tunnel(app string, timeout int) (Tunnel, error)

	// Gracefully terminates the connection removing all subscriptions and closing
	// all tunnels.
	//
	// The call blocks until the connection is torn down or an error occurs.
	Close() error
}

// Communication stream between the local application and a remote endpoint. The
// ordered delivery of messages is guaranteed.
//
// Note, a tunnel is designed to be used by a single thread. Concurrent access
// will result in undefined behavior.
type Tunnel interface {
	// Sends a message over the tunnel to the remote pair.
	//
	// The method blocks until the lcaol relay node receives the message, or an
	// error occurs, in which case an iris.Error is returned.
	//
	// The timeout is in milliseconds. Inifinite timeouts are supported by setting
	// a value of 0. Anything less will result in a panic!
	Send(msg []byte, timeout int) error

	// Retrieves a message from the tunnel, blocking until one is available. As
	// with the Send method, Recv too returns an iris.Error in case of a failure.
	//
	// The timeout is in milliseconds. Inifinite timeouts are supported by setting
	// a value of 0. Anything less will result in a panic!
	Recv(timeout int) ([]byte, error)

	// Closes the tunnel between the pair. Any blocked read and write operations
	// will terminate with a failure.
	//
	// The method blocks until the connection is torn down or an error occurs, in
	// which case an iris.Error is returned.
	//
	// Double close is considered a programming error and will result in a panic!
	Close() error
}

// Handler for the connection scope events.
type ConnectionHandler interface {
	// Handles a message broadcasted to all applications of the local type.
	HandleBroadcast(msg []byte)

	// Handles a request (msg), returning the reply that should be forwarded back
	// to the caller. If the method crashes, nothing is retuned and the caller
	// will eventually time out.
	HandleRequest(msg []byte) []byte

	// Handles the request to open a direct tunnel.
	HandleTunnel(tun Tunnel)

	// Handles the unexpecter termination of the relay conenction.
	HandleDrop(reason error)
}

// Subscription handler receiving events from a single subscribed topic.
type SubscriptionHandler interface {
	// Handles an event published to the subscribed topic.
	HandleEvent(msg []byte)
}
