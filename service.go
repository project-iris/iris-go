// Copyright (c) 2014 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package iris

import "errors"

// Callback interface for processing inbound messages designated to a particular
// service instance.
type ServiceHandler interface {
	// Called once after the service is registered in the Iris network, but before
	// and handlers are activated. Its goal is to initialize any internal state
	// dependent on the connection.
	Init(conn *Connection) error

	// Callback invoked whenever a broadcast message arrives designated to the
	// cluster of which this particular service instance is part of.
	HandleBroadcast(message []byte)

	// Callback invoked whenever a request designated to the service's cluster is
	// load-balanced to this particular service instance.
	//
	// The method should service the request and return either a reply or the
	// error encountered, which will be delivered to the request originator.
	//
	// Returning nil for both or none of the results will result in a panic. Also,
	// since the requests cross language boundaries, only the error string gets
	// delivered remotely (any associated type information is effectively lost).
	HandleRequest(request []byte) ([]byte, error)

	// Callback invoked whenever a tunnel designated to the service's cluster is
	// constructed from a remote node to this particular instance.
	HandleTunnel(tunnel *Tunnel)

	// Callback notifying the service that the local relay dropped its connection.
	HandleDrop(reason error)
}

// Service instance belonging to a particular cluster in the network.
type Service struct {
	conn *Connection    // Network connection to the local Iris relay
	hand ServiceHandler // Handler for inbound messages
}

// Connects to the Iris network and registers a new service instance as a member
// of the specified service cluster.
func Register(port int, cluster string, handler ServiceHandler) (*Service, error) {
	// Sanity check on the arguments
	if len(cluster) == 0 {
		return nil, errors.New("empty cluster identifier")
	}
	if handler == nil {
		return nil, errors.New("nil service handler")
	}
	// Connect to the Iris relay as a service
	conn, err := newConnection(port, cluster, handler)
	if err != nil {
		return nil, err
	}
	// Assemble the service object and initialize it
	serv := &Service{
		conn: conn,
		hand: handler,
	}
	if err := serv.hand.Init(conn); err != nil {
		conn.Close()
		return nil, err
	}
	// All ok, return the service instance
	return serv, nil
}

// Unregisters the service instance from the Iris network.
func (s *Service) Unregister() error {
	return s.conn.Close()
}
