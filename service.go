// Copyright (c) 2014 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package iris

import (
	"errors"
	"fmt"
	"sync/atomic"

	"gopkg.in/inconshreveable/log15.v2"
)

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
	conn *Connection  // Network connection to the local Iris relay
	Log  log15.Logger // Logger with service id injected
}

// Id to assign to the next service (used for logging purposes).
var nextServId uint64

// Connects to the Iris network and registers a new service instance as a member
// of the specified service cluster.
func Register(port int, cluster string, handler ServiceHandler, limits *ServiceLimits) (*Service, error) {
	// Sanity check on the arguments
	if len(cluster) == 0 {
		return nil, errors.New("empty cluster identifier")
	}
	if handler == nil {
		return nil, errors.New("nil service handler")
	}
	// Make sure the service limits have valid values
	limits = finalizeServiceLimits(limits)

	logger := Log.New("service", atomic.AddUint64(&nextServId, 1))
	logger.Info("registering new service", "relay_port", port, "cluster", cluster,
		"broadcast_limits", log15.Lazy{func() string {
			return fmt.Sprintf("%dT|%dB", limits.BroadcastThreads, limits.BroadcastMemory)
		}},
		"request_limits", log15.Lazy{func() string {
			return fmt.Sprintf("%dT|%dB", limits.RequestThreads, limits.RequestMemory)
		}})

	// Connect to the Iris relay as a service
	conn, err := newConnection(port, cluster, handler, limits, logger)
	if err != nil {
		logger.Warn("failed to register new service", "reason", err)
		return nil, err
	}
	// Assemble the service object and initialize it
	serv := &Service{
		conn: conn,
		Log:  logger,
	}
	if err := handler.Init(conn); err != nil {
		logger.Warn("user failed to initialize service", "reason", err)
		conn.Close()
		return nil, err
	}
	logger.Info("service registration completed")

	// Start the handler pools
	conn.bcastPool.Start()
	conn.reqPool.Start()

	return serv, nil
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

// Unregisters the service instance from the Iris network.
func (s *Service) Unregister() error {
	// Tear-down the connection
	err := s.conn.Close()

	// Stop all the thread pools (drop unprocessed messages)
	s.conn.reqPool.Terminate(true)
	s.conn.bcastPool.Terminate(true)

	// Return the result of the connection close
	return err
}
