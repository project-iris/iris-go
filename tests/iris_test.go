// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package tests

import (
	"fmt"
	"testing"

	"gopkg.in/project-iris/iris-go.v0"
)

// Tests multiple concurrent client connections.
func TestConnect(t *testing.T) {
	// Test specific configurations
	conf := struct {
		clients int
	}{100}

	// Start a batch of parallel connections
	barrier := newBarrier(conf.clients)
	for i := 0; i < conf.clients; i++ {
		go func() {
			// Connect to the local relay
			conn, err := iris.Connect(config.relay)
			if err != nil {
				barrier.Exit(fmt.Errorf("connection failed: %v", err))
				return
			}
			barrier.Sync()

			// Disconnect from the local relay
			if err := conn.Close(); err != nil {
				barrier.Exit(fmt.Errorf("connection close failed: %v", err))
				return
			}
			barrier.Exit(nil)
		}()
	}
	// Schedule the parallel operations
	if errs := barrier.Wait(); len(errs) != 0 {
		t.Fatalf("connection phase failed: %v.", errs)
	}
	if errs := barrier.Wait(); len(errs) != 0 {
		t.Fatalf("termination phase failed: %v.", errs)
	}
}

// Service handler for the registration tests.
type registerTestHandler struct{}

func (r *registerTestHandler) Init(conn *iris.Connection) error         { return nil }
func (r *registerTestHandler) HandleBroadcast(msg []byte)               { panic("not implemented") }
func (r *registerTestHandler) HandleRequest(req []byte) ([]byte, error) { panic("not implemented") }
func (r *registerTestHandler) HandleTunnel(tun *iris.Tunnel)            { panic("not implemented") }
func (r *registerTestHandler) HandleDrop(reason error)                  { panic("not implemented") }

// Tests multiple concurrent service registrations.
func TestRegister(t *testing.T) {
	// Test specific configurations
	conf := struct {
		services int
	}{100}

	// Start a batch of parallel connections
	barrier := newBarrier(conf.services)
	for i := 0; i < conf.services; i++ {
		go func() {
			// Register a new service to the relay
			serv, err := iris.Register(config.relay, config.cluster, new(registerTestHandler))
			if err != nil {
				barrier.Exit(fmt.Errorf("registration failed: %v", err))
				return
			}
			barrier.Sync()

			// Unregister the service
			if err := serv.Unregister(); err != nil {
				barrier.Exit(fmt.Errorf("unregistration failed: %v", err))
				return
			}
			barrier.Exit(nil)
		}()
	}
	// Schedule the parallel operations
	if errs := barrier.Wait(); len(errs) != 0 {
		t.Fatalf("registration phase failed: %v.", errs)
	}
	if errs := barrier.Wait(); len(errs) != 0 {
		t.Fatalf("unregistration phase failed: %v.", errs)
	}
}

// Benchmarks client connection.
func BenchmarkConnect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if conn, err := iris.Connect(config.relay); err != nil {
			b.Fatalf("iteration %d: connection failed: %v.", i, err)
		} else {
			defer conn.Close()
		}
	}
	// Stop the timer and clean up
	b.StopTimer()
}

// Benchmarks service registration.
func BenchmarkRegister(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if serv, err := iris.Register(config.relay, config.cluster, new(registerTestHandler)); err != nil {
			b.Fatalf("iteration %d: register failed: %v.", i, err)
		} else {
			defer serv.Unregister()
		}
	}
	// Stop the timer and clean up
	b.StopTimer()
}
