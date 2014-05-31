// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package tests

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/project-iris/iris/pool"
	"gopkg.in/project-iris/iris-go.v0"
)

// Service handler for the request/reply tests.
type reqrepTestHandler struct {
	conn *iris.Connection
}

func (r *reqrepTestHandler) Init(conn *iris.Connection) error         { r.conn = conn; return nil }
func (r *reqrepTestHandler) HandleBroadcast(msg []byte)               { panic("not implemented") }
func (r *reqrepTestHandler) HandleRequest(req []byte) ([]byte, error) { return req, nil }
func (r *reqrepTestHandler) HandleTunnel(tun *iris.Tunnel)            { panic("not implemented") }
func (r *reqrepTestHandler) HandleDrop(reason error)                  { panic("not implemented") }

// Service handler for the request/reply failure tests.
type reqrepFailTestHandler struct {
	conn *iris.Connection
}

func (r *reqrepFailTestHandler) Init(conn *iris.Connection) error { r.conn = conn; return nil }
func (r *reqrepFailTestHandler) HandleBroadcast(msg []byte)       { panic("not implemented") }
func (r *reqrepFailTestHandler) HandleTunnel(tun *iris.Tunnel)    { panic("not implemented") }
func (r *reqrepFailTestHandler) HandleDrop(reason error)          { panic("not implemented") }

func (r *reqrepFailTestHandler) HandleRequest(req []byte) ([]byte, error) {
	return nil, errors.New(string(req))
}

// Tests multiple concurrent client and service requests.
func TestRequest(t *testing.T) {
	// Test specific configurations
	conf := struct {
		clients  int
		servers  int
		requests int
	}{25, 25, 25}

	barrier := newBarrier(conf.clients + conf.servers)

	// Start up the concurrent requesting clients
	for i := 0; i < conf.clients; i++ {
		go func(client int) {
			// Connect to the local relay
			conn, err := iris.Connect(config.relay)
			if err != nil {
				barrier.Exit(fmt.Errorf("connection failed: %v", err))
				return
			}
			defer conn.Close()
			barrier.Sync()

			// Request from the service cluster
			for i := 0; i < conf.requests; i++ {
				request := fmt.Sprintf("client #%d, request %d", client, i)
				if reply, err := conn.Request(config.cluster, []byte(request), time.Second); err != nil {
					barrier.Exit(fmt.Errorf("client request failed: %v", err))
					return
				} else if string(reply) != request {
					barrier.Exit(fmt.Errorf("client invalid reply: have %v, want %v", string(reply), request))
					return
				}
			}
			barrier.Exit(nil)
		}(i)
	}
	// Start up the concurrent request services
	for i := 0; i < conf.servers; i++ {
		go func(server int) {
			// Create the service handler
			handler := new(reqrepTestHandler)

			// Register a new service to the relay
			serv, err := iris.Register(config.relay, config.cluster, handler)
			if err != nil {
				barrier.Exit(fmt.Errorf("registration failed: %v", err))
				return
			}
			defer serv.Unregister()
			barrier.Sync()

			// Request from the service cluster
			for i := 0; i < conf.requests; i++ {
				request := fmt.Sprintf("server #%d, request %d", server, i)
				if reply, err := handler.conn.Request(config.cluster, []byte(request), time.Second); err != nil {
					barrier.Exit(fmt.Errorf("server request failed: %v", err))
					return
				} else if string(reply) != request {
					barrier.Exit(fmt.Errorf("server invalid reply: have %v, want %v", string(reply), request))
					return
				}
			}
			barrier.Exit(nil)
		}(i)
	}
	// Schedule the parallel operations
	if errs := barrier.Wait(); len(errs) != 0 {
		t.Fatalf("startup phase failed: %v.", errs)
	}
	if errs := barrier.Wait(); len(errs) != 0 {
		t.Fatalf("request phase failed: %v.", errs)
	}
}

// Tests request failure forwarding.
func TestRequestFail(t *testing.T) {
	// Test specific configurations
	conf := struct {
		requests int
	}{125}

	// Create the service handler
	handler := new(reqrepFailTestHandler)

	// Register a new service to the relay
	serv, err := iris.Register(config.relay, config.cluster, handler)
	if err != nil {
		t.Fatalf("registration failed: %v", err)
	}
	defer serv.Unregister()

	// Request from the failing service cluster
	for i := 0; i < conf.requests; i++ {
		request := fmt.Sprintf("failure %d", i)
		reply, err := handler.conn.Request(config.cluster, []byte(request), time.Second)
		switch {
		case err == nil:
			t.Fatalf("request didn't fail: %v.", reply)
		case err.Error() != request:
			t.Fatalf("error message mismatch: have %v, want %v.", err, request)
		}
	}
}

// Benchmarks the latency of a single request/reply operation.
func BenchmarkRequestLatency(b *testing.B) {
	// Create the service handler
	handler := new(reqrepTestHandler)

	// Register a new service to the relay
	serv, err := iris.Register(config.relay, config.cluster, handler)
	if err != nil {
		b.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Reset timer and benchmark the message transfer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := handler.conn.Request(config.cluster, []byte{byte(i)}, time.Second); err != nil {
			b.Fatalf("request failed: %v.", err)
		}
	}
	// Stop the timer (don't measure deferred cleanup)
	b.StopTimer()
}

// Benchmarks the throughput of a stream of concurrent requests.
func BenchmarkRequestThroughput1Threads(b *testing.B) {
	benchmarkRequestThroughput(1, b)
}

func BenchmarkRequestThroughput2Threads(b *testing.B) {
	benchmarkRequestThroughput(2, b)
}

func BenchmarkRequestThroughput4Threads(b *testing.B) {
	benchmarkRequestThroughput(4, b)
}

func BenchmarkRequestThroughput8Threads(b *testing.B) {
	benchmarkRequestThroughput(8, b)
}

func BenchmarkRequestThroughput16Threads(b *testing.B) {
	benchmarkRequestThroughput(16, b)
}

func BenchmarkRequestThroughput32Threads(b *testing.B) {
	benchmarkRequestThroughput(32, b)
}

func BenchmarkRequestThroughput64Threads(b *testing.B) {
	benchmarkRequestThroughput(64, b)
}

func BenchmarkRequestThroughput128Threads(b *testing.B) {
	benchmarkRequestThroughput(128, b)
}

func benchmarkRequestThroughput(threads int, b *testing.B) {
	// Create the service handler
	handler := new(reqrepTestHandler)

	// Register a new service to the relay
	serv, err := iris.Register(config.relay, config.cluster, handler)
	if err != nil {
		b.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Create the thread pool with the concurrent requests
	workers := pool.NewThreadPool(threads)
	for i := 0; i < b.N; i++ {
		workers.Schedule(func() {
			if _, err := handler.conn.Request(config.cluster, []byte{byte(i)}, 10*time.Second); err != nil {
				b.Fatalf("request failed: %v.", err)
			}
		})
	}
	// Reset timer and benchmark the message transfer
	b.ResetTimer()
	workers.Start()
	workers.Terminate(false)

	// Stop the timer (don't measure deferred cleanup)
	b.StopTimer()
}
