// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package iris

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-iris/iris/pool"
)

// Service handler for the request/reply tests.
type requestTestHandler struct {
	conn *Connection
}

func (r *requestTestHandler) Init(conn *Connection) error              { r.conn = conn; return nil }
func (r *requestTestHandler) HandleBroadcast(msg []byte)               { panic("not implemented") }
func (r *requestTestHandler) HandleRequest(req []byte) ([]byte, error) { return req, nil }
func (r *requestTestHandler) HandleTunnel(tun *Tunnel)                 { panic("not implemented") }
func (r *requestTestHandler) HandleDrop(reason error)                  { panic("not implemented") }

// Service handler for the request/reply failure tests.
type requestFailTestHandler struct {
	conn *Connection
}

func (r *requestFailTestHandler) Init(conn *Connection) error { r.conn = conn; return nil }
func (r *requestFailTestHandler) HandleBroadcast(msg []byte)  { panic("not implemented") }
func (r *requestFailTestHandler) HandleTunnel(tun *Tunnel)    { panic("not implemented") }
func (r *requestFailTestHandler) HandleDrop(reason error)     { panic("not implemented") }

func (r *requestFailTestHandler) HandleRequest(req []byte) ([]byte, error) {
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
	shutdown := new(sync.WaitGroup)

	// Start up the concurrent requesting clients
	for i := 0; i < conf.clients; i++ {
		shutdown.Add(1)
		go func(client int) {
			defer shutdown.Done()

			// Connect to the local relay
			conn, err := Connect(config.relay)
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
		shutdown.Add(1)
		go func(server int) {
			defer shutdown.Done()

			// Create the service handler
			handler := new(requestTestHandler)

			// Register a new service to the relay
			serv, err := Register(config.relay, config.cluster, handler, nil)
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
	// Make sure all children terminated
	shutdown.Wait()
}

// Tests request failure forwarding.
func TestRequestFail(t *testing.T) {
	// Test specific configurations
	conf := struct {
		requests int
	}{125}

	// Create the service handler
	handler := new(requestFailTestHandler)

	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, nil)
	if err != nil {
		t.Fatalf("registration failed: %v", err)
	}
	defer serv.Unregister()

	// Request from the failing service cluster
	for i := 0; i < conf.requests; i++ {
		request := fmt.Sprintf("failure %d", i)
		reply, err := handler.conn.Request(config.cluster, []byte(request), time.Second)
		if err == nil {
			t.Fatalf("request didn't fail: %v.", reply)
		} else if _, ok := err.(*RemoteError); !ok {
			t.Fatalf("request didn't fail remotely: %v.", err)
		} else if err.Error() != request {
			t.Fatalf("error message mismatch: have %v, want %v.", err, request)
		}
	}
}

// Service handler for the request/reply limit tests.
type requestTestTimedHandler struct {
	conn  *Connection
	sleep time.Duration
}

func (r *requestTestTimedHandler) Init(conn *Connection) error { r.conn = conn; return nil }
func (r *requestTestTimedHandler) HandleBroadcast(msg []byte)  { panic("not implemented") }
func (r *requestTestTimedHandler) HandleTunnel(tun *Tunnel)    { panic("not implemented") }
func (r *requestTestTimedHandler) HandleDrop(reason error)     { panic("not implemented") }

func (r *requestTestTimedHandler) HandleRequest(req []byte) ([]byte, error) {
	time.Sleep(r.sleep)
	return req, nil
}

// Tests the request timeouts.
func TestRequestTimeout(t *testing.T) {
	// Test specific configurations
	conf := struct {
		sleep time.Duration
	}{25 * time.Millisecond}

	// Create the service handler
	handler := &requestTestTimedHandler{
		sleep: conf.sleep,
	}
	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, nil)
	if err != nil {
		t.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Check that the timeouts are complied with.
	if _, err := handler.conn.Request(config.cluster, []byte{0x00}, conf.sleep*2); err != nil {
		t.Fatalf("longer timeout failed: %v.", err)
	}
	if rep, err := handler.conn.Request(config.cluster, []byte{0x00}, conf.sleep/2); err == nil {
		t.Fatalf("shorter timeout succeeded: %v.", rep)
	}
}

// Tests the request thread limitation.
func TestRequestThreadLimit(t *testing.T) {
	// Test specific configurations
	conf := struct {
		requests int
		sleep    time.Duration
	}{4, 25 * time.Millisecond}

	// Create the service handler and limiter
	handler := &requestTestTimedHandler{
		sleep: conf.sleep,
	}
	limits := &ServiceLimits{RequestThreads: 1}

	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, limits)
	if err != nil {
		t.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Start a batch of requesters
	done := make(chan struct{}, conf.requests)
	for i := 0; i < conf.requests; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			if _, err := handler.conn.Request(config.cluster, []byte{0x00}, time.Duration(conf.requests+1)*conf.sleep); err != nil {
				t.Fatalf("request failed: %v.", err)
			}
		}()
	}
	// Wait for half of them to complete and verify completion
	time.Sleep(time.Duration(conf.requests/2)*conf.sleep + conf.sleep/2)
	for i := 0; i < conf.requests/2; i++ {
		select {
		case <-done:
		default:
			t.Fatalf("request #%d not completed.", i)
		}
	}
	select {
	case <-done:
		t.Fatalf("extra request completed.")
	default:
	}
	// Wait for the rest to complete
	time.Sleep(time.Duration(conf.requests/2)*conf.sleep + conf.sleep/2)
	for i := conf.requests / 2; i < conf.requests; i++ {
		select {
		case <-done:
		default:
			t.Fatalf("request #%d not completed.", i)
		}
	}
}

// Tests the request memory limitation.
func TestRequestMemoryLimit(t *testing.T) {
	// Create the service handler and limiter
	handler := new(requestTestHandler)
	limits := &ServiceLimits{RequestMemory: 1}

	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, limits)
	if err != nil {
		t.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Check that a 1 byte request passes
	if _, err := handler.conn.Request(config.cluster, []byte{0x00}, 25*time.Millisecond); err != nil {
		t.Fatalf("small request failed: %v.", err)
	}
	// Check that a 2 byte request is dropped
	if rep, err := handler.conn.Request(config.cluster, []byte{0x00, 0x00}, 25*time.Millisecond); err != ErrTimeout {
		t.Fatalf("large request didn't time out: %v : %v.", rep, err)
	}
}

// Service handler for the request/reply expiry tests.
type requestTestExpiryHandler struct {
	conn  *Connection
	sleep time.Duration
	done  int32
}

func (r *requestTestExpiryHandler) Init(conn *Connection) error { r.conn = conn; return nil }
func (r *requestTestExpiryHandler) HandleBroadcast(msg []byte)  { panic("not implemented") }
func (r *requestTestExpiryHandler) HandleTunnel(tun *Tunnel)    { panic("not implemented") }
func (r *requestTestExpiryHandler) HandleDrop(reason error)     { panic("not implemented") }

func (r *requestTestExpiryHandler) HandleRequest(req []byte) ([]byte, error) {
	time.Sleep(r.sleep)
	atomic.AddInt32(&r.done, 1)
	return req, nil
}

// Tests that enqueued but expired requests don't get executed.
func TestRequestExpiration(t *testing.T) {
	// Test specific configurations
	conf := struct {
		requests int
		sleep    time.Duration
	}{4, 25 * time.Millisecond}

	// Create the service handler and limiter
	handler := &requestTestExpiryHandler{
		sleep: conf.sleep,
	}
	limits := &ServiceLimits{RequestThreads: 1}

	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, limits)
	if err != nil {
		t.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Start a batch of concurrent requesters (all but one should be scheduled remotely)
	var pend sync.WaitGroup
	for i := 0; i < conf.requests; i++ {
		pend.Add(1)
		go func() {
			defer pend.Done()
			handler.conn.Request(config.cluster, []byte{0x00}, time.Millisecond)
		}()
	}
	// Wait for all of them to complete and verify that all but 1 expired
	pend.Wait()
	time.Sleep(time.Duration(conf.requests+1) * conf.sleep)
	if done := atomic.LoadInt32(&handler.done); done != 1 {
		t.Fatalf("executed request count mismatch: have %v, want %v.", done, 1)
	}
}

// Benchmarks the latency of a single request/reply operation.
func BenchmarkRequestLatency(b *testing.B) {
	// Create the service handler
	handler := new(requestTestHandler)

	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, nil)
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
	handler := new(requestTestHandler)

	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, nil)
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
