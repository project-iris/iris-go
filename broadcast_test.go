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
	"testing"
	"time"

	"github.com/project-iris/iris/pool"
)

// Service handler for the broadcast tests.
type broadcastTestHandler struct {
	conn     *Connection
	delivers chan []byte
}

func (b *broadcastTestHandler) Init(conn *Connection) error              { b.conn = conn; return nil }
func (b *broadcastTestHandler) HandleBroadcast(msg []byte)               { b.delivers <- msg }
func (b *broadcastTestHandler) HandleRequest(req []byte) ([]byte, error) { panic("not implemented") }
func (b *broadcastTestHandler) HandleTunnel(tun *Tunnel)                 { panic("not implemented") }
func (b *broadcastTestHandler) HandleDrop(reason error)                  { panic("not implemented") }

// Tests multiple concurrent client and service broadcasts.
func TestBroadcast(t *testing.T) {
	// Test specific configurations
	conf := struct {
		clients  int
		servers  int
		messages int
	}{25, 25, 25}

	barrier := newBarrier(conf.clients + conf.servers)
	shutdown := new(sync.WaitGroup)

	// Start up the concurrent broadcasting clients
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

			// Broadcast to the whole service cluster
			for i := 0; i < conf.messages; i++ {
				message := fmt.Sprintf("client #%d, broadcast %d", client, i)
				if err := conn.Broadcast(config.cluster, []byte(message)); err != nil {
					barrier.Exit(fmt.Errorf("client broadcast failed: %v", err))
					return
				}
			}
			barrier.Exit(nil)
		}(i)
	}
	// Start up the concurrent broadcast services
	for i := 0; i < conf.servers; i++ {
		shutdown.Add(1)
		go func(server int) {
			defer shutdown.Done()

			// Create the service handler
			handler := &broadcastTestHandler{
				delivers: make(chan []byte, (conf.clients+conf.servers)*conf.messages),
			}
			// Register a new service to the relay
			serv, err := Register(config.relay, config.cluster, handler, nil)
			if err != nil {
				barrier.Exit(fmt.Errorf("registration failed: %v", err))
				return
			}
			defer serv.Unregister()
			barrier.Sync()

			// Broadcast to the whole service cluster
			for i := 0; i < conf.messages; i++ {
				message := fmt.Sprintf("server #%d, broadcast %d", server, i)
				if err := handler.conn.Broadcast(config.cluster, []byte(message)); err != nil {
					barrier.Exit(fmt.Errorf("server broadcast failed: %v", err))
					return
				}
			}
			barrier.Sync()

			// Retrieve all the arrived broadcasts
			messages := make(map[string]struct{})
			for i := 0; i < (conf.clients+conf.servers)*conf.messages; i++ {
				select {
				case msg := <-handler.delivers:
					messages[string(msg)] = struct{}{}
				case <-time.After(time.Second):
					barrier.Exit(errors.New("broadcast receive timeout"))
					return
				}
			}
			// Verify all the individual broadcasts
			for i := 0; i < conf.clients; i++ {
				for j := 0; j < conf.messages; j++ {
					msg := fmt.Sprintf("client #%d, broadcast %d", i, j)
					if _, ok := messages[msg]; !ok {
						barrier.Exit(fmt.Errorf("broadcast not found: %s", msg))
						return
					}
					delete(messages, msg)
				}
			}
			for i := 0; i < conf.servers; i++ {
				for j := 0; j < conf.messages; j++ {
					msg := fmt.Sprintf("server #%d, broadcast %d", i, j)
					if _, ok := messages[msg]; !ok {
						barrier.Exit(fmt.Errorf("broadcast not found: %s", msg))
						return
					}
					delete(messages, msg)
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
		t.Fatalf("broadcasting phase failed: %v.", errs)
	}
	if errs := barrier.Wait(); len(errs) != 0 {
		t.Fatalf("verification phase failed: %v.", errs)
	}
	// Make sure all children terminated
	shutdown.Wait()
}

// Service handler for the broadcast limit tests.
type broadcastLimitTestHandler struct {
	conn     *Connection
	delivers chan []byte
	sleep    time.Duration
}

func (b *broadcastLimitTestHandler) Init(conn *Connection) error          { b.conn = conn; return nil }
func (b *broadcastLimitTestHandler) HandleRequest([]byte) ([]byte, error) { panic("not implemented") }
func (b *broadcastLimitTestHandler) HandleTunnel(tun *Tunnel)             { panic("not implemented") }
func (b *broadcastLimitTestHandler) HandleDrop(reason error)              { panic("not implemented") }

func (b *broadcastLimitTestHandler) HandleBroadcast(msg []byte) {
	time.Sleep(b.sleep)
	b.delivers <- msg
}

// Tests the broadcast thread limitation.
func TestBroadcastThreadLimit(t *testing.T) {
	// Test specific configurations
	conf := struct {
		messages int
		sleep    time.Duration
	}{4, 100 * time.Millisecond}

	// Create the service handler and limiter
	handler := &broadcastLimitTestHandler{
		delivers: make(chan []byte, conf.messages),
		sleep:    conf.sleep,
	}
	limits := &ServiceLimits{BroadcastThreads: 1}

	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, limits)
	if err != nil {
		t.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Send a few broadcasts
	for i := 0; i < conf.messages; i++ {
		if err := handler.conn.Broadcast(config.cluster, []byte{byte(i)}); err != nil {
			t.Fatalf("broadcast failed: %v.", err)
		}
	}
	// Wait for half time and verify that only half was processed
	time.Sleep(time.Duration(conf.messages/2)*conf.sleep + conf.sleep/2)
	for i := 0; i < conf.messages/2; i++ {
		select {
		case <-handler.delivers:
		default:
			t.Errorf("broadcast #%d not received", i)
		}
	}
	select {
	case <-handler.delivers:
		t.Errorf("additional broadcast received")
	default:
	}
}

// Tests the broadcast memory limitation.
func TestBroadcastMemoryLimit(t *testing.T) {
	// Create the service handler and limiter
	handler := &broadcastTestHandler{
		delivers: make(chan []byte, 1),
	}
	limits := &ServiceLimits{BroadcastMemory: 1}

	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, limits)
	if err != nil {
		t.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Check that a 1 byte broadcast passes
	if err := handler.conn.Broadcast(config.cluster, []byte{0x00}); err != nil {
		t.Fatalf("small broadcast failed: %v.", err)
	}
	time.Sleep(time.Millisecond)
	select {
	case <-handler.delivers:
	default:
		t.Fatalf("small broadcast not received.")
	}
	// Check that a 2 byte broadcast is dropped
	if err := handler.conn.Broadcast(config.cluster, []byte{0x00, 0x00}); err != nil {
		t.Fatalf("large broadcast failed: %v.", err)
	}
	time.Sleep(time.Millisecond)
	select {
	case <-handler.delivers:
		t.Fatalf("large broadcast received.")
	default:
	}
	// Check that space freed gets replenished
	if err := handler.conn.Broadcast(config.cluster, []byte{0x00}); err != nil {
		t.Fatalf("second small broadcast failed: %v.", err)
	}
	time.Sleep(time.Millisecond)
	select {
	case <-handler.delivers:
	default:
		t.Fatalf("second small broadcast not received.")
	}
}

// Benchmarks broadcasting a single message.
func BenchmarkBroadcastLatency(b *testing.B) {
	// Create the service handler
	handler := &broadcastTestHandler{
		delivers: make(chan []byte, b.N),
	}
	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, nil)
	if err != nil {
		b.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Reset timer and benchmark the message transfer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.conn.Broadcast(config.cluster, []byte{byte(i)})
		<-handler.delivers
	}
	// Stop the timer (don't measure deferred cleanup)
	b.StopTimer()
}

// Benchmarks broadcasting a stream of messages.
func BenchmarkBroadcastThroughput1Threads(b *testing.B) {
	benchmarkBroadcastThroughput(1, b)
}

func BenchmarkBroadcastThroughput2Threads(b *testing.B) {
	benchmarkBroadcastThroughput(2, b)
}

func BenchmarkBroadcastThroughput4Threads(b *testing.B) {
	benchmarkBroadcastThroughput(4, b)
}

func BenchmarkBroadcastThroughput8Threads(b *testing.B) {
	benchmarkBroadcastThroughput(8, b)
}

func BenchmarkBroadcastThroughput16Threads(b *testing.B) {
	benchmarkBroadcastThroughput(16, b)
}

func BenchmarkBroadcastThroughput32Threads(b *testing.B) {
	benchmarkBroadcastThroughput(32, b)
}

func BenchmarkBroadcastThroughput64Threads(b *testing.B) {
	benchmarkBroadcastThroughput(64, b)
}

func BenchmarkBroadcastThroughput128Threads(b *testing.B) {
	benchmarkBroadcastThroughput(128, b)
}

func benchmarkBroadcastThroughput(threads int, b *testing.B) {
	// Create the service handler
	handler := &broadcastTestHandler{
		delivers: make(chan []byte, b.N),
	}
	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, nil)
	if err != nil {
		b.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Create the thread pool with the concurrent broadcasts
	workers := pool.NewThreadPool(threads)
	for i := 0; i < b.N; i++ {
		workers.Schedule(func() {
			if err := handler.conn.Broadcast(config.cluster, []byte{byte(i)}); err != nil {
				b.Fatalf("broadcast failed: %v.", err)
			}
		})
	}
	// Reset timer and benchmark the message transfer
	b.ResetTimer()
	workers.Start()
	for i := 0; i < b.N; i++ {
		<-handler.delivers
	}
	workers.Terminate(false)

	// Stop the timer (don't measure deferred cleanup)
	b.StopTimer()
}
