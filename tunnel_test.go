// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package iris

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Service handler for the tunnel tests.
type tunnelTestHandler struct {
	conn *Connection
}

func (t *tunnelTestHandler) Init(conn *Connection) error              { t.conn = conn; return nil }
func (t *tunnelTestHandler) HandleBroadcast(msg []byte)               { panic("not implemented") }
func (t *tunnelTestHandler) HandleRequest(req []byte) ([]byte, error) { panic("not implemented") }
func (t *tunnelTestHandler) HandleDrop(reason error)                  { panic("not implemented") }

func (t *tunnelTestHandler) HandleTunnel(tun *Tunnel) {
	defer tun.Close()

	for {
		msg, err := tun.Recv(0)
		switch {
		case err == ErrClosed:
			return
		case err == nil:
			if err := tun.Send(msg, 0); err != nil {
				panic(fmt.Sprintf("tunnel send failed: %v", err))
			}
		default:
			panic(fmt.Sprintf("tunnel receive failed: %v", err))
		}
	}
}

// Tests multiple concurrent client and service tunnels.
func TestTunnel(t *testing.T) {
	// Test specific configurations
	conf := struct {
		clients   int
		servers   int
		tunnels   int
		exchanges int
	}{7, 7, 7, 7}

	barrier := newBarrier(conf.clients + conf.servers)
	shutdown := new(sync.WaitGroup)

	// Start up the concurrent tunneling clients
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

			// Execute the tunnel construction, message exchange and verification
			id := fmt.Sprintf("client #%d", client)
			if err := tunnelBuildExchangeVerify(id, conn, conf.tunnels, conf.exchanges); err != nil {
				barrier.Exit(fmt.Errorf("exchanges failed: %v", err))
				return
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
			handler := new(tunnelTestHandler)

			// Register a new service to the relay
			serv, err := Register(config.relay, config.cluster, handler, nil)
			if err != nil {
				barrier.Exit(fmt.Errorf("registration failed: %v", err))
				return
			}
			defer serv.Unregister()
			barrier.Sync()

			// Execute the tunnel construction, message exchange and verification
			id := fmt.Sprintf("server #%d", server)
			if err := tunnelBuildExchangeVerify(id, handler.conn, conf.tunnels, conf.exchanges); err != nil {
				barrier.Exit(fmt.Errorf("exchanges failed: %v", err))
				return
			}
			barrier.Exit(nil)
		}(i)
	}
	// Schedule the parallel operations
	if errs := barrier.Wait(); len(errs) != 0 {
		t.Fatalf("startup phase failed: %v.", errs)
	}
	if errs := barrier.Wait(); len(errs) != 0 {
		t.Fatalf("tunneling phase failed: %v.", errs)
	}
	// Make sure all children terminated
	shutdown.Wait()
}

// Opens a batch of concurrent tunnels, and executes a data exchange.
func tunnelBuildExchangeVerify(id string, conn *Connection, tunnels, exchanges int) error {
	barrier := newBarrier(tunnels)
	for i := 0; i < tunnels; i++ {
		go func(tunnel int) {
			// Open a tunnel to the service cluster
			tun, err := conn.Tunnel(config.cluster, time.Second)
			if err != nil {
				barrier.Exit(fmt.Errorf("tunnel construction failed: %v", err))
				return
			}
			defer tun.Close()

			// Serialize a batch of messages
			for i := 0; i < exchanges; i++ {
				message := fmt.Sprintf("%s, tunnel #%d, message #%d", id, tunnel, i)
				if err := tun.Send([]byte(message), time.Second); err != nil {
					barrier.Exit(fmt.Errorf("tunnel send failed: %v", err))
					return
				}
			}
			// Read back the echo stream and verify
			for i := 0; i < exchanges; i++ {
				message, err := tun.Recv(time.Second)
				if err != nil {
					barrier.Exit(fmt.Errorf("tunnel receive failed: %v", err))
					return
				}
				original := fmt.Sprintf("%s, tunnel #%d, message #%d", id, tunnel, i)
				if string(message) != original {
					barrier.Exit(fmt.Errorf("message mismatch: have %v, want %v", string(message), original))
					return
				}
			}
			barrier.Exit(nil)
		}(i)
	}
	if errs := barrier.Wait(); len(errs) != 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

// Tests that unanswered tunnels timeout correctly.
func TestTunnelTimeout(t *testing.T) {
	// Connect to the local relay
	conn, err := Connect(config.relay)
	if err != nil {
		t.Fatalf("connection failed: %v", err)
	}
	defer conn.Close()

	// Open a new tunnel to a non existent server
	if tun, err := conn.Tunnel(config.cluster, 100*time.Millisecond); err != ErrTimeout {
		t.Fatalf("mismatching tunneling result: have %v/%v, want %v/%v", tun, err, nil, ErrTimeout)
	}
}

// Tests that large messages get delivered properly.
func TestTunnelChunking(t *testing.T) {
	// Create the service handler
	handler := new(tunnelTestHandler)

	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, nil)
	if err != nil {
		t.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Construct the tunnel
	tunnel, err := handler.conn.Tunnel(config.cluster, time.Second)
	if err != nil {
		t.Fatalf("tunnel construction failed: %v.", err)
	}
	defer tunnel.Close()

	// Create and transfer a huge message
	blob := make([]byte, 16*1024*1024)
	for i := 0; i < len(blob); i++ {
		blob[i] = byte(i)
	}
	if err := tunnel.Send(blob, 10*time.Second); err != nil {
		t.Fatalf("failed to send blob: %v.", err)
	}
	back, err := tunnel.Recv(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to retrieve blob: %v.", err)
	}
	// Verify that they indeed match
	if bytes.Compare(back, blob) != 0 {
		t.Fatalf("data blob mismatch")
	}
}

// Tests that a tunnel remains operational even after overloads (partially
// transferred huge messages timeouting).
func TestTunnelOverload(t *testing.T) {
	// Create the service handler
	handler := new(tunnelTestHandler)

	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, nil)
	if err != nil {
		t.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Construct the tunnel
	tunnel, err := handler.conn.Tunnel(config.cluster, time.Second)
	if err != nil {
		t.Fatalf("tunnel construction failed: %v.", err)
	}
	defer tunnel.Close()

	// Overload the tunnel by partially transferring huge messages
	blob := make([]byte, 64*1024*1024)
	for i := 0; i < 10; i++ {
		if err := tunnel.Send(blob, time.Millisecond); err != ErrTimeout {
			t.Fatalf("unexpected send result: have %v, want %v.", err, ErrTimeout)
		}
	}
	// Verify that the tunnel is still operational
	data := []byte{0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04}
	for i := 0; i < 10; i++ { // Iteration's important, the first will always cross (allowance ignore)
		if err := tunnel.Send(data, time.Second); err != nil {
			t.Fatalf("failed to send data: %v.", err)
		}
		back, err := tunnel.Recv(time.Second)
		if err != nil {
			t.Fatalf("failed to retrieve data: %v.", err)
		}
		// Verify that they indeed match
		if bytes.Compare(back, data) != 0 {
			t.Fatalf("data mismatch: have %v, want %v.", back, data)
		}
	}
}

// Benchmarks the latency of a single tunnel send (actually two way, so halves
// it).
func BenchmarkTunnelLatency(b *testing.B) {
	// Create the service handler
	handler := new(tunnelTestHandler)

	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, nil)
	if err != nil {
		b.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Construct the tunnel
	tunnel, err := handler.conn.Tunnel(config.cluster, time.Second)
	if err != nil {
		b.Fatalf("tunnel construction failed: %v.", err)
	}
	defer tunnel.Close()

	// Reset the timer and measure the latency
	b.ResetTimer()
	for i := 0; i < b.N/2; i++ {
		if err := tunnel.Send([]byte{byte(i)}, time.Second); err != nil {
			b.Fatalf("tunnel send failed: %v.", err)
		}
		if _, err := tunnel.Recv(time.Second); err != nil {
			b.Fatalf("tunnel receive failed: %v.", err)
		}
	}
	// Stop the timer (don't measure deferred cleanup)
	b.StopTimer()
}

// Measures the throughput of the tunnel data transfer (actually two ways, so
// halves it).
func BenchmarkTunnelThroughput(b *testing.B) {
	// Create the service handler
	handler := new(tunnelTestHandler)

	// Register a new service to the relay
	serv, err := Register(config.relay, config.cluster, handler, nil)
	if err != nil {
		b.Fatalf("registration failed: %v.", err)
	}
	defer serv.Unregister()

	// Construct the tunnel
	tunnel, err := handler.conn.Tunnel(config.cluster, time.Second)
	if err != nil {
		b.Fatalf("tunnel construction failed: %v.", err)
	}
	defer tunnel.Close()

	// Reset the timer and measure the throughput
	b.ResetTimer()
	go func() {
		for i := 0; i < b.N/2; i++ {
			if err := tunnel.Send([]byte{byte(i)}, time.Second); err != nil {
				b.Fatalf("tunnel send failed: %v.", err)
			}
		}
	}()
	for i := 0; i < b.N/2; i++ {
		if _, err := tunnel.Recv(time.Second); err != nil {
			b.Fatalf("tunnel receive failed: %v.", err)
		}
	}
	// Stop the timer (don't measure deferred cleanup)
	b.StopTimer()
}
