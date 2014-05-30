// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package tests

/*
import (
	"fmt"
	"sync"
	"testing"
	"time"

	"gopkg.in/project-iris/iris-go.v0"
)

// Connection handler for the tunnel tests.
type tunneler struct {
}

func (t *tunneler) HandleBroadcast(msg []byte) {
	panic("Broadcast passed to tunnel handler")
}

func (t *tunneler) HandleRequest(req []byte) ([]byte, error) {
	panic("Request passed to tunnel handler")
}

func (t *tunneler) HandleTunnel(tun iris.Tunnel) {
	defer tun.Close()
	for done := false; !done; {
		if msg, err := tun.Recv(0); err == nil {
			if err := tun.Send(msg, 0); err != nil {
				panic(err)
			}
		} else {
			done = true
		}
	}
}

func (t *tunneler) HandleDrop(reason error) {
	panic("Connection dropped on tunnel handler")
}

// Opens a tunnel to itself and streams a batch of messages.
func TestTunnelSingle(t *testing.T) {
	// Configure the test
	messages := 1000

	// Connect to the Iris network
	cluster := "test-tunnel-single"
	conn, err := iris.Connect(relayPort, cluster, new(tunneler))
	if err != nil {
		t.Fatalf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Open a tunnel to self
	tun, err := conn.Tunnel(cluster, 250*time.Millisecond)
	if err != nil {
		t.Fatalf("tunneling failed: %v.", err)
	}
	defer tun.Close()

	// Serialize a load of messages
	for i := 0; i < messages; i++ {
		if err := tun.Send([]byte(fmt.Sprintf("%d", i)), 250*time.Millisecond); err != nil {
			t.Fatalf("send failed: %v.", err)
		}
	}
	// Read back the echo stream and verify
	for i := 0; i < messages; i++ {
		if msg, err := tun.Recv(250 * time.Millisecond); err != nil {
			t.Fatalf("receive failed: %v.", err)
		} else {
			if res := fmt.Sprintf("%d", i); res != string(msg) {
				t.Fatalf("message mismatch: have %v, want %v.", msg, string(res))
			}
		}
	}
}

// Starts a batch of servers, each sending and echoing a stream of messages.
func TestTunnelMulti(t *testing.T) {
	// Configure the test
	servers := 75
	messages := 50

	start := new(sync.WaitGroup)
	proc := new(sync.WaitGroup)
	proc.Add(1)
	done := new(sync.WaitGroup)
	term := new(sync.WaitGroup)
	term.Add(1)
	kill := new(sync.WaitGroup)

	// Start up the concurrent tunnelers
	errs := make(chan error, servers)
	for i := 0; i < servers; i++ {
		start.Add(1)
		done.Add(1)
		kill.Add(1)
		go func() {
			defer kill.Done()

			// Connect to the relay
			cluster := "test-tunnel-multi"
			conn, err := iris.Connect(relayPort, cluster, new(tunneler))
			if err != nil {
				errs <- fmt.Errorf("connection failed: %v", err)
				start.Done()
				return
			}
			defer conn.Close()

			// Notify parent and wait for continuation permission
			start.Done()
			proc.Wait()

			// Open a tunnel to the group
			tun, err := conn.Tunnel(cluster, 5*time.Second)
			if err != nil {
				errs <- fmt.Errorf("tunneling failed: %v", err)
				done.Done()
				return
			}
			// Serialize a load of messages
			for i := 0; i < messages; i++ {
				if err := tun.Send([]byte(fmt.Sprintf("%d", i)), 5*time.Second); err != nil {
					errs <- fmt.Errorf("send failed: %v", err)
					done.Done()
					return
				}
			}
			// Read back the echo stream and verify
			for i := 0; i < messages; i++ {
				if msg, err := tun.Recv(5 * time.Second); err != nil {
					errs <- fmt.Errorf("receive failed: %v", err)
					done.Done()
					return
				} else {
					if res := fmt.Sprintf("%d", i); res != string(msg) {
						errs <- fmt.Errorf("message mismatch: have %v, want %v", msg, string(res))
						done.Done()
						return
					}
				}
			}
			// Close up the tunnel
			tun.Close()

			// Wait till everybody else finishes
			done.Done()
			term.Wait()
		}()
	}
	// Wait for all go-routines to attach and verify
	start.Wait()
	select {
	case err := <-errs:
		t.Fatalf("startup failed: %v.", err)
	default:
	}
	// Permit the go-routines to continue
	proc.Done()
	done.Wait()
	select {
	case err := <-errs:
		t.Fatalf("requesting failed: %v.", err)
	default:
	}
	// Sync up the terminations
	term.Done()
	kill.Wait()
}

func BenchmarkTunnelTransferLatency(b *testing.B) {
	// Set up the connection
	cluster := "bench-tunnel-latency"
	conn, err := iris.Connect(relayPort, cluster, new(tunneler))
	if err != nil {
		b.Fatalf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Create the tunnel
	tun, err := conn.Tunnel(cluster, 250*time.Millisecond)
	if err != nil {
		b.Fatalf("tunneling failed: %v.", err)
	}
	// Reset the timer and measure the transfers
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tun.Send([]byte{0x00}, 100*time.Millisecond); err != nil {
			b.Fatalf("recv failed: %v.", err)
		}
		if _, err := tun.Recv(100 * time.Millisecond); err != nil {
			b.Fatalf("recv failed: %v.", err)
		}
	}
	b.StopTimer()
}

func BenchmarkTunnelTransferThroughput(b *testing.B) {
	// Set up the connection
	cluster := "bench-tunnel-throughput"
	conn, err := iris.Connect(relayPort, cluster, new(tunneler))
	if err != nil {
		b.Fatalf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Create the tunnel
	tun, err := conn.Tunnel(cluster, 250*time.Millisecond)
	if err != nil {
		b.Fatalf("tunneling failed: %v.", err)
	}
	// Reset the timer and measure the transfers
	b.ResetTimer()
	go func() {
		for i := 0; i < b.N; i++ {
			if err := tun.Send([]byte{byte(i)}, 1000*time.Millisecond); err != nil {
				b.Fatalf("send failed: %v.", err)
			}
		}
	}()
	for i := 0; i < b.N; i++ {
		if _, err := tun.Recv(1000 * time.Millisecond); err != nil {
			b.Fatalf("recv failed: %v.", err)
		}
	}
	b.StopTimer()
}
*/
