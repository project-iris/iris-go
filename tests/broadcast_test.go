// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package tests

/*
import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/project-iris/iris/pool"
	"gopkg.in/project-iris/iris-go.v0"
)

// Connection handler for the broadcast tests.
type broadcaster struct {
	msgs chan []byte
}

func (b *broadcaster) HandleBroadcast(msg []byte) {
	b.msgs <- msg
}

func (b *broadcaster) HandleRequest(req []byte) ([]byte, error) {
	panic("Request passed to broadcast handler")
}

func (b *broadcaster) HandleTunnel(tun iris.Tunnel) {
	panic("Inbound tunnel on broadcast handler")
}

func (b *broadcaster) HandleDrop(reason error) {
	panic("Connection dropped on broadcast handler")
}

// Broadcasts to one-self a handful of messages.
func TestBroadcastSingle(t *testing.T) {
	// Create the channel to receive the broadcasts
	count := 1000
	input := make(chan []byte, count)

	// Connect to the Iris network
	cluster := "test-broadcast-single"
	conn, err := iris.Connect(relayPort, cluster, &broadcaster{msgs: input})
	if err != nil {
		t.Fatalf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Broadcast a handful of messages to oneself
	messages := make(map[string]struct{})
	for i := 0; i < count; i++ {
		// Generate a new random message and store it
		msg := make([]byte, 128)
		io.ReadFull(rand.Reader, msg)
		messages[string(msg)] = struct{}{}

		// Broadcast the message
		if err := conn.Broadcast(cluster, msg); err != nil {
			t.Fatalf("broadcast failed: %v.", err)
		}
	}
	// Retrieve and verify all broadcasts
	for i := 0; i < count; i++ {
		select {
		case msg := <-input:
			if _, ok := messages[string(msg)]; !ok {
				t.Fatalf("invalid message: %v.", msg)
			}
			delete(messages, string(msg))
		case <-time.After(5 * time.Second):
			// Make sure we don't block till eternity
			t.Fatalf("broadcast receive timeout")
		}
	}
}

// Starts a number of concurrent processes, each broadcasting to the whole pool.
func TestBroadcastMulti(t *testing.T) {
	// Configure the test
	servers := 100
	broadcasts := 25

	start := new(sync.WaitGroup)
	term := new(sync.WaitGroup)
	proc := new(sync.WaitGroup)
	proc.Add(1)

	// Start up the concurrent broadcasters
	errs := make(chan error, servers)
	for i := 0; i < servers; i++ {
		start.Add(1)
		term.Add(1)
		go func() {
			defer term.Done()

			// Connect to the relay
			cluster := "test-broadcast-multi"
			input := make(chan []byte, servers*broadcasts)
			conn, err := iris.Connect(relayPort, cluster, &broadcaster{msgs: input})
			if err != nil {
				errs <- fmt.Errorf("connection failed: %v", err)
				start.Done()
				return
			}
			defer conn.Close()

			// Notify parent and wait for continuation permission
			start.Done()
			proc.Wait()

			// Broadcast the whole group
			for j := 0; j < broadcasts; j++ {
				if err := conn.Broadcast(cluster, []byte("BROADCAST")); err != nil {
					errs <- fmt.Errorf("broadcast failed: %v", err)
					return
				}
			}
			// Retrieve and verify all broadcasts
			for j := 0; j < servers*broadcasts; j++ {
				select {
				case msg := <-input:
					if bytes.Compare(msg, []byte("BROADCAST")) != 0 {
						errs <- fmt.Errorf("broadcast message mismatch: have %v, want %v", msg, []byte("BROADCAST"))
						return
					}
				case <-time.After(5 * time.Second):
					errs <- fmt.Errorf("broadcast timed out")
					return
				}
			}
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
	term.Wait()
	select {
	case err := <-errs:
		t.Fatalf("broadcasting failed: %v.", err)
	default:
	}
}

// Benchmarks broadcasting a single message
func BenchmarkBroadcastLatency(b *testing.B) {
	// Configure the benchmark
	cluster := "bench-broadcast-latency"
	handler := &broadcaster{
		msgs: make(chan []byte, b.N),
	}
	// Set up the connection
	conn, err := iris.Connect(relayPort, cluster, handler)
	if err != nil {
		b.Fatalf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Reset timer and benchmark the message transfer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Broadcast(cluster, []byte{byte(i)})
		<-handler.msgs
	}
}

// Benchmarks broadcasting a stream of messages
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
	// Configure the benchmark
	cluster := "bench-broadcast-throughput"
	handler := &broadcaster{
		msgs: make(chan []byte, b.N),
	}
	// Set up the connection
	conn, err := iris.Connect(relayPort, cluster, handler)
	if err != nil {
		b.Fatalf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Create the thread pool with the concurrent broadcasts
	workers := pool.NewThreadPool(threads)
	for i := 0; i < b.N; i++ {
		workers.Schedule(func() {
			if err := conn.Broadcast(cluster, []byte{byte(i)}); err != nil {
				b.Fatalf("broadcast failed: %v.", err)
			}
		})
	}
	// Reset timer and benchmark the message transfer
	b.ResetTimer()
	workers.Start()
	for i := 0; i < b.N; i++ {
		<-handler.msgs
	}
	b.StopTimer()
	workers.Terminate(true)
}
*/
