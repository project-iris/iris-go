// Iris Go Binding
// Copyright 2013 Peter Szilagyi. All rights reserved.
//
// The current language binding is an official support library of the Iris
// decentralized messaging framework, and as such, the same licensing terms
// hold. For details please see http://github.com/karalabe/iris/LICENSE.md
//
// Author: peterke@gmail.com (Peter Szilagyi)

package tests

import (
	"bytes"
	"crypto/rand"
	"github.com/karalabe/iris-go"
	"github.com/karalabe/iris/pool"
	"io"
	"sync"
	"testing"
	"time"
)

// Connection handler for the req/rep tests.
type requester struct {
}

func (r *requester) HandleBroadcast(msg []byte) {
	panic("Broadcast passed to request handler")
}

func (r *requester) HandleRequest(req []byte) []byte {
	return req
}

func (r *requester) HandleTunnel(tun iris.Tunnel) {
	panic("Inbound tunnel on request handler")
}

func (r *requester) HandleDrop(reason error) {
	panic("Connection dropped on request handler")
}

// Sends a few requests to one-self, waiting for the echo.
func TestReqRepSingle(t *testing.T) {
	// Configure the test
	requests := 1000

	// Connect to the Iris network
	app := "test-reqrep-single"
	conn, err := iris.Connect(relayPort, app, new(requester))
	if err != nil {
		t.Fatalf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Send a handful of requests, verifying the replies
	for i := 0; i < requests; i++ {
		// Generate a new random message
		req := make([]byte, 128)
		io.ReadFull(rand.Reader, req)

		// Send request, verify reply
		rep, err := conn.Request(app, req, 250*time.Millisecond)
		if err != nil {
			t.Fatalf("request failed: %v.", err)
		}
		if bytes.Compare(rep, req) != 0 {
			t.Fatalf("reply mismatch: have %v, want %v.", rep, req)
		}
	}
}

// Starts a handful of concurrent servers which send requests to each other.
func TestReqRepMulti(t *testing.T) {
	// Configure the test
	servers := 75
	requests := 75

	start := new(sync.WaitGroup)
	proc := new(sync.WaitGroup)
	proc.Add(1)
	done := new(sync.WaitGroup)
	term := new(sync.WaitGroup)
	term.Add(1)
	kill := new(sync.WaitGroup)

	// Start up the concurrent requesters
	for i := 0; i < servers; i++ {
		start.Add(1)
		done.Add(1)
		kill.Add(1)
		go func() {
			// Connect to the relay
			app := "test-reqrep-multi"
			conn, err := iris.Connect(relayPort, app, new(requester))
			if err != nil {
				t.Fatalf("connection failed: %v.", err)
			}
			// Nofity parent and wait for continuation permission
			start.Done()
			proc.Wait()

			// Send the requests to the group and wait for the replies
			for j := 0; j < requests; j++ {
				// Generate a new random message
				req := make([]byte, 128)
				io.ReadFull(rand.Reader, req)

				// Send request, verify reply
				rep, err := conn.Request(app, req, 250*time.Millisecond)
				if err != nil {
					t.Fatalf("request failed: %v.", err)
				}
				if bytes.Compare(rep, req) != 0 {
					t.Fatalf("reply mismatch: have %v, want %v.", rep, req)
				}
			}
			// Wait till everybody else finishes
			done.Done()
			term.Wait()

			// Terminate the server and signal tester
			conn.Close()
			kill.Done()
		}()
	}
	// Schedule the parallel operations
	start.Wait()
	proc.Done()
	done.Wait()
	term.Done()
	kill.Wait()
}

// Benchmarks the passthrough of a single request-reply.
func BenchmarkReqRepLatency(b *testing.B) {
	// Set up the connection
	app := "bench-reqrep-latency"
	conn, err := iris.Connect(relayPort, app, new(requester))
	if err != nil {
		b.Fatalf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Reset timer and benchmark the message transfer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Request(app, []byte{byte(i)}, 10*time.Second); err != nil {
			b.Fatalf("request failed: %v.", err)
		}
	}
}

// Benchmarks parallel request-reply.
func BenchmarkReqRepThroughput2Threads(b *testing.B) {
	benchmarkReqRepThroughput(2, b)
}

func BenchmarkReqRepThroughput4Threads(b *testing.B) {
	benchmarkReqRepThroughput(4, b)
}

func BenchmarkReqRepThroughput8Threads(b *testing.B) {
	benchmarkReqRepThroughput(8, b)
}

func BenchmarkReqRepThroughput16Threads(b *testing.B) {
	benchmarkReqRepThroughput(16, b)
}

func BenchmarkReqRepThroughput32Threads(b *testing.B) {
	benchmarkReqRepThroughput(32, b)
}

func BenchmarkReqRepThroughput64Threads(b *testing.B) {
	benchmarkReqRepThroughput(64, b)
}

func BenchmarkReqRepThroughput128Threads(b *testing.B) {
	benchmarkReqRepThroughput(128, b)
}

func benchmarkReqRepThroughput(threads int, b *testing.B) {
	// Set up the connection
	app := "bench-reqrep-throughput"
	conn, err := iris.Connect(relayPort, app, new(requester))
	if err != nil {
		b.Fatalf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Create the thread pool with the concurrent requests
	workers := pool.NewThreadPool(threads)
	done := make(chan struct{}, b.N)
	for i := 0; i < b.N; i++ {
		workers.Schedule(func() {
			defer func() { done <- struct{}{} }()
			if _, err := conn.Request(app, []byte{byte(i)}, 60*time.Second); err != nil {
				b.Fatalf("request failed: %v.", err)
			}
		})
	}
	// Reset timer and benchmark the message transfer
	b.ResetTimer()
	workers.Start()
	for i := 0; i < b.N; i++ {
		<-done
	}
}
