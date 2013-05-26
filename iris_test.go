// Iris Go Binding
// Copyright 2013 Peter Szilagyi. All rights reserved.
//
// The current language binding is an official support library of the Iris
// decentralized messaging framework, and as such, the same licensing terms
// hold. For details please see http://github.com/karalabe/iris/LICENSE.md
//
// Author: peterke@gmail.com (Peter Szilagyi)

// Note, all tests in this file assume a running Iris node on a fixed port.
// Also note that the benchmarks are solely for the relay protocol testing and
// haven't got much to do with reality.

package iris

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

// Local Iris node's listener port
var relayPort = 55555

// Tests connection setup and teardown.
func TestBasics(t *testing.T) {
	relays := []Connection{}
	for i := 0; i < 100; i++ {
		app := fmt.Sprintf("test-basics-%d", i)
		if conn, err := Connect(relayPort, app, nil); err != nil {
			t.Errorf("test %d: connection failed: %v.", i, err)
		} else {
			relays = append(relays, conn)
		}
	}
	for i, conn := range relays {
		if err := conn.Close(); err != nil {
			t.Errorf("test %d: teardown failed: %v.", i, err)
		}
	}
}

// Connection handler for the broadcast tests.
type broadcaster struct {
	msgs chan []byte
}

func (b *broadcaster) HandleBroadcast(msg []byte) {
	b.msgs <- msg
}

func (b *broadcaster) HandleRequest(req []byte) []byte {
	panic("Request passed to broadcast handler")
}

func (b *broadcaster) HandleTunnel(tun Tunnel) {
	panic("Inbound tunnel on broadcast handler")
}

func (b *broadcaster) HandleDrop(reason error) {
	panic("Connection dropped on broadcast handler")
}

// Tests broadcasting and correct connection.
func TestBroadcast(t *testing.T) {
	for i := 0; i < 100; i++ {
		handler := &broadcaster{
			msgs: make(chan []byte, 64),
		}
		// Set up the connection
		app := fmt.Sprintf("test-broadcast-%d", i)
		conn, err := Connect(relayPort, app, handler)
		if err != nil {
			t.Errorf("test %d: connection failed: %v.", i, err)
		}
		defer conn.Close()

		// Try a few self broadcasts
		for rep := 0; rep < 10; rep++ {
			out := []byte{byte(i + rep), byte(i + rep + 1), byte(i + rep + 2)}
			if err := conn.Broadcast(app, out); err != nil {
				t.Errorf("test %d: failed to broadcast: %v.", i, err)
			} else {
				select {
				case msg := <-handler.msgs:
					if len(msg) != len(out) {
						t.Errorf("test %d, rep %d: message size mismatch: have %v, want %v.", i, rep, len(msg), len(out))
					} else if bytes.Compare(msg, out) != 0 {
						t.Errorf("test %d, rep %d: message mismatch: have %v, want %v.", i, rep, msg, out)
					}
				case <-time.After(25 * time.Millisecond):
					t.Errorf("test %d, rep %d: broadcast timed out", i, rep)
				}
			}
		}
		// Tear down the connection
		conn.Close()
	}
}

// Connection handler for the req/rep tests.
type requester struct {
	sleep int
}

func (r *requester) HandleBroadcast(msg []byte) {
	panic("Broadcast passed to request handler")
}

func (r *requester) HandleRequest(req []byte) []byte {
	time.Sleep(time.Duration(r.sleep) * time.Millisecond)
	return req
}

func (r *requester) HandleTunnel(tun Tunnel) {
	panic("Inbound tunnel on request handler")
}

func (r *requester) HandleDrop(reason error) {
	panic("Connection dropped on request handler")
}

// Tests the request-reply scheme.
func TestReqRep(t *testing.T) {
	for i := 0; i < 100; i++ {
		handler := &requester{
			sleep: 50,
		}
		// Set up the connection
		app := fmt.Sprintf("test-reqrep-%d", i)
		conn, err := Connect(relayPort, app, handler)
		if err != nil {
			t.Errorf("test %d: connection failed: %v.", i, err)
		}
		defer conn.Close()

		// Verify concurrent requests
		done := make(chan struct{}, 25)
		for rep := 0; rep < cap(done); rep++ {
			go func() {
				req := []byte(fmt.Sprintf("request-%d-%d", i, rep))
				res, err := conn.Request(app, req, 250)
				if err != nil {
					t.Errorf("test %d, rep %d: request failed: %v.", i, rep, err)
				}
				if bytes.Compare(req, res) != 0 {
					t.Errorf("test %d, rep %d: reply mismatch: have %v, want %v.", i, rep, res, req)
				}
				done <- struct{}{}
			}()
		}
		for rep := 0; rep < cap(done); rep++ {
			<-done
		}
		// Verify timeouts
		req := []byte(fmt.Sprintf("request-%d-timeout", i))
		rep, err := conn.Request(app, req, 25)
		if err == nil {
			t.Errorf("test %d: timeout expected, nil error received, reply: %v.", i, string(rep))
		} else if !err.(Error).Timeout() {
			t.Errorf("test %d: error mismatch, have %v, want timeout.", i, err)
		}
		// Tear down the connection
		conn.Close()
	}
}

// Connection handler for the pub/sub tests.
type subscriber struct {
	msgs chan []byte
}

func (s *subscriber) HandleEvent(msg []byte) {
	s.msgs <- msg
}

// Tests the publish subscribe scheme.
func TestPubSub(t *testing.T) {
	for i := 0; i < 10; i++ {
		// Set up the connection
		app := fmt.Sprintf("test-pubsub-%d", i)
		conn, err := Connect(relayPort, app, nil)
		if err != nil {
			t.Errorf("test %d: connection failed: %v.", i, err)
		}
		// Repeat for a handfull of subscriptions
		for sub := 0; sub < 10; sub++ {
			// Subscribe
			topic := fmt.Sprintf("test-topic-%d", sub)
			handler := &subscriber{
				msgs: make(chan []byte, 64),
			}
			if err := conn.Subscribe(topic, handler); err != nil {
				t.Errorf("test %d, sub %d: failed to subscribe: %v", i, sub, err)
			}
			// Publish
			for pub := 0; pub < 10; pub++ {
				out := []byte{byte(i), byte(sub), byte(pub)}
				if err := conn.Publish(topic, out); err != nil {
					t.Errorf("test %d, sub %d, pub %d: failed to publish: %v.", i, sub, pub, err)
				} else {
					select {
					case msg := <-handler.msgs:
						if len(msg) != len(out) {
							t.Errorf("test %d, sub %d, pub %d: message size mismatch: have %v, want %v.", i, sub, pub, len(msg), len(out))
						} else if bytes.Compare(msg, out) != 0 {
							t.Errorf("test %d, sub %d, pub %d: message mismatch: have %v, want %v.", i, sub, pub, msg, out)
						}
					case <-time.After(50 * time.Millisecond):
						t.Errorf("test %d, sub %d, pub %d: publish timed out", i, sub, pub)
					}
				}
			}
			// Unsubscribe
			if err := conn.Unsubscribe(topic); err != nil {
				t.Errorf("test %d, sub %d: failed to unsubscribe: %v", i, sub, err)
			}
			// Make sure publish doesn't pass
			out := []byte{byte(i), byte(sub)}
			if err := conn.Publish(topic, out); err != nil {
				t.Errorf("test %d, sub %d: failed to post-publish: %v.", i, sub, err)
			} else {
				select {
				case msg := <-handler.msgs:
					t.Errorf("test %d, sub %d: message arrived after unsubscribe: %v.", i, sub, msg)
				case <-time.After(50 * time.Millisecond):
					// Ok, publish didn't arrive
				}
			}
		}
		// Tear down the connection
		conn.Close()
	}
}

// Benchmarks connection setup
func BenchmarkConnect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		app := fmt.Sprintf("bench-connect-%d", i)
		if conn, err := Connect(relayPort, app, nil); err != nil {
			b.Errorf("iteration %d: connection failed: %v.", i, err)
		} else {
			defer conn.Close()
		}
	}
	// Stop the timer and clean up
	b.StopTimer()
}

// Benchmarks connection teardown
func BenchmarkClose(b *testing.B) {
	for i := 0; i < b.N; i++ {
		app := fmt.Sprintf("bench-close-%d", i)
		if conn, err := Connect(relayPort, app, nil); err != nil {
			b.Errorf("iteration %d: connection failed: %v.", i, err)
		} else {
			defer conn.Close()
		}
	}
	// Reset the timer and execute deferred closes
	b.ResetTimer()
}

// Benchmarks broadcasting a single message
func BenchmarkBroadcast(b *testing.B) {
	// Configure the benchmark
	app := fmt.Sprintf("bench-broadcast")
	handler := &broadcaster{
		msgs: make(chan []byte, 1024),
	}
	// Set up the connection
	conn, err := Connect(relayPort, app, handler)
	if err != nil {
		b.Errorf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Reset timer and benchmark the message transfer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Broadcast(app, []byte{byte(i)})
		<-handler.msgs
	}
}

// Benchmarks broadcasting a stream of messages
func BenchmarkBroadcastThroughput(b *testing.B) {
	// Configure the benchmark
	app := fmt.Sprintf("bench-broadcast")
	handler := &broadcaster{
		msgs: make(chan []byte, 1024),
	}
	// Set up the connection
	conn, err := Connect(relayPort, app, handler)
	if err != nil {
		b.Errorf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Reset timer and benchmark the message transfer
	b.ResetTimer()
	go func() {
		for i := 0; i < b.N; i++ {
			if err := conn.Broadcast(app, []byte{byte(i)}); err != nil {
				fmt.Printf("broadcast failed: %v.", err)
			}
		}
	}()
	for i := 0; i < b.N; i++ {
		<-handler.msgs
	}
}

// Benchmarks the passthrough of a single request-reply.
func BenchmarkReqRep(b *testing.B) {
	// Configure the benchmark
	app := fmt.Sprintf("bench-reqrep")
	handler := &requester{
		sleep: 0,
	}
	// Set up the connection
	conn, err := Connect(relayPort, app, handler)
	if err != nil {
		b.Errorf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Reset timer and benchmark the message transfer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Request(app, []byte{byte(i)}, 1000); err != nil {
			b.Errorf("request failed: %v.", err)
		}
	}
}

// Benchmarks parallel request-reply.
func BenchmarkReqRepThroughput(b *testing.B) {
	// Configure the benchmark
	app := fmt.Sprintf("bench-reqrep")
	handler := &requester{
		sleep: 0,
	}
	// Set up the connection
	conn, err := Connect(relayPort, app, handler)
	if err != nil {
		b.Errorf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Reset timer and benchmark the message transfer
	b.ResetTimer()
	done := make(chan struct{}, b.N)
	for i := 0; i < b.N; i++ {
		go func() {
			if _, err := conn.Request(app, []byte{byte(i)}, 1000); err != nil {
				b.Errorf("request failed: %v.", err)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < b.N; i++ {
		<-done
	}
}
