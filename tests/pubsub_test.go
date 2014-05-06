// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package tests

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

// Connection handler for the pub/sub tests.
type subscriber struct {
	msgs chan []byte
}

func (s *subscriber) HandleEvent(msg []byte) {
	s.msgs <- msg
}

// Subscribes to a handful of topics, and publishes to each a batch of messages.
func TestPubSubSingle(t *testing.T) {
	// Configure the test
	topics := 75
	events := 75
	names := make([]string, topics)
	for i := 0; i < topics; i++ {
		names[i] = fmt.Sprintf("test-topic-%v", i)
	}
	// Connect to the Iris network
	cluster := "test-pubsub-single"
	conn, err := iris.Connect(relayPort, cluster, nil)
	if err != nil {
		t.Fatalf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Subscribe to the topics
	subscriptions := make(map[string]chan []byte)
	for i := 0; i < topics; i++ {
		// Create the event buffer
		buffer := make(chan []byte, events)
		subscriptions[names[i]] = buffer

		// Subscribe with the buffer (and sleep a bit for state propagation)
		if err := conn.Subscribe(names[i], &subscriber{msgs: buffer}); err != nil {
			t.Fatalf("subscription failed: %v.", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Send some random events and store them for verification
	messages := make(map[[2]string]struct{})
	for i := 0; i < topics; i++ {
		for j := 0; j < events; j++ {
			// Generate and store new random message
			event := make([]byte, 128)
			io.ReadFull(rand.Reader, event)
			messages[[2]string{names[i], string(event)}] = struct{}{}

			// Publish the message
			if err := conn.Publish(names[i], event); err != nil {
				t.Fatalf("publish failed: %v.", err)
			}
		}
	}
	// Retrieve and verify all published events
	for topic, buffer := range subscriptions {
		for i := 0; i < events; i++ {
			select {
			case event := <-buffer:
				// Make sure event is valid
				if _, ok := messages[[2]string{topic, string(event)}]; !ok {
					t.Fatalf("invalid event: %v-%v.", topic, event)
				}
				delete(messages, [2]string{topic, string(event)})
			case <-time.After(5 * time.Second):
				t.Fatalf("publish receive timeout")
			}
		}
	}
}

// Multiple connections subscribe to the same batch of topics and publish to all.
func TestPubSubMulti(t *testing.T) {
	// Configure the test
	servers := 10
	topics := 10
	events := 10
	names := make([]string, topics)
	for i := 0; i < topics; i++ {
		names[i] = fmt.Sprintf("test-topic-%v", i)
	}

	start := new(sync.WaitGroup)
	term := new(sync.WaitGroup)
	proc := new(sync.WaitGroup)
	proc.Add(1)

	// Start up the concurrent subscribers (and publishers)
	errs := make(chan error, servers)
	for i := 0; i < servers; i++ {
		start.Add(1)
		term.Add(1)
		go func() {
			defer term.Done()

			// Connect to the Iris network
			cluster := "test-pubsub-multi"
			conn, err := iris.Connect(relayPort, cluster, nil)
			if err != nil {
				errs <- fmt.Errorf("connection failed: %v", err)
				start.Done()
				return
			}
			defer conn.Close()

			// Subscribe to the topics
			subscriptions := make(map[string]chan []byte)
			for j := 0; j < topics; j++ {
				// Create the even buffer
				buffer := make(chan []byte, events)
				subscriptions[names[j]] = buffer

				// Subscribe with the buffer
				if err := conn.Subscribe(names[j], &subscriber{msgs: buffer}); err != nil {
					errs <- fmt.Errorf("subscription failed: %v.", err)
					start.Done()
					return
				}
			}
			// Wait for permission to continue
			start.Done()
			proc.Wait()

			// Publish to the whole group on every topic
			for j := 0; j < topics; j++ {
				for k := 0; k < events; k++ {
					if err := conn.Publish(names[j], []byte(names[j])); err != nil {
						errs <- fmt.Errorf("publish failed: %v", err)
						return
					}
				}
			}
			// Verify the inbound events
			for topic, buffer := range subscriptions {
				for j := 0; j < events; j++ {
					select {
					case event := <-buffer:
						// Make sure event is valid
						if bytes.Compare([]byte(topic), event) != 0 {
							errs <- fmt.Errorf("invalid event: %v-%v", topic, event)
							return
						}
					case <-time.After(5 * time.Second):
						errs <- fmt.Errorf("publish receive timeout")
						return
					}
				}
			}
		}()
	}
	// Schedule the parallel operations
	// Wait for all go-routines to attach and verify
	start.Wait()
	select {
	case err := <-errs:
		t.Fatalf("startup failed: %v.", err)
	default:
	}

	// Sleep a bit to ensure subscriptions propagate through the system
	time.Sleep(100 * time.Millisecond)

	// Permit the go-routines to continue
	proc.Done()
	term.Wait()
	select {
	case err := <-errs:
		t.Fatalf("publishing failed: %v.", err)
	default:
	}
}

// Benchmarks the pass-through of a single message publish.
func BenchmarkPubSubLatency(b *testing.B) {
	// Configure the benchmark
	cluster := "bench-pubsub-latency"
	topic := "bench-topic-latency"
	handler := &subscriber{
		msgs: make(chan []byte, b.N),
	}
	// Set up the connection
	conn, err := iris.Connect(relayPort, cluster, nil)
	if err != nil {
		b.Fatalf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Subscribe (and sleep a bit for state propagation)
	if err := conn.Subscribe(topic, handler); err != nil {
		b.Fatalf("failed to subscribe: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Reset timer and time sync publish
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := conn.Publish(topic, []byte{byte(i)}); err != nil {
			b.Fatalf("iter %d: failed to publish: %v.", i, err)
		}
		<-handler.msgs
	}
}

// Benchmarks the pass-through of a stream of publishes.
func BenchmarkPubSubThroughput1Threads(b *testing.B) {
	benchmarkPubSubThroughput(1, b)
}

func BenchmarkPubSubThroughput2Threads(b *testing.B) {
	benchmarkPubSubThroughput(2, b)
}

func BenchmarkPubSubThroughput4Threads(b *testing.B) {
	benchmarkPubSubThroughput(4, b)
}

func BenchmarkPubSubThroughput8Threads(b *testing.B) {
	benchmarkPubSubThroughput(8, b)
}

func BenchmarkPubSubThroughput16Threads(b *testing.B) {
	benchmarkPubSubThroughput(16, b)
}

func BenchmarkPubSubThroughput32Threads(b *testing.B) {
	benchmarkPubSubThroughput(32, b)
}

func BenchmarkPubSubThroughput64Threads(b *testing.B) {
	benchmarkPubSubThroughput(64, b)
}

func BenchmarkPubSubThroughput128Threads(b *testing.B) {
	benchmarkPubSubThroughput(128, b)
}

func benchmarkPubSubThroughput(threads int, b *testing.B) {
	// Configure the benchmark
	cluster := "bench-pubsub-throughput"
	topic := "bench-topic-throughput"
	handler := &subscriber{
		msgs: make(chan []byte, b.N),
	}
	// Set up the connection
	conn, err := iris.Connect(relayPort, cluster, nil)
	if err != nil {
		b.Fatalf("connection failed: %v.", err)
	}
	defer conn.Close()

	// Subscribe (and sleep a bit for state propagation)
	if err := conn.Subscribe(topic, handler); err != nil {
		b.Fatalf("failed to subscribe: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Create the thread pool with the concurrent publishes
	workers := pool.NewThreadPool(threads)
	for i := 0; i < b.N; i++ {
		workers.Schedule(func() {
			if err := conn.Publish(topic, []byte{byte(i)}); err != nil {
				b.Fatalf("iter %d: failed to publish: %v.", i, err)
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
