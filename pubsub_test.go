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

// Service handler for the publish/subscribe tests.
type pubsubTestServiceHandler struct {
	conn *Connection
}

func (p *pubsubTestServiceHandler) Init(conn *Connection) error              { p.conn = conn; return nil }
func (p *pubsubTestServiceHandler) HandleBroadcast(msg []byte)               { panic("not implemented") }
func (p *pubsubTestServiceHandler) HandleRequest(req []byte) ([]byte, error) { panic("not implemented") }
func (p *pubsubTestServiceHandler) HandleTunnel(tun *Tunnel)                 { panic("not implemented") }
func (p *pubsubTestServiceHandler) HandleDrop(reason error)                  { panic("not implemented") }

// Topic handler for the publish/subscribe tests.
type pubsubTestTopicHandler struct {
	delivers chan []byte
}

func (p *pubsubTestTopicHandler) HandleEvent(event []byte) { p.delivers <- event }

// Multiple connections subscribe to the same batch of topics and publish to all.
func TestPublish(t *testing.T) {
	// Test specific configurations
	conf := struct {
		clients int
		servers int
		topics  int
		events  int
	}{10, 10, 10, 10}

	// Pre-generate the topic names
	topics := make([]string, conf.topics)
	for i := 0; i < conf.topics; i++ {
		topics[i] = fmt.Sprintf("%s-%d", config.topic, i)
	}

	barrier := newBarrier(conf.clients + conf.servers)
	shutdown := new(sync.WaitGroup)

	// Start up the concurrent publishing clients
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

			// Subscribe to the batch of topics
			hands := []*pubsubTestTopicHandler{}
			for _, topic := range topics {
				hand := &pubsubTestTopicHandler{
					delivers: make(chan []byte, (conf.clients+conf.servers)*conf.events),
				}
				if err := conn.Subscribe(topic, hand); err != nil {
					barrier.Exit(fmt.Errorf("client subscription failed: %v", err))
					return
				}
				hands = append(hands, hand)
				defer func(topic string) {
					conn.Unsubscribe(topic)
					time.Sleep(100 * time.Millisecond)
				}(topic)
			}
			time.Sleep(100 * time.Millisecond)
			barrier.Sync()

			// Publish to all subscribers
			for i := 0; i < conf.events; i++ {
				event := fmt.Sprintf("client #%d, event %d", client, i)
				for _, topic := range topics {
					if err := conn.Publish(topic, []byte(event)); err != nil {
						barrier.Exit(fmt.Errorf("client publish failed: %v", err))
						return
					}
				}
			}
			barrier.Sync()

			// Verify all the topic deliveries
			if err := publishVerifyEvents(conf.clients, conf.servers, conf.events, hands); err != nil {
				barrier.Exit(err)
				return
			}
			barrier.Exit(nil)
		}(i)
	}
	// Start up the concurrent publishing services
	for i := 0; i < conf.servers; i++ {
		shutdown.Add(1)
		go func(server int) {
			defer shutdown.Done()

			// Create the service handler
			handler := new(pubsubTestServiceHandler)

			// Register a new service to the relay
			serv, err := Register(config.relay, config.cluster, handler)
			if err != nil {
				barrier.Exit(fmt.Errorf("registration failed: %v", err))
				return
			}
			defer serv.Unregister()

			// Subscribe to the batch of topics
			hands := []*pubsubTestTopicHandler{}
			for _, topic := range topics {
				hand := &pubsubTestTopicHandler{
					delivers: make(chan []byte, (conf.clients+conf.servers)*conf.events),
				}
				if err := handler.conn.Subscribe(topic, hand); err != nil {
					barrier.Exit(fmt.Errorf("service subscription failed: %v", err))
					return
				}
				hands = append(hands, hand)
				defer func(topic string) {
					handler.conn.Unsubscribe(topic)
					time.Sleep(100 * time.Millisecond)
				}(topic)
			}
			time.Sleep(100 * time.Millisecond)
			barrier.Sync()

			// Publish to all subscribers
			for i := 0; i < conf.events; i++ {
				event := fmt.Sprintf("server #%d, event %d", server, i)
				for _, topic := range topics {
					if err := handler.conn.Publish(topic, []byte(event)); err != nil {
						barrier.Exit(fmt.Errorf("server publish failed: %v", err))
						return
					}
				}
			}
			barrier.Sync()

			// Verify all the topic deliveries
			if err := publishVerifyEvents(conf.clients, conf.servers, conf.events, hands); err != nil {
				barrier.Exit(err)
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
		t.Fatalf("publishing phase failed: %v.", errs)
	}
	if errs := barrier.Wait(); len(errs) != 0 {
		t.Fatalf("verification phase failed: %v.", errs)
	}
	// Make sure all children terminated
	shutdown.Wait()
}

// Verifies the delivered topic events.
func publishVerifyEvents(clients, servers, publishes int, hands []*pubsubTestTopicHandler) error {
	// Verify each topic handler separately
	for i := 0; i < len(hands); i++ {
		// Retrieve all the published events
		events := make(map[string]struct{})
		for j := 0; j < (clients+servers)*publishes; j++ {
			select {
			case event := <-hands[i].delivers:
				events[string(event)] = struct{}{}
			case <-time.After(time.Second):
				return errors.New("event retrieve timeout")
			}
		}
		// Verify all the individual events
		for j := 0; j < clients; j++ {
			for k := 0; k < publishes; k++ {
				msg := fmt.Sprintf("client #%d, event %d", j, k)
				if _, ok := events[msg]; !ok {
					return fmt.Errorf("event not found: %s", msg)
				}
				delete(events, msg)
			}
		}
		for j := 0; j < servers; j++ {
			for k := 0; k < publishes; k++ {
				msg := fmt.Sprintf("server #%d, event %d", j, k)
				if _, ok := events[msg]; !ok {
					return fmt.Errorf("event not found: %s", msg)
				}
				delete(events, msg)
			}
		}
	}
	return nil
}

// Benchmarks the latency of a single publish operation.
func BenchmarkPublishLatency(b *testing.B) {
	// Connect to the local relay
	conn, err := Connect(config.relay)
	if err != nil {
		b.Fatalf("connection failed: %v", err)
	}
	defer conn.Close()

	// Subscribe to a topic and wait for state propagation
	handler := &pubsubTestTopicHandler{
		delivers: make(chan []byte, b.N),
	}
	if err := conn.Subscribe(config.topic, handler); err != nil {
		b.Fatalf("subscription failed: %v", err)
	}
	defer conn.Unsubscribe(config.topic)
	time.Sleep(100 * time.Millisecond)

	// Reset timer and time sync publish
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := conn.Publish(config.topic, []byte{byte(i)}); err != nil {
			b.Fatalf("failed to publish: %v.", err)
		}
		<-handler.delivers
	}
	// Stop the timer (don't measure deferred cleanup)
	b.StopTimer()
}

// Benchmarks the throughput of a stream of concurrent publishes.
func BenchmarkPublishThroughput1Threads(b *testing.B) {
	benchmarkPublishThroughput(1, b)
}

func BenchmarkPublishThroughput2Threads(b *testing.B) {
	benchmarkPublishThroughput(2, b)
}

func BenchmarkPublishThroughput4Threads(b *testing.B) {
	benchmarkPublishThroughput(4, b)
}

func BenchmarkPublishThroughput8Threads(b *testing.B) {
	benchmarkPublishThroughput(8, b)
}

func BenchmarkPublishThroughput16Threads(b *testing.B) {
	benchmarkPublishThroughput(16, b)
}

func BenchmarkPublishThroughput32Threads(b *testing.B) {
	benchmarkPublishThroughput(32, b)
}

func BenchmarkPublishThroughput64Threads(b *testing.B) {
	benchmarkPublishThroughput(64, b)
}

func BenchmarkPublishThroughput128Threads(b *testing.B) {
	benchmarkPublishThroughput(128, b)
}

func benchmarkPublishThroughput(threads int, b *testing.B) {
	// Connect to the local relay
	conn, err := Connect(config.relay)
	if err != nil {
		b.Fatalf("connection failed: %v", err)
	}
	defer conn.Close()

	// Subscribe to a topic and wait for state propagation
	handler := &pubsubTestTopicHandler{
		delivers: make(chan []byte, b.N),
	}
	if err := conn.Subscribe(config.topic, handler); err != nil {
		b.Fatalf("subscription failed: %v", err)
	}
	defer conn.Unsubscribe(config.topic)
	time.Sleep(100 * time.Millisecond)

	// Create the thread pool with the concurrent publishes
	workers := pool.NewThreadPool(threads)
	for i := 0; i < b.N; i++ {
		workers.Schedule(func() {
			if err := conn.Publish(config.topic, []byte{byte(i)}); err != nil {
				b.Fatalf("failed to publish: %v.", err)
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
