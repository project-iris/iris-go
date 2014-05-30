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

	"gopkg.in/project-iris/iris-go.v0"
)

// Service handler for the publish/subscribe tests.
type pubsubTestServiceHandler struct {
	conn *iris.Connection
}

func (p *pubsubTestServiceHandler) Init(conn *iris.Connection) error         { p.conn = conn; return nil }
func (p *pubsubTestServiceHandler) HandleBroadcast(msg []byte)               { panic("not implemented") }
func (p *pubsubTestServiceHandler) HandleRequest(req []byte) ([]byte, error) { panic("not implemented") }
func (p *pubsubTestServiceHandler) HandleTunnel(tun *iris.Tunnel)            { panic("not implemented") }
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

	// Start up the concurrent publishing clients
	for i := 0; i < conf.clients; i++ {
		go func(client int) {
			// Connect to the local relay
			conn, err := iris.Connect(config.relay)
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
				defer conn.Unsubscribe(topic)
			}
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
			for i := 0; i < len(hands); i++ {
				// Retrieve all the published events
				events := make(map[string]struct{})
				for j := 0; j < (conf.clients+conf.servers)*conf.events; j++ {
					select {
					case event := <-hands[i].delivers:
						events[string(event)] = struct{}{}
					case <-time.After(time.Second):
						barrier.Exit(errors.New("event retrieve timeout"))
						return
					}
				}
				// Verify all the individual events
				for j := 0; j < conf.clients; j++ {
					for k := 0; k < conf.events; k++ {
						msg := fmt.Sprintf("client #%d, event %d", j, k)
						if _, ok := events[msg]; !ok {
							barrier.Exit(fmt.Errorf("event not found: %s", msg))
							return
						}
						delete(events, msg)
					}
				}
				for j := 0; j < conf.servers; j++ {
					for k := 0; k < conf.events; k++ {
						msg := fmt.Sprintf("server #%d, event %d", j, k)
						if _, ok := events[msg]; !ok {
							barrier.Exit(fmt.Errorf("event not found: %s", msg))
							return
						}
						delete(events, msg)
					}
				}
			}
			barrier.Exit(nil)
		}(i)
	}
	// Start up the concurrent publishing services
	for i := 0; i < conf.servers; i++ {
		go func(server int) {
			// Create the service handler
			handler := new(pubsubTestServiceHandler)

			// Register a new service to the relay
			serv, err := iris.Register(config.relay, config.cluster, handler)
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
				defer handler.conn.Unsubscribe(topic)
			}
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
			for i := 0; i < len(hands); i++ {
				// Retrieve all the published events
				events := make(map[string]struct{})
				for j := 0; j < (conf.clients+conf.servers)*conf.events; j++ {
					select {
					case event := <-hands[i].delivers:
						events[string(event)] = struct{}{}
					case <-time.After(time.Second):
						barrier.Exit(errors.New("event retrieve timeout"))
						return
					}
				}
				// Verify all the individual events
				for j := 0; j < conf.clients; j++ {
					for k := 0; k < conf.events; k++ {
						msg := fmt.Sprintf("client #%d, event %d", j, k)
						if _, ok := events[msg]; !ok {
							barrier.Exit(fmt.Errorf("event not found: %s", msg))
							return
						}
						delete(events, msg)
					}
				}
				for j := 0; j < conf.servers; j++ {
					for k := 0; k < conf.events; k++ {
						msg := fmt.Sprintf("server #%d, event %d", j, k)
						if _, ok := events[msg]; !ok {
							barrier.Exit(fmt.Errorf("event not found: %s", msg))
							return
						}
						delete(events, msg)
					}
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
		t.Fatalf("publishing phase failed: %v.", errs)
	}
	if errs := barrier.Wait(); len(errs) != 0 {
		t.Fatalf("verification phase failed: %v.", errs)
	}
}

/*
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
*/
