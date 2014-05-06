// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package tests

import (
	"fmt"
	"sync"
	"testing"

	"gopkg.in/project-iris/iris-go.v0"
)

// Tests a single startup and shutdown pair.
func TestConnectSingle(t *testing.T) {
	// Connect to the local relay
	cluster := "test-connect-single"
	if conn, err := iris.Connect(relayPort, cluster, nil); err != nil {
		t.Fatalf("connection failed: %v.", err)
	} else {
		// Disconnect from the local relay
		if err := conn.Close(); err != nil {
			t.Fatalf("connection close failed: %v.", err)
		}
	}
}

// Tests multiple concurrent startups and shutdowns.
func TestConnectConcurrent(t *testing.T) {
	wait := new(sync.WaitGroup)

	// Start a batch of concurrent connections
	for i := 0; i < 100; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			TestConnectSingle(t)
		}()
	}
	// Wait for termination and quit
	wait.Wait()
}

// Tests multiple parallel startups and shutdowns.
func TestConnectParallel(t *testing.T) {
	// Configure the test
	servers := 100

	start := new(sync.WaitGroup)
	term := new(sync.WaitGroup)
	proc := new(sync.WaitGroup)
	proc.Add(1)

	// Start a batch of parallel connections
	errs := make(chan error, servers)
	for i := 0; i < servers; i++ {
		start.Add(1)
		go func() {
			// Connect to the local relay
			cluster := "test-connect-parallel"
			conn, err := iris.Connect(relayPort, cluster, nil)
			if err != nil {
				errs <- fmt.Errorf("connection failed: %v", err)
				start.Done()
				return
			}
			start.Done()
			term.Add(1)

			// Wait till everybody connected
			proc.Wait()
			defer term.Done()

			// Disconnect from the local relay
			if err := conn.Close(); err != nil {
				errs <- fmt.Errorf("connection close failed: %v", err)
				return
			}
		}()
	}
	// Schedule the parallel operations
	start.Wait()
	proc.Done()
	term.Wait()
}

// Benchmarks connection setup
func BenchmarkConnect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		cluster := fmt.Sprintf("bench-connect-%d", i)
		if conn, err := iris.Connect(relayPort, cluster, nil); err != nil {
			b.Fatalf("iteration %d: connection failed: %v.", i, err)
		} else {
			defer conn.Close()
		}
	}
	// Stop the timer and clean up
	b.StopTimer()
}
