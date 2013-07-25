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
	"github.com/karalabe/iris-go"
	"sync"
	"testing"
)

// Tests a single startup and shutdown pair.
func TestConnectSingle(t *testing.T) {
	// Connect to the local relay
	app := "test-connect-single"
	if conn, err := iris.Connect(relayPort, app, nil); err != nil {
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
	start := new(sync.WaitGroup)
	term := new(sync.WaitGroup)
	proc := new(sync.WaitGroup)
	proc.Add(1)

	// Start a batch of parallel connections
	for i := 0; i < 100; i++ {
		start.Add(1)
		go func() {
			// Connect to the local relay
			app := "test-connect-parallel"
			conn, err := iris.Connect(relayPort, app, nil)
			if err != nil {
				t.Fatalf("connection failed: %v.", err)
			}
			start.Done()
			term.Add(1)

			// Wait till everybody connected
			proc.Wait()

			// Disconnect from the local relay
			if err := conn.Close(); err != nil {
				t.Fatalf("connection close failed: %v.", err)
			}
			term.Done()
		}()
	}
	// Schedule the parallel operations
	start.Wait()
	proc.Done()
	term.Wait()
}
