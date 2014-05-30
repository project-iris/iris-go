// Copyright (c) 2014 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package tests

import "sync"

// Configuration values shared by all the tests.
var config = struct {
	relay   int
	cluster string
	topic   string
}{
	relay:   55555,
	cluster: "go-binding-test-cluster",
	topic:   "go-binding-test-topic",
}

// Simple barrier to support synchronizing a batch of goroutines.
type barrier struct {
	pend sync.WaitGroup
	hold sync.WaitGroup
	pass sync.WaitGroup
	cont sync.WaitGroup
	errc chan error
}

func newBarrier(size int) *barrier {
	b := &barrier{
		errc: make(chan error, size),
	}
	b.pend.Add(size)
	b.hold.Add(1)
	return b
}

// Syncs the goroutines up to begin the next phase.
func (b *barrier) Sync() {
	b.pass.Add(1)
	b.pend.Done()
	b.hold.Wait()
	b.pass.Done()
	b.pend.Add(1)
	b.cont.Wait()
}

// Removes one goroutine from the barrier after the phase.
func (b *barrier) Exit(err error) {
	if err != nil {
		b.errc <- err
	}
	b.pend.Done()
	b.hold.Wait()
}

// Waits for all goroutines to reach the barrier and permits continuation.
func (b *barrier) Wait() []error {
	// Wait for all the goroutines to arrive
	b.cont.Add(1)
	b.pend.Wait()

	// Collect all the occurred errors
	errs := []error{}
	for done := false; !done; {
		select {
		case err := <-b.errc:
			errs = append(errs, err)
		default:
			done = true
		}
	}
	// Permit all goroutines to continue
	b.hold.Done()
	b.pass.Wait()
	b.hold.Add(1)
	b.cont.Done()

	// Report the results
	return errs
}
