// Iris Go Binding
// Copyright 2013 Peter Szilagyi. All rights reserved.
//
// The current language binding is an official support library of the Iris
// decentralized messaging framework, and as such, the same licensing terms
// hold. For details please see http://github.com/karalabe/iris/LICENSE.md
//
// Author: peterke@gmail.com (Peter Szilagyi)

package iris

// Relay error implemneting the net.Error interface.
type relayError struct {
	message   string
	temporary bool
	timeout   bool
}

// Implements error.Error.
func (e *relayError) Error() string {
	return e.message
}

// Implements net.Error.Temporary.
func (e *relayError) Temporary() bool {
	return e.temporary
}

// Implements net.Error.Timeout.
func (e *relayError) Timeout() bool {
	return e.timeout
}
