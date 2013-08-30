// Copyright 2013 Peter Szilagyi. All rights reserved.
//
// The current language binding is an official support library of the Iris
// decentralized messaging framework, and as such, the same licensing terms
// hold. For details please see http://github.com/karalabe/iris/LICENSE.md
//
// Author: peterke@gmail.com (Peter Szilagyi)

package iris

// Specialized error interface to allow querying timeout errors.
type Error interface {
	error
	Timeout() bool
}

// Relay error implementing the Error interface.
type relayError struct {
	message string
	timeout bool
}

// Implements error.Error.
func (e *relayError) Error() string {
	return e.message
}

// Implements Error.Timeout.
func (e *relayError) Timeout() bool {
	return e.timeout
}

// Creates a timeout error.
func timeError(err error) error {
	return &relayError{
		message: err.Error(),
		timeout: true,
	}
}

// Creates a permanent error.
func permError(err error) error {
	return &relayError{
		message: err.Error(),
		timeout: false,
	}
}
