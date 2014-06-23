// Copyright (c) 2014 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package iris

import "errors"

// Returned whenever a time-limited operation expires.
var ErrTimeout = errors.New("operation timed out")

// Returned if an operation is requested on a closed entity.
var ErrClosed = errors.New("entity closed")

// Wrapper to differentiate between local and remote errors.
type RemoteError struct {
	error
}
