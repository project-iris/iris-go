// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

// Package iris contains the Go binding to the Iris cloud messaging framework.
package iris

import "errors"

var ErrTimeout = errors.New("timeout")
var ErrClosed = errors.New("closed")

// Returns the relay protocol version implemented. Connecting to an Iris node
// will fail unless the versions match exactly.
func Version() string {
	return protoVersion
}
