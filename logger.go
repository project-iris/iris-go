// Copyright (c) 2014 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

// Contains the user configurable logger instance.

package iris

import (
	"gopkg.in/inconshreveable/log15.v2"
)

// User configurable leveled logger.
var Log = log15.New()

// Disables logging by default.
func init() {
	//Log.SetHandler(log15.DiscardHandler())
}
